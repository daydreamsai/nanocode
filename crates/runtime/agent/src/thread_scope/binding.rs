use std::{future::Future, sync::Arc};

use nanocode_scope::ScopeError;
use nanocode_sync::Atom;

use super::{ThreadLog, ThreadLogs, sim};

pub(crate) struct ThreadBinding {
    logs: Arc<ThreadLogs>,
    thread: Arc<str>,
    log: Atom<Option<Arc<ThreadLog>>>,
}

impl ThreadBinding {
    pub(crate) fn new(logs: Arc<ThreadLogs>, thread: &str, log: Option<Arc<ThreadLog>>) -> Self {
        Self {
            logs,
            thread: Arc::from(thread),
            log: Atom::new(log),
        }
    }

    pub(crate) fn log(&self) -> Option<Arc<ThreadLog>> {
        self.log.read(Clone::clone)
    }

    pub(crate) async fn rebound(
        &self,
        stale: Option<&Arc<ThreadLog>>,
    ) -> Result<Arc<ThreadLog>, ScopeError> {
        if let Some(current) = self.log()
            && !stale.is_some_and(|stale| Arc::ptr_eq(&current, stale))
        {
            return Ok(current);
        }
        let fresh = Arc::new(self.logs.get(&self.thread).await?);
        let bound = self.log.update(|log| {
            let replaced = log
                .as_ref()
                .is_none_or(|log| stale.is_some_and(|stale| Arc::ptr_eq(log, stale)));
            if replaced {
                *log = Some(Arc::clone(&fresh));
            }
            log.clone()
        });
        assert!(bound.is_some(), "a rebound binding holds a thread log");
        Ok(bound.unwrap_or(fresh))
    }
}

#[derive(Clone)]
pub(super) enum Bind {
    Fixed(Arc<ThreadLog>),
    Rebinds(Arc<ThreadBinding>),
}

impl Bind {
    pub(super) fn log(&self) -> Option<Arc<ThreadLog>> {
        match self {
            Self::Fixed(log) => Some(Arc::clone(log)),
            Self::Rebinds(binding) => binding.log(),
        }
    }

    pub(super) async fn again(
        &self,
        stale: Option<&Arc<ThreadLog>>,
        unretried: sim::Mutation,
    ) -> Option<Result<Arc<ThreadLog>, ScopeError>> {
        match self {
            Self::Rebinds(binding) if !sim::mutated(unretried) => {
                Some(binding.rebound(stale).await)
            }
            Self::Fixed(_) | Self::Rebinds(_) => None,
        }
    }

    pub(super) async fn serving(
        &self,
        unretried: sim::Mutation,
    ) -> Result<Arc<ThreadLog>, ScopeError> {
        let log = self.log();
        if let Some(log) = &log
            && !log.retired()
        {
            return Ok(Arc::clone(log));
        }
        match self.again(log.as_ref(), unretried).await {
            Some(fresh) => fresh,
            None => log.ok_or(ScopeError::Lost),
        }
    }

    pub(super) async fn settled(
        &self,
        log: &Arc<ThreadLog>,
        unretried: sim::Mutation,
    ) -> Result<(), ScopeError> {
        if !log.retired() {
            return Ok(());
        }
        match self.again(Some(log), unretried).await {
            Some(fresh) => fresh?.flush(None).await.map(drop),
            None => Ok(()),
        }
    }

    pub(super) async fn retried<T, F, Fut>(
        &self,
        unretried: sim::Mutation,
        command: F,
    ) -> Result<T, ScopeError>
    where
        F: Fn(Arc<ThreadLog>) -> Fut,
        Fut: Future<Output = Result<T, ScopeError>>,
    {
        let log = self.serving(unretried).await?;
        match command(Arc::clone(&log)).await {
            Err(ScopeError::Lost) => match self.again(Some(&log), unretried).await {
                Some(fresh) => command(fresh?).await,
                None => Err(ScopeError::Lost),
            },
            answered => answered,
        }
    }
}
