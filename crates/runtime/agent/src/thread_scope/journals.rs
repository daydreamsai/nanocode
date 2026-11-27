use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Arc,
};

use nanocode_sync::Atom;
use nanocode_tool_host::{
    code_mode::{
        CellJournal, CellJournalSink, CellJournalUpdate, CellJournals, JournalEntry,
        JournaledCallState,
    },
    embedded::HostFuture,
};
use nanocode_turnlog::{EffectClass, Effects, Record};
use serde_json::{Value, json, value::RawValue};

use nanocode_scope::ScopeError;

use super::{
    Staged, ThreadBinding, ThreadLog, ThreadState, binding::Bind, facts::refused, sim,
    state::invalid,
};
use crate::{
    durability::StoreError,
    one_log::{STORED_MARK, TOOLS_MARK, raw, shared, stored_key},
};

type Cell = (String, u64);

const UNRETRIED: sim::Mutation = sim::Mutation::JournalLostUnretried;

struct Inner {
    bind: Bind,
    effects: Arc<Atom<Effects>>,
    live: Atom<BTreeSet<Cell>>,
    unsaved: Atom<BTreeMap<Cell, u64>>,
    answered: Atom<BTreeMap<Cell, u64>>,
    dispatched: Atom<BTreeMap<Cell, BTreeSet<u64>>>,
}

#[derive(Clone)]
pub struct ThreadJournals(Arc<Inner>);

impl ThreadJournals {
    #[must_use]
    pub fn new(log: Arc<ThreadLog>, effects: Arc<Atom<Effects>>) -> Self {
        Self::bound(Bind::Fixed(log), effects)
    }

    pub(crate) fn rebinding(binding: Arc<ThreadBinding>, effects: Arc<Atom<Effects>>) -> Self {
        Self::bound(Bind::Rebinds(binding), effects)
    }

    fn bound(bind: Bind, effects: Arc<Atom<Effects>>) -> Self {
        Self(Arc::new(Inner {
            bind,
            effects,
            live: Atom::new(BTreeSet::new()),
            unsaved: Atom::new(BTreeMap::new()),
            answered: Atom::new(BTreeMap::new()),
            dispatched: Atom::new(BTreeMap::new()),
        }))
    }

    #[must_use]
    pub fn rebound(&self, log: Arc<ThreadLog>) -> Self {
        Self(Arc::new(Inner {
            bind: Bind::Fixed(log),
            effects: Arc::clone(&self.0.effects),
            live: Atom::new(self.0.live.read(Clone::clone)),
            unsaved: Atom::new(self.0.unsaved.read(Clone::clone)),
            answered: Atom::new(self.0.answered.read(Clone::clone)),
            dispatched: Atom::new(self.0.dispatched.read(Clone::clone)),
        }))
    }

    #[must_use]
    pub fn log(&self) -> Option<Arc<ThreadLog>> {
        self.0.bind.log()
    }

    #[must_use]
    pub fn live(&self) -> Vec<(String, u64)> {
        self.0.live.read(|live| live.iter().cloned().collect())
    }

    #[must_use]
    pub fn sink(&self) -> Arc<dyn CellJournalSink> {
        Arc::new(self.clone())
    }

    #[must_use]
    pub fn journals(&self) -> CellJournals {
        CellJournals::with_sink(self.sink())
    }

    async fn read<T: Send + 'static>(
        &self,
        read: impl Fn(&ThreadState) -> Result<T, StoreError> + Send + Sync + 'static,
    ) -> Result<T, StoreError> {
        let read = Arc::new(read);
        self.0
            .bind
            .retried(UNRETRIED, |log| {
                let read = Arc::clone(&read);
                async move { log.job_staged(move |staged| read(staged.state())).await }
            })
            .await
            .map_err(refused)
    }
}

fn staged_update(staged: &mut Staged, update: &CellJournalUpdate) -> Result<bool, StoreError> {
    let mut update = update.clone();
    let fresh = update.header.is_some();
    if let Some(header) = update.header.as_mut() {
        let catalog = serde_json::to_string(&header.tools)
            .map_err(|error| invalid(format!("tool catalog: {error}")))?;
        let digest = nanocode_digest::blake3(catalog.as_bytes())
            .to_hex()
            .to_string();
        let key = stored_key(TOOLS_MARK, &digest);
        if staged.state().stored(&key).is_none() {
            staged.push(Record::Stored {
                key,
                value: catalog,
            })?;
        }
        header.tools = [json!({ (TOOLS_MARK): digest })].into();
    }
    let encoded = match shared::shared_results(&update)? {
        Some(shared) => {
            for (key, value) in shared.values {
                let key = stored_key(&update.session_id, &key);
                if staged.state().stored(&key) != Some(&*value) {
                    staged.push(Record::Stored { key, value })?;
                }
            }
            shared.update
        }
        None => raw(&update)?,
    };
    staged.push(Record::Cell {
        cell: update.cell_id,
        fresh,
        session: update.session_id.clone(),
        update: encoded,
    })?;
    Ok(fresh)
}

struct Saved<'a> {
    update: &'a CellJournalUpdate,
    cell: Cell,
    fresh: bool,
    seq: u64,
    started: Vec<u64>,
    external: bool,
}

impl Inner {
    fn needs_row(&self, tool: &str) -> bool {
        self.effects.read(|effects| effects.of(tool)).needs_row()
    }

    fn external(&self, update: &CellJournalUpdate) -> Result<Vec<Record>, StoreError> {
        update
            .calls
            .iter()
            .filter(|call| {
                matches!(call.state, JournaledCallState::Running) && self.needs_row(&call.name)
            })
            .map(|call| {
                Ok(Record::Call {
                    call_id: call.call_id.clone(),
                    effect: EffectClass::External,
                    input: raw(&call.input)?,
                    tool: call.name.clone(),
                    turn: None,
                })
            })
            .collect()
    }

    fn due(&self, saved: &Saved<'_>) -> bool {
        let Saved {
            update,
            cell,
            fresh,
            seq,
            started,
            external,
        } = saved;
        if update.finished {
            self.live.update(|live| live.remove(cell));
        } else if *fresh {
            self.live.update(|live| live.insert(cell.clone()));
        }
        let timed = update
            .entries
            .iter()
            .any(|entry| matches!(entry, JournalEntry::Timeout { .. }));
        let pending = self.unsaved.read(|unsaved| unsaved.get(cell) == Some(seq));
        let results = update
            .entries
            .iter()
            .filter_map(|entry| match entry {
                JournalEntry::Result { id, .. } => Some(*id),
                _ => None,
            })
            .collect::<Vec<_>>();
        let answered = self.dispatched.update(|dispatched| {
            let calls = dispatched.entry(cell.clone()).or_default();
            calls.extend(started.iter().copied());
            let before = calls.len();
            calls.retain(|id| !results.contains(id));
            let answered = calls.len() < before;
            if calls.is_empty() || update.finished {
                dispatched.remove(cell);
            }
            answered
        });
        let owed = self.answered.read(|owed| owed.get(cell) == Some(seq));
        let due = *external
            || owed
            || (answered && (timed || update.finished))
            || (timed && (*fresh || pending));
        if due {
            self.unsaved.update(|unsaved| unsaved.remove(cell));
            self.answered.update(|owed| owed.remove(cell));
            return true;
        }
        self.unsaved.update(|unsaved| {
            if update.finished {
                unsaved.remove(cell);
            } else if *fresh {
                unsaved.insert(cell.clone(), *seq);
            }
        });
        self.answered.update(|owed| {
            if update.finished {
                owed.remove(cell);
            } else if answered {
                owed.insert(cell.clone(), *seq);
            }
        });
        false
    }

    async fn save(self: Arc<Self>, update: CellJournalUpdate) -> Result<(), ScopeError> {
        let external = self.external(&update).map_err(ScopeError::Store)?;
        let started = update
            .calls
            .iter()
            .filter(|call| {
                matches!(call.state, JournaledCallState::Running) && self.needs_row(&call.name)
            })
            .map(|call| call.id)
            .collect::<Vec<_>>();
        let update = Arc::new(update);
        let cell = (update.session_id.clone(), update.cell_id);
        if !external.is_empty() {
            let external: Arc<[Record]> = Arc::from(external);
            let (fresh, seq) = self
                .bind
                .retried(UNRETRIED, |log| {
                    let (update, external) = (Arc::clone(&update), Arc::clone(&external));
                    async move { written(&log, update, external).await }
                })
                .await?;
            let saved = Saved {
                update: &update,
                cell,
                fresh,
                seq,
                started,
                external: true,
            };
            assert!(
                self.due(&saved),
                "a cell journal update that starts an external call is due"
            );
            return Ok(());
        }
        let first = self.bind.serving(UNRETRIED).await?;
        let (log, (fresh, seq), retries) = match staged_on(&first, &update).await {
            Err(ScopeError::Lost) => match self.bind.again(Some(&first), UNRETRIED).await {
                Some(log) => {
                    let log = log?;
                    let staged = staged_on(&log, &update).await?;
                    (log, staged, false)
                }
                None => return Err(ScopeError::Lost),
            },
            staged => (first, staged?, true),
        };
        let saved = Saved {
            update: &update,
            cell,
            fresh,
            seq,
            started,
            external: false,
        };
        if !self.due(&saved) {
            return Ok(());
        }
        let log = match log.flush(None).await {
            Err(ScopeError::Lost) if retries => {
                match self.bind.again(Some(&log), UNRETRIED).await {
                    Some(log) => {
                        let log = log?;
                        staged_on(&log, &update).await?;
                        log.flush(None).await?;
                        log
                    }
                    None => return Err(ScopeError::Lost),
                }
            }
            flushed => flushed.map(|_| log)?,
        };
        self.bind.settled(&log, UNRETRIED).await
    }
}

fn seq_of(log: &ThreadLog) -> Result<u64, ScopeError> {
    log.next().ok_or_else(|| ScopeError::Routed {
        holder: log.holder().unwrap_or_default().to_owned(),
    })
}

async fn written(
    log: &ThreadLog,
    update: Arc<CellJournalUpdate>,
    external: Arc<[Record]>,
) -> Result<(bool, u64), ScopeError> {
    let fresh = log
        .job(move |staged| {
            let fresh = staged_update(staged, &update)?;
            staged.extend(external.to_vec())?;
            Ok(fresh)
        })
        .await?;
    Ok((fresh, seq_of(log)?))
}

async fn staged_on(
    log: &ThreadLog,
    update: &Arc<CellJournalUpdate>,
) -> Result<(bool, u64), ScopeError> {
    let fresh = {
        let update = Arc::clone(update);
        log.job_staged(move |staged| staged_update(staged, &update))
            .await?
    };
    Ok((fresh, seq_of(log)?))
}

fn restored(state: &ThreadState, session: &str, whole: &BTreeSet<u64>, shared: &[String]) {
    let (mut cells, mut finished, mut updates, mut finished_updates) = (0_u64, 0_u64, 0_u64, 0_u64);
    for (cell, held) in state.cells_of(session) {
        let bytes = held
            .iter()
            .map(|update| update.get().len() as u64)
            .sum::<u64>();
        cells += 1;
        updates += bytes;
        if !whole.contains(&cell) {
            finished += 1;
            finished_updates += bytes;
        }
    }
    let results = shared
        .iter()
        .filter_map(|key| state.stored(key))
        .map(|value| value.len() as u64)
        .sum::<u64>();
    nanocode_trace::event!(target: "nc_metrics", nanocode_trace::Level::INFO, {
        monotonic_counter.nc.cells.restored = cells,
        monotonic_counter.nc.cells.restored_finished = finished,
        monotonic_counter.nc.cells.restored_results = shared.len() as u64,
        histogram.nc.cells.restore_update_bytes = updates,
        histogram.nc.cells.restore_finished_update_bytes = finished_updates,
        histogram.nc.cells.restore_result_bytes = results
    });
}

fn catalog(
    state: &ThreadState,
    catalogs: &mut HashMap<String, Arc<[Value]>>,
    digest: &str,
) -> Result<Arc<[Value]>, StoreError> {
    if let Some(tools) = catalogs.get(digest) {
        return Ok(Arc::clone(tools));
    }
    let held = state
        .stored(&stored_key(TOOLS_MARK, digest))
        .ok_or_else(|| invalid(format!("tool catalog {digest}")))?;
    let tools: Arc<[Value]> = serde_json::from_str::<Vec<Value>>(held)
        .map_err(|error| invalid(format!("tool catalog: {error}")))?
        .into();
    catalogs.insert(digest.to_owned(), Arc::clone(&tools));
    Ok(tools)
}

fn decoded(
    state: &ThreadState,
    session: &str,
    loaded: bool,
    updates: &[Box<RawValue>],
    catalogs: &mut HashMap<String, Arc<[Value]>>,
) -> Result<Vec<CellJournalUpdate>, StoreError> {
    let mut decoded = Vec::with_capacity(updates.len());
    for update in updates {
        let update = if loaded {
            shared::with_results(state, session, update)?
        } else {
            shared::without_results(update)?
        };
        let mut update = serde_json::from_str::<CellJournalUpdate>(&update)
            .map_err(|error| invalid(format!("cell journal update: {error}")))?;
        if let Some(header) = update.header.as_mut()
            && let [Value::Object(mark)] = &*header.tools
            && let Some(digest) = mark.get(TOOLS_MARK).and_then(Value::as_str)
        {
            header.tools = catalog(state, catalogs, digest)?;
        }
        decoded.push(update);
    }
    Ok(decoded)
}

fn cells(
    state: &ThreadState,
    session: &str,
    only: Option<u64>,
) -> Result<Vec<CellJournal>, StoreError> {
    let held: Vec<(u64, Vec<Box<RawValue>>)> = state
        .cells_of(session)
        .filter(|(cell, _)| only.is_none_or(|only| only == *cell))
        .map(|(cell, updates)| (cell, updates.iter().cloned().collect()))
        .collect();
    let mut whole = BTreeSet::new();
    let mut shared = Vec::new();
    for (cell, updates) in &held {
        let wanted = match only {
            Some(_) => true,
            None => {
                !shared::finished(updates)? || sim::mutated(sim::Mutation::JournalLoadsFinished)
            }
        };
        if wanted {
            whole.insert(*cell);
            shared.extend(shared::result_keys(session, updates)?);
        }
    }
    restored(state, session, &whole, &shared);
    let mut journals = Vec::new();
    let mut catalogs = HashMap::new();
    for (cell, updates) in &held {
        let loaded = whole.contains(cell);
        let decoded = decoded(state, session, loaded, updates, &mut catalogs)?;
        if let Some(mut journal) = CellJournal::fold(decoded) {
            journal.unloaded = !loaded;
            journals.push(journal);
        }
    }
    Ok(journals)
}

impl CellJournalSink for ThreadJournals {
    fn save(&self, update: CellJournalUpdate) -> HostFuture<'static, Result<(), String>> {
        let inner = Arc::clone(&self.0);
        Box::pin(async move {
            inner
                .save(update)
                .await
                .map_err(|error| refused(error).to_string())
        })
    }

    fn forget(&self, session_id: &str, cell_id: u64) -> HostFuture<'static, ()> {
        let inner = Arc::clone(&self.0);
        let cell = (session_id.to_owned(), cell_id);
        inner.unsaved.update(|unsaved| unsaved.remove(&cell));
        inner.answered.update(|owed| owed.remove(&cell));
        inner
            .dispatched
            .update(|dispatched| dispatched.remove(&cell));
        let unfinished = inner.live.update(|live| live.remove(&cell));
        Box::pin(async move {
            let (session, cell) = cell;
            let session: Arc<str> = Arc::from(session);
            let bind = inner.bind.clone();
            let forgotten = inner
                .bind
                .retried(UNRETRIED, |log| {
                    let session = Arc::clone(&session);
                    let bind = bind.clone();
                    async move {
                        log.job_staged(move |staged| {
                            staged.push(Record::Forget {
                                cell,
                                session: session.as_ref().to_owned(),
                            })
                        })
                        .await?;
                        if unfinished {
                            log.flush(None).await?;
                            bind.settled(&log, UNRETRIED).await?;
                        }
                        Ok(())
                    }
                })
                .await;
            if let Err(error) = forgotten {
                nanocode_trace::warn!(%error, "thread log could not forget a cell journal");
            }
        })
    }

    fn load(&self, session_id: &str) -> HostFuture<'static, Result<Vec<CellJournal>, String>> {
        let journals = self.clone();
        let session = session_id.to_owned();
        Box::pin(async move {
            journals
                .read(move |state| cells(state, &session, None))
                .await
                .map_err(|error| error.to_string())
        })
    }

    fn load_cell(
        &self,
        session_id: &str,
        cell_id: u64,
    ) -> HostFuture<'static, Result<Option<CellJournal>, String>> {
        let journals = self.clone();
        let session = session_id.to_owned();
        Box::pin(async move {
            journals
                .read(move |state| cells(state, &session, Some(cell_id)))
                .await
                .map(|journals| {
                    journals
                        .into_iter()
                        .find(|journal| journal.cell_id == cell_id)
                })
                .map_err(|error| error.to_string())
        })
    }

    fn save_stored(
        &self,
        session_id: &str,
        stored: String,
    ) -> HostFuture<'static, Result<(), String>> {
        let inner = Arc::clone(&self.0);
        let key: Arc<str> = Arc::from(stored_key(session_id, STORED_MARK));
        let stored: Arc<str> = Arc::from(stored);
        Box::pin(async move {
            inner
                .bind
                .retried(UNRETRIED, |log| {
                    let (key, stored) = (Arc::clone(&key), Arc::clone(&stored));
                    async move {
                        log.job_staged(move |staged| {
                            staged.push(Record::Stored {
                                key: key.as_ref().to_owned(),
                                value: stored.as_ref().to_owned(),
                            })
                        })
                        .await
                    }
                })
                .await
                .map_err(|error| refused(error).to_string())
        })
    }

    fn load_stored(&self, session_id: &str) -> HostFuture<'static, Result<Option<String>, String>> {
        let journals = self.clone();
        let key = stored_key(session_id, STORED_MARK);
        let legacy = stored_key(STORED_MARK, session_id);
        Box::pin(async move {
            journals
                .read(move |state| {
                    Ok(state
                        .stored(&key)
                        .or_else(|| state.stored(&legacy))
                        .map(str::to_owned))
                })
                .await
                .map_err(|error| error.to_string())
        })
    }
}
