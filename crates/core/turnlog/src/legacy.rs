use std::sync::Arc;

use nanocode_durability_core::StoreError;

use crate::{Follower, LogHost, TurnLog};

pub async fn open(
    host: Arc<dyn LogHost>,
    scope: impl Into<Arc<str>>,
) -> Result<TurnLog, StoreError> {
    TurnLog::open(host, scope).await
}

pub async fn claim(follower: Follower) -> Result<TurnLog, StoreError> {
    follower.claim().await
}

pub async fn claimed(log: &TurnLog) -> Result<(), StoreError> {
    log.write(|writing| Box::pin(writing.claimed())).await
}
