use std::sync::atomic::{AtomicU8, Ordering};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Mutation {
    None,
    HoldForgetsAnswer,
    HoldPrunesUnsettled,
    PlacedInline,
    FencedServesReads,
    CellRootsSkipped,
    HostUnbounded,
    HostBoundIgnored,
    ReadsUnbounded,
    AnswersUnbounded,
    ChainMemberDropped,
    ResidentUntilSnapshot,
    UnindexedPinsPages,
    PlacedStayResident,
    FinishedKeepsPrompt,
    SealedRelowered,
    FactsResident,
    CacheNeverDue,
    ScopedInputLowered,
    BlobsResident,
    CacheInline,
    OpenMigrates,
    PrepareMigrates,
    ConflictSkipsReadBack,
    RetrySkipsReadBack,
    ScansUnbatched,
    SnapshotResetsRecovery,
    AbandonRunningJob,
    WritesUnreleasedFormat,
    OwnRowFences,
}

static MUTATION: AtomicU8 = AtomicU8::new(Mutation::None as u8);
static FORCED: AtomicU8 = AtomicU8::new(UNFORCED);
const UNFORCED: u8 = 0;
const FORCED_TAGGED: u8 = 1;
const FORCED_STAMPED: u8 = 2;

pub fn mutate(mutation: Mutation) {
    MUTATION.store(mutation as u8, Ordering::SeqCst);
}

pub fn mutated(mutation: Mutation) -> bool {
    MUTATION.load(Ordering::Relaxed) == mutation as u8
}

pub fn force(format: Option<crate::Format>) {
    let code = match format {
        None => UNFORCED,
        Some(crate::Format::Tagged) => FORCED_TAGGED,
        Some(crate::Format::Stamped) => FORCED_STAMPED,
    };
    FORCED.store(code, Ordering::SeqCst);
}

pub(crate) fn forced() -> Option<crate::Format> {
    match FORCED.load(Ordering::Relaxed) {
        FORCED_TAGGED => Some(crate::Format::Tagged),
        FORCED_STAMPED => Some(crate::Format::Stamped),
        code => {
            assert_eq!(code, UNFORCED, "a forced row format code names a format");
            None
        }
    }
}

pub async fn open(
    host: std::sync::Arc<dyn crate::LogHost>,
    scope: impl Into<std::sync::Arc<str>>,
) -> Result<crate::TurnLog, nanocode_durability_core::StoreError> {
    crate::TurnLog::open(host, scope).await
}

pub async fn follow(
    host: std::sync::Arc<dyn crate::LogHost>,
    scope: impl Into<std::sync::Arc<str>>,
) -> Result<crate::Follower, nanocode_durability_core::StoreError> {
    crate::TurnLog::follow(host, scope).await
}

pub async fn claim(
    follower: crate::Follower,
) -> Result<crate::TurnLog, nanocode_durability_core::StoreError> {
    follower.claim().await
}

#[must_use]
pub fn stamped(stamp: crate::Stamp, body: &str) -> String {
    crate::formats::stamp_row(stamp, body)
}
