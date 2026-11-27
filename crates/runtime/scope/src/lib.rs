mod checkpoint;
mod lease;
mod owner;
mod route;
mod scopes;
pub mod sim;
mod sql;
pub mod switches;

use std::sync::Arc;

use nanocode_durability_core::{StoreError, StoreFuture};
use nanocode_turnlog::LogHost;

pub use checkpoint::{
    Checkpoints, Fence, Listing, Manifest, MemoryCheckpoints, MemoryStats, Written, assemble, digest,
};
pub use lease::{Leases, PartedLeases};
pub use route::Route;
pub use scopes::{ClaimHook, Clock, Reached, Routed, Scope, Scopes, Stats, Wiring};
pub use sql::SqlCheckpoints;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Part {
    pub name: String,
    pub body: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Committed<R> {
    pub seq: u64,
    pub reply: R,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Carry {
    pub host: usize,
    pub statements: Vec<(&'static str, Vec<String>)>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Flushed {
    pub next: Option<u64>,
    pub carried: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Policy {
    pub checkpoint_rows: usize,
    pub checkpoint_ms: u64,
    pub resident_bytes_max: usize,
    pub forward_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Adopted<S> {
    pub state: S,
    pub next: u64,
    pub rows: u64,
    pub bytes: u64,
}

pub trait Resident: Send + Sync + 'static {
    type State: Clone + Send + Sync + 'static;
    type Command: Send + 'static;
    type Reply: Send + 'static;

    fn empty(&self) -> Self::State;

    fn decide(&self, state: &Self::State, command: &Self::Command) -> (Vec<String>, Self::Reply);

    fn apply(&self, state: &mut Self::State, seq: u64, row: &str) -> Result<(), StoreError>;

    fn encode(&self, state: &Self::State) -> Vec<Part>;

    fn decode(&self, parts: Vec<Part>) -> Result<Self::State, StoreError>;

    fn weight(&self, state: &Self::State) -> usize;

    fn first_row(&self) -> u64 {
        0
    }

    fn foreign(&self, _row: &str) -> bool {
        false
    }

    fn routable(&self, _command: &Self::Command) -> bool {
        true
    }

    fn adopt<'a>(
        &'a self,
        _log: &'a Arc<dyn LogHost>,
        _scope: &'a str,
    ) -> StoreFuture<'a, Result<Option<Adopted<Self::State>>, StoreError>> {
        Box::pin(std::future::ready(Ok(None)))
    }
}

#[derive(Clone, Debug)]
pub enum ScopeError {
    Busy,
    Unreachable {
        holder: String,
    },
    Routed {
        holder: String,
    },
    Moved,
    Lost,
    Uncertain,
    Bound {
        bound: &'static str,
        limit: usize,
        asked: usize,
    },
    Store(StoreError),
}

impl std::fmt::Display for ScopeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Busy => formatter
                .write_str("the scope is held by a live owner that did not take the command"),
            Self::Unreachable { holder } => write!(
                formatter,
                "the scope's holder {holder} did not answer within the forward deadline; the command may have reached it"
            ),
            Self::Routed { holder } => write!(
                formatter,
                "the command runs only on the scope's owner, and {holder} holds the scope; nothing was written"
            ),
            Self::Moved => formatter.write_str(
                "the scope's owner stopped before the command was written; nothing was written",
            ),
            Self::Lost => {
                formatter.write_str("the scope's lease epoch moved; the command was not committed")
            }
            Self::Uncertain => formatter
                .write_str("the command's append ended without an answer; it may have landed"),
            Self::Bound {
                bound,
                limit,
                asked,
            } => {
                write!(
                    formatter,
                    "{bound} is {limit}, the command asked for {asked}"
                )
            }
            Self::Store(error) => write!(formatter, "{error}"),
        }
    }
}

impl std::error::Error for ScopeError {}

impl From<StoreError> for ScopeError {
    fn from(error: StoreError) -> Self {
        match error {
            StoreError::Busy => Self::Busy,
            StoreError::Fenced => Self::Lost,
            error => Self::Store(error),
        }
    }
}
