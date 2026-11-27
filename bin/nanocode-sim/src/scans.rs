use std::{
    future::Future,
    ops::Range,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use nanocode_durability_core::{StoreError, StoreFuture};
use nanocode_sync::FutureExt;
use nanocode_turnlog::{LogHost, ScopeRows};

use crate::{disk::Disk, turnlog::SimHost};

const LOAD_LEVELS: u64 = 4;

struct Counted {
    calls: AtomicU64,
    host: SimHost,
}

impl Counted {
    fn called(&self) {
        self.calls.fetch_add(1, Ordering::SeqCst);
    }
}

impl LogHost for Counted {
    fn call_bound(&self) -> Option<std::time::Duration> {
        None
    }

    fn scan<'a>(
        &'a self,
        scope: &'a str,
        from: u64,
        to: u64,
    ) -> StoreFuture<'a, Result<Vec<(u64, String)>, StoreError>> {
        self.called();
        self.host.scan(scope, from, to)
    }

    fn scan_ranges<'a>(
        &'a self,
        scope: &'a str,
        ranges: &'a [Range<u64>],
    ) -> StoreFuture<'a, Result<Vec<ScopeRows>, StoreError>> {
        self.called();
        self.host.scan_ranges(scope, ranges)
    }

    fn load<'a>(
        &'a self,
        scope: &'a str,
    ) -> StoreFuture<'a, Result<Vec<(u64, String)>, StoreError>> {
        self.host.load(scope)
    }

    fn append<'a>(
        &'a self,
        scope: &'a str,
        rows: &'a [(u64, String)],
    ) -> StoreFuture<'a, Result<(), StoreError>> {
        self.host.append(scope, rows)
    }
}

pub fn probe(scope: &str, stored: &[(u64, String)], seq: u64) {
    let disk = Disk::new();
    disk.with(|rows| rows.insert(scope, stored));
    let counted = Arc::new(Counted {
        calls: AtomicU64::new(0),
        host: SimHost::new(disk),
    });
    let host: Arc<dyn LogHost> = Arc::clone(&counted) as Arc<dyn LogHost>;
    let log = refused(scope, ready(nanocode_turnlog::sim::open(host, scope)));
    refused(scope, ready(log.load_all()));
    let stats = ready(log.stats());
    let loads = stats.index_loads + stats.section_loads;
    let calls = counted.calls.load(Ordering::SeqCst);
    assert!(
        loads < 2 * LOAD_LEVELS || calls * 2 <= loads,
        "{scope} loads {loads} sections cold in {calls} host calls after commit {seq}: a cold load reads each level's sections together"
    );
}

fn ready<T>(future: impl Future<Output = T>) -> T {
    future
        .now_or_never()
        .expect("the simulated turn log host answers every call at once")
}

fn refused<T>(scope: &str, result: Result<T, StoreError>) -> T {
    result.unwrap_or_else(|error| panic!("{scope} refused a scan probe: {error}"))
}
