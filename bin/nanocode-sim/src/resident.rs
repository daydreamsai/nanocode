use std::{future::Future, sync::Arc};

use nanocode_durability_core::StoreError;
use nanocode_sync::FutureExt;
use nanocode_turnlog::{Fold, LogHost, Point, Record, TurnLog};
use serde_json::value::to_raw_value;

use crate::{disk::Disk, turnlog::SimHost};

const PROBE_STATE: &str = "resident probe";

pub fn probe(scope: &str, stored: &[(u64, String)], seq: u64, limit: Option<usize>, ratio: usize) {
    let disk = Disk::new();
    disk.with(|rows| rows.insert(scope, stored));
    let host: Arc<dyn LogHost> = Arc::new(SimHost::new(Arc::clone(&disk)));
    let log = refused(scope, ready(nanocode_turnlog::sim::open(host, scope)))
        .row_limit(limit)
        .snapshot_ratio(ratio);
    refused(scope, ready(log.load_index()));
    let key = format!("{PROBE_STATE}\nb3:{seq:064x}");
    let value = format!("a value the residency probe wrote after commit {seq}");
    let written = Record::Stored {
        key: key.clone(),
        value: value.clone(),
    };
    committed(&log, scope, seq, Some(written), &[]);
    assert!(
        held(&log, &key).is_none(),
        "{scope} keeps a value resident past the commit that wrote it while nobody read it, probed after commit {seq}"
    );
    committed(&log, scope, seq, None, std::slice::from_ref(&key));
    assert!(
        held(&log, &key).as_deref() == Some(value.as_str()),
        "{scope} evicts a value loaded before a commit by the end of that commit, probed after commit {seq}"
    );
    committed(&log, scope, seq, None, &[]);
    assert!(
        held(&log, &key).is_none(),
        "{scope} keeps a loaded value resident past the commit after its load, probed after commit {seq}"
    );
    let loaded = ready(log.read(Fold::loaded_pages));
    assert!(
        loaded == 0,
        "{scope} keeps {loaded} index pages resident past a commit that read none of the owners they list, probed after commit {seq}"
    );
    let owners = ready(log.read(Fold::resident_owners));
    assert!(
        owners == 0,
        "{scope} keeps {owners} owners resident past a commit that read none of them, probed after commit {seq}"
    );
}

fn committed(log: &TurnLog, scope: &str, seq: u64, record: Option<Record>, loads: &[String]) {
    let scope = scope.to_owned();
    let loads = loads.to_vec();
    crate::facts::writing(log, move |writing| {
        refused(&scope, ready(writing.load_owners(&loads)));
        let record = record.unwrap_or_else(|| Record::Host {
            key: PROBE_STATE.to_owned(),
            value: to_raw_value(&seq).expect("a commit number encodes as JSON"),
        });
        refused(&scope, writing.push(record));
        refused(&scope, ready(writing.commit(Point::Progress)));
    });
}

fn held(log: &TurnLog, key: &str) -> Option<String> {
    let key = key.to_owned();
    ready(log.read(move |fold| fold.stored(&key).map(str::to_owned)))
}

fn ready<T>(future: impl Future<Output = T>) -> T {
    future
        .now_or_never()
        .expect("the simulated turn log host answers every call at once")
}

fn refused<T>(scope: &str, result: Result<T, StoreError>) -> T {
    result.unwrap_or_else(|error| panic!("{scope} refused a residency probe: {error}"))
}
