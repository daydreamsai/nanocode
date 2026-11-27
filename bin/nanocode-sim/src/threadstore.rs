use std::{collections::BTreeMap, fmt::Debug, ops::Range, sync::Arc};

use nanocode::turnlog::{OneLog, OneLogStore, Policy, ThreadStore};
use nanocode_durability_core::{
    OwnerId, OwnerToken, Settle, StateCommit, StateStore, StoreRecord, SummaryUpdate,
};
use nanocode_turnlog::{LogHost, MemoryLog};

use crate::rng::Rng;
use crate::threadscope::{Node, PARTS, ThreadScopeStats, World, driven, flag, publish};

const THREAD: &str = "store";
const OPS: Range<u64> = 40..160;
const STATES: u64 = 3;
const KEYS: u64 = 4;
const VALUES: u64 = 3;
const RECORDS_MAX: u64 = 3;
const STALE_PPM: u64 = 250_000;
const SKEW_PPM: u64 = 150_000;
const REOPEN_PPM: u64 = 60_000;

struct Sides {
    one: OneLogStore,
    thread: ThreadStore,
    node: Node,
    _log: Arc<OneLog>,
}

fn shown<T: Debug>(answer: &Option<T>) -> String {
    format!("{answer:?}")
}

fn open(world: &World, host: &Arc<dyn LogHost>, generation: u64) -> Option<Sides> {
    let one = driven(
        world,
        "a turn log open",
        nanocode::turnlog::thread_scope_sim::open(Arc::clone(host), THREAD, Policy::default()),
    );
    let one = match one? {
        Ok(one) => one,
        Err(error) => {
            flag(&world.wrong, format!("store: the turn log opens: {error}"));
            return None;
        }
    };
    let node = world.node(&format!("store{generation}"));
    publish(world, std::slice::from_ref(&node));
    let log = match driven(world, "a thread get", node.logs.get(THREAD))? {
        Ok(log) => log,
        Err(error) => {
            flag(&world.wrong, format!("store: the thread claims: {error}"));
            return None;
        }
    };
    Some(Sides {
        one: one.store(),
        thread: ThreadStore::new(Arc::new(log)),
        node,
        _log: one,
    })
}

fn reopened(
    world: &World,
    host: &Arc<dyn LogHost>,
    mut sides: Sides,
    generation: u64,
) -> Option<Sides> {
    let ours = shown(&driven(world, "a flush", sides.one.flush()));
    let theirs = shown(&driven(world, "a flush", sides.thread.flush()));
    if ours != theirs {
        flag(
            &world.wrong,
            format!(
                "store: the flush before reopen {generation} answers {ours} on the turn log but {theirs} on the thread"
            ),
        );
        return None;
    }
    for part in 0..PARTS {
        let released = driven(
            world,
            "a release",
            sides
                .node
                .logs
                .scopes()
                .release(nanocode_partitions::Partition(part)),
        );
        if let Some(Err(error)) = released {
            flag(
                &world.wrong,
                format!("store: the thread's node releases partition {part}: {error}"),
            );
            return None;
        }
    }
    drop(sides);
    open(world, host, generation)
}

fn records(rng: &mut Rng, id: u64) -> Vec<StoreRecord> {
    (0..rng.below(RECORDS_MAX + 1))
        .map(|_| StoreRecord {
            key: format!("k{}", rng.below(KEYS)),
            value: format!("v{}-{id}", rng.below(VALUES)).into(),
        })
        .collect()
}

struct Owners(BTreeMap<String, Vec<OwnerToken>>);

impl Owners {
    fn token(&self, rng: &mut Rng, state: &str) -> OwnerToken {
        match self.0.get(state).map(Vec::as_slice) {
            None | Some([]) => OwnerToken::new(OwnerId::from("unowned".to_owned()), 0),
            Some(held) if held.len() > 1 && rng.chance(STALE_PPM) => held
                [usize::try_from(rng.below(held.len() as u64 - 1)).expect("a token index fits")]
            .clone(),
            Some(held) => held[held.len() - 1].clone(),
        }
    }
}

fn revision(world: &World, sides: &mut Sides, rng: &mut Rng, state: &str) -> u64 {
    let held = driven(world, "a peek", sides.one.peek(state))
        .and_then(Result::ok)
        .map_or(0, |stored| stored.revision);
    if rng.chance(SKEW_PPM) { held + 1 } else { held }
}

fn op(
    rng: &mut Rng,
    world: &World,
    sides: &mut Sides,
    owners: &mut Owners,
    id: u64,
) -> Option<(&'static str, String, String)> {
    let state = format!("s{}", rng.below(STATES));
    let key = format!("k{}", rng.below(KEYS));
    let answered = match rng.below(9) {
        0 => {
            let owner = OwnerId::from(format!("owner{id}"));
            let ours = driven(
                world,
                "an acquire",
                sides.one.acquire(&state, owner.clone()),
            );
            let theirs = driven(world, "an acquire", sides.thread.acquire(&state, owner));
            if let Some(Ok(owned)) = &ours {
                owners.0.entry(state).or_default().push(owned.owner.clone());
            }
            ("acquire", shown(&ours), shown(&theirs))
        }
        1 | 2 => {
            let token = owners.token(rng, &state);
            let expected = revision(world, sides, rng, &state);
            let records = records(rng, id);
            let payload = format!("p{id}");
            let summary = SummaryUpdate::default();
            let settle = if rng.chance(500_000) {
                Settle::Durable
            } else {
                Settle::Ordered
            };
            let commit = |store: &mut dyn StateStore| {
                let commit = StateCommit {
                    payload: &payload,
                    records: &records,
                    events: &[],
                    summary: &summary,
                    settle,
                };
                driven(
                    world,
                    "a commit",
                    store.commit(&state, &token, expected, commit),
                )
            };
            let ours = commit(&mut sides.one);
            let theirs = commit(&mut sides.thread);
            ("commit", shown(&ours), shown(&theirs))
        }
        3 => {
            let token = owners.token(rng, &state);
            let expected = revision(world, sides, rng, &state);
            let records = records(rng, id);
            let payload = format!("p{id}");
            let ours = driven(
                world,
                "a replace",
                sides
                    .one
                    .replace(&state, &token, expected, &payload, &records),
            );
            let theirs = driven(
                world,
                "a replace",
                sides
                    .thread
                    .replace(&state, &token, expected, &payload, &records),
            );
            ("replace", shown(&ours), shown(&theirs))
        }
        4 => {
            let records = records(rng, id);
            let ours = driven(world, "a put", sides.one.put_records(&state, &records));
            let theirs = driven(world, "a put", sides.thread.put_records(&state, &records));
            ("put_records", shown(&ours), shown(&theirs))
        }
        5 => {
            let ours = driven(world, "a read", sides.one.read_record(&state, &key));
            let theirs = driven(world, "a read", sides.thread.read_record(&state, &key));
            ("read_record", shown(&ours), shown(&theirs))
        }
        6 => {
            let limit = usize::try_from(rng.below(STATES + 1)).expect("a limit fits");
            let ours = driven(world, "a listing", sides.one.list_states(limit));
            let theirs = driven(world, "a listing", sides.thread.list_states(limit));
            ("list_states", shown(&ours), shown(&theirs))
        }
        7 => {
            let ours = driven(world, "an owner read", sides.one.owner(&state));
            let theirs = driven(world, "an owner read", sides.thread.owner(&state));
            ("owner", shown(&ours), shown(&theirs))
        }
        _ => {
            let ours = driven(world, "a flush", sides.one.flush());
            let theirs = driven(world, "a flush", sides.thread.flush());
            ("flush", shown(&ours), shown(&theirs))
        }
    };
    Some(answered)
}

fn compared(world: &World, sides: &mut Sides, step: u64) {
    let keys: Vec<String> = (0..KEYS).map(|key| format!("k{key}")).collect();
    for state in 0..STATES {
        let state = format!("s{state}");
        let reads = [
            (
                "peek",
                shown(&driven(world, "a peek", sides.one.peek(&state))),
                shown(&driven(world, "a peek", sides.thread.peek(&state))),
            ),
            (
                "records",
                shown(&driven(world, "a listing", sides.one.list_records(&state))),
                shown(&driven(
                    world,
                    "a listing",
                    sides.thread.list_records(&state),
                )),
            ),
            (
                "values",
                shown(&driven(
                    world,
                    "a read",
                    sides.one.read_records(&state, &keys),
                )),
                shown(&driven(
                    world,
                    "a read",
                    sides.thread.read_records(&state, &keys),
                )),
            ),
        ];
        for (what, ours, theirs) in reads {
            if ours != theirs {
                flag(
                    &world.wrong,
                    format!(
                        "store: after step {step} the turn log's {what} of {state} is {ours} but the thread's {theirs}"
                    ),
                );
                return;
            }
        }
    }
}

pub(crate) fn run(rng: &mut Rng, world: &World, stats: &mut ThreadScopeStats) {
    let host: Arc<dyn LogHost> = Arc::new(MemoryLog::new());
    let mut generation = 0;
    let mut owners = Owners(BTreeMap::new());
    let Some(mut sides) = open(world, &host, generation) else {
        return;
    };
    for step in 0..rng.range(OPS.start, OPS.end) {
        if rng.chance(REOPEN_PPM) {
            generation += 1;
            stats.store_reopens += 1;
            owners.0.clear();
            let Some(next) = reopened(world, &host, sides, generation) else {
                return;
            };
            sides = next;
            continue;
        }
        stats.store_ops += 1;
        let Some((what, ours, theirs)) = op(rng, world, &mut sides, &mut owners, step) else {
            return;
        };
        if ours != theirs {
            flag(
                &world.wrong,
                format!(
                    "store: step {step} {what} answers {ours} on the turn log but {theirs} on the thread"
                ),
            );
            return;
        }
        compared(world, &mut sides, step);
        if world.wrong.read(Option::is_some) {
            return;
        }
    }
}
