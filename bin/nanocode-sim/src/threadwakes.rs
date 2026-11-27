use std::{collections::BTreeMap, fmt::Debug, ops::Range, sync::Arc};

use nanocode::{
    backend::Prompt,
    session_host::Submission,
    turnlog::{OneLog, Policy, ThreadState, ThreadWakes, Trigger, Wake, WakeRider, WakeSummary},
};
use nanocode_durability_core::{StoreError, StoreFuture};
use nanocode_sync::Atom;
use nanocode_turnlog::{LogHost, MemoryLog, Statements};
use serde_json::{Map, json};

use crate::rng::Rng;
use crate::threadscope::{ThreadScopeStats, World, driven, flag, publish, raw};

const THREAD: &str = "wakes";
const OPS: Range<u64> = 40..160;
const KEYS: u64 = 4;
const TOPICS: u64 = 2;
const TURNS: u64 = 6;
const WAKES_MAX: Range<u64> = 1..4;
const HEARD_MAX: Range<u64> = 1..3;
const DUE_MS_MAX: u64 = 2_000;
const FOREIGN_HOST: usize = 2;

#[derive(Default)]
struct Index(Atom<BTreeMap<String, WakeSummary>>);

impl Index {
    fn held(&self) -> Option<WakeSummary> {
        self.0.read(|held| held.get(THREAD).cloned())
    }
}

impl WakeRider for Index {
    fn host(&self) -> usize {
        FOREIGN_HOST
    }

    fn statements(&self, _scope: &str, _summary: &WakeSummary) -> Statements {
        Vec::new()
    }

    fn prepare(&self) -> StoreFuture<'_, Result<(), StoreError>> {
        Box::pin(async { Ok(()) })
    }

    fn write<'a>(
        &'a self,
        scope: &'a str,
        summary: &'a WakeSummary,
    ) -> StoreFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            self.0.update(|held| {
                held.insert(scope.to_owned(), summary.clone());
            });
            Ok(())
        })
    }
}

struct Sides {
    one: Arc<OneLog>,
    wakes: ThreadWakes,
    ours: Arc<Index>,
    theirs: Arc<Index>,
}

fn shown<T: Debug>(answer: &T) -> String {
    crate::threadturns::normalized(format!("{answer:?}"))
}

fn open(world: &World, wakes_max: usize) -> Option<Sides> {
    let host: Arc<dyn LogHost> = Arc::new(MemoryLog::new());
    let policy = Policy {
        wakes_max: Some(wakes_max),
        ..Policy::default()
    };
    let one = match driven(
        world,
        "a turn log open",
        nanocode::turnlog::thread_scope_sim::open(host, THREAD, policy),
    )? {
        Ok(one) => one,
        Err(error) => {
            flag(&world.wrong, format!("wakes: the turn log opens: {error}"));
            return None;
        }
    };
    let node = world.node("wakes0");
    publish(world, std::slice::from_ref(&node));
    let log = match driven(world, "a thread get", node.logs.get(THREAD))? {
        Ok(log) => log,
        Err(error) => {
            flag(&world.wrong, format!("wakes: the thread claims: {error}"));
            return None;
        }
    };
    let wakes = match ThreadWakes::new(Arc::new(log), THREAD, Some(wakes_max)) {
        Ok(wakes) => wakes,
        Err(error) => {
            flag(&world.wrong, format!("wakes: the thread is local: {error}"));
            return None;
        }
    };
    let (ours, theirs) = (Arc::new(Index::default()), Arc::new(Index::default()));
    let indexed = (
        driven(world, "an index", one.index_wakes(ours.clone()))?,
        driven(world, "an index", wakes.index_wakes(theirs.clone()))?,
    );
    if let (Err(error), _) | (_, Err(error)) = indexed {
        flag(&world.wrong, format!("wakes: the index opens: {error}"));
        return None;
    }
    Some(Sides {
        one,
        wakes,
        ours,
        theirs,
    })
}

fn wake(rng: &mut Rng) -> Wake {
    let due_at = rng.chance(600_000).then(|| rng.below(DUE_MS_MAX));
    let on = (due_at.is_none() || rng.chance(500_000)).then(|| Trigger {
        filter: BTreeMap::from([("n".to_owned(), vec![json!(rng.below(2))])]),
        heard_max: rng.range(HEARD_MAX.start, HEARD_MAX.end),
        topic: format!("topic{}", rng.below(TOPICS)),
    });
    Wake {
        due_at,
        on,
        payload: raw(&json!({ "note": "check" })),
    }
}

fn op(
    rng: &mut Rng,
    world: &World,
    sides: &Sides,
    id: u64,
) -> Option<(&'static str, String, String)> {
    let key = format!("k{}", rng.below(KEYS));
    let (one, wakes) = (&sides.one, &sides.wakes);
    let answered = match rng.below(7) {
        0 | 1 => {
            let wake = wake(rng);
            (
                "arm",
                shown(&driven(world, "an arm", one.arm(&key, wake.clone()))?),
                shown(&driven(world, "an arm", wakes.arm(&key, wake))?),
            )
        }
        2 => (
            "disarm",
            shown(&driven(world, "a disarm", one.disarm(&key))?),
            shown(&driven(world, "a disarm", wakes.disarm(&key))?),
        ),
        3 => {
            let topic = format!("topic{}", rng.below(TOPICS));
            let fields = Map::from_iter([("n".to_owned(), json!(rng.below(2)))]);
            let event = raw(&json!({ "heard": id }));
            (
                "hear",
                shown(&driven(
                    world,
                    "a hear",
                    one.hear(&topic, &fields, event.clone()),
                )?),
                shown(&driven(
                    world,
                    "a hear",
                    wakes.hear(&topic, &fields, event),
                )?),
            )
        }
        4 | 5 => {
            let held = wakes.wake(&key).ok().flatten();
            let generation = match held {
                Some(armed) if rng.chance(800_000) => armed.generation,
                _ => rng.below(8),
            };
            let submission = Submission {
                request_id: format!("w{}", rng.below(TURNS)),
                prompt: Prompt::new(format!("fire {}", rng.below(2))),
                hand: None,
                metadata: raw(&json!(null)),
            };
            let next = rng.chance(300_000).then(|| wake(rng));
            let now_ms = rng.below(DUE_MS_MAX);
            (
                "fire",
                shown(&driven(
                    world,
                    "a fire",
                    one.fire(&key, generation, now_ms, &submission, next.clone()),
                )?),
                shown(&driven(
                    world,
                    "a fire",
                    wakes.fire(&key, generation, now_ms, &submission, next),
                )?),
            )
        }
        _ => {
            let now_ms = rng.below(DUE_MS_MAX);
            (
                "read",
                shown(&(
                    driven(world, "a read", one.wake(&key))?,
                    driven(world, "a read", one.due_wakes(now_ms))?,
                    driven(world, "a read", one.next_due())?,
                )),
                shown(&(
                    wakes.wake(&key).ok()?,
                    wakes.due_wakes(now_ms).ok()?,
                    wakes.next_due().ok()?,
                )),
            )
        }
    };
    Some(answered)
}

fn summary(state: &ThreadState) -> WakeSummary {
    let mut topics: Vec<String> = state
        .wakes()
        .filter(|(_, armed)| !armed.full())
        .filter_map(|(_, armed)| armed.wake.on.as_ref().map(|on| on.topic.clone()))
        .collect();
    topics.sort();
    topics.dedup();
    let next_due = state.wakes().filter_map(|(_, armed)| armed.due_at()).min();
    WakeSummary { next_due, topics }
}

fn indexed(world: &World, sides: &Sides, step: u64) -> bool {
    let Some(state) = sides.wakes.log().state() else {
        flag(
            &world.wrong,
            format!("wakes: after step {step} the thread is not local"),
        );
        return false;
    };
    let expected = summary(&state);
    let (ours, theirs) = (sides.ours.held(), sides.theirs.held());
    if theirs.as_ref() != Some(&expected) {
        flag(
            &world.wrong,
            format!(
                "wakes: after step {step} the thread's wake index holds {theirs:?}, its wakes need {expected:?}"
            ),
        );
        return false;
    }
    if ours != theirs {
        flag(
            &world.wrong,
            format!(
                "wakes: after step {step} the turn log's wake index holds {ours:?} but the thread's {theirs:?}"
            ),
        );
        return false;
    }
    true
}

pub(crate) fn run(rng: &mut Rng, world: &World, stats: &mut ThreadScopeStats) {
    let wakes_max = rng.range(WAKES_MAX.start, WAKES_MAX.end) as usize;
    let Some(sides) = open(world, wakes_max) else {
        return;
    };
    for step in 0..rng.range(OPS.start, OPS.end) {
        stats.wake_ops += 1;
        let Some((what, ours, theirs)) = op(rng, world, &sides, step) else {
            return;
        };
        if ours != theirs {
            flag(
                &world.wrong,
                format!(
                    "wakes: step {step} {what} answers {ours} on the turn log but {theirs} on the thread"
                ),
            );
            return;
        }
        if !indexed(world, &sides, step) {
            return;
        }
    }
}
