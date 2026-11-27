use std::{ops::Range, sync::Arc};

use nanocode::{
    durability::facts::{Fact, Intent, IntentTarget},
    turnlog::{Effects, OneLog, Policy, ThreadFacts, ThreadHolds},
};
use nanocode_durability_core::{FactLog, FactLogs, OwnedFact, StateStore, commit_fact, fold_facts};
use nanocode_tool_runtime::{
    ToolInput, ToolOutput,
    effects::{Carried, EffectCall, EffectGate, RemoteBarrier},
    hold::{Answer, HoldCall, HoldLedger, Running},
};
use nanocode_turnlog::{EffectClass, Fold, LogHost, MemoryLog};
use serde_json::json;

use crate::rng::Rng;
use crate::threadscope::{Node, PARTS, Tally, ThreadScopeStats, World, driven, flag, publish, raw};

const THREAD: &str = "holds";
const OPS: Range<u64> = 40..160;
const KEYS: u64 = 4;
const CALLS: u64 = 6;
const STATES: u64 = 2;
const TOOLS: [&str; 2] = ["deploy", "send"];

struct Sides {
    one: Arc<OneLog>,
    holds: ThreadHolds,
    node: Node,
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
            flag(&world.wrong, format!("holds: the turn log opens: {error}"));
            return None;
        }
    };
    let node = world.node(&format!("holds{generation}"));
    publish(world, std::slice::from_ref(&node));
    let log = match driven(world, "a thread get", node.logs.get(THREAD))? {
        Ok(log) => log,
        Err(error) => {
            flag(&world.wrong, format!("holds: the thread claims: {error}"));
            return None;
        }
    };
    match ThreadHolds::new(Arc::new(log), Effects::default()) {
        Ok(holds) => Some(Sides { one, holds, node }),
        Err(error) => {
            flag(
                &world.wrong,
                format!("holds: the thread is local to its claimer: {error}"),
            );
            None
        }
    }
}

fn held(world: &World, sides: &Sides, key: &str, id: u64) -> (String, String) {
    let card = raw(&json!({ "card": id }));
    let input = raw(&json!({ "n": id }));
    let call_id = format!("h{key}");
    let call = || HoldCall {
        call_id: &call_id,
        input: &input,
        session_id: THREAD,
        tool: "send",
        turn_id: None,
    };
    let ours = driven(world, "a hold", sides.one.ledger().hold(key, call(), &card));
    let theirs = driven(world, "a hold", sides.holds.hold(key, call(), &card));
    (
        format!(
            "{:?}",
            ours.map(|held| held.map_err(|error| error.to_string()))
        ),
        format!(
            "{:?}",
            theirs.map(|held| held.map_err(|error| error.to_string()))
        ),
    )
}

fn reopened(
    world: &World,
    host: &Arc<dyn LogHost>,
    sides: Sides,
    generation: u64,
) -> Option<Sides> {
    let (ours, theirs) = held(world, &sides, &format!("reopen{generation}"), generation);
    if ours != theirs {
        flag(
            &world.wrong,
            format!(
                "holds: the hold before reopen {generation} answers {ours} on the turn log but {theirs} on the thread"
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
                format!("holds: the thread's node releases partition {part}: {error}"),
            );
            return None;
        }
    }
    drop(sides);
    open(world, host, generation)
}

fn intent(rng: &mut Rng, world: &World, sides: &Sides, call_id: &str) -> Option<(String, String)> {
    let state = format!("state{}", rng.below(STATES));
    let fact: OwnedFact<u64> = Fact::Intent(Intent {
        turn: None,
        call_id: call_id.to_owned(),
        tool: TOOLS[0].to_owned(),
        target: IntentTarget::Nested {
            parent: format!("parent{call_id}"),
            input: raw(&json!({ "call": call_id })),
        },
    });
    let Some(ours) = sides.one.store().facts() else {
        flag(
            &world.wrong,
            "holds: the turn log's store has no fact logs".to_owned(),
        );
        return None;
    };
    let ours = ours.scoped(&state, true);
    let theirs = ThreadFacts::new(Arc::clone(sides.holds.log())).scoped(&state, true);
    let committed = |log: &dyn FactLog, fact: OwnedFact<u64>| {
        driven(world, "a fact commit", async move {
            let folded = fold_facts::<Tally>(log).await?;
            commit_fact(log, folded, fact).await.map(|(_, seq)| seq)
        })
    };
    let one = committed(&*ours, fact.clone());
    let other = committed(&*theirs, fact);
    Some((
        format!("{:?}", one.map(|seq| seq.is_ok())),
        format!("{:?}", other.map(|seq| seq.is_ok())),
    ))
}

fn answer(rng: &mut Rng, id: u64) -> Answer {
    match rng.below(5) {
        0 => Answer::Approved,
        1 => Answer::Edited {
            input: raw(&json!({ "edited": id })),
        },
        2 => Answer::Denied { reason: None },
        3 => Answer::Replied {
            message: format!("reply{id}"),
        },
        _ => Answer::Expired,
    }
}

fn shown_running(running: Option<Option<Running>>) -> String {
    format!(
        "{:?}",
        running.map(|running| running.map(|running| (running.key, running.input.get().to_owned())))
    )
}

fn gated(rng: &mut Rng, world: &World, sides: &Sides, call_id: &str) -> (String, String) {
    let input = ToolInput::Function(raw(&json!({ "call": call_id })));
    let session = if rng.chance(500_000) {
        format!("state{}", rng.below(STATES))
    } else {
        "plain".to_owned()
    };
    let tool = TOOLS[usize::from(rng.chance(300_000))];
    let nested = format!("{call_id}/start");
    let call_id = if rng.chance(200_000) {
        nested.as_str()
    } else {
        call_id
    };
    let call = || EffectCall {
        call_id,
        effect: EffectClass::External,
        input: &input,
        session_id: &session,
        tool,
        turn_id: None,
    };
    let ours = driven(world, "a gate", sides.one.gate().before(call()));
    let theirs = driven(world, "a gate", sides.holds.before(call()));
    (format!("{ours:?}"), format!("{theirs:?}"))
}

fn op(
    rng: &mut Rng,
    world: &World,
    sides: &Sides,
    id: u64,
) -> Option<(&'static str, String, String)> {
    let key = format!("k{}", rng.below(KEYS));
    let call_id = format!("g{}", rng.below(CALLS));
    let (one, holds) = (&sides.one, &sides.holds);
    let answered = match rng.below(12) {
        0 | 1 => {
            let (ours, theirs) = held(world, sides, &key, id);
            ("hold", ours, theirs)
        }
        2 | 3 => {
            let answer = answer(rng, id);
            let ours = driven(
                world,
                "a resolve",
                one.ledger().resolve(&key, answer.clone()),
            );
            let theirs = driven(world, "a resolve", holds.resolve(&key, answer));
            (
                "resolve",
                format!(
                    "{:?}",
                    ours.map(|resolved| resolved.map_err(|error| error.to_string()))
                ),
                format!(
                    "{:?}",
                    theirs.map(|resolved| resolved.map_err(|error| error.to_string()))
                ),
            )
        }
        4 => {
            let held = format!("h{key}");
            (
                "running",
                shown_running(driven(world, "a running read", one.ledger().running(&held))),
                shown_running(driven(world, "a running read", holds.running(&held))),
            )
        }
        5 => (
            "claim",
            format!("{:?}", driven(world, "a claim", one.ledger().claim(&key))),
            format!("{:?}", driven(world, "a claim", holds.claim(&key))),
        ),
        6 => {
            let ours = driven(world, "a release", one.ledger().release(&key));
            let theirs = driven(world, "a release", holds.release(&key));
            (
                "release",
                format!(
                    "{:?}",
                    ours.map(|released| released.map_err(|error| error.to_string()))
                ),
                format!(
                    "{:?}",
                    theirs.map(|released| released.map_err(|error| error.to_string()))
                ),
            )
        }
        7 => {
            let (ours, theirs) = gated(rng, world, sides, &call_id);
            ("before", ours, theirs)
        }
        8 => {
            let output = if rng.chance(500_000) {
                ToolOutput::text("ok")
            } else {
                ToolOutput::error("failed")
            };
            (
                "after",
                format!(
                    "{:?}",
                    driven(world, "an after", one.gate().after(&call_id, &output))
                ),
                format!(
                    "{:?}",
                    driven(world, "an after", holds.after(&call_id, &output))
                ),
            )
        }
        9 => (
            "abandon",
            format!(
                "{:?}",
                driven(world, "an abandon", one.gate().abandon(&call_id))
                    .map(|abandoned| abandoned.map(|output| output.map(|output| output.success)))
            ),
            format!(
                "{:?}",
                driven(world, "an abandon", EffectGate::abandon(holds, &call_id))
                    .map(|abandoned| abandoned.map(|output| output.map(|output| output.success)))
            ),
        ),
        10 => {
            let (ours, theirs) = intent(rng, world, sides, &call_id)?;
            ("intent", ours, theirs)
        }
        _ => {
            let carried = || Carried {
                host: 0,
                statements: vec![("select 1", Vec::new())],
            };
            (
                "carry",
                format!(
                    "{:?}",
                    driven(world, "a carry", one.barrier().carry(carried()))
                ),
                format!("{:?}", driven(world, "a carry", holds.carry(carried()))),
            )
        }
    };
    Some(answered)
}

fn holds_of(fold: &Fold) -> Vec<String> {
    fold.holds()
        .map(|(key, hold)| format!("{key}: {hold:?}"))
        .collect()
}

fn calls_of(fold: &Fold) -> Vec<String> {
    fold.open_calls()
        .map(|(id, call)| format!("{id}: {} {:?}", call.tool, call.effect))
        .collect()
}

fn compared(world: &World, sides: &Sides, step: u64) {
    let ours = driven(world, "a hold read", sides.one.read(holds_of));
    let theirs = sides.holds.log().state().map(|state| {
        state
            .holds()
            .map(|(key, hold)| format!("{key}: {hold:?}"))
            .collect::<Vec<_>>()
    });
    if ours != theirs {
        flag(
            &world.wrong,
            format!(
                "holds: after step {step} the turn log holds {ours:?} but the thread commits {theirs:?}"
            ),
        );
        return;
    }
    let ours = driven(world, "an uncertain read", sides.one.uncertain_holds());
    let theirs = Some(sides.holds.uncertain_holds());
    if ours != theirs {
        flag(
            &world.wrong,
            format!(
                "holds: after step {step} the turn log's uncertain holds are {ours:?} but the thread's {theirs:?}"
            ),
        );
        return;
    }
    let ours = driven(world, "a call read", sides.one.read(calls_of));
    let theirs = driven(
        world,
        "a call read",
        sides.holds.log().job_staged(|staged| {
            Ok(staged
                .state()
                .open_calls()
                .map(|(id, call)| format!("{id}: {} {:?}", call.tool, call.effect))
                .collect::<Vec<_>>())
        }),
    )
    .and_then(Result::ok);
    if ours != theirs {
        flag(
            &world.wrong,
            format!(
                "holds: after step {step} the turn log has open calls {ours:?} but the thread {theirs:?}"
            ),
        );
    }
}

fn settled(world: &World, sides: &Sides, step: u64) {
    sides.holds.owe();
    let settled = driven(world, "a settle", RemoteBarrier::settle(&sides.holds));
    if !matches!(settled, Some(Ok(()))) {
        flag(
            &world.wrong,
            format!("holds: step {step} an owed settle answers {settled:?}"),
        );
        return;
    }
    let staged = driven(
        world,
        "a staged read",
        sides.holds.log().job_staged(|staged| Ok(staged.staged())),
    );
    let committed = sides
        .holds
        .log()
        .state()
        .map(|state| state.open_calls().count());
    let ours = driven(
        world,
        "a call read",
        sides.one.read(|fold| fold.open_calls().count()),
    );
    if !matches!(staged, Some(Ok(0))) || committed != ours {
        flag(
            &world.wrong,
            format!(
                "holds: step {step} an owed settle leaves {staged:?} rows staged and commits {committed:?} open calls where the turn log holds {ours:?}"
            ),
        );
    }
}

pub(crate) fn run(rng: &mut Rng, world: &World, stats: &mut ThreadScopeStats) {
    let host: Arc<dyn LogHost> = Arc::new(MemoryLog::new());
    let mut generation = 0;
    let Some(mut sides) = open(world, &host, generation) else {
        return;
    };
    for step in 0..rng.range(OPS.start, OPS.end) {
        if rng.chance(80_000) {
            generation += 1;
            stats.hold_reopens += 1;
            let Some(next) = reopened(world, &host, sides, generation) else {
                return;
            };
            sides = next;
            continue;
        }
        stats.hold_ops += 1;
        let Some((what, ours, theirs)) = op(rng, world, &sides, step) else {
            return;
        };
        if ours != theirs {
            flag(
                &world.wrong,
                format!(
                    "holds: step {step} {what} answers {ours} on the turn log but {theirs} on the thread"
                ),
            );
            return;
        }
        compared(world, &sides, step);
        if world.wrong.read(Option::is_none) && rng.chance(100_000) {
            stats.hold_settles += 1;
            settled(world, &sides, step);
        }
        if world.wrong.read(Option::is_some) {
            return;
        }
    }
}
