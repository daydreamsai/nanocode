use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Debug,
    ops::Range,
    sync::Arc,
};

use nanocode::{
    backend::Prompt,
    durability::facts::{Fact, Intent, IntentTarget},
    session_host::{LogEntry, Submission, TurnState},
    turnlog::{OneLog, Policy, ThreadFacts, ThreadTurns},
};
use nanocode_durability_core::{FactLog, FactLogs, OwnedFact, StateStore, commit_fact, fold_facts};
use nanocode_turnlog::{LogHost, MemoryLog, Outcome, Phase};
use serde_json::{Map, json};

use crate::rng::Rng;
use crate::threadscope::{Node, PARTS, Tally, ThreadScopeStats, World, driven, flag, publish, raw};

const THREAD: &str = "turns";
const OPS: Range<u64> = 40..160;
const RESTARTS: Range<u64> = 1..4;
const TURNS: u64 = 6;
const STATES: u64 = 2;
const START: Range<u64> = 3..5;

struct Sides {
    one: Arc<OneLog>,
    turns: ThreadTurns,
    node: Node,
    recovering: BTreeSet<String>,
}

const RENUMBERED: [&str; 3] = ["created_at: ", "seq: ", "seq: Some("];

pub(crate) fn normalized(text: String) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text.as_str();
    while let Some((at, key)) = RENUMBERED
        .iter()
        .filter_map(|key| rest.find(key).map(|at| (at, *key)))
        .min_by_key(|(at, key)| (*at, std::cmp::Reverse(key.len())))
    {
        let (head, tail) = rest.split_at(at + key.len());
        out.push_str(head);
        let digits = tail.len() - tail.trim_start_matches(|c: char| c.is_ascii_digit()).len();
        if digits > 0 {
            out.push('0');
        }
        rest = &tail[digits..];
    }
    out.push_str(rest);
    out
}

fn shown<T: Debug, E: Debug>(answer: &Option<Result<T, E>>) -> String {
    normalized(format!("{answer:?}"))
}

fn open(world: &World, host: &Arc<dyn LogHost>, policy: &Policy, generation: u64) -> Option<Sides> {
    let one = driven(
        world,
        "a turn log open",
        nanocode::turnlog::thread_scope_sim::open(Arc::clone(host), THREAD, policy.clone()),
    );
    let one = match one? {
        Ok(one) => one,
        Err(error) => {
            flag(&world.wrong, format!("turns: the turn log opens: {error}"));
            return None;
        }
    };
    let node = world.node(&format!("turns{generation}"));
    publish(world, std::slice::from_ref(&node));
    let log = match driven(world, "a thread get", node.logs.get(THREAD))? {
        Ok(log) => log,
        Err(error) => {
            flag(&world.wrong, format!("turns: the thread claims: {error}"));
            return None;
        }
    };
    let recovering = log.state().map_or_else(BTreeSet::new, |state| {
        state.pending().map(|turn| turn.id.clone()).collect()
    });
    match ThreadTurns::new(Arc::new(log), policy.restarts) {
        Ok(turns) => Some(Sides {
            one,
            turns,
            node,
            recovering,
        }),
        Err(error) => {
            flag(
                &world.wrong,
                format!("turns: the thread is local to its claimer: {error}"),
            );
            None
        }
    }
}

fn reopened(
    world: &World,
    host: &Arc<dyn LogHost>,
    policy: &Policy,
    sides: Sides,
    generation: u64,
) -> Option<Sides> {
    let marker = Submission {
        request_id: format!("r{generation}"),
        prompt: Prompt::new(format!("reopen {generation}")),
        hand: None,
        metadata: raw(&json!(null)),
    };
    let ours = shown(&driven(world, "a submit", sides.one.submit(&marker)));
    let theirs = shown(&driven(world, "a submit", sides.turns.submit(&marker)));
    if ours != theirs {
        flag(
            &world.wrong,
            format!(
                "turns: the submit before reopen {generation} answers {ours} on the turn log but {theirs} on the thread"
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
                format!("turns: the thread's node releases partition {part}: {error}"),
            );
            return None;
        }
    }
    drop(sides);
    open(world, host, policy, generation)
}

fn intent(rng: &mut Rng, world: &World, sides: &Sides, id: u64) -> Option<(String, String)> {
    let state = format!("state{}", rng.below(STATES));
    let fact: OwnedFact<u64> = Fact::Intent(Intent {
        turn: rng
            .chance(700_000)
            .then(|| format!("t{}", rng.below(TURNS))),
        call_id: format!("call{id}"),
        tool: "deploy".to_owned(),
        target: IntentTarget::Nested {
            parent: format!("parent{id}"),
            input: raw(&json!({ "call": id })),
        },
    });
    let Some(ours) = sides.one.store().facts() else {
        flag(
            &world.wrong,
            "turns: the turn log's store has no fact logs".to_owned(),
        );
        return None;
    };
    let ours = ours.scoped(&state, true);
    let theirs = ThreadFacts::new(Arc::clone(sides.turns.log())).scoped(&state, true);
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

fn op(
    rng: &mut Rng,
    world: &World,
    sides: &Sides,
    kind: u64,
    id: u64,
) -> Option<(&'static str, String, String)> {
    let turn = format!("t{}", rng.below(TURNS));
    let (one, turns) = (&sides.one, &sides.turns);
    let fields = Map::from_iter([("n".to_owned(), json!(id))]);
    let answered = match kind {
        0 | 1 => {
            let submission = Submission {
                request_id: turn,
                prompt: Prompt::new(format!("prompt {}", rng.below(3))),
                hand: None,
                metadata: raw(&json!(null)),
            };
            if rng.chance(500_000) {
                (
                    "submit",
                    shown(&driven(world, "a submit", one.submit(&submission))),
                    shown(&driven(world, "a submit", turns.submit(&submission))),
                )
            } else {
                (
                    "submit_buffered",
                    shown(&driven(world, "a submit", one.submit_buffered(&submission))),
                    shown(&driven(
                        world,
                        "a submit",
                        turns.submit_buffered(&submission),
                    )),
                )
            }
        }
        2 => {
            let entries: Vec<LogEntry> = (0..1 + rng.below(3))
                .map(|at| LogEntry {
                    request_id: rng
                        .chance(500_000)
                        .then(|| format!("t{}", rng.below(TURNS))),
                    event: raw(&json!({ "type": "note", "n": id, "at": at })),
                })
                .collect();
            (
                "append",
                shown(&driven(world, "an append", one.append(&entries))),
                shown(&driven(world, "an append", turns.append(&entries))),
            )
        }
        5 => {
            let state = if rng.chance(500_000) {
                TurnState::Completed
            } else {
                TurnState::Failed
            };
            (
                "finish",
                shown(&driven(
                    world,
                    "a finish",
                    one.finish(&turn, state, &fields),
                )),
                shown(&driven(
                    world,
                    "a finish",
                    turns.finish(&turn, state, &fields),
                )),
            )
        }
        6 => (
            "cancel",
            shown(&driven(world, "a cancel", one.cancel(&turn, &fields))),
            shown(&driven(world, "a cancel", turns.cancel(&turn, &fields))),
        ),
        7 => {
            let held = driven(world, "a read", one.turn(&turn))
                .and_then(Result::ok)
                .flatten();
            let digest = match held {
                Some(record) if rng.chance(700_000) => record.digest,
                _ => format!("digest{id}"),
            };
            (
                "withdraw_steered",
                shown(&driven(
                    world,
                    "a withdrawal",
                    one.withdraw_steered(&turn, &digest),
                )),
                shown(&driven(
                    world,
                    "a withdrawal",
                    turns.withdraw_steered(&turn, &digest),
                )),
            )
        }
        8 => {
            let note = format!("note{}", rng.below(2));
            (
                "interrupted",
                shown(&driven(
                    world,
                    "an interrupt",
                    one.interrupted(&turn, &note),
                )),
                shown(&driven(
                    world,
                    "an interrupt",
                    turns.interrupted(&turn, &note),
                )),
            )
        }
        9 => {
            let error = format!("error{}", rng.below(2));
            let kind = format!("kind{}", rng.below(2));
            let at = rng.below(1_000_000);
            (
                "retry",
                shown(&driven(
                    world,
                    "a retry",
                    one.retry(&turn, &error, &kind, at),
                )),
                shown(&driven(
                    world,
                    "a retry",
                    turns.retry(&turn, &error, &kind, at),
                )),
            )
        }
        10 => {
            let (one, other) = intent(rng, world, sides, id)?;
            ("intent", one, other)
        }
        _ => (
            "settle_intents",
            shown(&driven(world, "a settle", one.settle_intents(&turn))),
            shown(&driven(world, "a settle", turns.settle_intents(&turn))),
        ),
    };
    Some(answered)
}

fn compared(world: &World, sides: &Sides, step: u64) {
    let ours = driven(world, "a pending read", sides.one.pending());
    let theirs = driven(world, "a pending read", sides.turns.pending());
    let (ours, theirs) = (shown(&ours), shown(&theirs));
    if ours != theirs {
        flag(
            &world.wrong,
            format!(
                "turns: after step {step} the turn log holds pending turns {ours} but the thread {theirs}"
            ),
        );
        return;
    }
    for turn in 0..TURNS {
        let turn = format!("t{turn}");
        let ours = shown(&driven(world, "a turn read", sides.one.turn(&turn)));
        let theirs = shown(&driven(world, "a turn read", sides.turns.turn(&turn)));
        if ours != theirs {
            flag(
                &world.wrong,
                format!(
                    "turns: after step {step} the turn log holds {turn} as {ours} but the thread as {theirs}"
                ),
            );
            return;
        }
    }
}

struct Restart<'a> {
    turn: &'a str,
    restarts: u64,
    step: u64,
}

fn started(
    world: &World,
    sides: &mut Sides,
    attempts: &mut BTreeMap<String, u64>,
    restart: &Restart<'_>,
) -> bool {
    let Restart {
        turn,
        restarts,
        step,
    } = *restart;
    let held = driven(world, "a turn read", sides.turns.turn(turn))
        .and_then(Result::ok)
        .flatten();
    let live =
        held.is_some_and(|record| matches!(record.state, TurnState::Queued | TurnState::Running));
    let recovering = sides.recovering.remove(turn) && live;
    let attempt = attempts.get(turn).copied().unwrap_or(0) + 1;
    let ours = driven(world, "a start", sides.one.start(turn));
    let theirs = driven(world, "a start", sides.turns.start(turn));
    let after = sides.turns.log().state();
    let kept = after.as_ref().and_then(|state| state.turn(turn));
    if recovering && attempt > restarts {
        let failed = kept.is_some_and(|kept| {
            matches!(
                kept.phase,
                Phase::Finished {
                    outcome: Outcome::Failed,
                    ..
                }
            )
        });
        if !matches!(theirs, Some(Ok(None))) || !failed {
            flag(
                &world.wrong,
                format!(
                    "turns: step {step} started {turn} on the thread at recovery attempt {attempt}, past its {restarts} restarts: {}",
                    shown(&theirs)
                ),
            );
            return false;
        }
    } else if recovering {
        let counted = kept
            .and_then(|kept| kept.recovered)
            .map(|recovered| recovered.attempt);
        if counted != Some(attempt) {
            flag(
                &world.wrong,
                format!(
                    "turns: step {step} recovered {turn} on the thread as attempt {counted:?}, not {attempt}"
                ),
            );
            return false;
        }
        attempts.insert(turn.to_owned(), attempt);
    }
    let (ours, theirs) = (shown(&ours), shown(&theirs));
    if ours != theirs {
        flag(
            &world.wrong,
            format!(
                "turns: step {step} start answers {ours} on the turn log but {theirs} on the thread"
            ),
        );
        return false;
    }
    true
}

pub(crate) fn run(rng: &mut Rng, world: &World, stats: &mut ThreadScopeStats) {
    let host: Arc<dyn LogHost> = Arc::new(MemoryLog::new());
    let policy = Policy {
        restarts: rng.range(RESTARTS.start, RESTARTS.end),
        ..Policy::default()
    };
    let mut generation = 0;
    let mut attempts = BTreeMap::new();
    let Some(mut sides) = open(world, &host, &policy, generation) else {
        return;
    };
    for step in 0..rng.range(OPS.start, OPS.end) {
        if rng.chance(80_000) {
            generation += 1;
            stats.turn_reopens += 1;
            let Some(next) = reopened(world, &host, &policy, sides, generation) else {
                return;
            };
            sides = next;
            continue;
        }
        let kind = rng.below(12);
        stats.turn_ops += 1;
        if START.contains(&kind) {
            let turn = format!("t{}", rng.below(TURNS));
            let restart = Restart {
                turn: &turn,
                restarts: policy.restarts,
                step,
            };
            if !started(world, &mut sides, &mut attempts, &restart) {
                return;
            }
            compared(world, &sides, step);
            if world.wrong.read(Option::is_some) {
                return;
            }
            continue;
        }
        let Some((what, ours, theirs)) = op(rng, world, &sides, kind, step) else {
            return;
        };
        if ours != theirs {
            flag(
                &world.wrong,
                format!(
                    "turns: step {step} {what} answers {ours} on the turn log but {theirs} on the thread"
                ),
            );
            return;
        }
        compared(world, &sides, step);
        if world.wrong.read(Option::is_some) {
            return;
        }
    }
}
