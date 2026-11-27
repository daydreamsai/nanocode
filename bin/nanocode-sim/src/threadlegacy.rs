use std::{collections::BTreeMap, ops::Range, sync::Arc};

use nanocode::{backend::Prompt, turnlog::ThreadState};
use nanocode_durability_core::{
    Decision, Digest, Fact, Input, InputKind, Marker, Outcome as FactOutcome, StoreError,
    StoredFact, TurnStatus, VALUE_INLINE_BYTES, Value,
};
use nanocode_turnlog::{
    EffectClass, Event, Fold, Hold, LogHost, Phase, Point, Record, Turn, Wake, Writing,
};
use serde::Serialize;
use serde_json::{json, value::RawValue};

use crate::rng::Rng;

const STEPS: Range<u64> = 16..48;
const SNAPSHOT_ROWS: Range<u64> = 2..6;
const RETAIN: Range<u64> = 0..3;
const ROW_BYTES: Range<u64> = 700..3_000;
const BLOB_PPM: u64 = 300_000;
const SESSIONS: [&str; 2] = ["s0", "s1"];
const STATES: [&str; 2] = ["state0", "state1"];
const KEYS: u64 = 3;
const HOSTS: u64 = 3;
const RETIRED: &str = "retired:";

pub struct Legacy {
    pub next: u64,
    ids: Vec<String>,
    owners: Vec<String>,
    stored: Vec<String>,
    hosts: Vec<String>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct View {
    pub retired: u64,
    pub blobs: u64,
    turns: Vec<String>,
    events: Vec<String>,
    facts: Vec<String>,
    cells: Vec<String>,
    holds: Vec<String>,
    calls: Vec<String>,
    checkpoints: Vec<String>,
    stored: Vec<String>,
    hosts: Vec<String>,
    wakes: Vec<String>,
}

enum Op {
    Submit {
        id: String,
        prompt: Box<RawValue>,
    },
    Start {
        id: String,
    },
    Finish {
        id: String,
    },
    Note {
        turn: Option<String>,
        note: u64,
    },
    Cell {
        session: String,
        cell: u64,
        fresh: bool,
        update: u64,
    },
    Hold {
        key: String,
    },
    Host {
        key: String,
        value: u64,
    },
    Stored {
        key: String,
        value: String,
    },
    Checkpoint {
        state: String,
        revision: u64,
        payload: u64,
    },
    Wake {
        key: String,
        payload: u64,
    },
    Call {
        call_id: String,
    },
    Result {
        call_id: String,
    },
}

fn line(value: &impl Serialize) -> String {
    serde_json::to_string(value).expect("the simulator's values encode as JSON")
}

fn raw(value: &serde_json::Value) -> Box<RawValue> {
    serde_json::value::to_raw_value(value).expect("a JSON value encodes")
}

fn failed(what: &'static str) -> impl Fn(StoreError) -> String {
    move |error| format!("{what}: {error}")
}

fn lifecycle(kind: &str, id: &str) -> Box<RawValue> {
    raw(&json!({ "type": kind, "request_id": id }))
}

fn fact(
    writing: &mut Writing,
    fact: &Fact<Box<RawValue>, Box<RawValue>>,
) -> Result<u64, StoreError> {
    writing
        .append_owned(None, fact)
        .map_err(|error| StoreError::NotCommitted(error.to_string()))
}

fn status(writing: &mut Writing, id: &str, status: TurnStatus) -> Result<u64, StoreError> {
    fact(
        writing,
        &Fact::Marker(Marker::Status {
            turn: id.to_owned(),
            status,
        }),
    )
}

fn event(
    writing: &mut Writing,
    seq: u64,
    turn: Option<&str>,
    body: Box<RawValue>,
) -> Result<(), StoreError> {
    writing.push(Record::Event {
        at: seq,
        event: body,
        seq,
        turn: turn.map(str::to_owned),
    })
}

fn turn_op(writing: &mut Writing, op: Op) -> Result<Point, StoreError> {
    match op {
        Op::Submit { id, prompt } => {
            let first = writing.next_event();
            fact(
                writing,
                &Fact::Input(Input {
                    turn: id.clone(),
                    kind: InputKind::Prompt,
                    value: prompt,
                    metadata: raw(&json!({ "legacy": true })),
                    hand: None,
                    first: Some(first),
                }),
            )?;
            event(writing, first, Some(&id), lifecycle("queued", &id))?;
            Ok(Point::Admission)
        }
        Op::Start { id } => {
            status(writing, &id, TurnStatus::Running)?;
            let seq = writing.next_event();
            event(writing, seq, Some(&id), lifecycle("running", &id))?;
            Ok(Point::Progress)
        }
        Op::Finish { id } => {
            let last = writing.next_event();
            let result = raw(&json!({ "type": "result", "request_id": id, "status": "completed" }));
            event(writing, last, Some(&id), result.clone())?;
            status(
                writing,
                &id,
                TurnStatus::Finished {
                    last,
                    outcome: FactOutcome::Completed,
                    result,
                },
            )?;
            Ok(Point::Finish)
        }
        Op::Note { turn, note } => {
            let seq = writing.next_event();
            event(
                writing,
                seq,
                turn.as_deref(),
                raw(&json!({ "type": "note", "n": note })),
            )?;
            Ok(Point::Progress)
        }
        op => keep_op(writing, op),
    }
}

fn keep_op(writing: &mut Writing, op: Op) -> Result<Point, StoreError> {
    let record = match op {
        Op::Cell {
            session,
            cell,
            fresh,
            update,
        } => Record::Cell {
            cell,
            fresh,
            session,
            update: raw(&json!({ "update": update })),
        },
        Op::Hold { key } => Record::Hold {
            hold: Hold::Waiting {
                call_id: format!("{key}-call"),
                card: raw(&json!({})),
                input: raw(&json!({})),
                tool: "approve".to_owned(),
                turn: None,
            },
            key,
        },
        Op::Host { key, value } => {
            fact(
                writing,
                &Fact::Decision(Decision::Host {
                    key,
                    value: raw(&json!(value)),
                }),
            )?;
            return Ok(Point::Progress);
        }
        Op::Stored { key, value } => Record::Stored { key, value },
        Op::Checkpoint {
            state,
            revision,
            payload,
        } => Record::Checkpoint {
            payload: format!("payload {payload}"),
            revision,
            state,
        },
        Op::Wake { key, payload } => Record::Armed {
            generation: writing.fold().wakes().next_generation(),
            key,
            wake: Wake {
                due_at: Some(payload),
                on: None,
                payload: raw(&json!({ "wake": payload })),
            },
        },
        Op::Call { call_id } => Record::Call {
            call_id,
            effect: EffectClass::External,
            input: raw(&json!({})),
            tool: "send".to_owned(),
            turn: None,
        },
        Op::Result { call_id } => Record::Result {
            call_id,
            output: raw(&json!({ "ok": true })),
        },
        Op::Submit { .. } | Op::Start { .. } | Op::Finish { .. } | Op::Note { .. } => {
            panic!("a turn operation is written as a turn operation")
        }
    };
    writing.push(record)?;
    Ok(Point::Progress)
}

fn prompt(rng: &mut Rng) -> Box<RawValue> {
    let text = if rng.chance(BLOB_PPM) {
        format!(
            "big {} {}",
            rng.below(1_000),
            "x".repeat(VALUE_INLINE_BYTES)
        )
    } else {
        format!("prompt {}", rng.below(1_000))
    };
    serde_json::value::to_raw_value(&Prompt::new(text)).expect("a prompt encodes")
}

struct Script {
    prefix: String,
    ids: Vec<String>,
    phases: BTreeMap<String, u8>,
    calls: Vec<String>,
    holds: u64,
}

fn turn_pick(rng: &mut Rng, script: &mut Script, wanted: u8) -> Option<String> {
    let found: Vec<&String> = script
        .phases
        .iter()
        .filter(|(_, phase)| **phase == wanted)
        .map(|(id, _)| id)
        .collect();
    if found.is_empty() {
        return None;
    }
    let id = found[rng.pick(found.len())].clone();
    script.phases.insert(id.clone(), wanted + 1);
    Some(id)
}

fn op(rng: &mut Rng, script: &mut Script) -> Op {
    let pick = rng.below(100);
    let picked = match pick {
        0..22 => None,
        22..36 => turn_pick(rng, script, 0).map(|id| Op::Start { id }),
        36..54 => turn_pick(rng, script, 1).map(|id| Op::Finish { id }),
        54..64 => Some(Op::Note {
            turn: script.ids.last().filter(|_| rng.chance(500_000)).cloned(),
            note: rng.below(1_000),
        }),
        64..72 => Some(Op::Cell {
            session: SESSIONS[rng.pick(SESSIONS.len())].to_owned(),
            cell: rng.below(3),
            fresh: rng.chance(200_000),
            update: rng.below(1_000),
        }),
        72..76 => {
            script.holds += 1;
            Some(Op::Hold {
                key: format!("{}hold{}", script.prefix, script.holds),
            })
        }
        76..80 => Some(Op::Host {
            key: format!("h{}", rng.below(HOSTS)),
            value: rng.below(1_000),
        }),
        80..86 => Some(Op::Stored {
            key: stored_key(rng.below(2 * KEYS)),
            value: format!("v{}", rng.below(1_000)),
        }),
        86..90 => Some(Op::Checkpoint {
            state: STATES[rng.pick(STATES.len())].to_owned(),
            revision: rng.below(100),
            payload: rng.below(1_000),
        }),
        90..94 => Some(Op::Wake {
            key: format!("w{}", rng.below(3)),
            payload: rng.below(10_000),
        }),
        94..97 => {
            let call_id = format!("{}c{}", script.prefix, script.calls.len());
            script.calls.push(call_id.clone());
            Some(Op::Call { call_id })
        }
        _ => script.calls.pop().map(|call_id| Op::Result { call_id }),
    };
    picked.unwrap_or_else(|| {
        let id = format!("{}{}", script.prefix, script.ids.len());
        script.ids.push(id.clone());
        script.phases.insert(id.clone(), 0);
        Op::Submit {
            id,
            prompt: prompt(rng),
        }
    })
}

fn stored_key(at: u64) -> String {
    if at < KEYS {
        format!("k{at}")
    } else {
        format!(
            "{}\nk{at}",
            STATES[usize::try_from(at % 2).expect("a state index fits")]
        )
    }
}

pub async fn write(rng: &mut Rng, host: Arc<dyn LogHost>, thread: &str) -> Result<Legacy, String> {
    write_named(rng, host, thread, "old").await
}

pub async fn write_named(
    rng: &mut Rng,
    host: Arc<dyn LogHost>,
    thread: &str,
    prefix: &str,
) -> Result<Legacy, String> {
    let log = nanocode_turnlog::sim::open(host, thread)
        .await
        .map_err(failed("open a legacy turn log"))?
        .snapshot_rows(Some(
            usize::try_from(rng.range(SNAPSHOT_ROWS.start, SNAPSHOT_ROWS.end))
                .expect("a row count fits"),
        ))
        .retain_finished(Some(
            usize::try_from(rng.range(RETAIN.start, RETAIN.end)).expect("a turn count fits"),
        ))
        .row_limit(Some(
            usize::try_from(rng.range(ROW_BYTES.start, ROW_BYTES.end)).expect("a byte count fits"),
        ));
    let mut script = Script {
        prefix: prefix.to_owned(),
        ids: Vec::new(),
        phases: BTreeMap::new(),
        calls: Vec::new(),
        holds: 0,
    };
    for _ in 0..rng.range(STEPS.start, STEPS.end) {
        let op = op(rng, &mut script);
        log.write(move |writing| {
            Box::pin(async move {
                let point = turn_op(writing, op)?;
                writing.commit(point).await?;
                writing.retire_finished();
                Ok::<_, StoreError>(())
            })
        })
        .await
        .map_err(failed("write a legacy turn log"))?;
    }
    Ok(Legacy::named(script.ids))
}

type Facts = Vec<(u64, Option<String>, StoredFact)>;

fn blobs_of(facts: &Facts) -> Vec<nanocode_durability_core::BlobRef> {
    let mut refs = BTreeMap::new();
    for (_, _, fact) in facts {
        let seen: Result<_, std::convert::Infallible> = fact.try_map(
            |value| {
                if let Value::Blob(blob) = value {
                    refs.insert(blob.index(), *blob);
                }
                Ok(())
            },
            |_| Ok(()),
        );
        let Ok(_) = seen;
    }
    refs.into_values().collect()
}

fn retired_line(digest: &str, first: u64, phase: &Phase, hand: Option<&str>) -> String {
    match phase {
        Phase::Finished {
            last,
            outcome,
            result,
        } => format!(
            "retired {digest} first={first} last={last} outcome={outcome:?} result={} hand={hand:?}",
            result.get()
        ),
        Phase::Queued { .. } | Phase::Running { .. } => format!("retired {digest} unfinished"),
    }
}

fn turn_line(turn: &Turn) -> String {
    match turn.digest.strip_prefix(RETIRED) {
        Some(digest) => format!(
            "{} {}",
            turn.id,
            retired_line(digest, turn.first, &turn.phase, turn.hand.as_deref())
        ),
        None => line(turn),
    }
}

struct Theirs {
    turns: Vec<String>,
    cells: Vec<String>,
    holds: Vec<String>,
    calls: Vec<String>,
    checkpoints: Vec<String>,
    stored: Vec<String>,
    hosts: Vec<String>,
    wakes: Vec<String>,
    digests: Vec<(u32, Option<Digest>)>,
}

fn theirs(
    fold: &Fold,
    legacy: &Legacy,
    events: &[Event],
    blobs: &[nanocode_durability_core::BlobRef],
) -> Theirs {
    let turns = legacy
        .ids
        .iter()
        .filter_map(|id| match fold.turn(id) {
            Some(turn) => Some(turn_line(turn)),
            None => fold.retired(id).map(|retired| {
                let result = events
                    .iter()
                    .find(|event| event.seq == retired.last)
                    .map_or_else(|| RawValue::NULL.to_owned(), |event| event.event.clone());
                let phase = Phase::Finished {
                    last: retired.last,
                    outcome: retired.outcome,
                    result,
                };
                format!(
                    "{id} {}",
                    retired_line(
                        &format!("{:032x}", retired.digest),
                        retired.first,
                        &phase,
                        retired.hand.as_deref()
                    )
                )
            }),
        })
        .collect();
    let mut wakes: Vec<String> = fold.wakes().iter().map(|wake| line(&wake)).collect();
    wakes.push(format!("generation {}", fold.wakes().generation()));
    Theirs {
        turns,
        cells: fold
            .cells()
            .map(|(session, cell, updates)| line(&(session, cell, updates)))
            .collect(),
        holds: fold.holds().map(|hold| line(&hold)).collect(),
        calls: fold.open_calls().map(|call| line(&call)).collect(),
        checkpoints: STATES
            .iter()
            .map(|state| line(&fold.checkpoint(state)))
            .collect(),
        stored: legacy
            .stored
            .iter()
            .map(|key| line(&fold.stored(key)))
            .collect(),
        hosts: legacy
            .hosts
            .iter()
            .map(|key| line(&fold.host(key)))
            .collect(),
        wakes,
        digests: blobs
            .iter()
            .map(|blob| (blob.index(), fold.values().digest(*blob).copied()))
            .collect(),
    }
}

fn inline(fact: &StoredFact, values: &BTreeMap<u32, Box<RawValue>>) -> Result<StoredFact, String> {
    fact.try_map(
        |value| match value {
            Value::Inline(json) => Ok(Value::Inline(json.clone())),
            Value::Blob(blob) => values
                .get(&blob.index())
                .map(|json| Value::Inline(json.clone()))
                .ok_or_else(|| format!("the legacy log misses blob {}", blob.index())),
        },
        |decision| Ok(decision.clone()),
    )
}

pub async fn expected(
    host: Arc<dyn LogHost>,
    thread: &str,
    legacy: &Legacy,
) -> Result<View, String> {
    let log = nanocode_turnlog::sim::open(host, thread)
        .await
        .map_err(failed("reopen a legacy turn log"))?;
    let events = log
        .history(0, Vec::new(), |events: &mut Vec<Event>, event| {
            events.push(event.clone());
            true
        })
        .await
        .map_err(failed("read a legacy history"))?;
    let facts = log
        .facts_from(0, Vec::new(), |facts: &mut Facts, seq, state, fact| {
            facts.push((seq, state.map(str::to_owned), fact.clone()));
        })
        .await
        .map_err(failed("read legacy facts"))?;
    let blobs = blobs_of(&facts);
    let blob_count = blobs.len() as u64;
    let probe = Legacy {
        next: legacy.next,
        ids: legacy.ids.clone(),
        owners: legacy.owners.clone(),
        stored: legacy.stored.clone(),
        hosts: legacy.hosts.clone(),
    };
    let seen = events.clone();
    let theirs = log
        .read_owners(&legacy.owners, move |fold| {
            theirs(fold, &probe, &seen, &blobs)
        })
        .await
        .map_err(failed("read legacy owners"))?;
    let mut digests = Vec::new();
    for (blob, digest) in &theirs.digests {
        digests.push(digest.ok_or_else(|| format!("legacy blob {blob} has no digest"))?);
    }
    let found = log
        .blobs(&digests)
        .await
        .map_err(failed("read legacy blobs"))?;
    let mut values = BTreeMap::new();
    for ((blob, _), digest) in theirs.digests.iter().zip(&digests) {
        if let Some(json) = found.get(digest) {
            values.insert(*blob, json.clone());
        }
    }
    let mut lines = Vec::with_capacity(facts.len());
    for (seq, state, fact) in &facts {
        lines.push(line(&(seq, state, inline(fact, &values)?)));
    }
    Ok(View {
        retired: theirs
            .turns
            .iter()
            .filter(|turn| turn.contains(" retired "))
            .count() as u64,
        blobs: blob_count,
        turns: theirs.turns,
        events: events.iter().map(line).collect(),
        facts: lines,
        cells: theirs.cells,
        holds: theirs.holds,
        calls: theirs.calls,
        checkpoints: theirs.checkpoints,
        stored: theirs.stored,
        hosts: theirs.hosts,
        wakes: theirs.wakes,
    })
}

pub fn ours(
    state: &ThreadState,
    legacy: &Legacy,
    events: &[Event],
    facts: &[(u64, Option<String>, StoredFact)],
) -> View {
    let mut wakes: Vec<String> = state.wakes().map(|wake| line(&wake)).collect();
    wakes.push(format!("generation {}", state.generation()));
    View {
        retired: 0,
        blobs: 0,
        turns: legacy
            .ids
            .iter()
            .filter_map(|id| state.turn(id).map(turn_line))
            .collect(),
        events: events.iter().map(line).collect(),
        facts: facts
            .iter()
            .map(|(seq, held, fact)| line(&(seq, held, fact)))
            .collect(),
        cells: state
            .cells()
            .map(|(session, cell, updates)| {
                let updates: Vec<&RawValue> = updates.iter().map(AsRef::as_ref).collect();
                line(&(session, cell, updates))
            })
            .collect(),
        holds: state.holds().map(|hold| line(&hold)).collect(),
        calls: state.open_calls().map(|call| line(&call)).collect(),
        checkpoints: STATES
            .iter()
            .map(|at| line(&state.checkpoint(at)))
            .collect(),
        stored: legacy
            .stored
            .iter()
            .map(|key| line(&state.stored(key)))
            .collect(),
        hosts: legacy
            .hosts
            .iter()
            .map(|key| line(&state.host(key)))
            .collect(),
        wakes,
    }
}

pub fn differs(theirs: &View, ours: &View) -> Option<String> {
    let fields: [(&str, &Vec<String>, &Vec<String>); 10] = [
        ("turns", &theirs.turns, &ours.turns),
        ("events", &theirs.events, &ours.events),
        ("facts", &theirs.facts, &ours.facts),
        ("cells", &theirs.cells, &ours.cells),
        ("holds", &theirs.holds, &ours.holds),
        ("open calls", &theirs.calls, &ours.calls),
        ("checkpoints", &theirs.checkpoints, &ours.checkpoints),
        ("stored values", &theirs.stored, &ours.stored),
        ("host fields", &theirs.hosts, &ours.hosts),
        ("wakes", &theirs.wakes, &ours.wakes),
    ];
    fields
        .iter()
        .find(|(_, theirs, ours)| theirs != ours)
        .map(|(name, theirs, ours)| {
            let at = theirs
                .iter()
                .zip(ours.iter())
                .position(|(a, b)| a != b)
                .unwrap_or(theirs.len().min(ours.len()));
            format!(
                "{name} differ at {at} ({} theirs, {} ours): theirs {:?}, ours {:?}",
                theirs.len(),
                ours.len(),
                theirs
                    .get(at)
                    .map(|text| text.chars().take(300).collect::<String>()),
                ours.get(at)
                    .map(|text| text.chars().take(300).collect::<String>())
            )
        })
}

impl Legacy {
    pub fn named(ids: Vec<String>) -> Self {
        let mut owners: Vec<String> = SESSIONS
            .iter()
            .chain(&STATES)
            .map(|owner| (*owner).to_owned())
            .collect();
        owners.extend((KEYS..2 * KEYS).map(stored_key));
        Self {
            next: 0,
            ids,
            owners,
            stored: (0..2 * KEYS).map(stored_key).collect(),
            hosts: (0..HOSTS).map(|at| format!("h{at}")).collect(),
        }
    }

    pub fn ids(&self) -> &[String] {
        &self.ids
    }

    pub fn at(mut self, next: u64) -> Self {
        self.next = next;
        self
    }

    pub fn turns(&self) -> usize {
        self.ids.len()
    }
}
