use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use nanocode_scope::Adopted;
use nanocode_turnlog::{
    ArmedWake, CachedAt, Checkpoint, Event, FOLD_SNAPSHOT_KEY, Fold, Hold, LogHost, OpenCall,
    Phase, RETIRED_KEY, Retired, Turn,
    adopt::{self, Adoption, Reading},
};
use serde_json::value::RawValue;

use super::{
    history::{HeldFact, Paging},
    sim::{self, Mutation},
    state::{FinishedFilter, Legacy, ThreadState, Window, invalid},
};
use crate::durability::{
    StoreError,
    facts::{BlobRef, Digest, StoredFact, Value},
};

pub(crate) const RETIRED_DIGEST: &str = "retired:";

fn plain(rows: &[(u64, String)]) -> bool {
    let mut state = ThreadState::default();
    let window = Window {
        recent_rows: rows.len().max(1),
        filter: FinishedFilter {
            bytes: 1,
            hashes: 1,
        },
    };
    rows.iter().all(|(seq, row)| state.load(*seq, row, window).is_ok())
}

type Facts = Vec<(u64, Option<String>, StoredFact)>;

struct View {
    turns: Vec<Turn>,
    retired: Vec<(String, Retired)>,
    retired_count: usize,
    cells: Vec<(String, u64, Vec<Box<RawValue>>)>,
    checkpoints: Vec<(String, Checkpoint)>,
    stored: Vec<(String, String)>,
    hosts: Vec<(String, Box<RawValue>)>,
    holds: Vec<(String, Hold)>,
    calls: Vec<(String, OpenCall)>,
    models: Vec<(String, Vec<Box<RawValue>>)>,
    wakes: Vec<(String, ArmedWake)>,
    generation: u64,
}

fn owned<'a, V: Clone + 'a>(values: impl Iterator<Item = (&'a str, &'a V)>) -> Vec<(String, V)> {
    values
        .map(|(key, value)| (key.to_owned(), value.clone()))
        .collect()
}

impl View {
    fn of(fold: &Fold, ids: &BTreeSet<String>) -> Self {
        let retired = ids
            .iter()
            .filter(|id| fold.turn(id).is_none())
            .filter_map(|id| {
                fold.retired(id)
                    .map(|retired| (id.clone(), retired.clone()))
            })
            .collect();
        Self {
            turns: fold.turns().to_vec(),
            retired,
            retired_count: fold.retired_turns(),
            cells: fold
                .cells()
                .map(|(session, cell, updates)| (session.to_owned(), cell, updates.to_vec()))
                .collect(),
            checkpoints: owned(fold.checkpoints()),
            stored: fold
                .stored_all()
                .map(|(key, value)| (key.to_owned(), value.to_owned()))
                .collect(),
            hosts: fold
                .hosts()
                .filter(|(key, _)| *key != RETIRED_KEY)
                .map(|(key, value)| (key.to_owned(), value.to_owned()))
                .collect(),
            holds: owned(fold.holds()),
            calls: owned(fold.open_calls()),
            models: fold
                .models_all()
                .map(|(turn, responses)| (turn.to_owned(), responses.to_vec()))
                .collect(),
            wakes: owned(fold.wakes().iter()),
            generation: fold.wakes().generation(),
        }
    }
}

fn blob_refs(facts: &Facts) -> Vec<BlobRef> {
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

fn inlined(
    seq: u64,
    fact: &StoredFact,
    values: &BTreeMap<u32, Box<RawValue>>,
) -> Result<StoredFact, StoreError> {
    if sim::mutated(Mutation::AdoptionSkipsBlobs) {
        return Ok(fact.clone());
    }
    fact.try_map(
        |value| match value {
            Value::Inline(json) => Ok(Value::Inline(json.clone())),
            Value::Blob(blob) => values
                .get(&blob.index())
                .map(|json| Value::Inline(json.clone()))
                .ok_or_else(|| {
                    invalid(format!(
                        "fact {seq} names blob {} that the turn log does not hold",
                        blob.index()
                    ))
                }),
        },
        |decision| Ok(decision.clone()),
    )
}

fn retired_turn(id: String, retired: &Retired, events: &[Event]) -> Result<Turn, StoreError> {
    let result = events
        .binary_search_by_key(&retired.last, |event| event.seq)
        .map(|at| events[at].event.clone())
        .map_err(|_| {
            invalid(format!(
                "retired turn {id} ends at event {}, which the turn log does not hold",
                retired.last
            ))
        })?;
    Ok(Turn {
        digest: format!("{RETIRED_DIGEST}{:032x}", retired.digest),
        edits: BTreeMap::new(),
        first: retired.first,
        hand: retired.hand.as_deref().map(str::to_owned),
        id,
        metadata: RawValue::NULL.to_owned(),
        phase: Phase::Finished {
            last: retired.last,
            outcome: retired.outcome,
            result,
        },
        prompt: RawValue::NULL.to_owned(),
        recovered: None,
    })
}

fn turns(state: &mut ThreadState, view: &mut View, events: &[Event]) -> Result<(), StoreError> {
    if view.retired.len() != view.retired_count {
        return Err(invalid(format!(
            "the turn log retired {} turns and its events name {} of them",
            view.retired_count,
            view.retired.len()
        )));
    }
    let mut turns = std::mem::take(&mut view.turns);
    if !sim::mutated(Mutation::AdoptionSkipsRetired) {
        for (id, retired) in std::mem::take(&mut view.retired) {
            turns.push(retired_turn(id, &retired, events)?);
        }
    }
    for turn in turns {
        let id: Arc<str> = Arc::from(turn.id.as_str());
        if state.admitted.insert(turn.first, Arc::clone(&id)).is_some() {
            return Err(invalid(format!(
                "turn {id} starts at event {}, where another turn starts",
                turn.first
            )));
        }
        if state.turns.insert(id, Arc::new(turn)).is_some() {
            return Err(invalid("the turn log holds one turn twice"));
        }
    }
    Ok(())
}

fn events(events: &[Event], last_event: u64) -> Result<(), StoreError> {
    for pair in events.windows(2) {
        if pair[1].seq <= pair[0].seq {
            return Err(invalid(format!(
                "the turn log answers event {} after event {}",
                pair[1].seq, pair[0].seq
            )));
        }
    }
    if let Some(last) = events.last()
        && last.seq > last_event
    {
        return Err(invalid(format!(
            "the turn log answers event {} past its last event {last_event}",
            last.seq
        )));
    }
    Ok(())
}

fn facts(state: &mut ThreadState, facts: &Facts) -> Result<(), StoreError> {
    for (seq, owner, fact) in facts {
        if *seq != state.next_fact() {
            return Err(invalid(format!(
                "the turn log answers fact {seq} after fact {}",
                state.next_fact() - 1
            )));
        }
        state.index_blob(fact);
        if let Some(owner) = owner {
            state.logged(owner, *seq);
        }
        state.fact_base = *seq;
    }
    Ok(())
}

fn keep(state: &mut ThreadState, view: View) {
    for (session, cell, updates) in view.cells {
        state.cells.insert(
            (Arc::from(session.as_str()), cell),
            updates.into_iter().collect(),
        );
    }
    for (key, checkpoint) in view.checkpoints {
        state
            .checkpoints
            .insert(Arc::from(key.as_str()), Arc::new(checkpoint));
    }
    for (key, value) in view.stored {
        state
            .stored
            .insert(Arc::from(key.as_str()), Arc::from(value.as_str()));
    }
    for (key, value) in view.hosts {
        state.hosts.insert(Arc::from(key.as_str()), value);
    }
    for (key, hold) in view.holds {
        state.holds.insert(Arc::from(key.as_str()), Arc::new(hold));
    }
    for (id, call) in view.calls {
        state.calls.insert(Arc::from(id.as_str()), Arc::new(call));
    }
    for (turn, responses) in view.models {
        state
            .models
            .insert(Arc::from(turn.as_str()), responses.into_iter().collect());
    }
    for (key, wake) in view.wakes {
        state.wakes.insert(Arc::from(key.as_str()), Arc::new(wake));
    }
    state.generation = view.generation;
}

fn cached_blobs(view: &View) -> BTreeSet<Digest> {
    view.hosts
        .iter()
        .filter(|(key, _)| {
            key.strip_prefix(FOLD_SNAPSHOT_KEY)
                .is_some_and(|rest| rest.starts_with('\n'))
        })
        .filter_map(|(_, value)| serde_json::from_str::<CachedAt>(value.get()).ok())
        .map(|at| at.blob)
        .collect()
}

async fn bodies(
    legacy: &Adoption,
    state: &mut ThreadState,
    cached: &BTreeSet<Digest>,
) -> Result<(), StoreError> {
    let named: BTreeSet<Digest> = state.blobs.iter().chain(cached).copied().collect();
    let wanted: Vec<Digest> = named
        .into_iter()
        .filter(|digest| !(cached.contains(digest) && sim::mutated(Mutation::ResidentDropsBlob)))
        .filter(|digest| state.blob_body(digest).is_none())
        .collect();
    if wanted.is_empty() {
        return Ok(());
    }
    let resolved = legacy.blobs(&wanted).await?;
    for digest in &wanted {
        let Some(json) = resolved.get(digest) else {
            return Err(invalid(format!(
                "blob {} is missing from the turn log",
                digest.key().as_str()
            )));
        };
        state
            .stored
            .insert(Arc::from(digest.key().as_str()), Arc::from(json.get()));
    }
    assert!(
        wanted.iter().all(|digest| state.blob_body(digest).is_some()),
        "an adopted thread holds the body of every blob it resolved"
    );
    Ok(())
}

async fn values(
    legacy: &Adoption,
    digests: &[(BlobRef, Option<Digest>)],
) -> Result<BTreeMap<u32, Box<RawValue>>, StoreError> {
    let mut wanted = Vec::with_capacity(digests.len());
    for (blob, digest) in digests {
        let Some(digest) = digest else {
            return Err(invalid(format!(
                "blob {} has no digest in the turn log",
                blob.index()
            )));
        };
        wanted.push(*digest);
    }
    let resolved = legacy.blobs(&wanted).await?;
    let mut values = BTreeMap::new();
    for ((blob, _), digest) in digests.iter().zip(&wanted) {
        let Some(json) = resolved.get(digest) else {
            return Err(invalid(format!(
                "blob {} is missing from the turn log",
                blob.index()
            )));
        };
        values.insert(blob.index(), json.clone());
    }
    Ok(values)
}

async fn opened(
    log: &Arc<dyn LogHost>,
    scope: &str,
    legacy: Legacy,
) -> Result<Adoption, StoreError> {
    let opened = adopt::read_all(log, scope, Reading::Below(legacy.next)).await?;
    let next = opened.next();
    if next != legacy.next {
        return Err(invalid(format!(
            "the turn log history of {scope} ends at row {next}, not at row {} where the thread adopted it",
            legacy.next
        )));
    }
    Ok(opened)
}

pub(super) async fn legacy_events(
    log: &Arc<dyn LogHost>,
    scope: &str,
    legacy: Legacy,
    after: u64,
    paging: Paging,
) -> Result<Paging, StoreError> {
    let last = legacy.last_event;
    opened(log, scope, legacy)
        .await?
        .history(after, paging, move |paging: &mut Paging, event| {
            event.seq <= last && paging.take(event)
        })
        .await
}

pub(super) async fn legacy_facts(
    log: &Arc<dyn LogHost>,
    scope: &str,
    legacy: Legacy,
    after: u64,
) -> Result<Vec<HeldFact>, StoreError> {
    let opened = opened(log, scope, legacy).await?;
    let last = legacy.last_fact;
    let read = opened
        .facts_from(after, Vec::new(), move |facts: &mut Facts, seq, owner, fact| {
            if seq > after && seq <= last {
                facts.push((seq, owner.map(str::to_owned), fact.clone()));
            }
        })
        .await?;
    let refs = blob_refs(&read);
    let digests = opened
        .read(move |fold| {
            refs.iter()
                .map(|blob| (*blob, fold.values().digest(*blob).copied()))
                .collect::<Vec<_>>()
        })
        .await;
    let values = values(&opened, &digests).await?;
    read.into_iter()
        .map(|(seq, owner, fact)| Ok((seq, owner, inlined(seq, &fact, &values)?)))
        .collect()
}

pub(super) async fn adopt(
    log: &Arc<dyn LogHost>,
    scope: &str,
) -> Result<Option<Adopted<ThreadState>>, StoreError> {
    let loaded = log.load(scope).await?;
    if loaded.is_empty() || plain(&loaded) {
        return Ok(None);
    }
    let legacy = adopt::read_all(log, scope, Reading::Loaded(loaded)).await?;
    let next = legacy.next();
    let history = legacy
        .history(0, Vec::new(), |events: &mut Vec<Event>, event| {
            events.push(event.clone());
            true
        })
        .await?;
    let read = legacy
        .facts_from(0, Vec::new(), |facts: &mut Facts, seq, owner, fact| {
            facts.push((seq, owner.map(str::to_owned), fact.clone()));
        })
        .await?;
    let ids: BTreeSet<String> = history
        .iter()
        .filter_map(|event| event.turn.clone())
        .collect();
    let (mut view, last_event) = legacy
        .read(move |fold| (View::of(fold, &ids), fold.last_event()))
        .await;
    let mut state = ThreadState::default();
    turns(&mut state, &mut view, &history)?;
    events(&history, last_event)?;
    facts(&mut state, &read)?;
    let cached = cached_blobs(&view);
    keep(&mut state, view);
    bodies(&legacy, &mut state, &cached).await?;
    state.last_event = last_event;
    state.legacy = Some(Legacy {
        next,
        last_event,
        last_fact: state.fact_base,
    });
    let usage = legacy.usage();
    Ok(Some(Adopted {
        state,
        next,
        rows: usage.rows,
        bytes: usage.bytes,
    }))
}
