use std::sync::Arc;

use nanocode_turnlog::{Event, LogHost, Record};

use super::{
    adopt,
    state::{Probe, Probing, ThreadState},
};
use crate::durability::{StoreError, facts::StoredFact};

pub type HeldFact = (u64, Option<String>, StoredFact);

pub struct Paging {
    turn: Option<String>,
    limit: usize,
    bytes_max: Option<usize>,
    bytes: usize,
    pub events: Vec<Event>,
    pub more: bool,
}

impl Paging {
    #[must_use]
    pub const fn new(turn: Option<String>, limit: usize, bytes_max: Option<usize>) -> Self {
        Self {
            turn,
            limit,
            bytes_max,
            bytes: 0,
            events: Vec::new(),
            more: false,
        }
    }

    pub fn take(&mut self, event: &Event) -> bool {
        if self
            .turn
            .as_deref()
            .is_some_and(|id| event.turn.as_deref() != Some(id))
        {
            return true;
        }
        let size = event.event.get().len();
        if self.events.len() == self.limit
            || self
                .bytes_max
                .is_some_and(|max| !self.events.is_empty() && self.bytes + size > max)
        {
            self.more = true;
            return false;
        }
        self.bytes += size;
        self.events.push(event.clone());
        true
    }
}

pub async fn events(
    host: &Arc<dyn LogHost>,
    scope: &str,
    state: &ThreadState,
    after: u64,
    mut paging: Paging,
) -> Result<Paging, StoreError> {
    if after >= state.last_event() {
        return Ok(paging);
    }
    let first = state
        .recent_events()
        .next()
        .map(|kept| (kept.event.seq, kept.row));
    let before = first.map_or(state.last_event() + 1, |(seq, _)| seq);
    if after + 1 < before {
        if let Some(legacy) = state.legacy()
            && after < legacy.last_event
        {
            paging = adopt::legacy_events(host, scope, legacy, after, paging).await?;
            if paging.more {
                return Ok(paging);
            }
        }
        let from = state.event_row(after);
        let to = first.map_or(state.row() + 1, |(_, row)| row);
        let rows = if from < to {
            host.scan(scope, from, to).await?
        } else {
            Vec::new()
        };
        for (row, body) in rows {
            for record in super::state::records(row, &body)? {
                if let Record::Event {
                    at,
                    event,
                    seq,
                    turn,
                } = record
                    && seq > after
                    && seq < before
                    && !paging.take(&Event {
                        at,
                        event,
                        seq,
                        turn,
                    })
                {
                    return Ok(paging);
                }
            }
        }
    }
    for kept in state.recent_events() {
        if kept.event.seq > after && !paging.take(&kept.event) {
            break;
        }
    }
    Ok(paging)
}

pub async fn probe(
    host: &Arc<dyn LogHost>,
    scope: &str,
    probing: &Probing,
) -> Result<Probe, StoreError> {
    let rows = if probing.from < probing.to {
        host.scan(scope, probing.from, probing.to).await?
    } else {
        Vec::new()
    };
    let mut traced = ThreadState::default();
    if let Some(legacy) = probing.legacy
        && let Some(turn) = adopt::legacy_turn(host, scope, legacy, &probing.request_id).await?
    {
        traced.seeded(turn);
    }
    for (seq, body) in rows {
        for record in super::state::records(seq, &body)? {
            traced.traced(&record, seq, &probing.request_id)?;
        }
    }
    Ok(Probe {
        through: probing.to,
        turn: traced.turns.get(probing.request_id.as_str()).cloned(),
    })
}

pub async fn facts(
    host: &Arc<dyn LogHost>,
    scope: &str,
    state: &ThreadState,
    after: u64,
    from: Option<u64>,
) -> Result<Vec<HeldFact>, StoreError> {
    let base = state.fact_base();
    let mut facts = Vec::new();
    if after < base {
        let legacy = state.legacy();
        if let Some(legacy) = legacy
            && after < legacy.last_fact
        {
            facts = adopt::legacy_facts(host, scope, legacy, after).await?;
        }
        let floor = legacy.map_or(0, |legacy| legacy.next);
        let start = from.map_or(floor, |from| from.max(floor));
        let to = state
            .recent_facts()
            .next()
            .map_or(state.row() + 1, |kept| kept.row);
        let skipped = legacy.map_or(after, |legacy| after.max(legacy.last_fact));
        let rows = if start < to {
            host.scan(scope, start, to).await?
        } else {
            Vec::new()
        };
        for (row, body) in rows {
            for record in super::state::records(row, &body)? {
                if let Record::Fact { fact, seq, state } = record
                    && seq > skipped
                    && seq <= base
                {
                    facts.push((seq, state, fact));
                }
            }
        }
        if from.is_none() {
            contiguous(&facts, after, base)?;
        }
    }
    facts.extend(
        state
            .facts_after(after)
            .map(|(seq, kept)| (seq, kept.state.clone(), kept.fact.clone())),
    );
    Ok(facts)
}

fn contiguous(facts: &[HeldFact], after: u64, base: u64) -> Result<(), StoreError> {
    for ((seq, _, _), expected) in facts.iter().zip(after + 1..) {
        if *seq != expected {
            return Err(StoreError::Backend(format!(
                "the thread's history answers fact {seq} where fact {expected} belongs"
            )));
        }
    }
    let read = facts.len() as u64;
    if read != base - after {
        return Err(StoreError::Backend(format!(
            "the thread's history answers {read} facts after fact {after}, not the {} up to fact {base}",
            base - after
        )));
    }
    Ok(())
}
