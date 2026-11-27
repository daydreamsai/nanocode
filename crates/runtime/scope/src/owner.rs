use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
};

use nanocode_durability_core::{SqlStatement, StoreError};
use nanocode_partitions::Partition;
use nanocode_sync::{Atom, Slot, Vector, oneshot, watch};
use nanocode_turnlog::{Compaction, Format, Landed, Rider, Stamps, Write};

use crate::{
    Carry, Committed, Flushed, Resident, ScopeError,
    checkpoint::{Fence, Listing, Manifest, Written, digest},
    meter::Tally,
    scopes::Inner,
    sim::{self, Mutation},
};

pub(crate) enum Ask<F: Resident> {
    Send(F::Command),
    Stage(F::Command),
    Flush(Option<Carry>),
    #[cfg_attr(target_family = "wasm", allow(dead_code))]
    Read,
    Checkpoint,
    Drain(Why),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Why {
    Release,
    Evict,
}

pub(crate) enum Turn<F: Resident> {
    Committed(Result<Committed<F::Reply>, ScopeError>),
    Moved(Option<F::Command>),
    #[cfg_attr(target_family = "wasm", allow(dead_code))]
    Read(Arc<F::State>),
    Flushed(Result<Flushed, ScopeError>),
    Checkpointed(Result<(), ScopeError>),
    Drained(Result<u64, ScopeError>),
}

struct Job<F: Resident> {
    ask: Ask<F>,
    reply: oneshot::Sender<Turn<F>>,
}

struct Decided<F: Resident> {
    at: u64,
    bodies: Vec<String>,
    answer: F::Reply,
}

struct Pending<F: Resident> {
    command: F::Command,
    decided: Option<Decided<F>>,
    durable: bool,
    reply: oneshot::Sender<Turn<F>>,
}

struct Batch<F: Resident> {
    pending: Vec<Pending<F>>,
    flushes: Vec<oneshot::Sender<Turn<F>>>,
    carrying: Option<(Carry, oneshot::Sender<Turn<F>>)>,
    force: bool,
}

impl<F: Resident> Batch<F> {
    const fn new() -> Self {
        Self {
            pending: Vec::new(),
            flushes: Vec::new(),
            carrying: None,
            force: false,
        }
    }

    fn is_empty(&self) -> bool {
        self.pending.is_empty() && self.flushes.is_empty() && self.carrying.is_none() && !self.force
    }

    fn forces(&self) -> bool {
        self.force
            || self.carrying.is_some()
            || (!self.flushes.is_empty() && !sim::mutated(Mutation::FlushSkipsStaged))
    }

    fn flushed(self, flushed: &Result<Flushed, ScopeError>, carried: bool) {
        for reply in self.flushes {
            drop(reply.send(Turn::Flushed(flushed.clone())));
        }
        if let Some((_, reply)) = self.carrying {
            let flushed = flushed.clone().map(|flushed| Flushed { carried, ..flushed });
            drop(reply.send(Turn::Flushed(flushed)));
        }
    }

    fn fail(mut self, error: &ScopeError) {
        for pending in std::mem::take(&mut self.pending) {
            pending.answer(Err(error.clone()));
        }
        self.flushed(&Err(error.clone()), false);
    }
}

struct Round<F: Resident> {
    working: F::State,
    next: u64,
    rows: Vec<(u64, String)>,
    answers: Vec<(oneshot::Sender<Turn<F>>, u64, F::Reply)>,
    rest: Vec<Pending<F>>,
    durable: bool,
}

struct Sending<'a> {
    rows: &'a [(u64, String)],
    stamps: &'a Stamps,
    op: u64,
    stamped: bool,
}

pub(crate) struct Staging<F: Resident> {
    state: F::State,
    next: u64,
    rows: Vec<(u64, String)>,
}

impl<F: Resident> Pending<F> {
    fn moved(self) {
        drop(self.reply.send(Turn::Moved(Some(self.command))));
    }

    fn answer(self, answer: Result<Committed<F::Reply>, ScopeError>) {
        drop(self.reply.send(Turn::Committed(answer)));
    }
}

struct Queued<F: Resident>(Arc<Slot<Job<F>>>);

impl<F: Resident> Clone for Queued<F> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<F: Resident> Queued<F> {
    fn take(&self) -> Job<F> {
        let Some(job) = self.0.take() else {
            panic!("a queued scope job leaves its queue once");
        };
        job
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Life {
    Serving,
    Draining,
    Released { seq: u64 },
    Lost,
}

struct Queue<F: Resident> {
    life: Life,
    leading: bool,
    waiting: Vector<Queued<F>>,
}

impl<F: Resident> Clone for Queue<F> {
    fn clone(&self) -> Self {
        Self {
            life: self.life,
            leading: self.leading,
            waiting: self.waiting.clone(),
        }
    }
}

struct Published<F: Resident> {
    state: Arc<F::State>,
    next: u64,
}

impl<F: Resident> Clone for Published<F> {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
            next: self.next,
        }
    }
}

pub(crate) struct Checkpointed {
    pub(crate) next: u64,
    pub(crate) at_ms: u64,
    pub(crate) parts: BTreeMap<String, String>,
    pub(crate) chain: Option<Chain>,
}

#[derive(Clone, Copy)]
pub(crate) struct Chain {
    pub(crate) base: u64,
    pub(crate) whole_bytes: usize,
    pub(crate) changed_bytes: usize,
}

impl Checkpointed {
    pub(crate) fn empty(at_ms: u64) -> Self {
        Self {
            next: 0,
            at_ms,
            parts: BTreeMap::new(),
            chain: None,
        }
    }

    fn listing(&self, named: &[(String, String)], next: u64) -> (Listing, Chain) {
        let whole = Listing::Whole(named.to_vec());
        let whole_bytes = whole.encoded().len();
        let fresh = Chain {
            base: next,
            whole_bytes,
            changed_bytes: 0,
        };
        let Some(chain) = self.chain else {
            return (whole, fresh);
        };
        let put: Vec<(String, String)> = named
            .iter()
            .filter(|(name, at)| self.parts.get(name) != Some(at))
            .cloned()
            .collect();
        let kept: BTreeSet<&str> = named.iter().map(|(name, _)| name.as_str()).collect();
        let dropped: Vec<String> = if sim::mutated(Mutation::ListingSkipsDropped) {
            Vec::new()
        } else {
            self.parts
                .keys()
                .filter(|name| !kept.contains(name.as_str()))
                .cloned()
                .collect()
        };
        let changed = Listing::Changed {
            base: chain.base,
            put,
            dropped,
        };
        let changed_bytes = chain.changed_bytes + changed.encoded().len();
        let bounded = changed_bytes <= chain.whole_bytes
            || sim::mutated(Mutation::ListingChainUnbounded);
        if !bounded {
            return (whole, fresh);
        }
        (
            changed,
            Chain {
                base: next,
                whole_bytes: chain.whole_bytes,
                changed_bytes,
            },
        )
    }
}

pub(crate) struct Core<F: Resident> {
    pub(crate) state: Arc<F::State>,
    pub(crate) next: u64,
    pub(crate) since: usize,
    pub(crate) checkpointed: Checkpointed,
    pub(crate) staged: Option<Staging<F>>,
    pub(crate) stamps: Stamps,
}

pub(crate) struct Placed {
    pub(crate) part: Partition,
    pub(crate) epoch: u64,
    pub(crate) tally: Arc<Tally>,
}

pub(crate) struct Owner<F: Resident> {
    pub(crate) scope: Arc<str>,
    pub(crate) part: Partition,
    pub(crate) epoch: u64,
    published: Atom<Published<F>>,
    watch: watch::Sender<Arc<F::State>>,
    queue: Atom<Queue<F>>,
    core: Slot<Core<F>>,
    pub(crate) used: AtomicU64,
    pub(crate) tally: Arc<Tally>,
    weight: AtomicUsize,
    buffered: AtomicUsize,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Send,
    Flush,
    Read,
    Checkpoint,
    Drain,
}

const fn kind<F: Resident>(ask: &Ask<F>) -> Kind {
    match ask {
        Ask::Send(_) | Ask::Stage(_) => Kind::Send,
        Ask::Flush(_) => Kind::Flush,
        Ask::Read => Kind::Read,
        Ask::Checkpoint => Kind::Checkpoint,
        Ask::Drain(_) => Kind::Drain,
    }
}

const fn admits(life: Life, kind: Kind) -> bool {
    match life {
        Life::Serving => true,
        Life::Draining => matches!(kind, Kind::Read | Kind::Flush),
        Life::Released { .. } | Life::Lost => false,
    }
}

struct Leading<'a, F: Resident> {
    owner: &'a Owner<F>,
    core: Option<Core<F>>,
    in_flight: bool,
    done: bool,
}

impl<F: Resident> Drop for Leading<'_, F> {
    fn drop(&mut self) {
        if self.done {
            return;
        }
        if let Some(core) = self.core.take() {
            self.owner.core.put(core);
        }
        let in_flight = self.in_flight;
        let waiting = self.owner.queue.update(|queue| {
            if in_flight {
                queue.life = Life::Lost;
            }
            queue.leading = false;
            std::mem::take(&mut queue.waiting)
        });
        for queued in &waiting {
            let job = queued.take();
            drop(job.reply.send(self.owner.refused(job.ask, in_flight)));
        }
    }
}

impl<F: Resident> Owner<F> {
    pub(crate) fn new(
        scope: Arc<str>,
        placed: Placed,
        core: Core<F>,
        weight: usize,
        used: u64,
    ) -> Self {
        let Placed { part, epoch, tally } = placed;
        let published = Published {
            state: Arc::clone(&core.state),
            next: core.next,
        };
        Self {
            scope,
            part,
            epoch,
            watch: watch::Sender::new(Arc::clone(&core.state)),
            published: Atom::new(published),
            queue: Atom::new(Queue {
                life: Life::Serving,
                leading: false,
                waiting: Vector::new(),
            }),
            core: Slot::new(Some(core)),
            used: AtomicU64::new(used),
            tally,
            weight: AtomicUsize::new(weight),
            buffered: AtomicUsize::new(0),
        }
    }

    fn refused(&self, ask: Ask<F>, landing: bool) -> Turn<F> {
        match ask {
            Ask::Send(command) | Ask::Stage(command) => Turn::Moved(Some(command)),
            Ask::Flush(_) => Turn::Flushed(self.unflushed(landing)),
            Ask::Read | Ask::Checkpoint | Ask::Drain(_) => Turn::Moved(None),
        }
    }

    fn unflushed(&self, landing: bool) -> Result<Flushed, ScopeError> {
        if self.buffered.load(Ordering::Acquire) == 0 {
            return Ok(Flushed {
                next: Some(self.next()),
                carried: false,
            });
        }
        match self.life() {
            Life::Serving | Life::Draining => Err(ScopeError::Busy),
            Life::Released { .. } | Life::Lost if landing => Err(ScopeError::Uncertain),
            Life::Released { .. } | Life::Lost => Err(ScopeError::Lost),
        }
    }

    pub(crate) fn state(&self) -> Arc<F::State> {
        self.published
            .read(|published| Arc::clone(&published.state))
    }

    pub(crate) fn next(&self) -> u64 {
        self.published.read(|published| published.next)
    }

    pub(crate) fn watch(&self) -> watch::Receiver<Arc<F::State>> {
        self.watch.subscribe()
    }

    pub(crate) fn life(&self) -> Life {
        self.queue.read(|queue| queue.life)
    }

    pub(crate) fn weight(&self) -> usize {
        self.weight.load(Ordering::Relaxed)
    }

    pub(crate) fn idle(&self) -> bool {
        self.queue
            .read(|queue| !queue.leading && queue.waiting.is_empty())
    }

    pub(crate) fn lose(&self) {
        self.queue.update(|queue| queue.life = Life::Lost);
    }

    fn publish(&self, state: &Arc<F::State>, next: u64) {
        self.published.set(Published {
            state: Arc::clone(state),
            next,
        });
        self.watch.send_replace(Arc::clone(state));
    }

    pub(crate) async fn ask(&self, ask: Ask<F>, inner: &Inner<F>) -> Turn<F> {
        let asked = kind(&ask);
        let (reply, answer) = oneshot::channel();
        let queued = Queued(Arc::new(Slot::new(Some(Job { ask, reply }))));
        let admitted = self.queue.update(|queue| {
            if asked == Kind::Drain && queue.life == Life::Serving {
                queue.life = Life::Draining;
                queue.waiting.push_back(queued.clone());
                return Some(!std::mem::replace(&mut queue.leading, true));
            }
            if !admits(queue.life, asked) {
                return None;
            }
            queue.waiting.push_back(queued.clone());
            Some(!std::mem::replace(&mut queue.leading, true))
        });
        let Some(lead) = admitted else {
            let landing = self.queue.read(|queue| queue.leading);
            return self.refused(queued.take().ask, landing);
        };
        if lead {
            self.lead(inner).await;
        }
        match answer.await {
            Ok(turn) => turn,
            Err(_) if asked == Kind::Send => Turn::Committed(Err(ScopeError::Uncertain)),
            Err(_) if asked == Kind::Flush => Turn::Flushed(Err(ScopeError::Uncertain)),
            Err(_) => Turn::Moved(None),
        }
    }

    async fn lead(&self, inner: &Inner<F>) {
        let Some(core) = self.core.take() else {
            panic!(
                "the leader of scope {} finds its state in place",
                self.scope
            );
        };
        let mut leading = Leading {
            owner: self,
            core: Some(core),
            in_flight: false,
            done: false,
        };
        loop {
            if let Some(core) = leading.core.take() {
                self.core.put(core);
            }
            let waiting = self.queue.update(|queue| {
                let waiting = std::mem::take(&mut queue.waiting);
                if waiting.is_empty() {
                    queue.leading = false;
                }
                waiting
            });
            if waiting.is_empty() {
                leading.done = true;
                return;
            }
            leading.core = self.core.take();
            assert!(
                leading.core.is_some(),
                "a leader takes back the state it put down"
            );
            let jobs: Vec<Job<F>> = waiting.iter().map(Queued::take).collect();
            self.serve(&mut leading, jobs, inner).await;
        }
    }

    async fn serve(&self, leading: &mut Leading<'_, F>, jobs: Vec<Job<F>>, inner: &Inner<F>) {
        let mut batch = Batch::new();
        for job in jobs {
            match job.ask {
                Ask::Send(command) => batch.pending.push(Pending {
                    command,
                    decided: None,
                    durable: true,
                    reply: job.reply,
                }),
                Ask::Stage(command) => batch.pending.push(Pending {
                    command,
                    decided: None,
                    durable: false,
                    reply: job.reply,
                }),
                Ask::Flush(None) => batch.flushes.push(job.reply),
                Ask::Flush(Some(carry)) => {
                    batch.carrying = Some((carry, job.reply));
                    self.commit(leading, std::mem::replace(&mut batch, Batch::new()), inner)
                        .await;
                }
                Ask::Read => {
                    drop(job.reply.send(Turn::Read(self.state())));
                }
                Ask::Checkpoint => {
                    self.commit(leading, std::mem::replace(&mut batch, Batch::new()), inner)
                        .await;
                    let due = self.due(leading, inner);
                    let checkpointed = match (due, leading.core.as_mut()) {
                        (true, Some(core)) => self.checkpoint(core, inner).await,
                        _ => Ok(()),
                    };
                    drop(job.reply.send(Turn::Checkpointed(checkpointed)));
                }
                Ask::Drain(why) => {
                    batch.force = true;
                    self.commit(leading, std::mem::replace(&mut batch, Batch::new()), inner)
                        .await;
                    let drained = self.drain(leading, why, inner).await;
                    drop(job.reply.send(Turn::Drained(drained)));
                }
            }
        }
        self.commit(leading, batch, inner).await;
    }

    fn due(&self, leading: &Leading<'_, F>, inner: &Inner<F>) -> bool {
        leading.core.as_ref().is_some_and(|core| {
            core.next != core.checkpointed.next
                && (inner.clock)().saturating_sub(core.checkpointed.at_ms)
                    >= inner.policy.checkpoint_ms
        }) && self.life() == Life::Serving
    }

    async fn drain(
        &self,
        leading: &mut Leading<'_, F>,
        why: Why,
        inner: &Inner<F>,
    ) -> Result<u64, ScopeError> {
        let Some(core) = leading.core.as_mut() else {
            panic!("a draining leader of scope {} holds its state", self.scope);
        };
        if self.life() != Life::Draining {
            return Err(ScopeError::Lost);
        }
        if core.staged.is_some() {
            self.queue.update(|queue| {
                if queue.life == Life::Draining {
                    queue.life = Life::Serving;
                }
            });
            return Err(ScopeError::Busy);
        }
        let skip = why == Why::Evict && sim::mutated(Mutation::EvictWithoutCheckpoint);
        if !skip && let Err(error) = self.checkpoint(core, inner).await {
            self.queue.update(|queue| {
                if queue.life == Life::Draining {
                    queue.life = Life::Serving;
                }
            });
            return Err(error);
        }
        let seq = core.next;
        self.queue
            .update(|queue| queue.life = Life::Released { seq });
        Ok(seq)
    }

    pub(crate) async fn seal(&self, inner: &Inner<F>) -> Result<(), ScopeError> {
        let Some(mut core) = self.core.take() else {
            panic!("scope {} is sealed before anyone leads it", self.scope);
        };
        let sealed = self.checkpoint(&mut core, inner).await;
        self.core.put(core);
        sealed
    }

    pub(crate) async fn checkpoint(
        &self,
        core: &mut Core<F>,
        inner: &Inner<F>,
    ) -> Result<(), ScopeError> {
        if core.next == core.checkpointed.next {
            core.since = 0;
            return Ok(());
        }
        let rewrite = sim::mutated(Mutation::CheckpointRewritesUnchangedParts);
        let parts = inner.fold.encode(&core.state);
        let mut names = BTreeSet::new();
        let mut digests = BTreeSet::new();
        let held: BTreeSet<&String> = core.checkpointed.parts.values().collect();
        let mut named = Vec::with_capacity(parts.len());
        let mut fresh = Vec::new();
        let mut kept = 0_u64;
        for part in parts {
            assert!(
                names.insert(part.name.clone()),
                "scope {} encodes part {} once",
                self.scope,
                part.name
            );
            let at = digest(&part.body);
            if digests.insert(at.clone()) {
                if rewrite || !held.contains(&at) {
                    fresh.push((at.clone(), part.body));
                } else {
                    kept += 1;
                }
            }
            named.push((part.name, at));
        }
        drop(held);
        let (listing, chain) = core.checkpointed.listing(&named, core.next);
        let manifest = Manifest {
            scope: self.scope.as_ref().to_owned(),
            epoch: self.epoch,
            seq: core.next,
            parts: named,
            changed_bytes: chain.changed_bytes,
        };
        let fence = Fence {
            part: self.part,
            epoch: self.epoch,
            holder: inner.leases.node().to_owned(),
        };
        let written = inner
            .meter
            .write(&fence, &manifest, &listing, &fresh)
            .await;
        match written {
            Ok(Written::Landed) => {
                let bytes: usize = fresh.iter().map(|(_, body)| body.len()).sum();
                inner.stats.update(|stats| {
                    stats.checkpoints += 1;
                    stats.checkpoint_parts += fresh.len() as u64;
                    stats.checkpoint_parts_kept += kept;
                    stats.checkpoint_part_bytes += bytes as u64;
                    stats.checkpoint_listing_bytes += listing.encoded().len() as u64;
                    if matches!(listing, Listing::Whole(_)) {
                        stats.checkpoint_listings_whole += 1;
                    }
                });
                core.checkpointed = Checkpointed {
                    next: core.next,
                    at_ms: (inner.clock)(),
                    parts: manifest.parts.into_iter().collect(),
                    chain: Some(chain),
                };
                core.since = 0;
                Ok(())
            }
            Ok(Written::Stale) => {
                self.lose();
                Err(ScopeError::Lost)
            }
            Err(error) => {
                inner.stats.update(|stats| stats.checkpoint_failures += 1);
                Err(error.into())
            }
        }
    }

    async fn commit(&self, leading: &mut Leading<'_, F>, mut batch: Batch<F>, inner: &Inner<F>) {
        while !batch.is_empty() {
            batch = self.commit_once(leading, batch, inner).await;
        }
    }

    fn movable(&self, inner: &Inner<F>) -> bool {
        let life = self.life();
        if !matches!(life, Life::Serving | Life::Draining) {
            return true;
        }
        if inner.leases.held(self.part) != Some(self.epoch) {
            self.lose();
            return true;
        }
        false
    }

    fn moved(&self, mut batch: Batch<F>) {
        std::mem::take(&mut batch.pending)
            .into_iter()
            .for_each(Pending::moved);
        batch.flushed(&self.unflushed(false), false);
    }

    fn round(&self, core: &Core<F>, batch: Vec<Pending<F>>, inner: &Inner<F>) -> Round<F> {
        let limit = inner.policy.checkpoint_rows;
        let room = limit - core.since;
        let mut round = match &core.staged {
            Some(staged) => Round {
                working: staged.state.clone(),
                next: staged.next,
                rows: staged.rows.clone(),
                answers: Vec::new(),
                rest: Vec::new(),
                durable: false,
            },
            None => Round {
                working: (*core.state).clone(),
                next: core.next,
                rows: Vec::new(),
                answers: Vec::new(),
                rest: Vec::new(),
                durable: false,
            },
        };
        assert!(
            round.rows.len() <= room,
            "scope {} stages no more rows than its checkpoint bound leaves room for",
            self.scope
        );
        for mut pending in batch {
            if !round.rest.is_empty() {
                round.rest.push(pending);
                continue;
            }
            let decided = match pending.decided.take() {
                Some(decided) if !sim::mutated(Mutation::DeferredRedecided) => {
                    assert_eq!(
                        decided.at, round.next,
                        "scope {} commits a deferred command at the row it was decided at",
                        self.scope
                    );
                    decided
                }
                Some(_) | None => {
                    let (bodies, answer) = inner.fold.decide(&round.working, &pending.command);
                    Decided {
                        at: round.next,
                        bodies,
                        answer,
                    }
                }
            };
            if decided.bodies.len() > limit {
                let bound = ScopeError::Bound {
                    bound: "checkpoint_rows",
                    limit,
                    asked: decided.bodies.len(),
                };
                pending.answer(Err(bound));
                continue;
            }
            if round.rows.len() + decided.bodies.len() > room {
                pending.decided = Some(decided);
                round.rest.push(pending);
                continue;
            }
            for body in decided.bodies {
                let next = round.next;
                if let Err(error) = inner.fold.apply(&mut round.working, next, &body) {
                    panic!(
                        "scope {} decided row {next} that its own fold refuses: {error}",
                        self.scope
                    );
                }
                round.rows.push((next, body));
                round.next += 1;
            }
            round.durable |= pending.durable;
            round
                .answers
                .push((pending.reply, round.next, decided.answer));
        }
        round
    }

    fn stage(&self, core: &mut Core<F>, round: Round<F>) {
        assert!(
            round.rest.is_empty(),
            "scope {} stages only a batch that fits",
            self.scope
        );
        if sim::mutated(Mutation::StagedPublished) {
            self.publish(&Arc::new(round.working.clone()), round.next);
        }
        self.buffered.store(round.rows.len(), Ordering::Release);
        core.staged = Some(Staging {
            state: round.working,
            next: round.next,
            rows: round.rows,
        });
        for (reply, seq, answer) in round.answers {
            drop(reply.send(Turn::Committed(Ok(Committed { seq, reply: answer }))));
        }
    }

    async fn commit_once(
        &self,
        leading: &mut Leading<'_, F>,
        mut batch: Batch<F>,
        inner: &Inner<F>,
    ) -> Batch<F> {
        if self.movable(inner) {
            self.moved(batch);
            return Batch::new();
        }
        let Some(core) = leading.core.as_mut() else {
            panic!(
                "a committing leader of scope {} holds its state",
                self.scope
            );
        };
        if core.since >= inner.policy.checkpoint_rows
            && let Err(error) = self.checkpoint(core, inner).await
        {
            batch.fail(&error);
            return Batch::new();
        }
        let round = self.round(core, std::mem::take(&mut batch.pending), inner);
        if round.rows.is_empty() {
            for (reply, seq, answer) in round.answers {
                drop(reply.send(Turn::Committed(Ok(Committed { seq, reply: answer }))));
            }
            if round.rest.is_empty() {
                let flushed = Flushed {
                    next: Some(core.next),
                    carried: false,
                };
                batch.flushed(&Ok(flushed), false);
                return Batch::new();
            }
            if let Err(error) = self.checkpoint(core, inner).await {
                batch.pending = round.rest;
                batch.fail(&error);
                return Batch::new();
            }
            batch.pending = round.rest;
            return batch;
        }
        if !(round.durable || batch.forces() || !round.rest.is_empty()) {
            let next = round.next;
            self.stage(core, round);
            let flushed = Flushed {
                next: Some(next),
                carried: false,
            };
            batch.flushed(&Ok(flushed), false);
            return Batch::new();
        }
        self.landed(leading, batch, round, inner).await
    }

    async fn landed(
        &self,
        leading: &mut Leading<'_, F>,
        mut batch: Batch<F>,
        round: Round<F>,
        inner: &Inner<F>,
    ) -> Batch<F> {
        let working = Arc::new(round.working);
        let next = round.next;
        if sim::mutated(Mutation::PublishBeforeCommit) {
            self.publish(&working, next);
        }
        let last = round.rest.is_empty();
        let carry = batch.carrying.as_ref().filter(|_| last).map(|(carry, _)| carry);
        let Some(core) = leading.core.as_mut() else {
            panic!(
                "a committing leader of scope {} holds its state",
                self.scope
            );
        };
        let op = core.stamps.op();
        let stamped = inner.fold.stamped() && Stamps::format() == Format::Stamped;
        let sent = if stamped {
            core.stamps.rows(op, round.rows.clone())
        } else {
            round.rows.clone()
        };
        let stamps = core.stamps;
        let sending = Sending {
            rows: &sent,
            stamps: &stamps,
            op,
            stamped,
        };
        leading.in_flight = true;
        let appended = self.append(&sending, carry, inner).await;
        leading.in_flight = false;
        let carried = match appended {
            Ok(carried) => carried,
            Err(error) => {
                for (reply, _, _) in round.answers {
                    drop(reply.send(Turn::Committed(Err(error.clone()))));
                }
                round.rest.into_iter().for_each(Pending::moved);
                batch.fail(&error);
                return Batch::new();
            }
        };
        let Some(core) = leading.core.as_mut() else {
            panic!(
                "a committing leader of scope {} holds its state",
                self.scope
            );
        };
        core.state = working;
        core.next = next;
        core.since += round.rows.len();
        core.staged = None;
        self.buffered.store(0, Ordering::Release);
        self.weight
            .store(inner.fold.weight(&core.state), Ordering::Relaxed);
        self.publish(&core.state, next);
        inner.stats.update(|stats| {
            stats.commits += 1;
            stats.rows += round.rows.len() as u64;
        });
        for (reply, seq, answer) in round.answers {
            drop(reply.send(Turn::Committed(Ok(Committed { seq, reply: answer }))));
        }
        if self.due(leading, inner)
            && let Some(core) = leading.core.as_mut()
        {
            let _ = self.checkpoint(core, inner).await;
        }
        if !last {
            batch.pending = round.rest;
            return batch;
        }
        let flushed = Flushed {
            next: Some(next),
            carried,
        };
        batch.flushed(&Ok(flushed), carried);
        Batch::new()
    }

    async fn append(
        &self,
        sending: &Sending<'_>,
        carry: Option<&Carry>,
        inner: &Inner<F>,
    ) -> Result<bool, ScopeError> {
        let rows = sending.rows;
        let (appended, carried) = match carry {
            None => (inner.logs.append.append(&self.scope, rows).await, false),
            Some(carry) => self.carried(rows, carry, inner).await,
        };
        let error = match appended {
            Ok(()) => return Ok(carried),
            Err(StoreError::Busy) => return Err(ScopeError::Busy),
            Err(error) => error,
        };
        let unknown = !matches!(error, StoreError::Fenced | StoreError::Conflict { .. });
        let landed = if sending.stamped {
            self.read_back(sending, inner).await
        } else {
            None
        };
        match landed {
            Some(Landed::Sent) => Ok(carried),
            Some(Landed::Own { op }) if op == sending.op => Ok(carried),
            Some(Landed::Other) => {
                self.lose();
                Err(ScopeError::Lost)
            }
            _ if unknown => {
                self.lose();
                Err(ScopeError::Uncertain)
            }
            _ => {
                self.lose();
                Err(ScopeError::Lost)
            }
        }
    }

    async fn read_back(&self, sending: &Sending<'_>, inner: &Inner<F>) -> Option<Landed> {
        let (Some((first, _)), Some((last, _))) = (sending.rows.first(), sending.rows.last())
        else {
            return Some(Landed::Absent);
        };
        assert!(
            first <= last,
            "scope {} appends rows in seq order",
            self.scope
        );
        let found = inner
            .logs
            .append
            .scan(&self.scope, *first, last + 1)
            .await
            .ok()?;
        sending.stamps.landed(&found, sending.rows, false).ok()
    }

    async fn carried(
        &self,
        rows: &[(u64, String)],
        carry: &Carry,
        inner: &Inner<F>,
    ) -> (Result<(), StoreError>, bool) {
        let statements: Vec<SqlStatement<'_>> = carry
            .statements
            .iter()
            .map(|(sql, params)| SqlStatement {
                sql,
                params: params.iter().map(String::as_str).collect(),
            })
            .collect();
        let rider = Rider {
            host: carry.host,
            statements: &statements,
        };
        let split = sim::mutated(Mutation::RiderSplit);
        if split && let Err(error) = inner.logs.append.append(&self.scope, rows).await {
            return (Err(error), false);
        }
        let write = Write::Append {
            rows: if split { &[] } else { rows },
            compaction: Compaction {
                archived: &[],
                dropped: 0..0,
                superseded: &[],
            },
        };
        match inner.logs.append.carry(&self.scope, write, rider) {
            Some(carrying) => (carrying.await, true),
            None if split => (Ok(()), false),
            None => (inner.logs.append.append(&self.scope, rows).await, false),
        }
    }
}
