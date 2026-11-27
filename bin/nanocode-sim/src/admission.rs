use std::{
    collections::BTreeMap,
    num::NonZeroU32,
    ops::Range,
    process::ExitCode,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use nanocode_durability_core::{StoreError, StoreFuture};
use nanocode_scope::{
    Checkpoints, Committed, MemoryCheckpoints, Part, PartedLeases, Policy, Reached, Resident,
    Route, Scope, ScopeError, Scopes, Wiring, Work,
    sim::{self as scope_sim, Mutation},
};
use nanocode_sync::{Atom, block_on};
use nanocode_turnlog::{
    LogHost, SCAN_END,
    parted::{ModelParts, PartStore, PartedLog},
};

use crate::{
    executor::{Clock, Ended, Executor, Task},
    rng::{FNV_OFFSET, Rng, fnv},
};

const PARTS: u32 = 4;
const CAPACITY: Range<u64> = 1..9;
const SCOPES: Range<u64> = 64..400;
const GETS_PER_SCOPE: Range<u64> = 2..12;
const ROWS: Range<u64> = 0..6;
const SENDS: Range<u64> = 1..4;
const READ_MS: u64 = 5;
const READS_PER_LOAD: u64 = 1;
const ADMIT_ROUNDS_MAX: u64 = 2;
const TTL_MS: u64 = 600_000;
const GRACE_MS: u64 = 300;
const MARGIN_MS: u64 = 100;
const START_MS: u64 = 1_000_000;
const STATE_BYTES: usize = 8;
const NODE: &str = "n0";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AdmissionMutation(Mutation);

impl AdmissionMutation {
    pub const NONE: Self = Self(Mutation::None);

    pub fn parse(name: &str) -> Option<Self> {
        Mutation::parse(name)
            .filter(|mutation| {
                *mutation == Mutation::None || Mutation::ADMISSION.contains(mutation)
            })
            .map(Self)
    }

    pub const fn name(self) -> &'static str {
        self.0.name()
    }

    pub fn names() -> String {
        Mutation::ADMISSION
            .iter()
            .map(|mutation| mutation.name())
            .collect::<Vec<_>>()
            .join("|")
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct AdmissionStats {
    pub seeds: u64,
    pub gets: u64,
    pub busy: u64,
    pub loads: u64,
    pub in_flight_max: u64,
    pub sends: u64,
    pub history_reads: u64,
    pub history_busy: u64,
}

impl AdmissionStats {
    pub fn add(&mut self, other: &Self) {
        self.seeds += other.seeds;
        self.gets += other.gets;
        self.busy += other.busy;
        self.loads += other.loads;
        self.in_flight_max = self.in_flight_max.max(other.in_flight_max);
        self.sends += other.sends;
        self.history_reads += other.history_reads;
        self.history_busy += other.history_busy;
    }
}

struct Sum;

impl Resident for Sum {
    type State = u64;
    type Command = u64;
    type Reply = u64;

    fn empty(&self) -> u64 {
        0
    }

    fn decide(&self, state: &u64, command: &u64) -> (Vec<String>, u64) {
        (vec![command.to_string()], state + command)
    }

    fn apply(&self, state: &mut u64, seq: u64, row: &str) -> Result<(), StoreError> {
        let value: u64 = row
            .parse()
            .map_err(|error| StoreError::Backend(format!("row {seq} {row}: {error}")))?;
        *state += value;
        Ok(())
    }

    fn encode(&self, state: &u64) -> Vec<Part> {
        vec![Part {
            name: "sum".to_owned(),
            body: state.to_string(),
        }]
    }

    fn decode(&self, parts: Vec<Part>) -> Result<u64, StoreError> {
        let [part] = <[Part; 1]>::try_from(parts).map_err(|parts| {
            StoreError::Backend(format!("a sum checkpoint has {} parts", parts.len()))
        })?;
        part.body
            .parse()
            .map_err(|error| StoreError::Backend(format!("sum part {}: {error}", part.body)))
    }

    fn weight(&self, _state: &u64) -> usize {
        STATE_BYTES
    }
}

struct Nowhere;

impl Route<Sum> for Nowhere {
    fn forward<'a>(
        &'a self,
        holder: &'a str,
        _scope: &'a str,
        _command: u64,
    ) -> StoreFuture<'a, Result<Committed<u64>, ScopeError>> {
        Box::pin(std::future::ready(Err(ScopeError::Unreachable {
            holder: holder.to_owned(),
        })))
    }
}

type Served = BTreeMap<String, (u64, u64, u64)>;

struct Slow {
    capacity: u32,
    rows: Atom<BTreeMap<String, Vec<(u64, String)>>>,
    in_flight: AtomicU64,
    in_flight_max: AtomicU64,
    reads: Atom<Served>,
    writes: Atom<Served>,
}

fn served(counts: &Atom<Served>, scope: &str, rows: &[(u64, String)]) {
    let bytes: usize = rows.iter().map(|(_, body)| body.len()).sum();
    counts.update(|counts| {
        let entry = counts.entry(scope.to_owned()).or_default();
        entry.0 += 1;
        entry.1 += rows.len() as u64;
        entry.2 += bytes as u64;
    });
}

impl Slow {
    async fn read(&self, scope: &str, from: u64, to: u64) -> Vec<(u64, String)> {
        let now = self.in_flight.fetch_add(1, Ordering::Relaxed) + 1;
        self.in_flight_max.fetch_max(now, Ordering::Relaxed);
        nanocode_platform::sleep(Duration::from_millis(READ_MS)).await;
        self.in_flight.fetch_sub(1, Ordering::Relaxed);
        let rows: Vec<(u64, String)> = self.rows.read(|rows| {
            rows.get(scope)
                .map(|rows| {
                    rows.iter()
                        .filter(|(seq, _)| *seq >= from && *seq < to)
                        .cloned()
                        .collect()
                })
                .unwrap_or_default()
        });
        served(&self.reads, scope, &rows);
        rows
    }
}

impl LogHost for Slow {
    fn call_bound(&self) -> Option<Duration> {
        None
    }

    fn read_capacity(&self) -> u32 {
        self.capacity
    }

    fn scan<'a>(
        &'a self,
        scope: &'a str,
        from: u64,
        to: u64,
    ) -> StoreFuture<'a, Result<Vec<(u64, String)>, StoreError>> {
        Box::pin(async move { Ok(self.read(scope, from, to).await) })
    }

    fn load<'a>(
        &'a self,
        scope: &'a str,
    ) -> StoreFuture<'a, Result<Vec<(u64, String)>, StoreError>> {
        Box::pin(async move { Ok(self.read(scope, 0, SCAN_END).await) })
    }

    fn append<'a>(
        &'a self,
        scope: &'a str,
        rows: &'a [(u64, String)],
    ) -> StoreFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            self.rows.update(|kept| {
                kept.entry(scope.to_owned())
                    .or_default()
                    .extend(rows.iter().cloned());
            });
            served(&self.writes, scope, rows);
            Ok(())
        })
    }
}

#[derive(Clone)]
enum Got {
    Local(Scope<Sum>),
    Busy,
    Other(String),
}

#[derive(Clone)]
struct Ran {
    scope: String,
    ns: u64,
    got: Got,
}

struct World {
    clock: Arc<Clock>,
    slow: Arc<Slow>,
    scopes: Scopes<Sum>,
    names: Vec<String>,
    capacity: u64,
    bound_ns: u64,
    wrong: Vec<String>,
}

fn counted(work: Work) -> (u64, u64, u64) {
    (work.calls, work.rows, work.bytes)
}

fn ran(world: &World, rng: &mut Rng, tasks: Vec<Task<'_>>, what: &str) -> Option<String> {
    match Executor::new(
        Arc::clone(&world.clock),
        Rng::derive(rng.below(u64::MAX), what),
    )
    .run(tasks, None)
    {
        Ended::Finished => None,
        ended => Some(format!(
            "{what}: the run ended {ended:?} with calls still waiting"
        )),
    }
}

fn gets(world: &World, rng: &mut Rng, stats: &mut AdmissionStats) -> Vec<Ran> {
    let mut asked = Vec::new();
    for name in &world.names {
        for _ in 0..rng.range(GETS_PER_SCOPE.start, GETS_PER_SCOPE.end) {
            asked.push(name.clone());
        }
    }
    stats.gets = asked.len() as u64;
    let answers: Atom<Vec<Ran>> = Atom::default();
    let tasks = asked
        .iter()
        .map(|name| {
            let (answers, clock, scopes) = (&answers, &world.clock, &world.scopes);
            Box::pin(async move {
                let started = clock.now_ns();
                let got = match scopes.get(name).await {
                    Ok(Reached::Local(scope)) => Got::Local(scope),
                    Ok(Reached::Routed(routed)) => {
                        Got::Other(format!("routed to {}", routed.holder()))
                    }
                    Err(ScopeError::Busy) => Got::Busy,
                    Err(error) => Got::Other(error.to_string()),
                };
                let ran = Ran {
                    scope: name.clone(),
                    ns: clock.now_ns() - started,
                    got,
                };
                answers.update(|answers| answers.push(ran.clone()));
            }) as Task<'_>
        })
        .collect();
    if let Some(stuck) = ran(world, rng, tasks, "cold gets") {
        return vec![Ran {
            scope: String::new(),
            ns: 0,
            got: Got::Other(stuck),
        }];
    }
    answers.read(Clone::clone)
}

fn judge_gets(world: &mut World, answers: &[Ran], stats: &mut AdmissionStats) {
    let in_flight = world.slow.in_flight_max.load(Ordering::Relaxed);
    stats.in_flight_max = in_flight;
    if in_flight > world.capacity {
        world.wrong.push(format!(
            "I17: {in_flight} reads in flight at once over a host of read capacity {}",
            world.capacity
        ));
    }
    for (scope, (calls, _, _)) in world.slow.reads.read(Clone::clone) {
        stats.loads += calls;
        if calls > READS_PER_LOAD {
            world.wrong.push(format!(
                "I17: concurrent cold gets of {scope} read it {calls} times, not once"
            ));
        }
    }
    for answer in answers {
        if answer.ns > world.bound_ns {
            world.wrong.push(format!(
                "I17: a get of {} answered after {} ns, past admit_ms plus the load bound ({} ns)",
                answer.scope, answer.ns, world.bound_ns
            ));
        }
        match &answer.got {
            Got::Local(scope) => {
                let usage = counted(scope.usage().load);
                let served = world
                    .slow
                    .reads
                    .read(|reads| reads.get(&answer.scope).copied());
                if Some(usage) != served {
                    world.wrong.push(format!(
                        "I16: the meter counts {usage:?} load work for {} where the host served {served:?}",
                        answer.scope
                    ));
                }
            }
            Got::Busy => stats.busy += 1,
            Got::Other(other) => world
                .wrong
                .push(format!("a cold get of {} answered {other}", answer.scope)),
        }
    }
}

fn loaded(answers: &[Ran]) -> BTreeMap<String, Scope<Sum>> {
    answers
        .iter()
        .filter_map(|answer| match &answer.got {
            Got::Local(scope) => Some((answer.scope.clone(), scope.clone())),
            Got::Busy | Got::Other(_) => None,
        })
        .collect()
}

fn sends(
    world: &mut World,
    rng: &mut Rng,
    handles: &BTreeMap<String, Scope<Sum>>,
    stats: &mut AdmissionStats,
) {
    let mut planned = Vec::new();
    for (name, scope) in handles {
        for value in 0..rng.range(SENDS.start, SENDS.end) {
            planned.push((name.clone(), scope.clone(), value));
        }
    }
    stats.sends = planned.len() as u64;
    let refused: Atom<Vec<String>> = Atom::default();
    let tasks = planned
        .iter()
        .map(|(name, scope, value)| {
            let refused = &refused;
            Box::pin(async move {
                if let Err(error) = scope.send(*value).await {
                    refused.update(|refused| refused.push(format!("a send to {name}: {error}")));
                }
            }) as Task<'_>
        })
        .collect();
    world.wrong.extend(ran(world, rng, tasks, "sends"));
    world.wrong.extend(refused.read(Clone::clone));
    for (name, scope) in handles {
        let usage = counted(scope.usage().append);
        let served = world.slow.writes.read(|writes| writes.get(name).copied());
        if Some(usage) != served {
            world.wrong.push(format!(
                "I16: the meter counts {usage:?} append work for {name} where the host served {served:?}"
            ));
        }
    }
}

fn history(
    world: &mut World,
    rng: &mut Rng,
    handles: &BTreeMap<String, Scope<Sum>>,
    stats: &mut AdmissionStats,
) {
    let before = world.slow.reads.read(Clone::clone);
    world.slow.in_flight_max.store(0, Ordering::Relaxed);
    let pages = world.scopes.history();
    let answers: Atom<Vec<(String, u64, Result<(), StoreError>)>> = Atom::default();
    let tasks = handles
        .keys()
        .map(|name| {
            let (answers, clock, pages) = (&answers, &world.clock, &pages);
            Box::pin(async move {
                let started = clock.now_ns();
                let read = pages.scan(name, 0, SCAN_END).await.map(drop);
                let ns = clock.now_ns() - started;
                answers.update(|answers| answers.push((name.clone(), ns, read.clone())));
            }) as Task<'_>
        })
        .collect();
    world.wrong.extend(ran(world, rng, tasks, "history reads"));
    let in_flight = world.slow.in_flight_max.load(Ordering::Relaxed);
    if in_flight > world.capacity {
        world.wrong.push(format!(
            "I17: {in_flight} history reads in flight at once over a host of read capacity {}",
            world.capacity
        ));
    }
    for (name, ns, read) in answers.read(Clone::clone) {
        stats.history_reads += 1;
        match read {
            Ok(()) => {}
            Err(StoreError::Busy) => stats.history_busy += 1,
            Err(error) => world
                .wrong
                .push(format!("a history read of {name}: {error}")),
        }
        if ns > world.bound_ns {
            world.wrong.push(format!(
                "I17: a history read of {name} answered after {ns} ns, past {} ns",
                world.bound_ns
            ));
        }
        let Some(scope) = handles.get(&name) else {
            continue;
        };
        let usage = counted(scope.usage().history);
        let was = before.get(&name).copied().unwrap_or_default();
        let now = world
            .slow
            .reads
            .read(|reads| reads.get(&name).copied().unwrap_or_default());
        let served = (now.0 - was.0, now.1 - was.1, now.2 - was.2);
        if usage != served {
            world.wrong.push(format!(
                "I16: the meter counts {usage:?} history work for {name} where the host served {served:?}"
            ));
        }
    }
}

struct Outcome {
    digest: u64,
    wrong: Option<String>,
    stats: AdmissionStats,
    shape: String,
}

fn world(rng: &mut Rng, clock: &Arc<Clock>) -> World {
    let store = Arc::new(ModelParts::new().with_clock(Arc::new(|| START_MS)));
    let count = NonZeroU32::new(PARTS).expect("partitions are counted from one");
    block_on(store.migrate(count)).expect("the model store migrates");
    let log = Arc::new(
        PartedLog::fenced(Arc::clone(&store), count, NODE)
            .with_clock(Arc::new(|| START_MS))
            .with_lease_bounds(GRACE_MS, MARGIN_MS),
    );
    let names: Vec<String> = (0..rng.range(SCOPES.start, SCOPES.end))
        .map(|at| format!("scope-{at}"))
        .collect();
    let mut rows = BTreeMap::new();
    for name in &names {
        let seeded: Vec<(u64, String)> = (0..rng.range(ROWS.start, ROWS.end))
            .map(|seq| (seq, rng.range(1, 10).to_string()))
            .collect();
        rows.insert(name.clone(), seeded);
    }
    let capacity = rng.range(CAPACITY.start, CAPACITY.end);
    let rounds = (names.len() as u64).div_ceil(capacity);
    let admit_ms = READ_MS * rng.range(1, ADMIT_ROUNDS_MAX * rounds + 1);
    let slow = Arc::new(Slow {
        capacity: u32::try_from(capacity).expect("a capacity fits"),
        rows: Atom::new(rows),
        in_flight: AtomicU64::new(0),
        in_flight_max: AtomicU64::new(0),
        reads: Atom::default(),
        writes: Atom::default(),
    });
    let policy = Policy {
        checkpoint_rows: usize::try_from(ROWS.end + SENDS.end).expect("a row bound fits"),
        checkpoint_ms: TTL_MS,
        resident_bytes_max: STATE_BYTES * names.len(),
        forward_ms: READ_MS,
        admit_ms,
    };
    let scopes = Scopes::new(Wiring {
        fold: Arc::new(Sum),
        log: Arc::clone(&slow) as Arc<dyn LogHost>,
        leases: Arc::new(PartedLeases::new(log, NODE, TTL_MS)),
        checkpoints: Arc::new(MemoryCheckpoints::new(store)) as Arc<dyn Checkpoints>,
        route: Arc::new(Nowhere),
        policy,
        clock: Arc::new(|| START_MS),
        claimed: None,
    });
    let bound_ms = admit_ms + READ_MS * READS_PER_LOAD;
    World {
        clock: Arc::clone(clock),
        slow,
        scopes,
        names,
        capacity,
        bound_ns: bound_ms * 1_000_000,
        wrong: Vec::new(),
    }
}

fn seeded(seed: u64, clock: &Arc<Clock>) -> Outcome {
    let mut rng = Rng::derive(seed, "admission");
    let mut stats = AdmissionStats {
        seeds: 1,
        ..AdmissionStats::default()
    };
    let mut world = world(&mut rng, clock);
    let answers = gets(&world, &mut rng, &mut stats);
    judge_gets(&mut world, &answers, &mut stats);
    let handles = loaded(&answers);
    sends(&mut world, &mut rng, &handles, &mut stats);
    history(&mut world, &mut rng, &handles, &mut stats);
    let shape = format!(
        "capacity={} scopes={} gets={} loaded={}",
        world.capacity,
        world.names.len(),
        stats.gets,
        handles.len()
    );
    let mut digest = fnv(FNV_OFFSET, shape.as_bytes());
    for (name, scope) in &handles {
        digest = fnv(fnv(digest, name.as_bytes()), &scope.state().to_le_bytes());
    }
    Outcome {
        digest,
        wrong: world.wrong.into_iter().next(),
        stats,
        shape,
    }
}

fn run_seed(seed: u64, mutation: AdmissionMutation) -> Outcome {
    scope_sim::mutate(mutation.0);
    let clock = Clock::new();
    let outcome = crate::host::entered(seed, &clock, || seeded(seed, &clock));
    scope_sim::mutate(Mutation::None);
    outcome
}

pub fn judge(
    seed: u64,
    mutation: AdmissionMutation,
    twice: bool,
    verbose: bool,
    totals: &mut AdmissionStats,
) -> Option<String> {
    let first = run_seed(seed, mutation);
    totals.add(&first.stats);
    if verbose {
        println!(
            "seed {seed} digest {:016x} {} stats={:?}",
            first.digest, first.shape, first.stats
        );
    }
    if first.wrong.is_some() || !twice {
        return first
            .wrong
            .map(|message| format!("{} :: {message}", first.shape));
    }
    let second = run_seed(seed, mutation);
    (second.digest != first.digest || second.wrong.is_some()).then(|| {
        format!(
            "{} :: nondeterminism: digest {:016x} then {:016x}",
            first.shape, first.digest, second.digest
        )
    })
}

pub fn replay(seed: u64, mutation: AdmissionMutation) -> String {
    let mut command = format!("nanocode-sim --scenario admission --seed {seed}");
    if mutation != AdmissionMutation::NONE {
        command.push_str(" --mutation ");
        command.push_str(mutation.name());
    }
    command
}

pub fn report(
    seeds: &Range<u64>,
    passed: u64,
    failed: u64,
    twice: bool,
    mutation: AdmissionMutation,
    totals: &AdmissionStats,
) -> ExitCode {
    println!(
        "RESULT seeds={}..{} scenario=admission mutation={} passed={passed} failed={failed} twice={twice} stats={totals:?}",
        seeds.start,
        seeds.end,
        mutation.name()
    );
    match mutation {
        AdmissionMutation::NONE if failed == 0 => ExitCode::SUCCESS,
        AdmissionMutation::NONE => ExitCode::FAILURE,
        _ if failed > 0 => {
            println!("MUTATION {} caught by {failed} seeds", mutation.name());
            ExitCode::SUCCESS
        }
        _ => {
            println!("MUTATION {} NOT caught", mutation.name());
            ExitCode::FAILURE
        }
    }
}
