use std::{
    collections::BTreeMap,
    ops::Range,
    process::ExitCode,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use nanocode::{
    backend::Prompt,
    session_host::{LogEntry, Submission, Submitted, TurnRecord, TurnState},
    turnlog::{
        OneLog, OneLogOver, Policy, ThreadCommand, ThreadPath, ThreadPaths, ThreadState,
        thread_scope_sim::{self, Mutation},
    },
};
use nanocode_durability_core::StoreError;
use nanocode_scope::{Resident, ScopeError, switches};
use nanocode_turnlog::{Format, Formats, LogHost, sim as log_sim};
use serde_json::{Map, json};

use crate::{
    executor::Clock,
    rng::{FNV_OFFSET, Rng, fnv},
    threadcompat, threadlegacy,
    threadsessions::{self, Sessions},
    threadscope::{Node, PARTS, World, driven, flag, publish, raw},
};

const THREAD: &str = "flips";
const ROUNDS: Range<u64> = 3..9;
const OPS: Range<u64> = 2..10;
const CRASH_PPM: u64 = 250_000;
const HANDOFF_PPM: u64 = 250_000;
const LEGACY_PPM: u64 = 500_000;
const START_LEGACY_PPM: u64 = 500_000;
const START_STAMPED_PPM: u64 = 500_000;
const SESSIONS_PPM: u64 = 500_000;
const RECENT_ROWS: usize = 1 << 16;
const DRIVEN: [&str; 2] = ["thread_path", "row_format"];
const RESTARTS: u64 = 2 * ROUNDS.end;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FlipsMutation {
    None,
    FlipOffUnreadable,
    LegacyOpenClaims,
    ResidentDropsBlob,
}

impl FlipsMutation {
    const ALL: [Self; 4] = [
        Self::None,
        Self::FlipOffUnreadable,
        Self::LegacyOpenClaims,
        Self::ResidentDropsBlob,
    ];

    pub const fn name(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::FlipOffUnreadable => "flip_off_unreadable",
            Self::LegacyOpenClaims => "legacy_open_claims",
            Self::ResidentDropsBlob => "resident_drops_blob",
        }
    }

    const fn mutation(self) -> Mutation {
        match self {
            Self::None => Mutation::None,
            Self::FlipOffUnreadable => Mutation::FlipOffUnreadable,
            Self::LegacyOpenClaims => Mutation::LegacyOpenClaims,
            Self::ResidentDropsBlob => Mutation::ResidentDropsBlob,
        }
    }

    pub fn parse(name: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|mutation| mutation.name() == name)
    }

    pub fn names() -> String {
        Self::ALL
            .iter()
            .filter(|mutation| **mutation != Self::None)
            .map(|mutation| mutation.name())
            .collect::<Vec<_>>()
            .join("|")
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct FlipsStats {
    pub seeds: u64,
    pub flips: u64,
    pub ons: u64,
    pub offs: u64,
    pub format_flips: u64,
    pub crashes: u64,
    pub handoffs: u64,
    pub acked: u64,
    pub checked: u64,
    pub sessions: u64,
}

impl FlipsStats {
    pub const fn add(&mut self, other: &Self) {
        self.seeds += other.seeds;
        self.flips += other.flips;
        self.ons += other.ons;
        self.offs += other.offs;
        self.format_flips += other.format_flips;
        self.crashes += other.crashes;
        self.handoffs += other.handoffs;
        self.acked += other.acked;
        self.checked += other.checked;
        self.sessions += other.sessions;
    }
}

struct Switched(AtomicBool);

impl ThreadPaths for Switched {
    fn path(&self, _thread: &str) -> ThreadPath {
        if self.0.load(Ordering::Relaxed) {
            ThreadPath::Legacy
        } else {
            ThreadPath::Resident
        }
    }
}

impl Switched {
    fn flip(&self) -> ThreadPath {
        let legacy = !self.0.load(Ordering::Relaxed);
        self.0.store(legacy, Ordering::Relaxed);
        self.path(THREAD)
    }
}

struct Run {
    paths: Arc<Switched>,
    node: Node,
    over: Arc<OneLogOver>,
    generation: u64,
    stamped: bool,
    turns: u64,
    ids: Vec<String>,
    acked: BTreeMap<String, Acked>,
    sessions: Option<Sessions>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Acked {
    state: TurnState,
    durable: bool,
}

fn registered(world: &World) -> bool {
    for switch in switches::ON_DISK {
        if switch.flipped().scenario() != "flips" {
            flag(
                &world.wrong,
                format!(
                    "flips: on-disk switch {} names scenario {} as its proof, not flips",
                    switch.name(),
                    switch.flipped().scenario()
                ),
            );
            return false;
        }
        if !DRIVEN.contains(&switch.name()) {
            flag(
                &world.wrong,
                format!("flips: on-disk switch {} has no driver", switch.name()),
            );
            return false;
        }
    }
    let versions: Vec<String> = Formats::all()
        .iter()
        .map(|(format, _)| format.version().to_string())
        .collect();
    if switches::ROW_FORMAT.settings() != versions.as_slice() {
        flag(
            &world.wrong,
            format!(
                "flips: the row format switch's settings {:?} are not the registered formats {versions:?}",
                switches::ROW_FORMAT.settings()
            ),
        );
        return false;
    }
    for path in ThreadPath::ALL {
        if ThreadPath::named(path.setting()) != Some(path) {
            flag(
                &world.wrong,
                format!("flips: thread path {path:?} is not a setting of its switch"),
            );
            return false;
        }
    }
    true
}

fn opened(world: &World, node: &Node, paths: &Arc<Switched>) -> Option<Arc<OneLogOver>> {
    let logs = Arc::new(
        node.logs
            .clone()
            .with_paths(Arc::clone(paths) as Arc<dyn ThreadPaths>),
    );
    let policy = Policy {
        restarts: RESTARTS,
        ..Policy::default()
    };
    let legacy = paths.path(THREAD) == ThreadPath::Legacy;
    let loads = node.logs.scopes().stats().loads;
    match driven(world, "a flips open", OneLog::over(logs, THREAD, &policy))? {
        Ok(_) if legacy && node.logs.scopes().stats().loads != loads => {
            flag(
                &world.wrong,
                "flips: a thread on the legacy path loads its scope when the facade opens"
                    .to_owned(),
            );
            None
        }
        Ok(over) => Some(over),
        Err(error) => {
            flag(&world.wrong, format!("flips: the thread opens: {error}"));
            None
        }
    }
}

fn acked(run: &mut Run, record: &TurnRecord, durable: bool) {
    let state = record.state;
    run.acked
        .insert(record.request_id.clone(), Acked { state, durable });
}

fn failed(world: &World, run: &Run, what: &str, error: &StoreError) {
    flag(
        &world.wrong,
        format!(
            "flips: {what} on the {} path answers {error}",
            run.paths.path(THREAD).setting()
        ),
    );
}

fn submit(world: &World, run: &mut Run) -> Option<()> {
    let id = format!("f{}", run.turns);
    run.turns += 1;
    let submission = Submission {
        request_id: id.clone(),
        prompt: Prompt::new(format!("flip turn {id}")),
        hand: None,
        metadata: raw(&json!(null)),
    };
    match driven(world, "a flips submit", run.over.submit(&submission))? {
        Ok(Submitted::Accepted { record, .. }) => {
            run.ids.push(id);
            acked(run, &record, true);
            Some(())
        }
        Ok(answer) => {
            flag(
                &world.wrong,
                format!("flips: a new turn {id} is answered {answer:?}"),
            );
            None
        }
        Err(error) => {
            failed(world, run, "a submit", &error);
            None
        }
    }
}

fn live(run: &Run, rng: &mut Rng, state: TurnState) -> Option<String> {
    let live: Vec<&String> = run
        .acked
        .iter()
        .filter(|(_, held)| held.state == state)
        .map(|(id, _)| id)
        .collect();
    if live.is_empty() {
        return None;
    }
    live.get(rng.pick(live.len())).map(|id| (*id).clone())
}

fn op(rng: &mut Rng, world: &World, run: &mut Run) -> Option<()> {
    let fields = Map::new();
    match rng.below(4) {
        0 => submit(world, run),
        1 => {
            let Some(id) = live(run, rng, TurnState::Queued) else {
                return submit(world, run);
            };
            match driven(world, "a flips start", run.over.start(&id))? {
                Ok(Some(started)) => acked(run, &started.record, false),
                Ok(None) => {}
                Err(error) => failed(world, run, "a start", &error),
            }
            Some(())
        }
        2 => {
            let Some(id) = live(run, rng, TurnState::Running) else {
                return submit(world, run);
            };
            let entry = LogEntry {
                request_id: Some(id.clone()),
                event: raw(&json!({ "type": "flip", "turn": id })),
            };
            if let Err(error) = driven(world, "a flips append", run.over.append(&[entry]))? {
                failed(world, run, "an append", &error);
                return None;
            }
            match driven(
                world,
                "a flips finish",
                run.over.finish(&id, TurnState::Completed, &fields),
            )? {
                Ok(Some((record, _))) => acked(run, &record, true),
                Ok(None) => {}
                Err(error) => failed(world, run, "a finish", &error),
            }
            Some(())
        }
        _ => {
            let Some(id) = live(run, rng, TurnState::Queued) else {
                return submit(world, run);
            };
            match driven(world, "a flips cancel", run.over.cancel(&id, &fields))? {
                Ok(Some((record, _))) => acked(run, &record, true),
                Ok(None) => {}
                Err(error) => failed(world, run, "a cancel", &error),
            }
            Some(())
        }
    }
}

fn decoded(world: &World) -> Option<ThreadState> {
    let host = world.host();
    let fold = world.fold();
    match driven(world, "a flips adoption", fold.adopt(&host, THREAD))? {
        Ok(Some(adopted)) => return Some(adopted.state),
        Ok(None) => {}
        Err(error) => {
            flag(
                &world.wrong,
                format!("flips: a resident claim would not adopt the thread: {error}"),
            );
            return None;
        }
    }
    let mut state = fold.empty();
    for (seq, row) in world.rows(THREAD) {
        if let Err(error) = fold.apply(&mut state, seq, &row) {
            flag(
                &world.wrong,
                format!("flips: the resident fold refuses row {seq}: {error}"),
            );
            return None;
        }
    }
    Some(state)
}

fn held(world: &World, run: &mut Run, lossy: bool, when: &str) -> Option<()> {
    let acked: Vec<(String, Acked)> = run
        .acked
        .iter()
        .map(|(id, acked)| (id.clone(), *acked))
        .collect();
    for (id, acked) in acked {
        let answer = driven(world, "a flips turn read", run.over.turn(&id))?;
        let read = match &answer {
            Ok(Some(record)) => Some(record.state),
            Ok(None) | Err(_) => None,
        };
        let staged = lossy
            && !acked.durable
            && acked.state == TurnState::Running
            && read == Some(TurnState::Queued);
        if staged {
            run.acked.insert(
                id,
                Acked {
                    state: TurnState::Queued,
                    durable: true,
                },
            );
            continue;
        }
        if read != Some(acked.state) {
            flag(
                &world.wrong,
                format!(
                    "flips: {when}, turn {id} was answered {} ({}) and the {} path reads {answer:?}",
                    acked.state.as_str(),
                    if acked.durable { "durable" } else { "staged" },
                    run.over.path().setting()
                ),
            );
            return None;
        }
    }
    Some(())
}

fn agree(
    world: &World,
    run: &mut Run,
    lossy: bool,
    when: &str,
    stats: &mut FlipsStats,
) -> Option<()> {
    formats_agree(world, when)?;
    held(world, run, lossy, when)?;
    let host: Arc<dyn LogHost> = world.host();
    let state = decoded(world)?;
    threadcompat::resident(world, THREAD, &host, &state, &run.ids, when)?;
    if run.over.path() == ThreadPath::Resident {
        let log = match driven(world, "a flips get", run.node.logs.get(THREAD))? {
            Ok(log) => log,
            Err(error) => {
                flag(
                    &world.wrong,
                    format!("flips: {when}, the resident thread is not reached: {error}"),
                );
                return None;
            }
        };
        let Some(state) = log.state() else {
            flag(
                &world.wrong,
                format!("flips: {when}, the resident thread is not local"),
            );
            return None;
        };
        threadcompat::resident(world, THREAD, &host, &state, &run.ids, when)?;
    }
    stats.checked += 1;
    Some(())
}

fn formats_agree(world: &World, when: &str) -> Option<()> {
    let rows = world.rows(THREAD);
    let mut inner = Vec::with_capacity(rows.len());
    for (seq, row) in &rows {
        match nanocode_turnlog::tagged_row(row) {
            Ok(tagged) => inner.push((*seq, tagged.to_owned())),
            Err(error) => {
                flag(
                    &world.wrong,
                    format!("flips: {when}, row {seq} opens to no format 1 row: {error}"),
                );
                return None;
            }
        }
    }
    let both = nanocode_turnlog::rows(&rows)
        .and_then(|stamped| nanocode_turnlog::rows(&inner).map(|tagged| (stamped, tagged)));
    match both {
        Ok((stamped, tagged)) => {
            let differ = stamped.iter().zip(&tagged).find(|(stamped, tagged)| {
                stamped.seq != tagged.seq || stamped.plain != tagged.plain
            });
            if let Some((row, _)) = differ {
                flag(
                    &world.wrong,
                    format!(
                        "flips: {when}, row {} decodes to other records with its stamp than without",
                        row.seq
                    ),
                );
                return None;
            }
            if stamped.len() != tagged.len() {
                flag(
                    &world.wrong,
                    format!(
                        "flips: {when}, the log decodes to {} rows with stamps and {} without",
                        stamped.len(),
                        tagged.len()
                    ),
                );
                return None;
            }
        }
        Err(error) => {
            flag(
                &world.wrong,
                format!("flips: {when}, the registry refuses the log: {error}"),
            );
            return None;
        }
    }
    threadcompat::previous_rows(world, &inner, when)
}

fn formatted(world: &World, run: &mut Run, stats: &mut FlipsStats) -> Option<()> {
    run.stamped = !run.stamped;
    log_sim::force(Some(row_format(run.stamped)));
    stats.format_flips += 1;
    agree(
        world,
        run,
        false,
        &format!(
            "after a flip to row format {}",
            row_format(run.stamped).version()
        ),
        stats,
    )
}

const fn row_format(stamped: bool) -> Format {
    if stamped {
        Format::Stamped
    } else {
        Format::Tagged
    }
}

fn missed(world: &World, node: &Node) -> Option<()> {
    let log = match driven(world, "a flips get", node.logs.get(THREAD))? {
        Ok(log) => log,
        Err(error) => {
            flag(
                &world.wrong,
                format!("flips: a node after a crash reaches the thread: {error}"),
            );
            return None;
        }
    };
    if log.state().is_some() {
        return Some(());
    }
    match driven(world, "a flips forward", log.send(ThreadCommand::Pending))? {
        Err(ScopeError::Unreachable { .. }) => Some(()),
        answer => {
            flag(
                &world.wrong,
                format!("flips: a forward to a crashed holder answers {answer:?}"),
            );
            None
        }
    }
}

fn moved(rng: &mut Rng, world: &World, run: &mut Run, stats: &mut FlipsStats) -> Option<()> {
    let crash = rng.chance(CRASH_PPM);
    let legacy = run.over.path() == ThreadPath::Legacy;
    if !crash && !rng.chance(HANDOFF_PPM) {
        return Some(());
    }
    if crash {
        stats.crashes += 1;
        world.expire();
    } else {
        stats.handoffs += 1;
        if run.over.path() == ThreadPath::Resident {
            driven(world, "a flips drain", run.node.logs.scopes().drain(THREAD))?.ok()?;
        }
        for part in 0..PARTS {
            let released = driven(
                world,
                "a flips release",
                run.node
                    .logs
                    .scopes()
                    .release(nanocode_partitions::Partition(part)),
            )?;
            if let Err(error) = released {
                flag(
                    &world.wrong,
                    format!("flips: a handoff releases partition {part}: {error}"),
                );
                return None;
            }
        }
    }
    run.generation += 1;
    let node = world.node(&format!("f{}", run.generation));
    publish(world, std::slice::from_ref(&node));
    if crash {
        missed(world, &node)?;
    }
    run.over = opened(world, &node, &run.paths)?;
    run.node = node;
    let lossy = crash || legacy;
    agree(world, run, lossy, "after a node moved", stats)
}

fn round(rng: &mut Rng, world: &World, run: &mut Run, stats: &mut FlipsStats) -> Option<()> {
    submit(world, run)?;
    let ops = rng.range(OPS.start, OPS.end);
    let flip_at = rng.below(ops);
    for at in 0..ops {
        if at == flip_at {
            formatted(world, run, stats)?;
        }
        op(rng, world, run)?;
    }
    moved(rng, world, run, stats)?;
    let to = run.paths.flip();
    match to {
        ThreadPath::Resident => stats.ons += 1,
        ThreadPath::Legacy => stats.offs += 1,
    }
    stats.flips += 1;
    if let Err(error) = driven(world, "a flips pending read", run.over.pending())? {
        failed(world, run, "the read after a flip", &error);
        return None;
    }
    if run.over.path() != to {
        flag(
            &world.wrong,
            format!(
                "flips: the host flipped the thread to {} and the facade serves {}",
                to.setting(),
                run.over.path().setting()
            ),
        );
        return None;
    }
    agree(
        world,
        run,
        false,
        &format!("after a flip to {}", to.setting()),
        stats,
    )?;
    sessions(rng, world, run, stats)
}

fn sessions(rng: &mut Rng, world: &World, run: &mut Run, stats: &mut FlipsStats) -> Option<()> {
    let Some(mut sessions) = run.sessions.take() else {
        return Some(());
    };
    if run.over.path() == ThreadPath::Resident
        && let Some(Ok(log)) = driven(world, "a flips get", run.node.logs.get(THREAD))
        && let Some(state) = log.state()
        && let Err(error) = threadsessions::held(&state, THREAD, &sessions)
    {
        flag(&world.wrong, format!("flips: {error}"));
        return None;
    }
    let states = match driven(world, "a flips state store", run.over.states())? {
        Ok(states) => states,
        Err(error) => {
            failed(world, run, "the state store", &error);
            return None;
        }
    };
    let Some(logs) = states.facts() else {
        flag(&world.wrong, "flips: the state store offers no fact log".to_owned());
        return None;
    };
    let checked = driven(
        world,
        "a flips session fold",
        threadsessions::agrees(rng, &*logs, THREAD, &mut sessions),
    )?;
    match checked {
        Ok(checked) => stats.sessions += checked,
        Err(error) => {
            flag(
                &world.wrong,
                format!("flips: on the {} path, {error}", run.over.path().setting()),
            );
            return None;
        }
    }
    run.sessions = Some(sessions);
    Some(())
}

fn started(rng: &mut Rng, world: &World) -> Option<Run> {
    let mut ids = Vec::new();
    let mut sessions = None;
    if rng.chance(LEGACY_PPM) && rng.chance(SESSIONS_PPM) {
        match driven(
            world,
            "an old writer's sessions",
            threadsessions::write(rng, world.host(), THREAD),
        )? {
            Ok(written) => {
                ids.extend(written.ids.iter().cloned());
                sessions = Some(written);
            }
            Err(error) => {
                flag(&world.wrong, format!("flips: the old writer's sessions: {error}"));
                return None;
            }
        }
    } else if rng.chance(LEGACY_PPM) {
        let written = driven(
            world,
            "an old path history",
            threadlegacy::write_named(rng, world.host(), THREAD, "old"),
        )?;
        match written {
            Ok(legacy) => ids.extend(legacy.ids().iter().cloned()),
            Err(error) => {
                flag(&world.wrong, format!("flips: the old path writes: {error}"));
                return None;
            }
        }
    }
    let paths = Arc::new(Switched(AtomicBool::new(rng.chance(START_LEGACY_PPM))));
    let stamped = rng.chance(START_STAMPED_PPM);
    log_sim::force(Some(row_format(stamped)));
    let node = world.node("f0");
    publish(world, std::slice::from_ref(&node));
    let over = opened(world, &node, &paths)?;
    Some(Run {
        paths,
        node,
        over,
        generation: 0,
        stamped,
        turns: 0,
        ids,
        acked: BTreeMap::new(),
        sessions,
    })
}

struct Outcome {
    digest: u64,
    wrong: Option<String>,
    stats: FlipsStats,
    shape: String,
}

fn seeded(seed: u64, mutation: FlipsMutation, time: &Arc<Clock>) -> Outcome {
    let mut rng = Rng::derive(seed, "flips");
    let mut stats = FlipsStats {
        seeds: 1,
        ..FlipsStats::default()
    };
    let world = crate::threadscope::world(&mut rng, time).keeping(RECENT_ROWS);
    let rounds = rng.range(ROUNDS.start, ROUNDS.end);
    let shape = format!("rounds={rounds}");
    thread_scope_sim::mutate(mutation.mutation());
    if registered(&world)
        && let Some(mut run) = started(&mut rng, &world)
    {
        for _ in 0..rounds {
            if round(&mut rng, &world, &mut run, &mut stats).is_none() {
                break;
            }
        }
        stats.acked += run.acked.len() as u64;
    }
    thread_scope_sim::mutate(Mutation::None);
    log_sim::force(None);
    publish(&world, &[]);
    let mut digest = fnv(FNV_OFFSET, &seed.to_le_bytes());
    for (seq, row) in world.rows(THREAD) {
        digest = fnv(fnv(digest, &seq.to_le_bytes()), row.as_bytes());
    }
    Outcome {
        digest,
        wrong: world.wrong.read(Clone::clone),
        stats,
        shape,
    }
}

fn run_seed(seed: u64, mutation: FlipsMutation) -> Outcome {
    let time = Clock::new();
    crate::host::entered(seed, &time, || seeded(seed, mutation, &time))
}

pub fn judge(
    seed: u64,
    mutation: FlipsMutation,
    twice: bool,
    verbose: bool,
    totals: &mut FlipsStats,
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

pub fn replay(seed: u64, mutation: FlipsMutation) -> String {
    let mut command = format!("nanocode-sim --scenario flips --seed {seed}");
    if mutation != FlipsMutation::None {
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
    mutation: FlipsMutation,
    totals: &FlipsStats,
) -> ExitCode {
    println!(
        "RESULT seeds={}..{} scenario=flips mutation={} passed={passed} failed={failed} twice={twice} stats={totals:?}",
        seeds.start,
        seeds.end,
        mutation.name()
    );
    match mutation {
        FlipsMutation::None if failed == 0 => ExitCode::SUCCESS,
        FlipsMutation::None => ExitCode::FAILURE,
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
