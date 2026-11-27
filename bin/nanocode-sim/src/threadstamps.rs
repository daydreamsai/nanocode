use std::{
    collections::BTreeSet,
    future::Future,
    ops::Range,
    pin::pin,
    sync::Arc,
    task::{Context, Waker},
};

use nanocode::{
    backend::Prompt,
    session_host::Submission,
    turnlog::{ThreadCommand, ThreadLog, ThreadReply},
};
use nanocode_durability_core::{StoreError, StoreFuture};
use nanocode_scope::ScopeError;
use nanocode_sync::Atom;
use nanocode_turnlog::{
    Compaction, Extension, Format, LogHost, Record, Rider, ScopeRows, Stamp, StoredCheckpoint,
    Write, sim as log_sim,
};
use serde_json::json;

use crate::{
    rng::Rng,
    threadscope::{Node, ThreadScopeStats, World, driven, flag, publish, raw},
};

const THREAD: &str = "stamps";
const OPS: Range<u64> = 6..16;
const FAULTS: u64 = 5;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Fault {
    Clean,
    Lost,
    Never,
    Stalled { lands: bool },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Fired {
    Not,
    Landed,
    Unlanded,
}

#[derive(Clone, Copy)]
struct Plan {
    fault: Fault,
    fired: Fired,
}

struct Lossy {
    inner: Arc<dyn LogHost>,
    plan: Atom<Plan>,
}

fn unknown() -> StoreError {
    StoreError::Backend("the store did not answer the append, so its outcome is unknown".to_owned())
}

impl Lossy {
    fn arm(&self, fault: Fault) {
        drop(self.plan.set(Plan {
            fault,
            fired: Fired::Not,
        }));
    }

    fn fired(&self) -> Fired {
        self.plan.read(|plan| plan.fired)
    }

    fn take(&self) -> Fault {
        self.plan
            .update(|plan| std::mem::replace(&mut plan.fault, Fault::Clean))
    }

    fn note(&self, landed: &Result<(), StoreError>) {
        let fired = if landed.is_ok() {
            Fired::Landed
        } else {
            Fired::Unlanded
        };
        self.plan.update(|plan| plan.fired = fired);
    }

    fn faulted<'a>(
        &'a self,
        call: StoreFuture<'a, Result<(), StoreError>>,
    ) -> StoreFuture<'a, Result<(), StoreError>> {
        let fault = self.take();
        Box::pin(async move {
            match fault {
                Fault::Clean => call.await,
                Fault::Lost => {
                    let landed = call.await;
                    self.note(&landed);
                    landed?;
                    Err(unknown())
                }
                Fault::Never => {
                    drop(call);
                    self.note(&Err(unknown()));
                    Err(unknown())
                }
                Fault::Stalled { lands } => {
                    if lands {
                        let landed = call.await;
                        self.note(&landed);
                    } else {
                        drop(call);
                        self.note(&Err(unknown()));
                    }
                    std::future::pending().await
                }
            }
        })
    }
}

impl LogHost for Lossy {
    fn call_bound(&self) -> Option<std::time::Duration> {
        self.inner.call_bound()
    }

    fn read_capacity(&self) -> u32 {
        self.inner.read_capacity()
    }

    fn scan<'a>(
        &'a self,
        scope: &'a str,
        from: u64,
        to: u64,
    ) -> StoreFuture<'a, Result<ScopeRows, StoreError>> {
        self.inner.scan(scope, from, to)
    }

    fn scans<'a>(
        &'a self,
        scans: &'a [(&'a str, u64)],
    ) -> StoreFuture<'a, Result<Vec<ScopeRows>, StoreError>> {
        self.inner.scans(scans)
    }

    fn scan_ranges<'a>(
        &'a self,
        scope: &'a str,
        ranges: &'a [Range<u64>],
    ) -> StoreFuture<'a, Result<Vec<ScopeRows>, StoreError>> {
        self.inner.scan_ranges(scope, ranges)
    }

    fn load<'a>(&'a self, scope: &'a str) -> StoreFuture<'a, Result<ScopeRows, StoreError>> {
        self.inner.load(scope)
    }

    fn append<'a>(
        &'a self,
        scope: &'a str,
        rows: &'a [(u64, String)],
    ) -> StoreFuture<'a, Result<(), StoreError>> {
        self.faulted(self.inner.append(scope, rows))
    }

    fn append_compacting<'a>(
        &'a self,
        scope: &'a str,
        rows: &'a [(u64, String)],
        compaction: Compaction<'a>,
    ) -> StoreFuture<'a, Result<(), StoreError>> {
        self.faulted(self.inner.append_compacting(scope, rows, compaction))
    }

    fn extend<'a>(
        &'a self,
        scope: &'a str,
        extension: Extension<'a>,
    ) -> Option<StoreFuture<'a, Result<(), StoreError>>> {
        let call = self.inner.extend(scope, extension)?;
        Some(self.faulted(call))
    }

    fn carry<'a>(
        &'a self,
        scope: &'a str,
        write: Write<'a>,
        rider: Rider<'a>,
    ) -> Option<StoreFuture<'a, Result<(), StoreError>>> {
        let call = self.inner.carry(scope, write, rider)?;
        Some(self.faulted(call))
    }

    fn checkpoint<'a>(
        &'a self,
        scope: &'a str,
    ) -> Option<StoreFuture<'a, Result<Option<StoredCheckpoint>, StoreError>>> {
        self.inner.checkpoint(scope)
    }

    fn save_checkpoint<'a>(
        &'a self,
        scope: &'a str,
        checkpoint: &'a StoredCheckpoint,
    ) -> Option<StoreFuture<'a, Result<(), StoreError>>> {
        self.inner.save_checkpoint(scope, checkpoint)
    }
}

fn reached(world: &World, node: &Node) -> Option<ThreadLog> {
    match driven(world, "a stamped thread get", node.logs.get(THREAD))? {
        Ok(log) if log.state().is_some() => Some(log),
        Ok(_) => {
            flag(&world.wrong, "stamps: the thread is not local".to_owned());
            None
        }
        Err(error) => {
            flag(
                &world.wrong,
                format!("stamps: the thread is reached: {error}"),
            );
            None
        }
    }
}

fn submit(rng: &mut Rng, at: u64) -> ThreadCommand {
    ThreadCommand::Submit {
        submission: Submission {
            request_id: format!("stamped{at}"),
            prompt: Prompt::new(format!("stamped prompt {}", rng.below(1_000))),
            hand: None,
            metadata: raw(&json!({})),
        },
        at,
    }
}

fn dropped(world: &World, lossy: &Lossy, send: impl Future) -> Option<()> {
    let mut send = pin!(send);
    let mut context = Context::from_waker(Waker::noop());
    loop {
        if send.as_mut().poll(&mut context).is_ready() {
            flag(
                &world.wrong,
                "stamps: a send whose append stalls answered".to_owned(),
            );
            return None;
        }
        if lossy.fired() != Fired::Not {
            return Some(());
        }
        if !world.tick() {
            flag(
                &world.wrong,
                "stamps: a send never reached its append".to_owned(),
            );
            return None;
        }
    }
}

fn judged(
    world: &World,
    fault: Fault,
    fired: Fired,
    answer: &Result<ThreadReply, ScopeError>,
    stats: &mut ThreadScopeStats,
) -> Option<()> {
    let wrong = match (fault, fired, answer) {
        (Fault::Clean, Fired::Not, Ok(_)) => None,
        (Fault::Lost, Fired::Landed, Ok(_)) => {
            stats.stamps_settled += 1;
            None
        }
        (Fault::Lost, Fired::Landed, Err(ScopeError::Lost)) => Some(
            "an append that landed unanswered was fenced by a row stamped with its own writer and epoch"
                .to_owned(),
        ),
        (Fault::Never | Fault::Lost, Fired::Unlanded, Err(ScopeError::Uncertain)) => {
            stats.stamps_uncertain += 1;
            None
        }
        (fault, fired, answer) => Some(format!(
            "a send with fault {fault:?} whose append fired {fired:?} answered {answer:?}"
        )),
    };
    match wrong {
        Some(wrong) => {
            flag(&world.wrong, format!("stamps: {wrong}"));
            None
        }
        None => Some(()),
    }
}

fn past(world: &World, node: &Node) -> Option<()> {
    let log = reached(world, node)?;
    let next = log.next()?;
    let last = world.rows(THREAD).last().map(|(seq, _)| *seq);
    if let Some(last) = last
        && next <= last
    {
        flag(
            &world.wrong,
            format!("stamps: the thread's next row is {next}, not past landed row {last}"),
        );
        return None;
    }
    Some(())
}

fn stamps(world: &World) -> Option<Vec<Stamp>> {
    let rows = match nanocode_turnlog::rows(&world.rows(THREAD)) {
        Ok(rows) => rows,
        Err(error) => {
            flag(
                &world.wrong,
                format!("stamps: the rows do not decode: {error}"),
            );
            return None;
        }
    };
    let mut seen = BTreeSet::new();
    let mut stamps = Vec::with_capacity(rows.len());
    for row in rows {
        let Some(stamp) = row.stamp else {
            flag(
                &world.wrong,
                format!(
                    "stamps: row {} was written at format 2 and carries no stamp",
                    row.seq
                ),
            );
            return None;
        };
        if !seen.insert(stamp) {
            flag(
                &world.wrong,
                format!(
                    "stamps: row {} repeats the stamp {stamp:?}: an op landed twice",
                    row.seq
                ),
            );
            return None;
        }
        stamps.push(stamp);
    }
    Some(stamps)
}

fn foreign(
    world: &World,
    node: &Node,
    rng: &mut Rng,
    at: u64,
    stats: &mut ThreadScopeStats,
) -> Option<()> {
    let own = *stamps(world)?.last()?;
    let log = reached(world, node)?;
    let (Some(state), Some(next)) = (log.state(), log.next()) else {
        flag(&world.wrong, "stamps: the thread is not local".to_owned());
        return None;
    };
    let record = Record::Event {
        at,
        event: raw(&json!({ "type": "stranger", "at": at })),
        seq: state.next_event(),
        turn: None,
    };
    let row = match nanocode_turnlog::standalone_row(&[record]) {
        Ok(row) => row,
        Err(error) => {
            flag(
                &world.wrong,
                format!("stamps: a stranger's row encodes: {error}"),
            );
            return None;
        }
    };
    let stranger = Stamp::new(own.writer().wrapping_add(1), own.epoch(), own.op());
    if let Err(error) = world.foreign_row(THREAD, next, log_sim::stamped(stranger, &row)) {
        flag(
            &world.wrong,
            format!("stamps: a stranger's row lands: {error}"),
        );
        return None;
    }
    match driven(
        world,
        "a send onto a stranger's row",
        log.send(submit(rng, at)),
    )? {
        Err(ScopeError::Lost) => {
            stats.stamps_fenced += 1;
            Some(())
        }
        answer => {
            flag(
                &world.wrong,
                format!(
                    "stamps: a send at a seq another writer's stamped row holds answered {answer:?}"
                ),
            );
            None
        }
    }
}

fn phase(
    rng: &mut Rng,
    world: &World,
    node: &Node,
    lossy: &Lossy,
    stats: &mut ThreadScopeStats,
) -> Option<()> {
    let ops = rng.range(OPS.start, OPS.end);
    for at in 0..ops {
        let fault = match rng.below(FAULTS) {
            0 => Fault::Clean,
            1 => Fault::Lost,
            2 => Fault::Never,
            3 => Fault::Stalled { lands: true },
            _ => Fault::Stalled { lands: false },
        };
        let log = reached(world, node)?;
        let command = submit(rng, at);
        lossy.arm(fault);
        if matches!(fault, Fault::Stalled { .. }) {
            dropped(world, lossy, log.send(command))?;
            stats.stamps_dropped += 1;
        } else {
            let answer = driven(world, "a stamped send", log.send(command))?;
            judged(world, fault, lossy.fired(), &answer, stats)?;
        }
        lossy.arm(Fault::Clean);
        past(world, node)?;
    }
    stamps(world)?;
    foreign(world, node, rng, ops, stats)
}

pub(crate) fn run(rng: &mut Rng, world: &World, stats: &mut ThreadScopeStats) {
    let mut held = None;
    let node = world.node_wrapped("stamps0", |log| {
        let lossy = Arc::new(Lossy {
            inner: log,
            plan: Atom::new(Plan {
                fault: Fault::Clean,
                fired: Fired::Not,
            }),
        });
        held = Some(Arc::clone(&lossy));
        (lossy as Arc<dyn LogHost>, None)
    });
    let Some(lossy) = held else {
        flag(
            &world.wrong,
            "stamps: the node's log is not wrapped".to_owned(),
        );
        return;
    };
    publish(world, std::slice::from_ref(&node));
    log_sim::force(Some(Format::Stamped));
    phase(rng, world, &node, &lossy, stats);
    log_sim::force(None);
}
