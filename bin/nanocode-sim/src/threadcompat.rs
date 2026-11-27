use std::{ops::Range, sync::Arc};

use nanocode::turnlog::{Policy as LogPolicy, ThreadState, thread_history};
use nanocode_partitions::Partition;
use nanocode_scope::ScopeError;
use nanocode_turnlog::LogHost;

use crate::rng::Rng;
use crate::threadlegacy::{self, Legacy};
use crate::threadscope::{PARTS, ThreadScopeStats, World, command, driven, flag, publish, reach};

const THREAD: &str = "compat";
const COMMANDS: Range<u64> = 12..40;
const LEGACY_PPM: u64 = 500_000;
const PREVIOUS_RELEASE_TAGS: [char; 7] = ['a', 'b', 'P', 'r', 'p', 's', '+'];

pub(crate) fn run(rng: &mut Rng, world: &World, stats: &mut ThreadScopeStats) {
    let host = world.host();
    let mut ids = Vec::new();
    if rng.chance(LEGACY_PPM) {
        let Some(legacy) = written(rng, world, &host, "old") else {
            return;
        };
        ids.extend(legacy.ids().iter().cloned());
    }
    let mut turns = 0;
    if stint(rng, world, "compat0", &mut turns, &mut ids, stats).is_none() {
        return;
    }
    if !world.checkpointed(THREAD) {
        flag(
            &world.wrong,
            "compat: a drained resident thread left no checkpoint".to_owned(),
        );
        return;
    }
    if previous(world, THREAD, "after a resident stint").is_none()
        || opened(world, &host, &ids, "after a resident stint").is_none()
    {
        return;
    }
    let Some(legacy) = written(rng, world, &host, "mid") else {
        return;
    };
    ids.extend(legacy.ids().iter().cloned());
    if previous(world, THREAD, "after the old path wrote").is_none() {
        return;
    }
    let Some(adoptions) = stint(rng, world, "compat1", &mut turns, &mut ids, stats) else {
        return;
    };
    if adoptions == 0 {
        flag(
            &world.wrong,
            "compat: a resident claim over rows the old path wrote past its checkpoint did not adopt them".to_owned(),
        );
        return;
    }
    if previous(world, THREAD, "after a second resident stint").is_none()
        || opened(world, &host, &ids, "after a second resident stint").is_none()
    {
        return;
    }
    stats.compat_flips += 1;
}

fn written(rng: &mut Rng, world: &World, host: &Arc<dyn LogHost>, prefix: &str) -> Option<Legacy> {
    match driven(
        world,
        "an old path write",
        threadlegacy::write_named(rng, Arc::clone(host), THREAD, prefix),
    )? {
        Ok(legacy) => Some(legacy),
        Err(error) => {
            flag(
                &world.wrong,
                format!("compat: the old path writes the thread: {error}"),
            );
            None
        }
    }
}

fn stint(
    rng: &mut Rng,
    world: &World,
    name: &str,
    turns: &mut u64,
    ids: &mut Vec<String>,
    stats: &mut ThreadScopeStats,
) -> Option<u64> {
    let node = world.node(name);
    publish(world, std::slice::from_ref(&node));
    let log = match reach(world, &node, THREAD)? {
        Ok(log) => log,
        Err(error) => {
            flag(
                &world.wrong,
                format!("compat: {name} claims the thread: {error}"),
            );
            return None;
        }
    };
    let Some(claimed) = log.state() else {
        flag(
            &world.wrong,
            format!("compat: {name} holds no state after its claim"),
        );
        return None;
    };
    let host = world.host();
    resident(
        world,
        THREAD,
        &host,
        &claimed,
        ids,
        &format!("on the claim of {name}"),
    )?;
    for _ in 0..rng.range(COMMANDS.start, COMMANDS.end) {
        let state = log.state();
        let counter = *turns;
        let next = command(rng, turns, state.as_deref());
        if *turns > counter {
            ids.push(format!("t{counter}"));
        }
        match driven(world, "a compat send", log.send(next))? {
            Ok(_) => stats.compat_commands += 1,
            Err(ScopeError::Store(_)) => stats.refused += 1,
            Err(error) => {
                flag(
                    &world.wrong,
                    format!("compat: {name} commits a command: {error}"),
                );
                return None;
            }
        }
    }
    let Some(state) = log.state() else {
        flag(
            &world.wrong,
            format!("compat: {name} holds no state after its commands"),
        );
        return None;
    };
    resident(
        world,
        THREAD,
        &host,
        &state,
        ids,
        &format!("after the commands of {name}"),
    )?;
    if let Err(error) = driven(world, "a compat drain", node.logs.scopes().drain(THREAD))? {
        flag(
            &world.wrong,
            format!("compat: {name} drains the thread: {error}"),
        );
        return None;
    }
    for part in 0..PARTS {
        if let Err(error) = driven(
            world,
            "a compat release",
            node.logs.scopes().release(Partition(part)),
        )? {
            flag(
                &world.wrong,
                format!("compat: {name} releases partition {part}: {error}"),
            );
            return None;
        }
    }
    publish(world, &[]);
    let adoptions = node.logs.scopes().stats().adoptions;
    stats.compat_adoptions += adoptions;
    Some(adoptions)
}

pub(crate) fn resident(
    world: &World,
    thread: &str,
    host: &Arc<dyn LogHost>,
    state: &ThreadState,
    ids: &[String],
    when: &str,
) -> Option<()> {
    let probe = Legacy::named(ids.to_vec());
    let theirs = match driven(
        world,
        "a compat turn log view",
        threadlegacy::expected(Arc::clone(host), thread, &probe),
    )? {
        Ok(theirs) => theirs,
        Err(error) => {
            flag(
                &world.wrong,
                format!("compat: {when}, the turn log does not open the thread: {error}"),
            );
            return None;
        }
    };
    let paging = thread_history::Paging::new(None, usize::MAX, None);
    let events = driven(
        world,
        "a compat history read",
        thread_history::events(host, thread, state, 0, paging),
    )?;
    let facts = driven(
        world,
        "a compat fact read",
        thread_history::facts(host, thread, state, 0, None),
    )?;
    let (Ok(events), Ok(facts)) = (events, facts) else {
        flag(
            &world.wrong,
            format!("compat: {when}, the resident history does not read back"),
        );
        return None;
    };
    let ours = threadlegacy::ours(state, &probe, &events.events, &facts);
    if let Some(difference) = threadlegacy::differs(&theirs, &ours) {
        flag(
            &world.wrong,
            format!(
                "compat: {when}, the turn log's fold differs from the resident thread: {difference}"
            ),
        );
        return None;
    }
    Some(())
}

pub(crate) fn previous(world: &World, thread: &str, when: &str) -> Option<()> {
    previous_rows(world, &world.rows(thread), when)
}

pub(crate) fn previous_rows(world: &World, rows: &[(u64, String)], when: &str) -> Option<()> {
    if let Some((seq, _)) = rows.iter().find(|(_, body)| {
        !body
            .chars()
            .next()
            .is_some_and(|tag| PREVIOUS_RELEASE_TAGS.contains(&tag))
    }) {
        flag(
            &world.wrong,
            format!("compat: {when}, row {seq} has a tag the previous release does not read"),
        );
        return None;
    }
    let decoded = match nanocode_turnlog::rows(rows) {
        Ok(decoded) => decoded,
        Err(error) => {
            flag(
                &world.wrong,
                format!(
                    "compat: {when}, the previous release's row decoder refuses the log: {error}"
                ),
            );
            return None;
        }
    };
    for row in &decoded {
        let Some((_, body)) = rows.iter().find(|(seq, _)| *seq == row.seq) else {
            continue;
        };
        match nanocode_turnlog::standalone_plain(body) {
            Ok(Some(plain)) if plain == row.plain => {}
            Ok(Some(_)) => {
                flag(
                    &world.wrong,
                    format!(
                        "compat: {when}, row {} decodes to other records alone than in the log",
                        row.seq
                    ),
                );
                return None;
            }
            Ok(None) => {}
            Err(error) => {
                flag(
                    &world.wrong,
                    format!(
                        "compat: {when}, row {} does not decode alone: {error}",
                        row.seq
                    ),
                );
                return None;
            }
        }
    }
    Some(())
}

fn opened(world: &World, host: &Arc<dyn LogHost>, ids: &[String], when: &str) -> Option<()> {
    let open = driven(
        world,
        "a compat open",
        nanocode::turnlog::thread_scope_sim::open(Arc::clone(host), THREAD, LogPolicy::default()),
    )?;
    let log = match open {
        Ok(log) => log,
        Err(error) => {
            flag(
                &world.wrong,
                format!("compat: {when}, OneLog::open refuses the thread: {error}"),
            );
            return None;
        }
    };
    let pending = match driven(world, "a compat pending read", log.pending())? {
        Ok(pending) => pending,
        Err(error) => {
            flag(
                &world.wrong,
                format!("compat: {when}, the opened thread reads no pending turns: {error}"),
            );
            return None;
        }
    };
    drop(log);
    let follower = match driven(
        world,
        "a compat follow",
        nanocode_turnlog::sim::follow(Arc::clone(host), THREAD),
    )? {
        Ok(follower) => follower,
        Err(error) => {
            flag(
                &world.wrong,
                format!("compat: {when}, the thread is not followed: {error}"),
            );
            return None;
        }
    };
    let claimed = match driven(
        world,
        "a compat claim",
        nanocode::turnlog::thread_scope_sim::claim(follower, LogPolicy::default()),
    )? {
        Ok(claimed) => claimed,
        Err(error) => {
            flag(
                &world.wrong,
                format!("compat: {when}, OneLog::claim refuses the thread: {error}"),
            );
            return None;
        }
    };
    let held = match driven(world, "a compat pending read", claimed.pending())? {
        Ok(held) => held,
        Err(error) => {
            flag(
                &world.wrong,
                format!("compat: {when}, the claimed thread reads no pending turns: {error}"),
            );
            return None;
        }
    };
    let opened: Vec<&str> = pending
        .iter()
        .map(|turn| turn.request_id.as_str())
        .collect();
    let claimed: Vec<&str> = held.iter().map(|turn| turn.request_id.as_str()).collect();
    if opened != claimed || opened.iter().any(|id| !ids.iter().any(|known| known == id)) {
        flag(
            &world.wrong,
            format!(
                "compat: {when}, OneLog::open holds pending turns {opened:?} and OneLog::claim {claimed:?}"
            ),
        );
        return None;
    }
    Some(())
}
