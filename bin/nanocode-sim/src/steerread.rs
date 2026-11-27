use std::{
    future::Future,
    pin::pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use nanocode::{
    ClaudeClient, HarnessFamily, HostFuture, ModelSelection, Nanocode, PromptRequest, Thinking,
    Tools,
    backend::Prompt,
    claude::transport::{HttpError, HttpFuture, HttpRequest, HttpResponse, HttpTransport},
    durability::{SessionRecord, SessionStore},
    session_host::{
        Delivery, InboxHost, LoggedEvent, Next, PassEnd, SessionLog, TurnInbox, TurnLog, Wake,
    },
    turnlog::{OneLog, Policy, ThreadTurns},
};
use nanocode_durability_core::KeyValueStore;
use nanocode_sync::Atom;
use serde_json::json;

use crate::{
    disk::Disk,
    executor::{Clock, Ended},
    faults::{Durability, Profile},
    rng::{FNV_OFFSET, Rng, fnv},
    steercarry::{epoch, event_stream},
    store::{Mutation as StoreMutation, SimKv},
    turnlog::SimHost,
};

const SESSION: &str = "0190f5a2-7c1e-7000-8000-00000057ee14";
const MODEL: &str = "claude-haiku-5-5";
const BASE_URL: &str = "http://model.sim/v1";
const REQUEST: &str = "turn-steered";
const PROMPT: &str = "STARTMARK look up the balance";
const STEER_ID: &str = "steer-read";
const STEER_TEXT: &str = "READMARK use the second ledger";
const STEER_MARK: &str = "READMARK";
const LATE_ID: &str = "steer-late";
const LATE_TEXT: &str = "LATEMARK check the totals";
const IDS: [&str; 2] = [STEER_ID, LATE_ID];
const THREAD: &str = "steer-read";
const SCOPE: &str = "steer-read";
const EPOCHS_MAX: u64 = 3;
const POLL: Duration = Duration::from_millis(5);
const WINDOW: Duration = Duration::from_millis(5);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Log {
    One,
    Session,
    Resident,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Crash {
    Never,
    InPass,
    AfterPass,
}

const CASES: [(Log, Crash); 5] = [
    (Log::One, Crash::Never),
    (Log::One, Crash::InPass),
    (Log::One, Crash::AfterPass),
    (Log::Session, Crash::Never),
    (Log::Resident, Crash::Never),
];

#[derive(Default)]
struct Wire {
    calls: AtomicU64,
    bodies: Atom<Vec<String>>,
    entered: AtomicBool,
    released: AtomicBool,
    hang_second: AtomicBool,
}

struct ClaudeWire(Arc<Wire>);

impl HttpTransport for ClaudeWire {
    fn post(&self, request: HttpRequest) -> HttpFuture<'_, Result<HttpResponse, HttpError>> {
        Box::pin(async move {
            let number = self.0.calls.fetch_add(1, Ordering::AcqRel) + 1;
            let saw = request.body.contains(STEER_MARK);
            self.0
                .bodies
                .update(|bodies| bodies.push(request.body.clone()));
            drop(request);
            if number == 1 {
                self.0.entered.store(true, Ordering::Release);
                while !self.0.released.load(Ordering::Acquire) {
                    nanocode_platform::sleep(POLL).await;
                }
            }
            if number == 2 && self.0.hang_second.load(Ordering::Acquire) {
                std::future::pending::<()>().await;
            }
            let text = if saw {
                "answer with the steer"
            } else {
                "answer"
            };
            let sse = crate::server::events(
                MODEL,
                &format!("sr{number}"),
                &json!({"type": "text", "text": ""}),
                Some(json!({"type": "text_delta", "text": text})),
                "end_turn",
            );
            Ok(HttpResponse::new(
                nanocode_http::types::StatusCode::OK,
                event_stream(),
                Box::pin(nanocode_sync::stream::once(
                    async move { Ok(sse.into_bytes()) },
                )),
            ))
        })
    }
}

#[derive(Clone, Debug)]
struct Ending {
    read: Vec<String>,
    given: Vec<String>,
    end: &'static str,
}

struct Host {
    wire: Arc<Wire>,
    wakes: AtomicU64,
    late: bool,
    hang_next: bool,
    ending: Atom<Option<Ending>>,
}

const fn ended(end: &PassEnd) -> &'static str {
    match end {
        PassEnd::Refused(_) => "refused",
        PassEnd::Completed(_) => "completed",
        PassEnd::Failed(_) => "failed",
        PassEnd::Stopped { .. } => "stopped",
        PassEnd::Left { .. } => "left",
    }
}

impl InboxHost for Host {
    fn publish(&self, _events: Vec<LoggedEvent>) -> HostFuture<'_, Result<(), String>> {
        Box::pin(async { Ok(()) })
    }

    fn inputs<'a>(&'a self, _request_id: &'a str) -> HostFuture<'a, Result<Vec<String>, String>> {
        Box::pin(async move {
            let entered = self.wire.entered.load(Ordering::Acquire);
            Ok(IDS
                .into_iter()
                .filter(|id| entered && (*id == STEER_ID || self.late))
                .map(str::to_owned)
                .collect())
        })
    }

    fn input<'a>(
        &'a self,
        _request_id: &'a str,
        id: &'a str,
    ) -> HostFuture<'a, Result<Prompt, String>> {
        Box::pin(async move {
            match id {
                STEER_ID => Ok(Prompt::new(STEER_TEXT)),
                LATE_ID => Ok(Prompt::new(LATE_TEXT)),
                _ => Err(format!("input {id} was never offered")),
            }
        })
    }

    fn wake<'a>(&'a self, _request_id: &'a str) -> HostFuture<'a, Wake> {
        Box::pin(async move {
            if self.wakes.fetch_add(1, Ordering::AcqRel) == 0 {
                while !self.wire.entered.load(Ordering::Acquire) {
                    nanocode_platform::sleep(POLL).await;
                }
                return Wake::Inputs;
            }
            self.wire.released.store(true, Ordering::Release);
            std::future::pending().await
        })
    }

    fn next<'a>(
        &'a self,
        _request_id: &'a str,
        end: PassEnd,
        delivery: &'a Delivery,
    ) -> HostFuture<'a, Next> {
        let mut read = delivery.read();
        read.sort_unstable();
        let mut given = delivery.given();
        given.sort_unstable();
        let ending = Ending {
            read,
            given,
            end: ended(&end),
        };
        drop(end);
        Box::pin(async move {
            if self.hang_next {
                std::future::pending::<()>().await;
            }
            self.ending.set(Some(ending));
            Next::Done
        })
    }
}

struct Media {
    kv: SimKv,
    disk: Arc<Disk>,
    sql: Arc<crate::slog::Store>,
}

fn runtime(wire: &Arc<Wire>, store: SessionStore) -> Result<Nanocode, String> {
    let client = ClaudeClient::messages_api(
        BASE_URL,
        "sk-sim",
        Duration::from_secs(10),
        Duration::from_secs(600),
    )
    .map_err(|error| format!("claude client: {error}"))?
    .with_transport(ClaudeWire(Arc::clone(wire)));
    let tools = Tools::builder()
        .without_defaults()
        .build()
        .map_err(|error| format!("tools: {error}"))?;
    Nanocode::builder()
        .claude(nanocode::ClaudeHarness::new(client))
        .tools(tools)
        .runner_management(false)
        .thinking(Thinking::Low)
        .workspace(std::env::temp_dir())
        .durability(store)
        .build()
        .map_err(|error| format!("runtime build: {error}"))
}

async fn pumped<T>(mut pump: HostFuture<'static, ()>, work: impl Future<Output = T>) -> T {
    let mut work = pin!(work);
    let mut pumping = true;
    std::future::poll_fn(|context| {
        if pumping && pump.as_mut().poll(context).is_ready() {
            pumping = false;
        }
        work.as_mut().poll(context)
    })
    .await
}

struct Turned {
    read: Vec<Option<bool>>,
}

async fn turned<L: TurnLog + ?Sized>(
    runtime: Nanocode,
    store: SessionStore,
    log: Arc<L>,
    host: &Arc<Host>,
) -> Result<Turned, String> {
    let record = SessionRecord::root(
        SESSION,
        ModelSelection::new(HarnessFamily::Claude, MODEL),
        None,
    );
    let inbox_host: Arc<dyn InboxHost> = Arc::clone(host) as Arc<dyn InboxHost>;
    let (inbox, pump) =
        TurnInbox::open((*runtime).clone(), &store, record, log, inbox_host, WINDOW)
            .await
            .map_err(|error| format!("open inbox: {error}"))?;
    let request = PromptRequest::new(PROMPT).request_id(REQUEST);
    pumped(pump, inbox.turn(REQUEST, request, Vec::new())).await;
    let mut read = Vec::with_capacity(IDS.len());
    for id in IDS {
        read.push(
            store
                .steer_read(SESSION, id)
                .await
                .map_err(|error| format!("steer_read {id}: {error}"))?,
        );
    }
    Ok(Turned { read })
}

fn policy() -> Policy {
    Policy {
        chain_bytes_max: None,
        effects: nanocode::turnlog::Effects::default(),
        host_call_bound: None,
        max_row_bytes: None,
        max_tail_bytes: None,
        restarts: u64::MAX,
        retain_finished: None,
        retired_max: None,
        snapshot_ratio: 2,
        snapshot_rows: None,
        wakes_max: None,
    }
}

async fn one_log(media: &Media, wire: &Arc<Wire>, host: &Arc<Host>) -> Result<Turned, String> {
    let log_host: Arc<dyn nanocode::turnlog::LogHost> =
        Arc::new(SimHost::new(Arc::clone(&media.disk)));
    let parts = crate::onelog::parts(log_host, "sim", policy())
        .await
        .map_err(|error| format!("turn log: {error}"))?;
    parts.log.settle_durably();
    let runtime = runtime(wire, parts.store.clone())?;
    turned(runtime, parts.store.clone(), Arc::clone(&parts.log), host).await
}

async fn session_log(media: &Media, wire: &Arc<Wire>, host: &Arc<Host>) -> Result<Turned, String> {
    let migrated = SessionLog::migrate(&*media.sql)
        .await
        .map_err(|error| format!("migrate the session log: {error}"))?;
    let log = Arc::new(SessionLog::new(Arc::clone(&media.sql), SCOPE, &migrated));
    let store = nanocode::session_store(KeyValueStore::new(media.kv.clone()))
        .map_err(|error| format!("session store: {error}"))?;
    let runtime = runtime(wire, store.clone())?;
    turned(runtime, store, log, host).await
}

async fn resident(
    world: Option<&crate::threadscope::World>,
    wire: &Arc<Wire>,
    host: &Arc<Host>,
) -> Result<Turned, String> {
    let Some(world) = world else {
        return Err("the resident case has no thread world".to_owned());
    };
    let node = world.node("steer-read");
    crate::threadscope::publish(world, std::slice::from_ref(&node));
    let logs = Arc::new(node.logs.clone());
    let over = OneLog::over(Arc::clone(&logs), THREAD, &policy())
        .await
        .map_err(|error| format!("over: {error}"))?;
    let store =
        nanocode::session_store(over.store()).map_err(|error| format!("session store: {error}"))?;
    let log = logs
        .get(THREAD)
        .await
        .map_err(|error| format!("thread get: {error}"))?;
    let turns = ThreadTurns::new(Arc::new(log), policy().restarts)
        .map_err(|error| format!("thread turns: {error}"))?;
    let runtime = runtime(wire, store.clone())?;
    turned(runtime, store, Arc::new(turns), host).await
}

fn judged(
    wire: &Wire,
    host: &Host,
    turned: &Turned,
    crash: Crash,
    crashes: u64,
) -> Result<u64, String> {
    if crash != Crash::Never && crashes == 0 {
        return Err("the crash never happened".to_owned());
    }
    let ending = host.ending.get();
    let Some(ending) = ending.as_ref() else {
        return Err("the pass never reached InboxHost::next".to_owned());
    };
    let saw = wire
        .bodies
        .read(|bodies| bodies.iter().any(|body| body.contains(STEER_MARK)));
    if !saw {
        return Err("no model call carried the steer".to_owned());
    }
    if turned.read.first() != Some(&Some(true)) {
        return Err(format!(
            "steer_read is {:?} after a model call carried the steer",
            turned.read
        ));
    }
    for (id, read) in IDS.into_iter().zip(&turned.read) {
        let held = ending.read.iter().any(|read| read == id);
        if held != (*read == Some(true)) {
            return Err(format!(
                "Delivery::read() is {:?} with {:?} given after the pass {}, but steer_read of {id} is {read:?}",
                ending.read, ending.given, ending.end
            ));
        }
    }
    Ok(wire.calls.load(Ordering::Acquire))
}

fn media(seed: u64, clock: &Arc<Clock>) -> Media {
    let mut profile = Profile::of(seed);
    profile.before_ppm = 0;
    profile.after_ppm = 0;
    profile.torn_ppm = 0;
    profile.read_ppm = 0;
    profile.durability = Durability::OnAck;
    Media {
        kv: SimKv::new(seed, profile, StoreMutation::None, false, Arc::clone(clock)),
        disk: Disk::new(),
        sql: Arc::new(crate::slog::Store::default()),
    }
}

fn case(seed: u64, log: Log, crash: Crash) -> Result<u64, String> {
    let clock = Clock::new();
    crate::host::enter(seed, &clock);
    let label = format!("steer_read {log:?} {crash:?}");
    let media = media(seed, &clock);
    let mut rng = Rng::derive(seed, &label);
    let world = (log == Log::Resident).then(|| crate::threadscope::world(&mut rng, &clock));
    let wire = Arc::new(Wire::default());
    wire.hang_second
        .store(crash == Crash::InPass, Ordering::Release);
    let mut crashes = 0;
    for number in 0..EPOCHS_MAX {
        let host = Arc::new(Host {
            wire: Arc::clone(&wire),
            wakes: AtomicU64::new(0),
            late: number > 0,
            hang_next: crash == Crash::AfterPass && number == 0,
            ending: Atom::new(None),
        });
        let work = async {
            match log {
                Log::One => one_log(&media, &wire, &host).await,
                Log::Session => session_log(&media, &wire, &host).await,
                Log::Resident => resident(world.as_ref(), &wire, &host).await,
            }
        };
        match epoch(seed, &clock, &format!("{label} {number}"), work) {
            (Ended::Finished, Some(Ok(turned))) => {
                return judged(&wire, &host, &turned, crash, crashes)
                    .map_err(|error| format!("{label}: {error}"));
            }
            (Ended::Finished, Some(Err(error))) => {
                return Err(format!("{label}: epoch {number}: {error}"));
            }
            (Ended::Finished, None) => {
                return Err(format!("{label}: epoch {number} ended without a result"));
            }
            (Ended::Crashed | Ended::Stuck, _) => {
                crashes += 1;
                wire.hang_second.store(false, Ordering::Release);
            }
        }
    }
    Err(format!("{label}: still open after {EPOCHS_MAX} epochs"))
}

pub(crate) fn check(seed: u64) -> Result<(u64, u64), String> {
    let mut calls = 0;
    let mut digest = FNV_OFFSET;
    let mut wrong = Vec::new();
    for (log, crash) in CASES {
        match case(seed, log, crash) {
            Ok(made) => {
                calls += made;
                digest = fnv(digest, format!("{log:?} {crash:?} {made}").as_bytes());
            }
            Err(error) => wrong.push(error),
        }
    }
    assert!(wrong.len() <= CASES.len());
    if wrong.is_empty() {
        Ok((calls, digest))
    } else {
        Err(wrong.join(" | "))
    }
}
