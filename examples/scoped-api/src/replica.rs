use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroU32,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use nanocode_durability_core::{Sql, SqlStatement, StoreError};
use nanocode_executor::{TcpListener, sleep, spawn, timeout};
use nanocode_partitions::{Member, Partition, Row, live, plan};
use nanocode_scope::{Leases, PartedLeases};
use nanocode_store_sql::PgSql;
use nanocode_sync::{Atom, Map, Slot, future::join_all, mpsc, oneshot};
use nanocode_turnlog::parted::{Append, PartStore, PartedLog, SqlParts, Verdict};
use serde_json::json;

use crate::http::{Conn, Request};

pub type Log = PartedLog<SqlParts<PgSql>>;

pub struct Settings {
    pub listen: String,
    pub parts: NonZeroU32,
    pub lease_ms: u64,
    pub member_ms: u64,
    pub grace_ms: u64,
    pub tick_ms: u64,
    pub renew_ms: u64,
    pub renew_deadline_ms: u64,
    pub batch: usize,
    pub queue: usize,
    pub forward_idle: usize,
}

#[derive(Clone, Debug, Default)]
pub struct Fold {
    head: u64,
    values: BTreeMap<String, String>,
}

#[derive(Clone, Debug)]
pub enum Refusal {
    NotOwner,
    Draining,
    Lost,
    Unowned,
    Unreachable,
    Busy,
    Conflict,
    Uncertain(String),
    Store(String),
    Gap { head: u64, seq: u64 },
    Body(String),
    Forward(String),
}

impl Refusal {
    const fn kind(&self) -> &'static str {
        match self {
            Self::NotOwner => "not_owner",
            Self::Draining => "draining",
            Self::Lost => "lost",
            Self::Unowned => "unowned",
            Self::Unreachable => "unreachable",
            Self::Busy => "busy",
            Self::Conflict => "conflict",
            Self::Uncertain(_) => "uncertain",
            Self::Store(_) => "store",
            Self::Gap { .. } => "gap",
            Self::Body(_) => "body",
            Self::Forward(_) => "forward",
        }
    }

    const fn status(&self) -> u16 {
        match self {
            Self::NotOwner | Self::Draining | Self::Lost | Self::Conflict => 409,
            Self::Unowned | Self::Unreachable | Self::Busy => 503,
            Self::Body(_) => 400,
            Self::Uncertain(_) | Self::Store(_) | Self::Gap { .. } | Self::Forward(_) => 500,
        }
    }

    fn body(&self) -> Vec<u8> {
        let detail = match self {
            Self::Uncertain(detail)
            | Self::Store(detail)
            | Self::Body(detail)
            | Self::Forward(detail) => detail.clone(),
            Self::Gap { head, seq } => format!("row {seq} after head {head}"),
            _ => String::new(),
        };
        json!({"error": self.kind(), "detail": detail})
            .to_string()
            .into_bytes()
    }
}

fn parsed(body: &str) -> Result<(String, String), Refusal> {
    let value: serde_json::Value =
        serde_json::from_str(body).map_err(|error| Refusal::Body(error.to_string()))?;
    match (
        value.get("k").and_then(|k| k.as_str()),
        value.get("v").and_then(|v| v.as_str()),
    ) {
        (Some(key), Some(value)) => Ok((key.to_owned(), value.to_owned())),
        _ => Err(Refusal::Body(
            "a body is {\"k\": string, \"v\": string}".to_owned(),
        )),
    }
}

impl Fold {
    fn apply(&mut self, seq: u64, body: &str) -> Result<(), Refusal> {
        if seq != self.head + 1 {
            return Err(Refusal::Gap {
                head: self.head,
                seq,
            });
        }
        let (key, value) = parsed(body)?;
        self.values.insert(key, value);
        self.head = seq;
        Ok(())
    }

    fn json(&self) -> Vec<u8> {
        json!({"head": self.head, "values": self.values})
            .to_string()
            .into_bytes()
    }
}

enum Work {
    Append {
        scope: Arc<str>,
        body: String,
        reply: oneshot::Sender<Result<u64, Refusal>>,
    },
    Read {
        scope: Arc<str>,
        reply: oneshot::Sender<Result<Arc<Fold>, Refusal>>,
    },
    Log {
        scope: Arc<str>,
        reply: oneshot::Sender<Result<Vec<(u64, String)>, Refusal>>,
    },
    Drain {
        reply: oneshot::Sender<()>,
    },
}

type View = Arc<Atom<Map<Arc<str>, Arc<Fold>>>>;

#[derive(Clone)]
struct Owner {
    epoch: u64,
    work: mpsc::Sender<Work>,
    view: View,
}

enum Command {
    Wake,
    Drain {
        reply: oneshot::Sender<serde_json::Value>,
    },
}

#[derive(Default)]
pub struct Counters {
    requests: AtomicU64,
    forwarded: AtomicU64,
    commits: AtomicU64,
    rows: AtomicU64,
    lost: AtomicU64,
    uncertain: AtomicU64,
    statement_timeouts: AtomicU64,
    lock_timeouts: AtomicU64,
    store_errors: AtomicU64,
    claims: AtomicU64,
    releases: AtomicU64,
    renew_failures: AtomicU64,
    renew_retries: AtomicU64,
}

impl Counters {
    fn bump(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    fn store(&self, error: &StoreError) {
        let text = error.to_string();
        eprintln!("store error: {text}");
        if text.contains("57014") {
            Self::bump(&self.statement_timeouts);
        } else if text.contains("55P03") {
            Self::bump(&self.lock_timeouts);
        }
        Self::bump(&self.store_errors);
    }

    fn json(&self) -> serde_json::Value {
        let read = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        json!({
            "requests": read(&self.requests),
            "forwarded": read(&self.forwarded),
            "commits": read(&self.commits),
            "rows": read(&self.rows),
            "lost": read(&self.lost),
            "uncertain": read(&self.uncertain),
            "statement_timeouts": read(&self.statement_timeouts),
            "lock_timeouts": read(&self.lock_timeouts),
            "store_errors": read(&self.store_errors),
            "claims": read(&self.claims),
            "releases": read(&self.releases),
            "renew_failures": read(&self.renew_failures),
            "renew_retries": read(&self.renew_retries),
        })
    }
}

struct Peer {
    idle: Vec<Slot<Conn>>,
}

impl Peer {
    /// An idle connection that is still open, dropping the closed ones it passes: a forward over
    /// a connection the peer closed fails, and its outcome would be unknown for an append.
    fn take(&self) -> Option<Conn> {
        self.idle.iter().filter_map(Slot::take).find(Conn::reusable)
    }

    fn put(&self, mut conn: Conn) {
        for slot in &self.idle {
            if slot.is_empty() {
                match slot.put(conn) {
                    Some(displaced) => conn = displaced,
                    None => return,
                }
            }
        }
    }
}

pub struct Shared {
    me: String,
    log: Arc<Log>,
    leases: PartedLeases<SqlParts<PgSql>>,
    sql: Arc<PgSql>,
    data: Arc<SqlParts<PgSql>>,
    settings: Settings,
    owners: Atom<BTreeMap<Partition, Owner>>,
    holders: Atom<BTreeMap<Partition, String>>,
    peers: Atom<BTreeMap<String, Arc<Peer>>>,
    commands: mpsc::Sender<Command>,
    draining: Atom<bool>,
    counters: Counters,
}

pub struct Stores {
    pub members: Arc<PgSql>,
    pub log: Arc<Log>,
    pub data: Arc<SqlParts<PgSql>>,
}

pub async fn serve(stores: Stores, settings: Settings) -> Result<(), String> {
    let listener = TcpListener::bind(settings.listen.as_str())
        .await
        .map_err(|error| format!("bind {}: {error}", settings.listen))?;
    let (commands, received) = mpsc::channel(settings.queue);
    let leases = PartedLeases::new(Arc::clone(&stores.log), &settings.listen, settings.lease_ms);
    let shared = Arc::new(Shared {
        me: settings.listen.clone(),
        log: stores.log,
        leases,
        sql: stores.members,
        data: stores.data,
        settings,
        owners: Atom::default(),
        holders: Atom::default(),
        peers: Atom::default(),
        commands,
        draining: Atom::new(false),
        counters: Counters::default(),
    });
    spawn(keep(Arc::clone(&shared), received));
    spawn(renew(Arc::clone(&shared)));
    loop {
        let (stream, _) = listener
            .accept()
            .await
            .map_err(|error| format!("accept: {error}"))?;
        let shared = Arc::clone(&shared);
        spawn(async move {
            if let Ok(conn) = Conn::new(stream) {
                connection(shared, conn).await;
            }
        });
    }
}

async fn connection(shared: Arc<Shared>, mut conn: Conn) {
    while let Ok(Some(request)) = conn.request().await {
        Counters::bump(&shared.counters.requests);
        let (status, body) = route(&shared, request).await;
        if conn.respond(status, &body).await.is_err() {
            return;
        }
    }
}

async fn route(shared: &Arc<Shared>, request: Request) -> (u16, Vec<u8>) {
    let path: Vec<&str> = request.path.trim_start_matches('/').split('/').collect();
    match (request.method.as_str(), path.as_slice()) {
        ("POST", ["scope", scope, "append"])
        | ("GET", ["scope", scope] | ["scope", scope, "log"])
            if !scope.is_empty() =>
        {
            scoped(shared, &request, scope).await
        }
        ("GET", ["admin", "held"]) => (200, held(shared).to_string().into_bytes()),
        ("GET", ["admin", "stats"]) => (200, shared.counters.json().to_string().into_bytes()),
        ("POST", ["admin", "drain"]) => {
            let (reply, answer) = oneshot::channel();
            if shared
                .commands
                .send(Command::Drain { reply })
                .await
                .is_err()
            {
                return (500, b"{\"error\":\"keeper stopped\"}".to_vec());
            }
            match answer.await {
                Ok(value) => (200, value.to_string().into_bytes()),
                Err(_) => (500, b"{\"error\":\"keeper stopped\"}".to_vec()),
            }
        }
        _ => (404, b"{\"error\":\"no route\"}".to_vec()),
    }
}

fn held(shared: &Shared) -> serde_json::Value {
    let held: Vec<(u32, u64)> = shared.owners.read(|owners| {
        owners
            .iter()
            .map(|(part, owner)| (part.0, owner.epoch))
            .collect()
    });
    json!({"node": shared.me, "held": held, "draining": shared.draining.read(|draining| *draining)})
}

async fn scoped(shared: &Arc<Shared>, request: &Request, scope: &str) -> (u16, Vec<u8>) {
    let part = shared.log.partition(scope);
    let owner = shared.owners.read(|owners| owners.get(&part).cloned());
    let answer = match owner {
        Some(owner) => local(&owner, request, scope).await,
        None => Err(Refusal::NotOwner),
    };
    match answer {
        Ok(body) => (200, body),
        Err(Refusal::NotOwner | Refusal::Draining | Refusal::Lost) if !request.forwarded => {
            forward(shared, part, request).await
        }
        Err(refusal) => (refusal.status(), refusal.body()),
    }
}

async fn local(owner: &Owner, request: &Request, scope: &str) -> Result<Vec<u8>, Refusal> {
    let scope: Arc<str> = Arc::from(scope);
    if request.method == "GET" && !request.path.ends_with("/log") {
        if let Some(fold) = owner.view.read(|view| view.get(&scope).cloned()) {
            return Ok(fold.json());
        }
        let (reply, answer) = oneshot::channel();
        submit(owner, Work::Read { scope, reply })?;
        return answer
            .await
            .map_err(|_| Refusal::Lost)?
            .map(|fold| fold.json());
    }
    if request.method == "GET" {
        let (reply, answer) = oneshot::channel();
        submit(owner, Work::Log { scope, reply })?;
        let rows = answer.await.map_err(|_| Refusal::Lost)??;
        return serde_json::to_vec(&rows).map_err(|error| Refusal::Store(error.to_string()));
    }
    let body = String::from_utf8(request.body.clone())
        .map_err(|_| Refusal::Body("not UTF-8".to_owned()))?;
    parsed(&body)?;
    let (reply, answer) = oneshot::channel();
    submit(owner, Work::Append { scope, body, reply })?;
    let seq = answer.await.map_err(|_| Refusal::Lost)??;
    Ok(json!({"seq": seq}).to_string().into_bytes())
}

fn submit(owner: &Owner, work: Work) -> Result<(), Refusal> {
    owner.work.try_send(work).map_err(|error| match error {
        mpsc::error::TrySendError::Full(_) => Refusal::Busy,
        mpsc::error::TrySendError::Closed(_) => Refusal::Lost,
    })
}

async fn holder(shared: &Shared, part: Partition, cached: bool) -> Result<Option<String>, Refusal> {
    if cached && let Some(holder) = shared.holders.read(|holders| holders.get(&part).cloned()) {
        return Ok(Some(holder));
    }
    let row = shared.data.fence(part).await.map_err(|error| {
        shared.counters.store(&error);
        Refusal::Store(error.to_string())
    })?;
    let holder = row.holder().filter(|holder| **holder != shared.me).cloned();
    shared.holders.update(|holders| match &holder {
        Some(holder) => holders.insert(part, holder.clone()),
        None => holders.remove(&part),
    });
    Ok(holder)
}

async fn forward(shared: &Arc<Shared>, part: Partition, request: &Request) -> (u16, Vec<u8>) {
    let mut last = (Refusal::Unowned.status(), Refusal::Unowned.body());
    for cached in [true, false] {
        let holder = match holder(shared, part, cached).await {
            Ok(Some(holder)) => holder,
            Ok(None) => {
                wake(shared);
                last = (Refusal::Unowned.status(), Refusal::Unowned.body());
                continue;
            }
            Err(refusal) => return (refusal.status(), refusal.body()),
        };
        Counters::bump(&shared.counters.forwarded);
        match send(shared, &holder, request).await {
            Ok((status, body)) if status != 409 && status != 503 => return (status, body),
            Ok(answer) => last = answer,
            Err(error) => {
                let refusal = Refusal::Forward(error);
                last = (refusal.status(), refusal.body());
            }
        }
        shared.holders.update(|holders| holders.remove(&part));
    }
    last
}

fn wake(shared: &Shared) {
    match shared.commands.try_send(Command::Wake) {
        Ok(()) | Err(mpsc::error::TrySendError::Full(_) | mpsc::error::TrySendError::Closed(_)) => {
        }
    }
}

fn peer(shared: &Shared, address: &str) -> Arc<Peer> {
    if let Some(peer) = shared.peers.read(|peers| peers.get(address).cloned()) {
        return peer;
    }
    let made = Arc::new(Peer {
        idle: (0..shared.settings.forward_idle)
            .map(|_| Slot::default())
            .collect(),
    });
    shared.peers.update(|peers| {
        Arc::clone(
            peers
                .entry(address.to_owned())
                .or_insert_with(|| Arc::clone(&made)),
        )
    })
}

async fn send(shared: &Shared, address: &str, request: &Request) -> Result<(u16, Vec<u8>), String> {
    let peer = peer(shared, address);
    let mut conn = match peer.take() {
        Some(conn) => conn,
        None => match Conn::connect(address).await {
            Ok(conn) => conn,
            Err(_) => return Ok((Refusal::Unreachable.status(), Refusal::Unreachable.body())),
        },
    };
    let bound = Duration::from_millis(shared.settings.lease_ms);
    let response = timeout(
        bound,
        conn.call(&request.method, &request.path, true, &request.body),
    )
    .await
    .map_err(|_| format!("{address}: no answer within lease_ms"))?
    .map_err(|error| format!("{address}: {error}"))?;
    peer.put(conn);
    Ok((response.status, response.body))
}

const HEARTBEAT: &str = "INSERT INTO scoped_api_member (node, seen_ms) VALUES ($1::text, CAST(extract(epoch FROM clock_timestamp()) * 1000 AS BIGINT)) ON CONFLICT (node) DO UPDATE SET seen_ms = EXCLUDED.seen_ms";
const MEMBERS: &str = "SELECT node, CAST(seen_ms AS TEXT), CAST(CAST(extract(epoch FROM clock_timestamp()) * 1000 AS BIGINT) AS TEXT) FROM scoped_api_member";
const LEAVE: &str = "DELETE FROM scoped_api_member WHERE node = $1::text";

fn start(shared: &Arc<Shared>, part: Partition, epoch: u64) -> Owner {
    let (work, inbox) = mpsc::channel(shared.settings.queue);
    let view: View = Arc::default();
    let owning = Owning {
        shared: Arc::clone(shared),
        part,
        epoch,
        view: Arc::clone(&view),
        folds: BTreeMap::new(),
    };
    spawn(owning.run(inbox));
    Owner { epoch, work, view }
}

struct Owning {
    shared: Arc<Shared>,
    part: Partition,
    epoch: u64,
    view: View,
    folds: BTreeMap<Arc<str>, Arc<Fold>>,
}

enum Next {
    Serve,
    Lost,
    Drained(Vec<oneshot::Sender<()>>),
}

type Appending = (Arc<str>, String, oneshot::Sender<Result<u64, Refusal>>);
type Acked = (Arc<str>, u64, oneshot::Sender<Result<u64, Refusal>>);
type Logging = (
    Arc<str>,
    oneshot::Sender<Result<Vec<(u64, String)>, Refusal>>,
);

fn refuse(work: Work, refusal: &Refusal) {
    match work {
        Work::Append { reply, .. } => drop(reply.send(Err(refusal.clone()))),
        Work::Read { reply, .. } => drop(reply.send(Err(refusal.clone()))),
        Work::Log { reply, .. } => drop(reply.send(Err(refusal.clone()))),
        Work::Drain { reply } => {
            let _ = reply.send(());
        }
    }
}

impl Owning {
    async fn run(mut self, mut inbox: mpsc::Receiver<Work>) {
        let mut works = Vec::with_capacity(self.shared.settings.batch);
        loop {
            if inbox
                .recv_many(&mut works, self.shared.settings.batch)
                .await
                == 0
            {
                return;
            }
            let (refusal, drained) = match self.step(std::mem::take(&mut works)).await {
                Next::Serve => continue,
                Next::Lost => (Refusal::Lost, Vec::new()),
                Next::Drained(drained) => (Refusal::Draining, drained),
            };
            inbox.close();
            while let Ok(work) = inbox.try_recv() {
                refuse(work, &refusal);
            }
            for reply in drained {
                let _ = reply.send(());
            }
            return;
        }
    }

    async fn step(&mut self, works: Vec<Work>) -> Next {
        let (mut appends, mut reads, mut logs, mut drains) =
            (Vec::new(), Vec::new(), Vec::new(), Vec::new());
        for work in works {
            match work {
                Work::Append { scope, body, reply } => appends.push((scope, body, reply)),
                Work::Read { scope, reply } => reads.push((scope, reply)),
                Work::Log { scope, reply } => logs.push((scope, reply)),
                Work::Drain { reply } => drains.push(reply),
            }
        }
        let wanted: BTreeSet<Arc<str>> = appends
            .iter()
            .map(|(scope, ..)| scope)
            .chain(reads.iter().map(|(scope, _)| scope))
            .filter(|scope| !self.folds.contains_key(*scope))
            .cloned()
            .collect();
        let failed = self.load(wanted).await;
        for (scope, reply) in reads {
            let answer = match failed.get(&scope) {
                Some(refusal) => Err(refusal.clone()),
                None => Ok(Arc::clone(&self.folds[&scope])),
            };
            drop(reply.send(answer));
        }
        if !appends.is_empty() && self.commit(appends, &failed).await {
            for (_, reply) in logs {
                drop(reply.send(Err(Refusal::Lost)));
            }
            return Next::Lost;
        }
        self.scan(logs).await;
        if drains.is_empty() {
            Next::Serve
        } else {
            Next::Drained(drains)
        }
    }

    async fn load(&mut self, wanted: BTreeSet<Arc<str>>) -> BTreeMap<Arc<str>, Refusal> {
        let store = &self.shared.data;
        let loaded = join_all(wanted.iter().map(|scope| store.load(self.part, scope))).await;
        let mut failed = BTreeMap::new();
        for (scope, rows) in wanted.into_iter().zip(loaded) {
            let folded = rows
                .map_err(|error| {
                    self.shared.counters.store(&error);
                    Refusal::Store(error.to_string())
                })
                .and_then(|rows| {
                    let mut fold = Fold::default();
                    rows.iter()
                        .try_for_each(|(seq, body)| fold.apply(*seq, body))?;
                    Ok(fold)
                });
            match folded {
                Ok(fold) => {
                    self.folds.insert(scope, Arc::new(fold));
                }
                Err(refusal) => {
                    failed.insert(scope, refusal);
                }
            }
        }
        self.publish(self.folds.keys().cloned().collect());
        failed
    }

    fn publish(&self, scopes: Vec<Arc<str>>) {
        let folds = &self.folds;
        self.view.update(|view| {
            for scope in &scopes {
                match folds.get(scope) {
                    Some(fold) => view.insert(Arc::clone(scope), Arc::clone(fold)),
                    None => view.remove(scope),
                };
            }
        });
    }

    async fn commit(
        &mut self,
        appends: Vec<Appending>,
        failed: &BTreeMap<Arc<str>, Refusal>,
    ) -> bool {
        let mut grouped: BTreeMap<Arc<str>, Vec<(u64, String)>> = BTreeMap::new();
        let mut replies = Vec::with_capacity(appends.len());
        for (scope, body, reply) in appends {
            if let Some(refusal) = failed.get(&scope) {
                drop(reply.send(Err(refusal.clone())));
                continue;
            }
            let head = self.folds.get(&scope).map(|fold| fold.head);
            let Some(head) = head else {
                panic!("an append's scope is loaded before its commit");
            };
            let rows = grouped.entry(Arc::clone(&scope)).or_default();
            let seq = head + rows.len() as u64 + 1;
            rows.push((seq, body));
            replies.push((scope, seq, reply));
        }
        if grouped.is_empty() {
            return false;
        }
        let batch: Vec<Append> = grouped
            .into_iter()
            .map(|(scope, rows)| Append { scope, rows })
            .collect();
        let answer = self
            .shared
            .data
            .commit(self.part, Some(self.epoch), &batch)
            .await;
        match answer {
            Ok(verdicts) => {
                self.landed(&batch, &verdicts, replies);
                false
            }
            Err(StoreError::Fenced) => {
                for (_, _, reply) in replies {
                    drop(reply.send(Err(Refusal::Lost)));
                }
                self.lost();
                true
            }
            Err(error) => {
                self.shared.counters.store(&error);
                Counters::bump(&self.shared.counters.uncertain);
                for append in &batch {
                    self.folds.remove(&append.scope);
                }
                self.publish(
                    batch
                        .iter()
                        .map(|append| Arc::clone(&append.scope))
                        .collect(),
                );
                for (_, _, reply) in replies {
                    drop(reply.send(Err(Refusal::Uncertain(error.to_string()))));
                }
                false
            }
        }
    }

    fn landed(&mut self, batch: &[Append], verdicts: &[Verdict], replies: Vec<Acked>) {
        assert_eq!(batch.len(), verdicts.len(), "one verdict per append");
        let mut conflicted = BTreeSet::new();
        let mut rows = 0;
        for (append, verdict) in batch.iter().zip(verdicts) {
            match verdict {
                Verdict::Committed => {
                    let Some(fold) = self.folds.get_mut(&append.scope) else {
                        panic!("a committed scope stays loaded");
                    };
                    let fold = Arc::make_mut(fold);
                    for (seq, body) in &append.rows {
                        if let Err(refusal) = fold.apply(*seq, body) {
                            panic!("a vetted append folds: {refusal:?}");
                        }
                    }
                    rows += append.rows.len() as u64;
                }
                Verdict::Conflict => {
                    self.folds.remove(&append.scope);
                    conflicted.insert(Arc::clone(&append.scope));
                }
            }
        }
        Counters::bump(&self.shared.counters.commits);
        self.shared.counters.rows.fetch_add(rows, Ordering::Relaxed);
        self.publish(
            batch
                .iter()
                .map(|append| Arc::clone(&append.scope))
                .collect(),
        );
        for (scope, seq, reply) in replies {
            let answer = if conflicted.contains(&scope) {
                Err(Refusal::Conflict)
            } else {
                Ok(seq)
            };
            drop(reply.send(answer));
        }
    }

    async fn scan(&self, logs: Vec<Logging>) {
        let store = &self.shared.data;
        let scanned = join_all(
            logs.iter()
                .map(|(scope, _)| store.scan(self.part, scope, 1, u64::MAX)),
        )
        .await;
        for ((_, reply), rows) in logs.into_iter().zip(scanned) {
            drop(reply.send(rows.map_err(|error| {
                self.shared.counters.store(&error);
                Refusal::Store(error.to_string())
            })));
        }
    }

    fn lost(&self) {
        Counters::bump(&self.shared.counters.lost);
        self.shared.log.forget(self.part);
        let (part, epoch) = (self.part, self.epoch);
        self.shared.owners.update(|owners| {
            if owners.get(&part).is_some_and(|owner| owner.epoch == epoch) {
                owners.remove(&part);
            }
        });
        wake(&self.shared);
    }
}

async fn keep(shared: Arc<Shared>, mut commands: mpsc::Receiver<Command>) {
    let tick = Duration::from_millis(shared.settings.tick_ms);
    loop {
        let mut drains = Vec::new();
        match timeout(tick, commands.recv()).await {
            Ok(None) => return,
            Ok(Some(Command::Drain { reply })) => drains.push(reply),
            Ok(Some(Command::Wake)) | Err(_) => {}
        }
        while let Ok(command) = commands.try_recv() {
            if let Command::Drain { reply } = command {
                drains.push(reply);
            }
        }
        if !drains.is_empty() {
            let answer = drain(&shared).await;
            for reply in drains {
                drop(reply.send(answer.clone()));
            }
        }
        if shared.draining.read(|draining| *draining) {
            continue;
        }
        if let Err(error) = balance(&shared).await {
            shared.counters.store(&error);
        }
    }
}

fn number(cells: &[Option<String>], index: usize) -> Result<u64, StoreError> {
    cells
        .get(index)
        .and_then(Option::as_deref)
        .and_then(|text| text.parse().ok())
        .ok_or_else(|| StoreError::Backend(format!("membership row lacks number {index}")))
}

async fn heartbeat(shared: &Shared) -> Result<(u64, Vec<Member<String>>), StoreError> {
    let statements = [
        SqlStatement {
            sql: HEARTBEAT,
            params: vec![shared.me.as_str()],
        },
        SqlStatement {
            sql: MEMBERS,
            params: Vec::new(),
        },
    ];
    let answer = shared
        .sql
        .transaction("scoped_api_heartbeat", &statements)
        .await?;
    let rows = answer.get(1).cloned().unwrap_or_default();
    let mut now = None;
    let mut members = Vec::with_capacity(rows.len());
    for cells in &rows {
        let node = cells
            .first()
            .and_then(Option::as_deref)
            .ok_or_else(|| StoreError::Backend("membership row lacks a node".to_owned()))?;
        members.push(Member {
            node: node.to_owned(),
            seen_at: number(cells, 1)?,
        });
        now = Some(number(cells, 2)?);
    }
    let now =
        now.ok_or_else(|| StoreError::Backend("membership lacks this node's own row".to_owned()))?;
    Ok((now, members))
}

async fn balance(shared: &Arc<Shared>) -> Result<(), StoreError> {
    let (now, members) = heartbeat(shared).await?;
    let live = live(&members, now, shared.settings.member_ms);
    let held: BTreeMap<Partition, u64> = shared.owners.read(|owners| {
        owners
            .iter()
            .map(|(part, owner)| (*part, owner.epoch))
            .collect()
    });
    let planned = plan(
        &shared.me,
        shared.settings.parts,
        &live,
        &held,
        &BTreeMap::new(),
        now,
        shared.settings.grace_ms,
    );
    join_all(
        planned
            .release
            .iter()
            .map(|token| release(shared, token.partition)),
    )
    .await;
    join_all(planned.claim.iter().map(|part| claim(shared, *part, now))).await;
    Ok(())
}

async fn claim(shared: &Arc<Shared>, part: Partition, now: u64) {
    let row: Row<String> = match shared.log.store().fence(part).await {
        Ok(row) => row,
        Err(error) => return shared.counters.store(&error),
    };
    if !row.claimable(&shared.me, now, shared.settings.grace_ms) {
        return;
    }
    match Leases::claim(&shared.leases, part).await {
        Ok(epoch) => {
            Counters::bump(&shared.counters.claims);
            let owner = start(shared, part, epoch);
            shared
                .owners
                .update(|owners| owners.insert(part, owner.clone()));
            shared.holders.update(|holders| holders.remove(&part));
        }
        Err(StoreError::Fenced) => {}
        Err(error) => shared.counters.store(&error),
    }
}

/// Renews every held lease each renew_ms, on its own task and lease pool so a slow heartbeat or
/// claim never delays it. Each attempt has renew_deadline_ms; one that misses it or fails is
/// retried at once, at the same epochs, on a fresh connection (the pool discards the stuck one),
/// until one lands or the round is over. A lease is lost only when the store says its epoch moved.
async fn renew(shared: Arc<Shared>) {
    let every = Duration::from_millis(shared.settings.renew_ms);
    let bound = Duration::from_millis(shared.settings.renew_deadline_ms);
    let pause = Duration::from_millis(shared.settings.tick_ms).min(bound);
    loop {
        let round = Instant::now();
        loop {
            match timeout(bound, Leases::renew(&shared.leases)).await {
                Ok(Ok(lost)) => {
                    for part in lost {
                        Counters::bump(&shared.counters.renew_failures);
                        shared.owners.update(|owners| owners.remove(&part));
                    }
                    break;
                }
                Ok(Err(error)) => {
                    shared.counters.store(&error);
                    Counters::bump(&shared.counters.renew_retries);
                    sleep(pause).await;
                }
                Err(_) => Counters::bump(&shared.counters.renew_retries),
            }
            if round.elapsed() >= every {
                break;
            }
        }
        sleep(every.saturating_sub(round.elapsed())).await;
    }
}

async fn release(shared: &Shared, part: Partition) {
    let owner = shared.owners.update(|owners| owners.remove(&part));
    shared.holders.update(|holders| holders.remove(&part));
    let bound = Duration::from_millis(shared.settings.lease_ms);
    if let Some(owner) = owner {
        let (reply, done) = oneshot::channel();
        let drained = timeout(bound, async {
            if owner.work.send(Work::Drain { reply }).await.is_ok() {
                drop(done.await);
            }
        })
        .await;
        if drained.is_err() {
            return Counters::bump(&shared.counters.store_errors);
        }
    }
    match Leases::release(&shared.leases, part).await {
        Ok(()) => Counters::bump(&shared.counters.releases),
        Err(error) => shared.counters.store(&error),
    }
}

async fn drain(shared: &Arc<Shared>) -> serde_json::Value {
    let started = Instant::now();
    shared.draining.set(true);
    let left = shared
        .sql
        .transaction(
            "scoped_api_leave",
            &[SqlStatement {
                sql: LEAVE,
                params: vec![shared.me.as_str()],
            }],
        )
        .await;
    if let Err(error) = &left {
        shared.counters.store(error);
    }
    let parts: Vec<Partition> = shared
        .owners
        .read(|owners| owners.keys().copied().collect());
    join_all(parts.iter().map(|part| release(shared, *part))).await;
    json!({
        "released": parts.iter().map(|part| part.0).collect::<Vec<_>>(),
        "left": left.is_ok(),
        "ms": started.elapsed().as_secs_f64() * 1000.0,
    })
}
