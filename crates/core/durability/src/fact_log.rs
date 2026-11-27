use std::collections::BTreeMap;

use serde_json::value::RawValue;

use crate::{
    Digest, DurableFold, FactSink, FoldError, Folded, OwnedFact, Snapshot, StoreError, StoreFuture,
    StoredFact,
};

pub struct Replay {
    pub snapshot: Option<Snapshot>,
    pub facts: Vec<(u64, StoredFact)>,
    pub blobs: BTreeMap<Digest, Box<RawValue>>,
}

pub type Append = Box<dyn FnOnce(Replay, &mut dyn FactSink) -> Result<u64, FoldError> + Send>;

pub trait FactLogs: Send + Sync {
    fn scoped(&self, state: &str, turns: bool) -> std::sync::Arc<dyn FactLog>;
}

pub trait FactLog: Send + Sync {
    fn replay(&self) -> StoreFuture<'_, Result<Replay, StoreError>>;

    fn replay_cached<'a>(
        &'a self,
        _family: &'a str,
        _format: u32,
    ) -> StoreFuture<'a, Result<Replay, StoreError>> {
        self.replay()
    }

    fn append(&self, after: u64, write: Append) -> StoreFuture<'_, Result<u64, StoreError>>;

    fn remember(&self, _snapshot: Snapshot) {}
}

struct Forward<'a>(&'a mut dyn FactSink);

impl FactSink for Forward<'_> {
    fn next(&self) -> u64 {
        self.0.next()
    }

    fn put_blob(&mut self, digest: &Digest, json: &RawValue) -> Result<(), FoldError> {
        self.0.put_blob(digest, json)
    }

    fn put_interned(&mut self, digest: &Digest, json: &RawValue) -> Result<(), FoldError> {
        self.0.put_interned(digest, json)
    }

    fn append(&mut self, fact: StoredFact) -> Result<u64, FoldError> {
        self.0.append(fact)
    }

    fn cache_due(&self) -> bool {
        self.0.cache_due()
    }

    fn cache(&mut self, snapshot: &Snapshot) -> Result<Vec<(u64, StoredFact)>, FoldError> {
        self.0.cache(snapshot)
    }
}

fn folded<F: DurableFold>(replay: Replay) -> Result<Folded<F>, FoldError> {
    let Replay {
        snapshot,
        facts,
        blobs,
    } = replay;
    let blob = |digest: &Digest| {
        blobs
            .get(digest)
            .map(|json| &**json)
            .ok_or(FoldError::MissingBlob { digest: *digest })
    };
    Folded::open(snapshot, facts, blob).map(|(folded, _)| folded)
}

fn caught_up<F: DurableFold>(
    folded: Folded<F>,
    mut unseen: Replay,
) -> Result<Folded<F>, FoldError> {
    assert!(
        unseen.snapshot.is_none(),
        "the facts a fold has not seen come without a snapshot"
    );
    assert!(
        unseen.facts.iter().all(|(seq, _)| *seq > folded.seq()),
        "the facts a fold has not seen follow it"
    );
    if unseen.facts.is_empty() {
        return Ok(folded);
    }
    unseen.snapshot = Some(folded.snapshot()?);
    self::folded(unseen)
}

pub async fn fold_facts<F: DurableFold>(log: &dyn FactLog) -> Result<Folded<F>, StoreError> {
    folded(log.replay_cached(F::FAMILY, F::FORMAT).await?).map_err(StoreError::from)
}

pub async fn commit_fact<F>(
    log: &dyn FactLog,
    folded: Folded<F>,
    fact: OwnedFact<F::Decision>,
) -> Result<(Folded<F>, u64), StoreError>
where
    F: DurableFold + Send + 'static,
    F::Decision: Send + 'static,
{
    let (slot, mut committed) = nanocode_sync::oneshot::channel();
    let after = folded.seq();
    let seq = log
        .append(
            after,
            Box::new(move |unseen, sink| {
                let folded = caught_up(folded, unseen)?;
                let mut forward = Forward(sink);
                let (mut folded, seq) = folded.commit(&mut forward, fact)?;
                if forward.cache_due() {
                    folded = folded.cache(&mut forward)?;
                }
                drop(slot.send(folded));
                Ok(seq)
            }),
        )
        .await?;
    let folded = committed
        .try_recv()
        .ok()
        .expect("an append that returned a seq committed its fold");
    Ok((folded, seq))
}
