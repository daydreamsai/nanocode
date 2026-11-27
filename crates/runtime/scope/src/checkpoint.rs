use std::sync::Arc;

use nanocode_durability_core::{StoreError, StoreFuture};
use nanocode_partitions::{Partition, Row};
use nanocode_sync::{Atom, OrdMap};
use nanocode_turnlog::parted::PartStore;

use crate::sim::{self, Mutation};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Manifest {
    pub scope: String,
    pub epoch: u64,
    pub seq: u64,
    pub parts: Vec<(String, String)>,
    pub changed_bytes: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Listing {
    Whole(Vec<(String, String)>),
    Changed {
        base: u64,
        put: Vec<(String, String)>,
        dropped: Vec<String>,
    },
}

impl Listing {
    #[must_use]
    pub fn encoded(&self) -> String {
        let encoded = match self {
            Self::Whole(parts) => serde_json::to_string(parts),
            Self::Changed { base, put, dropped } => serde_json::to_string(
                &serde_json::json!({ "base": base, "put": put, "dropped": dropped }),
            ),
        };
        encoded.expect("a checkpoint listing encodes as JSON")
    }

    pub fn decoded(scope: &str, seq: u64, encoded: &str) -> Result<Self, StoreError> {
        let wrong = |error: serde_json::Error| {
            StoreError::Backend(format!(
                "checkpoint listing of {scope} at {seq} does not decode: {error}"
            ))
        };
        if encoded.starts_with('[') {
            return serde_json::from_str(encoded).map(Self::Whole).map_err(wrong);
        }
        let mut row: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(encoded).map_err(wrong)?;
        let mut field = |name: &str| {
            row.remove(name).ok_or_else(|| {
                StoreError::Backend(format!(
                    "checkpoint listing of {scope} at {seq} has no {name}"
                ))
            })
        };
        Ok(Self::Changed {
            base: serde_json::from_value(field("base")?).map_err(wrong)?,
            put: serde_json::from_value(field("put")?).map_err(wrong)?,
            dropped: serde_json::from_value(field("dropped")?).map_err(wrong)?,
        })
    }

    #[must_use]
    pub fn applied(&self, before: &[(String, String)]) -> Vec<(String, String)> {
        match self {
            Self::Whole(parts) => parts.clone(),
            Self::Changed { put, dropped, .. } => {
                let mut named: std::collections::BTreeMap<&str, &str> = before
                    .iter()
                    .map(|(name, at)| (name.as_str(), at.as_str()))
                    .collect();
                for name in dropped {
                    named.remove(name.as_str());
                }
                for (name, at) in put {
                    named.insert(name, at);
                }
                named
                    .into_iter()
                    .map(|(name, at)| (name.to_owned(), at.to_owned()))
                    .collect()
            }
        }
    }
}

pub fn assemble(
    scope: &str,
    mut rows: Vec<(u64, u64, Listing)>,
) -> Result<Option<Manifest>, StoreError> {
    if !sim::mutated(Mutation::ChainInStoreOrder) {
        rows.sort_by_key(|(seq, _, _)| *seq);
    }
    let mut manifest: Option<Manifest> = None;
    for (seq, epoch, listing) in rows {
        let (parts, changed_bytes) = match (&listing, &manifest) {
            (Listing::Whole(_), _) => (listing.applied(&[]), 0),
            (Listing::Changed { base, .. }, Some(last)) if *base == last.seq => (
                listing.applied(&last.parts),
                last.changed_bytes + listing.encoded().len(),
            ),
            (Listing::Changed { base, .. }, last) => {
                return Err(StoreError::Backend(format!(
                    "checkpoint listing of {scope} at {seq} changes the one at {base}, but the chain reads {:?} before it",
                    last.as_ref().map(|last| last.seq)
                )));
            }
        };
        manifest = Some(Manifest {
            scope: scope.to_owned(),
            epoch,
            seq,
            parts,
            changed_bytes,
        });
    }
    Ok(manifest)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Fence {
    pub part: Partition,
    pub epoch: u64,
    pub holder: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Written {
    Landed,
    Stale,
}

pub trait Checkpoints: Send + Sync {
    fn latest<'a>(
        &'a self,
        scope: &'a str,
    ) -> StoreFuture<'a, Result<Option<Manifest>, StoreError>>;

    fn parts<'a>(
        &'a self,
        digests: &'a [String],
    ) -> StoreFuture<'a, Result<Vec<(String, String)>, StoreError>>;

    fn write<'a>(
        &'a self,
        fence: &'a Fence,
        manifest: &'a Manifest,
        listing: &'a Listing,
        parts: &'a [(String, String)],
    ) -> StoreFuture<'a, Result<Written, StoreError>>;
}

#[must_use]
pub fn digest(body: &str) -> String {
    nanocode_digest::blake3(body.as_bytes())
        .to_hex()
        .as_str()
        .to_owned()
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MemoryStats {
    pub manifests: u64,
    pub manifests_stale: u64,
    pub parts_written: u64,
    pub part_bytes_written: u64,
    pub listing_bytes_written: u64,
}

#[derive(Clone)]
struct Kept {
    manifest: Manifest,
    listing: Listing,
}

#[derive(Clone, Default)]
struct Shelf {
    manifests: OrdMap<Arc<str>, Arc<Vec<Kept>>>,
    parts: OrdMap<String, Arc<str>>,
    stats: MemoryStats,
}

pub struct MemoryCheckpoints<S: ?Sized> {
    fences: Arc<S>,
    shelf: Atom<Shelf>,
}

impl<S: PartStore + ?Sized> MemoryCheckpoints<S> {
    pub fn new(fences: Arc<S>) -> Self {
        Self {
            fences,
            shelf: Atom::default(),
        }
    }

    pub fn stats(&self) -> MemoryStats {
        self.shelf.read(|shelf| shelf.stats)
    }

    pub fn manifests(&self, scope: &str) -> Vec<Manifest> {
        self.shelf.read(|shelf| {
            shelf
                .manifests
                .get(scope)
                .map(|kept| kept.iter().map(|kept| kept.manifest.clone()).collect())
                .unwrap_or_default()
        })
    }

    pub fn part_bytes(&self) -> u64 {
        self.shelf.read(|shelf| {
            shelf
                .parts
                .values()
                .map(|body| u64::try_from(body.len()).unwrap_or(u64::MAX))
                .sum()
        })
    }

    fn admit(&self, row: &Row<String>, fence: &Fence) -> bool {
        sim::mutated(Mutation::CheckpointSkipsEpoch)
            || matches!(row, Row::Held { holder, epoch, .. } if *holder == fence.holder && *epoch == fence.epoch)
    }

    fn land(&self, manifest: &Manifest, listing: &Listing, parts: &[(String, String)]) -> Written {
        self.shelf.update(|shelf| {
            let kept = shelf
                .manifests
                .get(manifest.scope.as_str())
                .cloned()
                .unwrap_or_default();
            let last = kept.last().map(|kept| &kept.manifest);
            if let Some(last) = last {
                if last.seq == manifest.seq && last.epoch == manifest.epoch {
                    return Written::Landed;
                }
                if last.seq >= manifest.seq || last.epoch > manifest.epoch {
                    shelf.stats.manifests_stale += 1;
                    return Written::Stale;
                }
            }
            let based = match listing {
                Listing::Whole(_) => true,
                Listing::Changed { base, .. } => last.is_some_and(|last| last.seq == *base),
            };
            if !based {
                shelf.stats.manifests_stale += 1;
                return Written::Stale;
            }
            let assembled = listing.applied(last.map_or(&[][..], |last| &last.parts));
            let changed_bytes = match (listing, last) {
                (Listing::Changed { .. }, Some(last)) => {
                    last.changed_bytes + listing.encoded().len()
                }
                (Listing::Changed { .. } | Listing::Whole(_), _) => 0,
            };
            for (digest, body) in parts {
                shelf.stats.parts_written += 1;
                shelf.stats.part_bytes_written += u64::try_from(body.len()).unwrap_or(u64::MAX);
                shelf.parts.insert(digest.clone(), Arc::from(body.as_str()));
            }
            shelf.stats.listing_bytes_written +=
                u64::try_from(listing.encoded().len()).unwrap_or(u64::MAX);
            let mut next = kept.as_ref().clone();
            next.push(Kept {
                manifest: Manifest {
                    parts: assembled,
                    changed_bytes,
                    ..manifest.clone()
                },
                listing: listing.clone(),
            });
            shelf
                .manifests
                .insert(Arc::from(manifest.scope.as_str()), Arc::new(next));
            shelf.stats.manifests += 1;
            Written::Landed
        })
    }
}

fn chained(kept: &[Kept]) -> Vec<(u64, u64, Listing)> {
    let whole = kept
        .iter()
        .rposition(|kept| matches!(kept.listing, Listing::Whole(_)))
        .unwrap_or(0);
    let mut chain: Vec<(u64, u64, Listing)> = kept
        .iter()
        .skip(whole)
        .map(|kept| (kept.manifest.seq, kept.manifest.epoch, kept.listing.clone()))
        .collect();
    chain.sort_by_key(|(seq, _, _)| seq.to_string());
    chain
}

impl<S: PartStore + ?Sized> Checkpoints for MemoryCheckpoints<S> {
    fn latest<'a>(
        &'a self,
        scope: &'a str,
    ) -> StoreFuture<'a, Result<Option<Manifest>, StoreError>> {
        let chain = self.shelf.read(|shelf| {
            shelf
                .manifests
                .get(scope)
                .map(|kept| chained(kept))
                .unwrap_or_default()
        });
        Box::pin(async move { assemble(scope, chain) })
    }

    fn parts<'a>(
        &'a self,
        digests: &'a [String],
    ) -> StoreFuture<'a, Result<Vec<(String, String)>, StoreError>> {
        let found = self.shelf.read(|shelf| {
            digests
                .iter()
                .filter_map(|digest| {
                    shelf
                        .parts
                        .get(digest)
                        .map(|body| (digest.clone(), body.as_ref().to_owned()))
                })
                .collect()
        });
        Box::pin(async move { Ok(found) })
    }

    fn write<'a>(
        &'a self,
        fence: &'a Fence,
        manifest: &'a Manifest,
        listing: &'a Listing,
        parts: &'a [(String, String)],
    ) -> StoreFuture<'a, Result<Written, StoreError>> {
        Box::pin(async move {
            let row = self.fences.fence(fence.part).await?;
            if !self.admit(&row, fence) {
                self.shelf.update(|shelf| shelf.stats.manifests_stale += 1);
                return Ok(Written::Stale);
            }
            Ok(self.land(manifest, listing, parts))
        })
    }
}
