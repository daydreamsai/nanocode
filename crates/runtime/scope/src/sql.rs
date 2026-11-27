use std::sync::Arc;

use nanocode_durability_core::{
    Migrated, Migration, Sql, SqlAccess, SqlRow, SqlStatement, StoreError, StoreFuture,
};

use crate::{
    checkpoint::{Checkpoints, Fence, Listing, Manifest, Written, assemble},
    sim::{self, Mutation},
};

type Dialects = [&'static str; 2];

const COMPONENT: &str = "scope_checkpoint";

const CREATE_MANIFEST: Dialects = [
    "CREATE TABLE IF NOT EXISTS nanocode_checkpoint_manifest (scope TEXT NOT NULL, seq INTEGER NOT NULL, epoch INTEGER NOT NULL, parts TEXT NOT NULL, PRIMARY KEY (scope, seq)) WITHOUT ROWID",
    "CREATE TABLE IF NOT EXISTS nanocode_checkpoint_manifest (scope TEXT NOT NULL, seq BIGINT NOT NULL, epoch BIGINT NOT NULL, parts TEXT NOT NULL, PRIMARY KEY (scope, seq))",
];
const CREATE_PART: Dialects = [
    "CREATE TABLE IF NOT EXISTS nanocode_checkpoint_part (digest TEXT PRIMARY KEY, body TEXT NOT NULL) WITHOUT ROWID",
    "CREATE TABLE IF NOT EXISTS nanocode_checkpoint_part (digest TEXT PRIMARY KEY, body TEXT NOT NULL)",
];
const MIGRATIONS: [Migration; 1] = [Migration {
    statements: [
        &[CREATE_MANIFEST[0], CREATE_PART[0]],
        &[CREATE_MANIFEST[1], CREATE_PART[1]],
    ],
    adopt: None,
}];
const LATEST: Dialects = [
    "SELECT CAST(seq AS TEXT), CAST(epoch AS TEXT), parts FROM nanocode_checkpoint_manifest WHERE scope = ?1 AND seq >= COALESCE((SELECT seq FROM nanocode_checkpoint_manifest WHERE scope = ?1 AND parts LIKE '[%' ORDER BY seq DESC LIMIT 1), 0) ORDER BY nanocode_checkpoint_manifest.seq",
    "SELECT CAST(seq AS TEXT), CAST(epoch AS TEXT), parts FROM nanocode_checkpoint_manifest WHERE scope = $1::text AND seq >= COALESCE((SELECT seq FROM nanocode_checkpoint_manifest WHERE scope = $1::text AND parts LIKE '[%' ORDER BY seq DESC LIMIT 1), 0) ORDER BY nanocode_checkpoint_manifest.seq",
];
const PARTS: Dialects = [
    "SELECT digest, body FROM nanocode_checkpoint_part WHERE digest IN (SELECT value FROM json_each(?1))",
    "SELECT digest, body FROM nanocode_checkpoint_part WHERE digest IN (SELECT jsonb_array_elements_text($1::jsonb))",
];
const FENCED: Dialects = [
    "EXISTS (SELECT 1 FROM nanocode_log_fence WHERE part = CAST(?3 AS INTEGER) AND epoch = CAST(?4 AS INTEGER) AND holder = ?5)",
    "EXISTS (SELECT 1 FROM nanocode_log_fence WHERE part = CAST($3::text AS INTEGER) AND epoch = CAST($4::text AS BIGINT) AND holder = $5::text)",
];
const PUT_PART: [Dialects; 2] = [
    [
        "INSERT INTO nanocode_checkpoint_part (digest, body) SELECT ?1, ?2 WHERE EXISTS (SELECT 1 FROM nanocode_log_fence WHERE part = CAST(?3 AS INTEGER) AND epoch = CAST(?4 AS INTEGER) AND holder = ?5) ON CONFLICT (digest) DO NOTHING",
        "INSERT INTO nanocode_checkpoint_part (digest, body) SELECT $1::text, $2::text WHERE EXISTS (SELECT 1 FROM nanocode_log_fence WHERE part = CAST($3::text AS INTEGER) AND epoch = CAST($4::text AS BIGINT) AND holder = $5::text) ON CONFLICT (digest) DO NOTHING",
    ],
    [
        "INSERT INTO nanocode_checkpoint_part (digest, body) SELECT ?1, ?2 WHERE ?3 IS NOT NULL AND ?4 IS NOT NULL AND ?5 IS NOT NULL ON CONFLICT (digest) DO NOTHING",
        "INSERT INTO nanocode_checkpoint_part (digest, body) SELECT $1::text, $2::text WHERE $3::text IS NOT NULL AND $4::text IS NOT NULL AND $5::text IS NOT NULL ON CONFLICT (digest) DO NOTHING",
    ],
];
const PUT_MANIFEST: [Dialects; 2] = [
    [
        "INSERT INTO nanocode_checkpoint_manifest (scope, seq, epoch, parts) SELECT ?1, CAST(?2 AS INTEGER), CAST(?4 AS INTEGER), ?6 WHERE EXISTS (SELECT 1 FROM nanocode_log_fence WHERE part = CAST(?3 AS INTEGER) AND epoch = CAST(?4 AS INTEGER) AND holder = ?5) AND NOT EXISTS (SELECT 1 FROM nanocode_checkpoint_manifest WHERE scope = ?1 AND (seq >= CAST(?2 AS INTEGER) OR epoch > CAST(?4 AS INTEGER))) AND (?7 = '' OR (EXISTS (SELECT 1 FROM nanocode_checkpoint_manifest WHERE scope = ?1 AND seq = CAST(?7 AS INTEGER)) AND NOT EXISTS (SELECT 1 FROM nanocode_checkpoint_manifest WHERE scope = ?1 AND seq > CAST(?7 AS INTEGER))))",
        "INSERT INTO nanocode_checkpoint_manifest (scope, seq, epoch, parts) SELECT $1::text, CAST($2::text AS BIGINT), CAST($4::text AS BIGINT), $6::text WHERE EXISTS (SELECT 1 FROM nanocode_log_fence WHERE part = CAST($3::text AS INTEGER) AND epoch = CAST($4::text AS BIGINT) AND holder = $5::text) AND NOT EXISTS (SELECT 1 FROM nanocode_checkpoint_manifest WHERE scope = $1::text AND (seq >= CAST($2::text AS BIGINT) OR epoch > CAST($4::text AS BIGINT))) AND ($7::text = '' OR (EXISTS (SELECT 1 FROM nanocode_checkpoint_manifest WHERE scope = $1::text AND seq = CAST(NULLIF($7::text, '') AS BIGINT)) AND NOT EXISTS (SELECT 1 FROM nanocode_checkpoint_manifest WHERE scope = $1::text AND seq > CAST(NULLIF($7::text, '') AS BIGINT))))",
    ],
    [
        "INSERT INTO nanocode_checkpoint_manifest (scope, seq, epoch, parts) SELECT ?1, CAST(?2 AS INTEGER), CAST(?4 AS INTEGER), ?6 WHERE ?3 IS NOT NULL AND ?5 IS NOT NULL AND ?7 IS NOT NULL AND NOT EXISTS (SELECT 1 FROM nanocode_checkpoint_manifest WHERE scope = ?1 AND seq >= CAST(?2 AS INTEGER))",
        "INSERT INTO nanocode_checkpoint_manifest (scope, seq, epoch, parts) SELECT $1::text, CAST($2::text AS BIGINT), CAST($4::text AS BIGINT), $6::text WHERE $3::text IS NOT NULL AND $5::text IS NOT NULL AND $7::text IS NOT NULL AND NOT EXISTS (SELECT 1 FROM nanocode_checkpoint_manifest WHERE scope = $1::text AND seq >= CAST($2::text AS BIGINT))",
    ],
];
const LANDED: Dialects = [
    "SELECT CAST(epoch AS TEXT) FROM nanocode_checkpoint_manifest WHERE scope = ?1 AND seq = CAST(?2 AS INTEGER)",
    "SELECT CAST(epoch AS TEXT) FROM nanocode_checkpoint_manifest WHERE scope = $1::text AND seq = CAST($2::text AS BIGINT)",
];

pub struct SqlCheckpoints<H: ?Sized> {
    host: Arc<H>,
}

fn text(row: &SqlRow, at: usize, what: &str) -> Result<String, StoreError> {
    row.get(at)
        .cloned()
        .flatten()
        .ok_or_else(|| StoreError::Backend(format!("a checkpoint row has no {what}")))
}

fn number(row: &SqlRow, at: usize, what: &str) -> Result<u64, StoreError> {
    let value = text(row, at, what)?;
    value.parse().map_err(|_| {
        StoreError::Backend(format!("a checkpoint row's {what} {value} is not a number"))
    })
}

impl<H: Sql + ?Sized> SqlCheckpoints<H> {
    pub fn new(host: Arc<H>, migrated: &Migrated) -> Self {
        migrated.assert_opens(COMPONENT, &MIGRATIONS);
        Self { host }
    }

    pub async fn migrate(host: &H) -> Result<Migrated, StoreError> {
        Migrated::migrate(host, COMPONENT, &MIGRATIONS).await
    }

    pub async fn check(host: &H) -> Result<Migrated, StoreError> {
        Migrated::check(host, COMPONENT, &MIGRATIONS).await
    }

    fn pick(&self, statements: &Dialects) -> &'static str {
        self.host.dialect().pick(statements)
    }

    async fn read(&self, statement: SqlStatement<'_>) -> Result<Vec<SqlRow>, StoreError> {
        let mut answers = self
            .host
            .run(
                SqlAccess::Read,
                "scope checkpoint read",
                std::slice::from_ref(&statement),
            )
            .await?;
        answers
            .pop()
            .ok_or_else(|| StoreError::Backend("a checkpoint read returned no result".into()))
    }

    async fn latest_of(&self, scope: &str) -> Result<Option<Manifest>, StoreError> {
        let rows = self
            .read(SqlStatement {
                sql: self.pick(&LATEST),
                params: vec![scope],
            })
            .await?;
        let mut chain = Vec::with_capacity(rows.len());
        for row in &rows {
            let seq = number(row, 0, "seq")?;
            let listing = Listing::decoded(scope, seq, &text(row, 2, "parts")?)?;
            chain.push((seq, number(row, 1, "epoch")?, listing));
        }
        assemble(scope, chain)
    }

    async fn parts_of(&self, digests: &[String]) -> Result<Vec<(String, String)>, StoreError> {
        if digests.is_empty() {
            return Ok(Vec::new());
        }
        let wanted = serde_json::to_string(digests).map_err(|error| {
            StoreError::Backend(format!("checkpoint digests do not encode: {error}"))
        })?;
        let rows = self
            .read(SqlStatement {
                sql: self.pick(&PARTS),
                params: vec![&wanted],
            })
            .await?;
        rows.iter()
            .map(|row| Ok((text(row, 0, "digest")?, text(row, 1, "body")?)))
            .collect()
    }

    async fn written(
        &self,
        fence: &Fence,
        manifest: &Manifest,
        listing: &Listing,
        parts: &[(String, String)],
    ) -> Result<Written, StoreError> {
        let skip = usize::from(sim::mutated(Mutation::CheckpointSkipsEpoch));
        let seq = manifest.seq.to_string();
        let part = fence.part.0.to_string();
        let epoch = fence.epoch.to_string();
        let listed = listing.encoded();
        let base = match listing {
            Listing::Whole(_) => String::new(),
            Listing::Changed { base, .. } => base.to_string(),
        };
        let _ = FENCED;
        let mut statements = Vec::with_capacity(parts.len() + 2);
        for (digest, body) in parts {
            statements.push(SqlStatement {
                sql: self.pick(&PUT_PART[skip]),
                params: vec![digest, body, &part, &epoch, &fence.holder],
            });
        }
        statements.push(SqlStatement {
            sql: self.pick(&PUT_MANIFEST[skip]),
            params: vec![
                &manifest.scope,
                &seq,
                &part,
                &epoch,
                &fence.holder,
                &listed,
                &base,
            ],
        });
        statements.push(SqlStatement {
            sql: self.pick(&LANDED),
            params: vec![&manifest.scope, &seq],
        });
        let answers = self
            .host
            .run(SqlAccess::Write, "scope checkpoint write", &statements)
            .await?;
        let Some(landed) = answers.last() else {
            return Err(StoreError::Backend(
                "a checkpoint write returned no result".into(),
            ));
        };
        match landed.first() {
            Some(row) if number(row, 0, "epoch")? == fence.epoch => Ok(Written::Landed),
            Some(_) | None => Ok(Written::Stale),
        }
    }
}

impl<H: Sql + Send + Sync + ?Sized> Checkpoints for SqlCheckpoints<H> {
    fn latest<'a>(
        &'a self,
        scope: &'a str,
    ) -> StoreFuture<'a, Result<Option<Manifest>, StoreError>> {
        Box::pin(self.latest_of(scope))
    }

    fn parts<'a>(
        &'a self,
        digests: &'a [String],
    ) -> StoreFuture<'a, Result<Vec<(String, String)>, StoreError>> {
        Box::pin(self.parts_of(digests))
    }

    fn write<'a>(
        &'a self,
        fence: &'a Fence,
        manifest: &'a Manifest,
        listing: &'a Listing,
        parts: &'a [(String, String)],
    ) -> StoreFuture<'a, Result<Written, StoreError>> {
        Box::pin(self.written(fence, manifest, listing, parts))
    }
}
