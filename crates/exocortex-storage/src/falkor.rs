// crates/exocortex-storage/src/falkor.rs — the FalkorDB adapter (§6.5).
//
// The PRD skeleton targets the 0.1 client; this implementation uses the
// pinned falkordb 0.3 async API with identical structure and semantics: every
// mutation mints a backend LSN via Redis INCR (R-S3), every query goes
// through a registered parameterized template (R-S2, CR-10), and every
// invalidation is published to the org channel (§9.1).

use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
#[cfg(feature = "integration")]
use std::sync::atomic::Ordering;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use falkordb::{FalkorAsyncClient, FalkorClientBuilder, FalkorConnectionInfo, FalkorValue};
use futures::stream::BoxStream;
use futures::StreamExt;
use redis::AsyncCommands;
use smol_str::SmolStr;
use tracing::instrument;

use exocortex_kernel::{
    EntityId, Memory, MemoryId, Ontology, Relationship, RelationshipId, Visibility,
};

use crate::cypher;
use crate::types::*;
use crate::{Storage, StorageError};

/// Connection + identity configuration for the adapter (§6.5).
pub struct FalkorConfig {
    /// FalkorDB URL, e.g. `falkor://127.0.0.1:6379`.
    pub falkor_url: String,
    /// Redis URL for LSNs, leases, and pub-sub (same instance as FalkorDB).
    pub redis_url: String,
    /// Graph name, e.g. `exocortex:{org_id}`.
    pub graph_name: String,
    /// Owning org id.
    pub org_id: SmolStr,
    /// This node's identity for lease tokens.
    pub node_id: SmolStr,
}

/// The FalkorDB `Storage` implementation.
pub struct FalkorStorage {
    client: FalkorAsyncClient,
    graph: String,
    redis_client: redis::Client,
    redis: redis::aio::MultiplexedConnection,
    node_id: SmolStr,
    org_id: SmolStr,
    ontology: Arc<Ontology>,
    lsn_key: String,
    channel: String,
    publication_claim_seq: AtomicU64,
    #[cfg(feature = "integration")]
    stream_memory_pages: AtomicU64,
    #[cfg(feature = "integration")]
    stream_relationship_pages: AtomicU64,
    #[cfg(feature = "integration")]
    legacy_repair_queries: AtomicU64,
    #[cfg(feature = "integration")]
    migration_peak_rows: AtomicU64,
    #[cfg(feature = "integration")]
    publish_round_trips: AtomicU64,
    #[cfg(feature = "integration")]
    fail_next_publish: std::sync::atomic::AtomicBool,
    #[cfg(feature = "integration")]
    fail_next_backend_lsn: std::sync::atomic::AtomicBool,
    #[cfg(feature = "integration")]
    pause_next_publish: std::sync::atomic::AtomicBool,
    #[cfg(feature = "integration")]
    publish_paused: tokio::sync::Notify,
    #[cfg(feature = "integration")]
    publish_release: tokio::sync::Notify,
    #[cfg(feature = "integration")]
    pause_next_lsn: std::sync::atomic::AtomicBool,
    #[cfg(feature = "integration")]
    lsn_paused: tokio::sync::Notify,
    #[cfg(feature = "integration")]
    lsn_release: tokio::sync::Notify,
}

/// Encode a param value as a Cypher literal for the `CYPHER k=v` prefix the
/// falkordb client emits (params are passed as strings and re-parsed
/// server-side, R-S2: no string interpolation into the query body).
fn cypher_literal(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => {
            format!("'{}'", s.replace('\\', "\\\\").replace('\'', "\\'"))
        }
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Null => "null".into(),
        serde_json::Value::Array(xs) => {
            let inner: Vec<String> = xs.iter().map(cypher_literal).collect();
            format!("[{}]", inner.join(","))
        }
        serde_json::Value::Object(m) => {
            let inner: Vec<String> = m
                .iter()
                .map(|(k, v)| {
                    format!(
                        "{}: {}",
                        cypher_literal(&serde_json::Value::String(k.clone())),
                        cypher_literal(v)
                    )
                })
                .collect();
            format!("{{{}}}", inner.join(","))
        }
    }
}

/// Convert the JSON params of a `CypherQuery` into the string map the
/// falkordb client expects.
fn params_to_map(params: &serde_json::Value) -> Result<HashMap<String, String>, StorageError> {
    let serde_json::Value::Object(map) = params else {
        return Err(StorageError::Backend(
            "cypher params must be an object".into(),
        ));
    };
    Ok(map
        .iter()
        .map(|(k, v)| (k.clone(), cypher_literal(v)))
        .collect())
}

fn falkor_value_to_json(v: &FalkorValue) -> serde_json::Value {
    match v {
        FalkorValue::Node(n) => {
            let mut m = serde_json::Map::new();
            for (k, v) in &n.properties {
                m.insert(k.clone(), falkor_value_to_json(v));
            }
            serde_json::Value::Object(m)
        }
        FalkorValue::Edge(e) => {
            let mut m = serde_json::Map::new();
            for (k, v) in &e.properties {
                m.insert(k.clone(), falkor_value_to_json(v));
            }
            serde_json::Value::Object(m)
        }
        FalkorValue::Array(xs) => {
            serde_json::Value::Array(xs.iter().map(falkor_value_to_json).collect())
        }
        FalkorValue::Map(mp) => serde_json::Value::Object(
            mp.iter()
                .map(|(k, v)| (k.clone(), falkor_value_to_json(v)))
                .collect(),
        ),
        FalkorValue::Vec32(v) => {
            serde_json::Value::Array(v.values.iter().map(|f| serde_json::json!(f)).collect())
        }
        FalkorValue::String(s) => serde_json::Value::String(s.clone()),
        FalkorValue::Bool(b) => serde_json::Value::Bool(*b),
        FalkorValue::I64(i) => serde_json::json!(i),
        FalkorValue::F64(f) => serde_json::json!(f),
        FalkorValue::Point(p) => serde_json::json!([p.latitude, p.longitude]),
        FalkorValue::Path(_) => serde_json::Value::Null,
        FalkorValue::None => serde_json::Value::Null,
        FalkorValue::Unparseable(_) => serde_json::Value::Null,
    }
}

/// Pull the full `Memory` out of a returned `Memory` node's `props_json`.
fn memory_from_value(v: &FalkorValue) -> Result<Memory, StorageError> {
    let FalkorValue::Node(n) = v else {
        return Err(StorageError::Backend(format!(
            "expected a Memory node, got: {v:?}"
        )));
    };
    let Some(FalkorValue::String(json)) = n.properties.get("props_json") else {
        return Err(StorageError::Backend(
            "Memory node missing props_json".into(),
        ));
    };
    serde_json::from_str(json).map_err(|e| StorageError::Backend(format!("bad props_json: {e}")))
}

fn memories_by_id(
    rows: &[Vec<FalkorValue>],
) -> Result<std::collections::HashMap<MemoryId, Memory>, StorageError> {
    let mut by_id = std::collections::HashMap::new();
    for value in rows.iter().filter_map(|row| row.first()) {
        if matches!(value, FalkorValue::None) {
            continue;
        }
        let memory = memory_from_value(value)?;
        by_id.insert(memory.id, memory);
    }
    Ok(by_id)
}

fn relationship_from_value(v: &FalkorValue) -> Result<Relationship, StorageError> {
    let FalkorValue::Edge(edge) = v else {
        return Err(StorageError::Backend(format!(
            "expected a relationship edge, got: {v:?}"
        )));
    };
    let Some(FalkorValue::String(json)) = edge.properties.get("props_json") else {
        return Err(StorageError::Backend(
            "relationship edge missing props_json".into(),
        ));
    };
    serde_json::from_str(json)
        .map_err(|error| StorageError::Backend(format!("bad rel props_json: {error}")))
}

fn decode_persisted_fingerprint(
    rows: &[Vec<FalkorValue>],
) -> Result<Option<[u8; 32]>, StorageError> {
    let Some(value) = rows.first().and_then(|row| row.first()) else {
        return Ok(None);
    };
    let FalkorValue::String(encoded) = value else {
        return Err(StorageError::CorruptMetadata {
            key: "ontology_fingerprint",
            detail: "expected a 64-character hexadecimal string".into(),
        });
    };
    if encoded.len() != 64 {
        return Err(StorageError::CorruptMetadata {
            key: "ontology_fingerprint",
            detail: format!(
                "expected 64 hexadecimal characters, found {}",
                encoded.len()
            ),
        });
    }
    let mut fingerprint = [0u8; 32];
    for (index, byte) in fingerprint.iter_mut().enumerate() {
        let pair = &encoded[index * 2..index * 2 + 2];
        *byte = u8::from_str_radix(pair, 16).map_err(|_| StorageError::CorruptMetadata {
            key: "ontology_fingerprint",
            detail: format!("invalid hexadecimal byte at offset {}", index * 2),
        })?;
    }
    Ok(Some(fingerprint))
}

fn decode_settled_ingest(row: &[FalkorValue]) -> Result<SettledIngestBatch, StorageError> {
    fn integer(row: &[FalkorValue], index: usize, field: &str) -> Result<i64, StorageError> {
        match row.get(index) {
            Some(FalkorValue::I64(value)) => Ok(*value),
            Some(_) => Err(StorageError::CorruptMetadata {
                key: "ingest_settlement",
                detail: format!("{field} must be an integer"),
            }),
            None => Err(StorageError::CorruptMetadata {
                key: "ingest_settlement",
                detail: format!("missing {field}"),
            }),
        }
    }

    let accepted =
        u32::try_from(integer(row, 0, "accepted")?).map_err(|_| StorageError::CorruptMetadata {
            key: "ingest_settlement",
            detail: "accepted must be between 0 and 4294967295".into(),
        })?;
    let rejected =
        u32::try_from(integer(row, 1, "rejected")?).map_err(|_| StorageError::CorruptMetadata {
            key: "ingest_settlement",
            detail: "rejected must be between 0 and 4294967295".into(),
        })?;
    let assigned_lsn = u64::try_from(integer(row, 2, "assigned_lsn")?).map_err(|_| {
        StorageError::CorruptMetadata {
            key: "ingest_settlement",
            detail: "assigned_lsn must be non-negative".into(),
        }
    })?;
    Ok(SettledIngestBatch {
        accepted,
        rejected,
        assigned_lsn,
    })
}

fn decode_stream_lsn(
    row: &[FalkorValue],
    previous: u64,
    row_kind: &'static str,
) -> Result<u64, StorageError> {
    let value = match row.get(1) {
        Some(FalkorValue::I64(value)) => *value,
        Some(other) => {
            return Err(StorageError::CorruptMetadata {
                key: "stream_cursor",
                detail: format!("{row_kind} LSN must be an integer, found {other:?}"),
            });
        }
        None => {
            return Err(StorageError::CorruptMetadata {
                key: "stream_cursor",
                detail: format!("{row_kind} row is missing its LSN"),
            });
        }
    };
    let lsn = u64::try_from(value).map_err(|_| StorageError::CorruptMetadata {
        key: "stream_cursor",
        detail: format!("{row_kind} LSN must be non-negative, found {value}"),
    })?;
    if lsn <= previous {
        return Err(StorageError::CorruptMetadata {
            key: "stream_cursor",
            detail: format!("{row_kind} LSN {lsn} did not advance beyond {previous}"),
        });
    }
    Ok(lsn)
}

const STORAGE_SCHEMA_VERSION: i64 = 1;

fn schema_needs_migration(rows: &[Vec<FalkorValue>]) -> Result<bool, StorageError> {
    match rows.first().and_then(|row| row.first()) {
        Some(FalkorValue::I64(STORAGE_SCHEMA_VERSION)) => Ok(false),
        Some(FalkorValue::I64(0)) | None | Some(FalkorValue::None) => Ok(true),
        Some(FalkorValue::I64(version)) => Err(StorageError::CorruptMetadata {
            key: "schema_version",
            detail: format!(
                "unsupported schema version {version}; this build supports {STORAGE_SCHEMA_VERSION}"
            ),
        }),
        Some(other) => Err(StorageError::CorruptMetadata {
            key: "schema_version",
            detail: format!("expected integer, found {other:?}"),
        }),
    }
}

fn hex(id: &[u8; 16]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(32);
    for b in id {
        let _ = write!(out, "{b:02x}");
    }
    out
}

fn hex_digest(bytes: &[u8; 32]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(64);
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// R-T11 (audit ST3): v1 read paths treat `Public` as `Org`, so an
/// Org-or-wider caller fetches at the widest internal ceiling; narrower
/// callers fetch at their own scope.
fn fetch_ceiling(max: Visibility) -> u8 {
    if max >= Visibility::Org {
        Visibility::Public as u8
    } else {
        max as u8
    }
}

impl FalkorStorage {
    fn expand_relationships(&self, rs: &[Relationship]) -> Vec<Relationship> {
        let mut all_rels = Vec::with_capacity(rs.len() * 2);
        let mut seen: std::collections::HashSet<RelationshipId> =
            rs.iter().map(|relationship| relationship.id).collect();
        for relationship in rs {
            all_rels.push(relationship.clone());
            if let Some(inverse) =
                exocortex_kernel::materialize_inverse(&self.ontology, relationship)
            {
                if seen.insert(inverse.id) {
                    all_rels.push(inverse);
                }
            }
        }
        all_rels
    }

    /// Connect, then pin the ontology fingerprint (fail fast on mismatch,
    /// R-D5).
    pub async fn connect(cfg: FalkorConfig, ontology: Arc<Ontology>) -> Result<Self, StorageError> {
        let conn_info: FalkorConnectionInfo = cfg
            .falkor_url
            .as_str()
            .try_into()
            .map_err(|e: falkordb::FalkorDBError| StorageError::Backend(e.to_string()))?;
        let client = FalkorClientBuilder::new_async()
            .with_connection_info(conn_info)
            .build()
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        let redis_client = redis::Client::open(cfg.redis_url.as_str())
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        let redis = redis_client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        let this = Self {
            client,
            graph: cfg.graph_name,
            redis_client,
            redis,
            node_id: cfg.node_id,
            org_id: cfg.org_id.clone(),
            ontology,
            lsn_key: format!("exocortex:{}:lsn", cfg.org_id),
            channel: format!("exocortex:{}:inv", cfg.org_id),
            publication_claim_seq: AtomicU64::new(0),
            #[cfg(feature = "integration")]
            stream_memory_pages: AtomicU64::new(0),
            #[cfg(feature = "integration")]
            stream_relationship_pages: AtomicU64::new(0),
            #[cfg(feature = "integration")]
            legacy_repair_queries: AtomicU64::new(0),
            #[cfg(feature = "integration")]
            migration_peak_rows: AtomicU64::new(0),
            #[cfg(feature = "integration")]
            publish_round_trips: AtomicU64::new(0),
            #[cfg(feature = "integration")]
            fail_next_publish: std::sync::atomic::AtomicBool::new(false),
            #[cfg(feature = "integration")]
            fail_next_backend_lsn: std::sync::atomic::AtomicBool::new(false),
            #[cfg(feature = "integration")]
            pause_next_publish: std::sync::atomic::AtomicBool::new(false),
            #[cfg(feature = "integration")]
            publish_paused: tokio::sync::Notify::new(),
            #[cfg(feature = "integration")]
            publish_release: tokio::sync::Notify::new(),
            #[cfg(feature = "integration")]
            pause_next_lsn: std::sync::atomic::AtomicBool::new(false),
            #[cfg(feature = "integration")]
            lsn_paused: tokio::sync::Notify::new(),
            #[cfg(feature = "integration")]
            lsn_release: tokio::sync::Notify::new(),
        };
        let graphs = this
            .client
            .list_graphs()
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        let graph_exists = graphs.iter().any(|graph| graph == &this.graph);
        if graph_exists {
            let schema = this
                .run_template("read_schema_version", &serde_json::json!({}), true)
                .await?;
            schema_needs_migration(&schema)?;
        }
        this.pin_fingerprint(graph_exists).await?;
        this.migrate_schema().await?;
        this.repair_legacy_memories().await?;
        this.ensure_memory_attribute_index().await?;
        Ok(this)
    }

    /// Read the persisted fingerprint. If empty, write ours. If present and
    /// different, refuse to start (R-D5).
    async fn pin_fingerprint(&self, graph_exists: bool) -> Result<(), StorageError> {
        // FalkorDB refuses read queries on a graph key that does not exist
        // yet; only read the pinned fingerprint once the graph has data.
        let rows = if graph_exists {
            self.run_template("read_fingerprint", &serde_json::json!({}), true)
                .await?
        } else {
            vec![]
        };
        let stored = decode_persisted_fingerprint(&rows)?;
        let runtime = self.ontology.fingerprint.0;
        match stored {
            None => {
                let hexfp = {
                    use std::fmt::Write as _;
                    let mut out = String::with_capacity(64);
                    for b in runtime {
                        let _ = write!(out, "{b:02x}");
                    }
                    out
                };
                let rows = self
                    .run_template(
                        "write_fingerprint_if_schema_compatible",
                        &serde_json::json!({
                            "fp": hexfp,
                            "max_schema": STORAGE_SCHEMA_VERSION,
                        }),
                        false,
                    )
                    .await?;
                if rows.is_empty() {
                    let schema = self
                        .run_template("read_schema_version", &serde_json::json!({}), true)
                        .await?;
                    schema_needs_migration(&schema)?;
                    let fingerprint = self
                        .run_template("read_fingerprint", &serde_json::json!({}), true)
                        .await?;
                    return match decode_persisted_fingerprint(&fingerprint)? {
                        Some(storage) => {
                            Err(StorageError::FingerprintMismatch { storage, runtime })
                        }
                        None => Err(StorageError::Backend(
                            "fingerprint pin was rejected by schema guard".into(),
                        )),
                    };
                }
                tracing::info!(fingerprint = %hexfp, "pinned ontology fingerprint");
                Ok(())
            }
            Some(storage) if storage == runtime => Ok(()),
            Some(storage) => Err(StorageError::FingerprintMismatch { storage, runtime }),
        }
    }

    async fn migrate_schema(&self) -> Result<(), StorageError> {
        let rows = self
            .run_template("read_schema_version", &serde_json::json!({}), true)
            .await?;
        if !schema_needs_migration(&rows)? {
            return Ok(());
        }

        const LOCK_TTL_MS: u64 = 30_000;
        const WAIT_ATTEMPTS: usize = 6_000;
        let lock_key = format!("exocortex:{}:{}:schema-migration", self.org_id, self.graph);
        let sequence_key = format!("{lock_key}:sequence");
        let sequence: u64 = self
            .redis
            .clone()
            .incr(&sequence_key, 1u64)
            .await
            .map_err(|error| StorageError::Backend(error.to_string()))?;
        let token = format!("{}:{sequence}", self.node_id);
        for _ in 0..WAIT_ATTEMPTS {
            let acquired: Option<String> = redis::cmd("SET")
                .arg(&lock_key)
                .arg(&token)
                .arg("NX")
                .arg("PX")
                .arg(LOCK_TTL_MS)
                .query_async(&mut self.redis.clone())
                .await
                .map_err(|error| StorageError::Backend(error.to_string()))?;
            if acquired.is_some() {
                let outcome = self
                    .migrate_schema_owned(&lock_key, &token, LOCK_TTL_MS)
                    .await;
                if let Err(error) = self.release_schema_migration_lock(&lock_key, &token).await {
                    tracing::warn!(?error, "schema migration lock cleanup will rely on expiry");
                }
                return outcome;
            }
            let rows = self
                .run_template("read_schema_version", &serde_json::json!({}), true)
                .await?;
            if !schema_needs_migration(&rows)? {
                return Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        Err(StorageError::Backend(
            "timed out waiting for compatible schema migration owner".into(),
        ))
    }

    async fn migrate_schema_owned(
        &self,
        lock_key: &str,
        token: &str,
        lock_ttl_ms: u64,
    ) -> Result<(), StorageError> {
        self.renew_schema_migration_lock(lock_key, token, lock_ttl_ms)
            .await?;

        let claim = self
            .run_template(
                "claim_schema_v0",
                &serde_json::json!({ "migration_token": token }),
                false,
            )
            .await?;
        if claim.is_empty() {
            return Err(StorageError::CorruptMetadata {
                key: "schema_version",
                detail: "schema changed before v0 migration could claim it".into(),
            });
        }

        let mut memories = <Self as Storage>::stream_all_memories(self).await;
        while let Some(memory) = memories.next().await {
            self.renew_schema_migration_lock(lock_key, token, lock_ttl_ms)
                .await?;
            #[cfg(feature = "integration")]
            self.migration_peak_rows.fetch_max(1, Ordering::SeqCst);
            let mut memory = memory?;
            if memory.context.tenant_id.is_none() {
                memory.context.tenant_id = Some(self.org_id.clone());
            }
            let memory_type_label = self
                .ontology
                .memory_type_names
                .get(memory.memory_type as usize)
                .ok_or_else(|| {
                    StorageError::Backend(format!("bad memory_type {}", memory.memory_type))
                })?;
            let mut params = self.memory_params(&memory, memory.lsn.value, memory_type_label);
            params
                .as_object_mut()
                .expect("memory params are an object")
                .insert("expected_schema_version".into(), serde_json::json!(0));
            params
                .as_object_mut()
                .expect("memory params are an object")
                .insert("migration_token".into(), serde_json::json!(token));
            let migrated = self
                .run_template("migrate_memory_schema_v1", &params, false)
                .await?;
            // A rolling writer may replace the captured row. The captured-LSN
            // CAS deliberately skips it; connect-time repair below adopts the
            // replacement only when it lacks canonical assertion history.
            if migrated.is_empty() {
                self.ensure_schema_v0().await?;
            }
        }
        drop(memories);

        let mut relationships = <Self as Storage>::stream_all_relationships(self).await;
        while let Some(relationship) = relationships.next().await {
            self.renew_schema_migration_lock(lock_key, token, lock_ttl_ms)
                .await?;
            #[cfg(feature = "integration")]
            self.migration_peak_rows.fetch_max(1, Ordering::SeqCst);
            let relationship = relationship?;
            let migrated = self
                .run_template(
                    "migrate_relationship_schema_v1",
                    &serde_json::json!({
                        "rel_id": hex(&relationship.id.0),
                        "expected_schema_version": 0,
                        "migration_token": token,
                    }),
                    false,
                )
                .await?;
            if migrated.is_empty() {
                self.ensure_schema_v0().await?;
            }
        }
        drop(relationships);

        self.repair_legacy_memories_with_lock(Some((lock_key, token, lock_ttl_ms)))
            .await?;
        self.renew_schema_migration_lock(lock_key, token, lock_ttl_ms)
            .await?;

        let finished = self
            .run_template(
                "finish_schema_migration_v1",
                &serde_json::json!({
                    "from_version": 0,
                    "to_version": STORAGE_SCHEMA_VERSION,
                    "migration_token": token,
                }),
                false,
            )
            .await?;
        if finished.is_empty() {
            self.ensure_schema_v0().await?;
            return Err(StorageError::CorruptMetadata {
                key: "schema_version",
                detail: "v0 migration lost its final schema transition".into(),
            });
        }
        Ok(())
    }

    async fn renew_schema_migration_lock(
        &self,
        lock_key: &str,
        token: &str,
        ttl_ms: u64,
    ) -> Result<(), StorageError> {
        let renewed: i64 = redis::cmd("EVAL")
            .arg("if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('PEXPIRE', KEYS[1], ARGV[2]) else return 0 end")
            .arg(1)
            .arg(lock_key)
            .arg(token)
            .arg(ttl_ms)
            .query_async(&mut self.redis.clone())
            .await
            .map_err(|error| StorageError::Backend(error.to_string()))?;
        if renewed == 1 {
            Ok(())
        } else {
            Err(StorageError::Backend(
                "schema migration ownership expired or was replaced".into(),
            ))
        }
    }

    async fn release_schema_migration_lock(
        &self,
        lock_key: &str,
        token: &str,
    ) -> Result<(), StorageError> {
        let _: i64 = redis::cmd("EVAL")
            .arg("if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) else return 0 end")
            .arg(1)
            .arg(lock_key)
            .arg(token)
            .query_async(&mut self.redis.clone())
            .await
            .map_err(|error| StorageError::Backend(error.to_string()))?;
        Ok(())
    }

    async fn ensure_schema_v0(&self) -> Result<(), StorageError> {
        self.ensure_schema_version(0).await
    }

    async fn ensure_schema_version(&self, expected: i64) -> Result<(), StorageError> {
        let rows = self
            .run_template("read_schema_version", &serde_json::json!({}), true)
            .await?;
        match rows.first().and_then(|row| row.first()) {
            Some(FalkorValue::I64(actual)) if *actual == expected => Ok(()),
            other => Err(StorageError::CorruptMetadata {
                key: "schema_version",
                detail: format!(
                    "schema changed while operation required version {expected}: {other:?}"
                ),
            }),
        }
    }

    async fn ensure_memory_attribute_index(&self) -> Result<(), StorageError> {
        match self
            .run_template(
                "create_memory_attribute_key_index",
                &serde_json::json!({}),
                false,
            )
            .await
        {
            Ok(_) => {}
            Err(StorageError::Backend(detail))
                if detail.contains("already indexed") || detail.contains("already exists") => {}
            Err(error) => return Err(error),
        }
        self.run_template(
            "repair_memory_attribute_index_v1",
            &serde_json::json!({}),
            false,
        )
        .await?;
        Ok(())
    }

    /// Adopt only the unambiguous pre-v1 shape: a current row whose exact LSN
    /// has no canonical assertion. Current-shaped tenantless rows retain their
    /// assertion and remain fail-closed.
    async fn repair_legacy_memories(&self) -> Result<(), StorageError> {
        self.repair_legacy_memories_with_lock(None).await
    }

    async fn repair_legacy_memories_with_lock(
        &self,
        migration_lock: Option<(&str, &str, u64)>,
    ) -> Result<(), StorageError> {
        let schema = self
            .run_template("read_schema_version", &serde_json::json!({}), true)
            .await?;
        let expected_schema_version = if schema_needs_migration(&schema)? {
            0
        } else {
            STORAGE_SCHEMA_VERSION
        };
        loop {
            if let Some((key, token, ttl_ms)) = migration_lock {
                self.renew_schema_migration_lock(key, token, ttl_ms).await?;
            }
            let rows = self
                .run_template(
                    "legacy_memory_candidates",
                    &serde_json::json!({ "limit": 256 }),
                    true,
                )
                .await?;
            if rows.is_empty() {
                return Ok(());
            }
            let mut repaired = 0usize;
            for row in rows {
                let Some(value) = row.first() else {
                    return Err(StorageError::CorruptMetadata {
                        key: "legacy_memory",
                        detail: "candidate row was empty".into(),
                    });
                };
                let mut memory = memory_from_value(value)?;
                if memory.context.tenant_id.is_none() {
                    memory.context.tenant_id = Some(self.org_id.clone());
                }
                let label = self
                    .ontology
                    .memory_type_names
                    .get(memory.memory_type as usize)
                    .ok_or_else(|| {
                        StorageError::Backend(format!("bad memory_type {}", memory.memory_type))
                    })?;
                let mut params = self.memory_params(&memory, memory.lsn.value, label);
                params
                    .as_object_mut()
                    .expect("memory params are an object")
                    .insert(
                        "expected_schema_version".into(),
                        serde_json::json!(expected_schema_version),
                    );
                params
                    .as_object_mut()
                    .expect("memory params are an object")
                    .insert(
                        "migration_token".into(),
                        serde_json::json!(migration_lock.map(|(_, token, _)| token).unwrap_or("")),
                    );
                let changed = self
                    .run_template("repair_legacy_memory_v1", &params, false)
                    .await?;
                if changed.is_empty() {
                    self.ensure_schema_version(expected_schema_version).await?;
                }
                repaired += usize::from(!changed.is_empty());
            }
            if repaired == 0 {
                return Ok(());
            }
        }
    }

    /// Integration-only downgrade fixture for proving startup migration from
    /// a pre-v1 graph. The executable Cypher remains catalogue-confined.
    #[cfg(feature = "integration")]
    #[doc(hidden)]
    pub async fn make_legacy_schema_for_testing(&self) -> Result<(), StorageError> {
        self.run_template(
            "integration_make_legacy_schema",
            &serde_json::json!({}),
            false,
        )
        .await?;
        Ok(())
    }

    /// Integration-only fixture for proving future schema validation happens
    /// before an absent fingerprint can be pinned.
    #[cfg(feature = "integration")]
    #[doc(hidden)]
    pub async fn make_future_schema_without_fingerprint_for_testing(
        &self,
    ) -> Result<(), StorageError> {
        self.run_template(
            "integration_make_future_schema_without_fingerprint",
            &serde_json::json!({ "version": STORAGE_SCHEMA_VERSION + 1 }),
            false,
        )
        .await?;
        Ok(())
    }

    /// Assign the next monotonic backend LSN via Redis INCR (R-S3).
    async fn next_lsn(&self) -> Result<u64, StorageError> {
        let n: u64 = self
            .redis
            .clone()
            .incr(&self.lsn_key, 1_u64)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        #[cfg(feature = "integration")]
        if self
            .pause_next_lsn
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            self.lsn_paused.notify_one();
            self.lsn_release.notified().await;
        }
        Ok(n)
    }

    /// Convert a runtime `RelKindId` into the Cypher label used in FalkorDB.
    /// Labels are drawn from the ontology at startup (R-T2 / R-S2).
    fn kind_label(&self, k: exocortex_kernel::RelKindId) -> Result<&str, StorageError> {
        self.ontology
            .kinds_by_id
            .get(&k)
            .map(|m| m.display_name.as_str())
            .ok_or_else(|| StorageError::Backend(format!("unknown RelKindId {:?}", k)))
    }

    /// Execute a registered template and return its raw rows.
    async fn run_template(
        &self,
        template_id: &str,
        params: &serde_json::Value,
        read_only: bool,
    ) -> Result<Vec<Vec<FalkorValue>>, StorageError> {
        #[cfg(feature = "integration")]
        if matches!(
            template_id,
            "legacy_memory_candidates" | "repair_legacy_memory_v1"
        ) {
            self.legacy_repair_queries.fetch_add(1, Ordering::Relaxed);
        }
        let t = cypher::TEMPLATES.get(template_id).ok_or_else(|| {
            StorageError::Backend(format!("unregistered cypher template: {template_id}"))
        })?;
        let q = CypherQuery {
            template_id: t.id,
            params: params.clone(),
            read_only,
            deadline: Utc::now() + chrono::Duration::seconds(5),
        };
        let t = cypher::validate(&q)?;
        let map = params_to_map(&q.params)?;
        // FalkorDB does not accept parameters inside variable-length path
        // ranges (`*1..$max_depth` — "unhandled type in inlined properties"),
        // so the verbatim §6.4 traverse template gets its depth baked in as a
        // validated integer literal. This is the only non-parameterized
        // substitution in the adapter; the value is a CR-6-capped u8.
        let mut cypher_text: String = match (
            q.params.get("max_depth").and_then(|v| v.as_u64()),
            q.params.get("kind_labels"),
            q.params.get("kind_label"),
        ) {
            (None, None, None) => t.cypher.to_string(),
            (depth, kinds_list, kind_single) => {
                let mut text = t.cypher.to_string();
                if let Some(depth) = depth {
                    // FalkorDB does not accept parameters inside var-length
                    // ranges; bake the CR-6-capped depth as a literal.
                    text = text.replace("$max_depth", &format!("{depth}"));
                }
                if let Some(kinds_list) = kinds_list {
                    // Type-list substitution from the validated ontology
                    // allowlist (R-T2): `[:Solves|Fixes*1..N]`, or untyped
                    // when the caller wants every kind.
                    let types: Vec<String> = kinds_list
                        .as_array()
                        .map(|xs| {
                            xs.iter()
                                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                                .collect()
                        })
                        .unwrap_or_default();
                    let all_known = types.iter().all(|name| {
                        self.ontology
                            .kinds_by_id
                            .values()
                            .any(|k| k.display_name == name)
                    });
                    if !all_known {
                        return Err(StorageError::Backend(
                            "kind_labels not in ontology allowlist".into(),
                        ));
                    }
                    let pattern = if types.is_empty() {
                        String::new()
                    } else {
                        format!(":{}", types.join("|"))
                    };
                    let _ = &pattern;
                    text = text.replace("__KIND_TYPES__", &pattern);
                }
                if let Some(kind) = kind_single.and_then(|v| v.as_str()) {
                    if !self
                        .ontology
                        .kinds_by_id
                        .values()
                        .any(|k| k.display_name == kind)
                    {
                        return Err(StorageError::Backend(
                            "kind_label not in ontology allowlist".into(),
                        ));
                    }
                    text = text.replace("__KIND_TYPE__", kind);
                }
                text
            }
        };
        let mut graph = self.client.select_graph(self.graph.clone());
        let ordered_lsn = if t.read_only
            || template_id.starts_with("integration_")
            || matches!(
                template_id,
                "migrate_memory_schema_v1"
                    | "repair_legacy_memory_v1"
                    | "discovery_outbox_mark_published"
                    | "fenced_discovery_record_store"
            ) {
            None
        } else {
            params.get("lsn").and_then(serde_json::Value::as_u64)
        };
        if ordered_lsn.is_some() {
            cypher_text = format!(
                "MERGE (order:_ExocortexMeta {{key: 'committed_lsn'}}) \
                 ON CREATE SET order.value = 0 \
                 WITH order WHERE order.value < $lsn \
                 SET order.value = $lsn \
                 WITH 1 AS __ordered_step\n{cypher_text}"
            );
        }
        let mut builder = if t.read_only {
            graph.ro_query(cypher_text.as_str())
        } else {
            graph.query(cypher_text.as_str())
        };
        if !map.is_empty() {
            builder = builder.with_params(&map);
        }
        let result = builder
            .execute()
            .await
            .map_err(|e| StorageError::Backend(format!("{}: {e}", t.id)))?;
        let rows = result.data.collect::<Vec<_>>();
        if let Some(lsn) = ordered_lsn.filter(|_| rows.is_empty()) {
            return Err(StorageError::Backend(format!(
                "mutation LSN {lsn} lost graph commit ordering"
            )));
        }
        Ok(rows)
    }

    /// Publish an invalidation to the org change-feed channel (§9.1).
    async fn publish(&self, inv: Invalidation) {
        let _ = self.publish_checked(&inv).await;
    }

    async fn publish_checked(&self, inv: &Invalidation) -> Result<(), StorageError> {
        self.publish_checked_batch(std::slice::from_ref(inv)).await
    }

    async fn publish_batch(&self, invalidations: &[Invalidation]) {
        let _ = self.publish_checked_batch(invalidations).await;
    }

    async fn publish_checked_batch(
        &self,
        invalidations: &[Invalidation],
    ) -> Result<(), StorageError> {
        if invalidations.is_empty() {
            return Ok(());
        }
        #[cfg(feature = "integration")]
        if self
            .fail_next_publish
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(StorageError::Backend(
                "injected discovery publication failure".into(),
            ));
        }
        #[cfg(feature = "integration")]
        if self
            .pause_next_publish
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            self.publish_paused.notify_one();
            self.publish_release.notified().await;
        }
        let payloads = invalidations
            .iter()
            .map(crate::types::encode_feed_invalidation)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| StorageError::Backend(error.to_string()))?;
        let mut pipeline = redis::pipe();
        for payload in payloads {
            pipeline
                .cmd("PUBLISH")
                .arg(&self.channel)
                .arg(payload)
                .ignore();
        }
        #[cfg(feature = "integration")]
        self.publish_round_trips.fetch_add(1, Ordering::Relaxed);
        pipeline
            .query_async::<()>(&mut self.redis.clone())
            .await
            .map_err(|error| StorageError::Backend(error.to_string()))?;
        Ok(())
    }

    async fn publish_idempotent_batch(
        &self,
        operation_key: &str,
        claim_token: &str,
        invalidations: &[Invalidation],
    ) -> Result<(), StorageError> {
        let renewed = !self
            .run_template(
                "idempotent_batch_publication_renew",
                &serde_json::json!({
                    "operation_key": operation_key,
                    "claim_token": claim_token,
                    "lease_ms": 30_000,
                }),
                false,
            )
            .await?
            .is_empty();
        if !renewed {
            return Err(StorageError::Backend(
                "idempotent publication claim ownership was lost".into(),
            ));
        }
        let publication_error = match tokio::time::timeout(
            std::time::Duration::from_secs(10),
            self.publish_checked_batch(invalidations),
        )
        .await
        {
            Ok(Ok(())) => None,
            Ok(Err(error)) => Some(error),
            Err(_) => Some(StorageError::Backend(
                "idempotent publication timed out".into(),
            )),
        };
        if let Some(error) = publication_error {
            self.run_template(
                "idempotent_batch_publication_release",
                &serde_json::json!({
                    "operation_key": operation_key,
                    "claim_token": claim_token,
                }),
                false,
            )
            .await?;
            return Err(error);
        }
        let completed = self
            .run_template(
                "idempotent_batch_publication_complete",
                &serde_json::json!({
                    "operation_key": operation_key,
                    "claim_token": claim_token,
                }),
                false,
            )
            .await?;
        if completed.is_empty() {
            return Err(StorageError::Backend(
                "idempotent publication completion lost claim ownership".into(),
            ));
        }
        Ok(())
    }

    async fn drain_discovery_outbox(&self) -> Result<(), StorageError> {
        loop {
            let rows = self
                .run_template(
                    "discovery_outbox_pending",
                    &serde_json::json!({ "limit": 256 }),
                    true,
                )
                .await?;
            if rows.is_empty() {
                return Ok(());
            }
            for row in rows {
                let discovery_id = match row.first() {
                    Some(FalkorValue::String(value)) => value,
                    other => {
                        return Err(StorageError::CorruptMetadata {
                            key: "discovery_outbox_id",
                            detail: format!("expected string, found {other:?}"),
                        })
                    }
                };
                let record = match row.get(1) {
                    Some(FalkorValue::String(value)) => {
                        serde_json::from_str(value).map_err(|error| {
                            StorageError::CorruptMetadata {
                                key: "discovery_outbox_record",
                                detail: error.to_string(),
                            }
                        })?
                    }
                    other => {
                        return Err(StorageError::CorruptMetadata {
                            key: "discovery_outbox_record",
                            detail: format!("expected string, found {other:?}"),
                        })
                    }
                };
                let lsn = match row.get(2) {
                    Some(FalkorValue::I64(value)) if *value >= 0 => *value as u64,
                    other => {
                        return Err(StorageError::CorruptMetadata {
                            key: "discovery_outbox_lsn",
                            detail: format!("expected non-negative integer, found {other:?}"),
                        })
                    }
                };
                self.publish_checked(&Invalidation::DiscoveryAvailable { record, lsn })
                    .await?;
                let marked = self
                    .run_template(
                        "discovery_outbox_mark_published",
                        &serde_json::json!({ "discovery_id": discovery_id, "lsn": lsn }),
                        false,
                    )
                    .await?;
                if marked.is_empty() {
                    return Err(StorageError::Backend(
                        "discovery outbox acknowledgement lost durable row".into(),
                    ));
                }
            }
        }
    }

    async fn discovery_is_durably_pending(
        &self,
        discovery: &DiscoveryRecord,
    ) -> Result<bool, StorageError> {
        let rows = self
            .run_template(
                "discovery_record_state",
                &serde_json::json!({ "discovery_id": discovery.discovery_id }),
                true,
            )
            .await?;
        let Some(row) = rows.first() else {
            return Ok(false);
        };
        let stored = match row.first() {
            Some(FalkorValue::String(value)) => serde_json::from_str::<DiscoveryRecord>(value)
                .map_err(|error| StorageError::CorruptMetadata {
                    key: "discovery_record",
                    detail: error.to_string(),
                })?,
            other => {
                return Err(StorageError::CorruptMetadata {
                    key: "discovery_record",
                    detail: format!("expected string, found {other:?}"),
                })
            }
        };
        if stored != *discovery {
            return Err(StorageError::ProposalMismatch);
        }
        match row.get(1) {
            Some(FalkorValue::I64(value)) if *value >= 0 => {}
            other => {
                return Err(StorageError::CorruptMetadata {
                    key: "discovery_outbox_lsn",
                    detail: format!("expected non-negative integer, found {other:?}"),
                })
            }
        }
        Ok(matches!(row.get(2), Some(FalkorValue::Bool(false))))
    }

    fn memory_params(&self, m: &Memory, lsn: u64, mt_label: &str) -> serde_json::Value {
        let attribute_keys = m
            .tags
            .iter()
            .map(|tag| format!("t:{tag}"))
            .chain(
                m.context
                    .entities
                    .iter()
                    .map(|entity| format!("e:{}", hex(&entity.0))),
            )
            .collect::<Vec<_>>();
        serde_json::json!({
            "id": hex(&m.id.0),
            "memory_type_label": mt_label,
            "memory_type_id": m.memory_type,
            "props_json": FalkorStorage::props_json(m, lsn),
            "tags": m.tags.iter().map(|tag| tag.as_str()).collect::<Vec<_>>(),
            "entity_ids": m.context.entities.iter().map(|entity| hex(&entity.0)).collect::<Vec<_>>(),
            "attribute_keys": attribute_keys,
            "tenant_id": m.context.tenant_id,
            "user_id": m.context.user_id,
            "project_id": m.context.project_id,
            "team_id": m.context.team_id,
            "visibility": m.visibility as u8,
            "valid_from": m.valid_from.to_rfc3339(),
            "valid_until": m.valid_until.map(|t| t.to_rfc3339()),
            "invalidated_by": m.invalidated_by.map(|id| hex(&id.0)),
            "recorded_at": m.recorded_at.to_rfc3339(),
            "lsn": lsn,
        })
    }

    fn audit_params(audit: &AuditEvent, lsn: u64) -> serde_json::Value {
        serde_json::json!({
            "action": audit.action,
            "actor": audit.actor,
            "org_id": audit.org_id,
            "input_digest": hex_digest(&audit.input_digest),
            "output_ids": audit.output_ids,
            "fingerprint": hex_digest(&audit.fingerprint),
            "lease_epoch": audit.lease_epoch.map(|e| e.to_string()).unwrap_or_default(),
            "recorded_at": audit.recorded_at.to_rfc3339(),
            "lsn": lsn,
        })
    }

    fn proposal_params(proposal: &DiscoveryProposal) -> Result<serde_json::Value, StorageError> {
        Ok(serde_json::json!({
            "discovery_id": proposal.discovery_id,
            "org_id": proposal.region.org,
            "region_project": proposal.region.project,
            "region_memory_type": proposal.region.memory_type,
            "from": hex(&proposal.from.0),
            "to": hex(&proposal.to.0),
            "kind": proposal.kind.0,
            "visibility": proposal.proposed_visibility as u8,
            "caller_scope_json": serde_json::to_string(&proposal.caller_scope)
                .map_err(|e| StorageError::Backend(e.to_string()))?,
            "issued_at": proposal.issued_at.to_rfc3339(),
            "props_json": serde_json::to_string(proposal)
                .map_err(|e| StorageError::Backend(e.to_string()))?,
        }))
    }

    fn discovery_create_params(
        discovery: &DiscoveryRecord,
        lsn: u64,
    ) -> Result<serde_json::Value, StorageError> {
        Ok(serde_json::json!({
            "discovery_id": discovery.discovery_id,
            "org_id": discovery.region.org,
            "region_project": discovery.region.project,
            "region_memory_type": discovery.region.memory_type,
            "from": hex(&discovery.from.0),
            "to": hex(&discovery.to.0),
            "discovered_at": discovery.discovered_at.to_rfc3339(),
            "props_json": serde_json::to_string(discovery)
                .map_err(|error| StorageError::Backend(error.to_string()))?,
            "lsn": lsn,
        }))
    }

    /// Row write without inverse materialization (R-T4 terminal).
    async fn upsert_relationship_row(
        &self,
        r: &Relationship,
    ) -> Result<CommitRecord, StorageError> {
        let lsn = self.next_lsn().await?;
        let now = Utc::now();
        let kind_label = self.kind_label(r.kind)?;
        let params = serde_json::json!({
            "rel_id": hex(&r.id.0),
            "from": hex(&r.from.0),
            "to": hex(&r.to.0),
            "kind_label": kind_label,
            "props_json": FalkorStorage::props_json(r, lsn),
            "visibility": r.visibility as u8,
            "valid_from": r.valid_from.to_rfc3339(),
            "valid_until": r.valid_until.map(|t| t.to_rfc3339()),
            "invalidated_by": r.invalidated_by.map(|id| hex(&id.0)),
            "recorded_at": r.recorded_at.to_rfc3339(),
            "lsn": lsn,
        });
        let rows = self
            .run_template("upsert_relationship", &params, false)
            .await?;
        let edge_id = rows.first().and_then(|r| r.first()).and_then(|v| {
            if let FalkorValue::I64(i) = v {
                Some(*i as u64)
            } else {
                None
            }
        });
        self.publish(Invalidation::RelationshipUpserted {
            id: r.id,
            from: r.from,
            to: r.to,
            kind: r.kind,
            lsn,
        })
        .await;
        Ok(CommitRecord {
            lsn,
            committed_at: now,
            node_id: None,
            edge_id,
        })
    }

    /// Namespace one registered write template so several template bodies
    /// can be composed into one parameterized GRAPH.QUERY.
    fn namespaced_template(
        &self,
        template_id: &str,
        params: &serde_json::Value,
        prefix: &str,
    ) -> Result<(String, Vec<(String, String)>), StorageError> {
        let t = cypher::TEMPLATES.get(template_id).ok_or_else(|| {
            StorageError::Backend(format!("unregistered cypher template: {template_id}"))
        })?;
        if t.read_only {
            return Err(StorageError::Backend(format!(
                "batch template must be writable: {template_id}"
            )));
        }
        let serde_json::Value::Object(map) = params else {
            return Err(StorageError::Backend(
                "cypher params must be an object".into(),
            ));
        };
        for required in t.required_params {
            if !map.contains_key(*required) {
                return Err(StorageError::Backend(format!(
                    "missing parameter `{required}` for {template_id}"
                )));
            }
        }
        let mut text = t.cypher.to_string();
        if let Some(kind) = params.get("kind_label").and_then(|v| v.as_str()) {
            if !self
                .ontology
                .kinds_by_id
                .values()
                .any(|k| k.display_name == kind)
            {
                return Err(StorageError::Backend(
                    "kind_label not in ontology allowlist".into(),
                ));
            }
            text = text.replace("__KIND_TYPE__", kind);
        }

        // Longest first keeps a shorter parameter name from touching the
        // prefix of a longer one.
        let mut names: Vec<&str> = t.required_params.to_vec();
        names.sort_by_key(|name| std::cmp::Reverse(name.len()));
        let mut bound = Vec::with_capacity(names.len());
        for name in names {
            let namespaced = format!("{prefix}_{name}");
            text = text.replace(&format!("${name}"), &format!("${namespaced}"));
            bound.push((
                namespaced,
                cypher_literal(map.get(name).expect("required parameter checked")),
            ));
        }
        Ok((text, bound))
    }

    async fn run_atomic_parts(
        &self,
        parts: &[(&str, serde_json::Value)],
    ) -> Result<bool, StorageError> {
        let max_lsn = parts
            .iter()
            .filter_map(|(_, values)| values.get("lsn").and_then(serde_json::Value::as_u64))
            .max();
        let mut ordered_parts = Vec::with_capacity(parts.len() + usize::from(max_lsn.is_some()));
        if let Some(lsn) = max_lsn {
            ordered_parts.push(("mutation_lsn_guard", serde_json::json!({ "lsn": lsn })));
        }
        ordered_parts.extend(parts.iter().cloned());
        let mut params = Vec::new();
        let mut bodies = Vec::with_capacity(ordered_parts.len());
        for (index, (template, values)) in ordered_parts.iter().enumerate() {
            let (body, mut bound) =
                self.namespaced_template(template, values, &format!("a{index}"))?;
            bodies.push(body);
            params.append(&mut bound);
        }
        params.sort_by(|a, b| a.0.cmp(&b.0));
        let prefix = params
            .into_iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect::<Vec<_>>()
            .join(" ");
        let body = bodies.join("\nWITH 1 AS __atomic_step\n");
        let text =
            format!("CYPHER {prefix} WITH 1 AS __atomic_step\n{body}\nRETURN 1 AS committed");
        let mut graph = self.client.select_graph(self.graph.clone());
        let result = graph
            .query(text.as_str())
            .execute()
            .await
            .map_err(|e| StorageError::Backend(format!("atomic mutation failed: {e}")))?;
        Ok(result.data.count() > 0)
    }

    /// Allocate a contiguous block of `n` LSNs. Allocation may leave a gap
    /// when the subsequent graph transaction rejects; committed rows remain
    /// monotonic and the graph transaction is the mutation authority.
    async fn next_lsn_block(&self, n: usize) -> Result<std::ops::Range<u64>, StorageError> {
        if n == 0 {
            return Ok(0..0);
        }
        let end: u64 = self
            .redis
            .clone()
            .incr(&self.lsn_key, n as u64)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        let start = end + 1 - n as u64;
        let block = start..start + n as u64;
        #[cfg(feature = "integration")]
        if self
            .pause_next_lsn
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            self.lsn_paused.notify_one();
            self.lsn_release.notified().await;
        }
        Ok(block)
    }

    /// Opt-in deployment-chaos barrier. The production binary crosses this
    /// point only for a real journaled Dreams mutation after its atomic Falkor
    /// query commits and before the engine can complete the cycle. `SET NX`
    /// makes the barrier one-shot across replicas so a successor must recover
    /// both the active journal and the durable fire.
    async fn chaos_pause_journaled_write_after_commit(
        &self,
        block: &std::ops::Range<u64>,
        cycle_id: &str,
        lease: &OwnerLease,
    ) -> Result<(), StorageError> {
        let Ok(key) = std::env::var("EXOCORTEX_CHAOS_DREAMS_BARRIER_KEY") else {
            return Ok(());
        };
        if !key.starts_with("exocortex:chaos:") || key.len() > 200 {
            return Err(StorageError::Backend(
                "EXOCORTEX_CHAOS_DREAMS_BARRIER_KEY must use the exocortex:chaos: namespace".into(),
            ));
        }
        let claimed_key = format!("{key}:claimed");
        let mut redis = self.redis.clone();
        let claimed: bool = redis
            .set_nx(&claimed_key, format!("{}:{}", self.node_id, lease.epoch))
            .await
            .map_err(|error| StorageError::Backend(error.to_string()))?;
        if !claimed {
            return Ok(());
        }
        let reached = serde_json::json!({
            "node_id": self.node_id,
            "lease_epoch": lease.epoch,
            "cycle_id": cycle_id,
            "lsn_start": block.start,
            "lsn_end_exclusive": block.end,
        });
        redis
            .set::<_, _, ()>(format!("{key}:reached"), reached.to_string())
            .await
            .map_err(|error| StorageError::Backend(error.to_string()))?;
        tracing::warn!(
            node = %self.node_id,
            epoch = lease.epoch,
            "production Dreams mutation reached configured chaos barrier"
        );
        loop {
            let released: Option<String> = redis
                .get(format!("{key}:release"))
                .await
                .map_err(|error| StorageError::Backend(error.to_string()))?;
            if released.as_deref() == Some("1") {
                return Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }

    async fn chaos_record_successor_restore(
        &self,
        block: &std::ops::Range<u64>,
        lease: &OwnerLease,
    ) -> Result<(), StorageError> {
        let Ok(key) = std::env::var("EXOCORTEX_CHAOS_DREAMS_BARRIER_KEY") else {
            return Ok(());
        };
        if !key.starts_with("exocortex:chaos:") || key.len() > 200 {
            return Err(StorageError::Backend(
                "EXOCORTEX_CHAOS_DREAMS_BARRIER_KEY must use the exocortex:chaos: namespace".into(),
            ));
        }
        let mut redis = self.redis.clone();
        let reached: Option<String> = redis
            .get(format!("{key}:reached"))
            .await
            .map_err(|error| StorageError::Backend(error.to_string()))?;
        let Some(reached) = reached else {
            return Ok(());
        };
        let reached: serde_json::Value = serde_json::from_str(&reached)
            .map_err(|error| StorageError::Backend(error.to_string()))?;
        if reached["node_id"].as_str() == Some(self.node_id.as_str())
            || reached["lease_epoch"]
                .as_u64()
                .is_some_and(|epoch| lease.epoch <= epoch)
        {
            return Ok(());
        }
        let successor = serde_json::json!({
            "node_id": self.node_id,
            "lease_epoch": lease.epoch,
            "lsn_start": block.start,
            "lsn_end_exclusive": block.end,
        });
        redis
            .set::<_, _, ()>(format!("{key}:successor"), successor.to_string())
            .await
            .map_err(|error| StorageError::Backend(error.to_string()))
    }

    /// Every row, inverse companion, endpoint guard, and optional lease
    /// guard is one compound GRAPH.QUERY. FalkorDB makes a modifying query
    /// atomic; Redis MULTI/EXEC does not roll back an earlier command when a
    /// later queued command fails (R6-U02).
    async fn upsert_batch_inner(
        &self,
        ms: &[Memory],
        rs: &[Relationship],
        lease: Option<&OwnerLease>,
        journal: Option<(&FencedRestore, &str)>,
        import_key: Option<&str>,
        publication_claim_token: Option<&str>,
    ) -> Result<Option<FencedBatchCommit>, StorageError> {
        // R-T4 inverse companions join the same transaction.
        let all_rels = self.expand_relationships(rs);

        let total = ms.len() + all_rels.len();
        let block = self.next_lsn_block(total).await?;
        let now = Utc::now();

        let mut parts: Vec<(&str, serde_json::Value)> = Vec::with_capacity(total + 2);
        let mut records = Vec::with_capacity(total);
        let mut invalidations = Vec::with_capacity(total);
        let mut next = block.start;

        if let Some(last_lsn) = block.end.checked_sub(1) {
            parts.push(("mutation_lsn_guard", serde_json::json!({ "lsn": last_lsn })));
        }

        if let Some(lease) = lease {
            parts.push((
                "lease_fence_guard",
                serde_json::json!({
                    "lease_key": serde_json::to_string(&lease.key)
                        .map_err(|e| StorageError::Backend(e.to_string()))?,
                    "token": lease.fencing_token.as_str(),
                    "epoch": lease.epoch,
                    "now_ms": Utc::now().timestamp_millis(),
                }),
            ));
        }

        let batch_memory_ids: std::collections::HashSet<MemoryId> =
            ms.iter().map(|m| m.id).collect();
        let external_ids: std::collections::BTreeSet<String> = all_rels
            .iter()
            .flat_map(|r| [r.from, r.to])
            .filter(|id| !batch_memory_ids.contains(id))
            .map(|id| hex(&id.0))
            .collect();
        parts.push((
            "batch_endpoint_guard",
            serde_json::json!({
                "external_count": external_ids.len(),
                "external_ids": external_ids,
            }),
        ));

        for m in ms {
            let mt_label = self
                .ontology
                .memory_type_names
                .get(m.memory_type as usize)
                .ok_or_else(|| {
                    StorageError::Backend(format!("bad memory_type {}", m.memory_type))
                })?;
            let params = self.memory_params(m, next, mt_label);
            parts.push(("batch_upsert_memory", params));
            invalidations.push(Invalidation::MemoryUpserted {
                id: m.id,
                lsn: next,
            });
            records.push(CommitRecord {
                lsn: next,
                committed_at: now,
                node_id: None,
                edge_id: None,
            });
            next += 1;
        }
        for r in &all_rels {
            let kind_label = self.kind_label(r.kind)?;
            let params = serde_json::json!({
                "rel_id": hex(&r.id.0),
                "from": hex(&r.from.0),
                "to": hex(&r.to.0),
                "kind_label": kind_label,
                "props_json": FalkorStorage::props_json(r, next),
                "visibility": r.visibility as u8,
                "valid_from": r.valid_from.to_rfc3339(),
                "valid_until": r.valid_until.map(|t| t.to_rfc3339()),
                "invalidated_by": r.invalidated_by.map(|id| hex(&id.0)),
                "recorded_at": r.recorded_at.to_rfc3339(),
                "lsn": next,
            });
            parts.push(("batch_upsert_relationship", params));
            invalidations.push(Invalidation::RelationshipUpserted {
                id: r.id,
                from: r.from,
                to: r.to,
                kind: r.kind,
                lsn: next,
            });
            records.push(CommitRecord {
                lsn: next,
                committed_at: now,
                node_id: None,
                edge_id: None,
            });
            next += 1;
        }

        let mut committed = FencedBatchCommit {
            records,
            ..FencedBatchCommit::default()
        };
        for (memory, record) in ms.iter().zip(&committed.records) {
            committed
                .memory_lsns
                .entry(memory.id)
                .or_default()
                .insert(record.lsn);
        }
        for (relationship, record) in all_rels.iter().zip(committed.records.iter().skip(ms.len())) {
            committed
                .relationship_lsns
                .entry(relationship.id)
                .or_default()
                .insert(record.lsn);
        }
        if let Some((prepared_restore, cycle_id)) = journal {
            let lease = lease.ok_or_else(|| {
                StorageError::Backend("journaled batch requires an owner lease".into())
            })?;
            let mut restore = prepared_restore.clone();
            for (id, lsns) in &committed.memory_lsns {
                restore
                    .owned_memory_lsns
                    .entry(*id)
                    .or_default()
                    .extend(lsns);
            }
            for (id, lsns) in &committed.relationship_lsns {
                restore
                    .owned_relationship_lsns
                    .entry(*id)
                    .or_default()
                    .extend(lsns);
            }
            parts.push((
                "batch_cycle_journal_fragment",
                serde_json::json!({
                    "lease_key": serde_json::to_string(&lease.key)
                        .map_err(|error| StorageError::Backend(error.to_string()))?,
                    "cycle_id": cycle_id,
                    "lease_epoch": lease.epoch,
                    "fragment_id": block.start,
                    "restore_json": serde_json::to_string(&restore)
                        .map_err(|error| StorageError::Backend(error.to_string()))?,
                }),
            ));
        }

        if let Some(import_key) = import_key {
            parts.insert(
                0,
                (
                    "governed_import_guard",
                    serde_json::json!({
                        "import_key": import_key,
                        "publication_json": serde_json::to_string(&invalidations)
                            .map_err(|error| StorageError::Backend(error.to_string()))?,
                        "claim_token": publication_claim_token
                            .expect("idempotent operation has claim token"),
                        "lease_ms": 30_000,
                    }),
                ),
            );
        }

        let mut query_params = Vec::new();
        let mut bodies = Vec::with_capacity(parts.len());
        for (index, (template, params)) in parts.iter().enumerate() {
            let (body, mut bound) =
                self.namespaced_template(template, params, &format!("p{index}"))?;
            bodies.push(body);
            query_params.append(&mut bound);
        }
        query_params.sort_by(|a, b| a.0.cmp(&b.0));
        let prefix = query_params
            .into_iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect::<Vec<_>>()
            .join(" ");
        let body = bodies.join("\nWITH 1 AS __batch_step\n");
        let text = format!("CYPHER {prefix} WITH 1 AS __batch_step\n{body}\nRETURN 1 AS committed");
        let mut graph = self.client.select_graph(self.graph.clone());
        let result = graph
            .query(text.as_str())
            .execute()
            .await
            .map_err(|e| StorageError::Backend(format!("atomic batch failed: {e}")))?;
        let query_committed = result.data.count() > 0;
        if !query_committed {
            if import_key.is_some() {
                return Ok(None);
            }
            return match lease {
                Some(lease) => Err(StorageError::FencedWriteRejected {
                    lease_epoch: lease.epoch,
                }),
                None => Err(StorageError::Backend(
                    "atomic batch rejected: relationship endpoint missing".into(),
                )),
            };
        }

        if let (Some((_, cycle_id)), Some(lease)) = (journal, lease) {
            self.chaos_pause_journaled_write_after_commit(&block, cycle_id, lease)
                .await?;
        }

        if let Some(operation_key) = import_key {
            self.publish_idempotent_batch(
                operation_key,
                publication_claim_token.expect("checked"),
                &invalidations,
            )
            .await?;
        } else {
            self.publish_batch(&invalidations).await;
        }
        Ok(Some(committed))
    }

    async fn commit_ingest_batch_inner(
        &self,
        key: &IngestBatchKey,
        memories: &[Memory],
        relationships: &[Relationship],
        accepted: u32,
        effect: Option<&PostIngestEffect>,
    ) -> Result<IngestCommitOutcome, StorageError> {
        let all_relationships = self.expand_relationships(relationships);
        let total = memories.len() + all_relationships.len();
        let block = self.next_lsn_block(total).await?;
        let assigned_lsn = if total == 0 { 0 } else { block.end - 1 };
        let settled = SettledIngestBatch {
            accepted,
            rejected: 0,
            assigned_lsn,
        };
        let claim_token = format!(
            "{}:{}",
            self.node_id,
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or_else(|| Utc::now().timestamp_micros() * 1_000)
        );
        let mut parts = Vec::with_capacity(total + 3);
        let batch_memory_ids: std::collections::HashSet<MemoryId> =
            memories.iter().map(|memory| memory.id).collect();
        let external_ids: std::collections::BTreeSet<String> = all_relationships
            .iter()
            .flat_map(|relationship| [relationship.from, relationship.to])
            .filter(|id| !batch_memory_ids.contains(id))
            .map(|id| hex(&id.0))
            .collect();
        parts.push((
            "ingest_endpoint_guard",
            serde_json::json!({
                "external_count": external_ids.len(),
                "external_ids": external_ids,
            }),
        ));
        parts.push((
            "ingest_claim_guard",
            serde_json::json!({
                "org_id": key.org_id,
                "producer_id": key.producer_id,
                "batch_id": key.batch_id,
                "claim_token": claim_token,
            }),
        ));
        let now = Utc::now();
        let mut records = Vec::with_capacity(total);
        let mut invalidations = Vec::with_capacity(total);
        let mut next = block.start;
        for memory in memories {
            let label = self
                .ontology
                .memory_type_names
                .get(memory.memory_type as usize)
                .ok_or_else(|| {
                    StorageError::Backend(format!("bad memory_type {}", memory.memory_type))
                })?;
            parts.push((
                "batch_upsert_memory",
                self.memory_params(memory, next, label),
            ));
            records.push(CommitRecord {
                lsn: next,
                committed_at: now,
                node_id: None,
                edge_id: None,
            });
            invalidations.push(Invalidation::MemoryUpserted {
                id: memory.id,
                lsn: next,
            });
            next += 1;
        }
        for relationship in &all_relationships {
            parts.push((
                "batch_upsert_relationship",
                serde_json::json!({
                    "rel_id": hex(&relationship.id.0),
                    "from": hex(&relationship.from.0),
                    "to": hex(&relationship.to.0),
                    "kind_label": self.kind_label(relationship.kind)?,
                    "props_json": FalkorStorage::props_json(relationship, next),
                    "visibility": relationship.visibility as u8,
                    "valid_from": relationship.valid_from.to_rfc3339(),
                    "valid_until": relationship.valid_until.map(|time| time.to_rfc3339()),
                    "invalidated_by": relationship.invalidated_by.map(|id| hex(&id.0)),
                    "recorded_at": relationship.recorded_at.to_rfc3339(),
                    "lsn": next,
                }),
            ));
            records.push(CommitRecord {
                lsn: next,
                committed_at: now,
                node_id: None,
                edge_id: None,
            });
            invalidations.push(Invalidation::RelationshipUpserted {
                id: relationship.id,
                from: relationship.from,
                to: relationship.to,
                kind: relationship.kind,
                lsn: next,
            });
            next += 1;
        }
        parts.push((
            "ingest_settle",
            serde_json::json!({
                "org_id": key.org_id,
                "producer_id": key.producer_id,
                "batch_id": key.batch_id,
                "claim_token": claim_token,
                "accepted": settled.accepted,
                "rejected": settled.rejected,
                "assigned_lsn": settled.assigned_lsn,
                "effect_id": effect.map(|effect| effect.effect_id.as_str()),
                "effect_json": effect.map(serde_json::to_string).transpose()
                    .map_err(|error| StorageError::Backend(error.to_string()))?,
            }),
        ));
        if !self.run_atomic_parts(&parts).await? {
            let rows = self
                .run_template(
                    "ingest_get_settled",
                    &serde_json::json!({
                        "org_id": key.org_id,
                        "producer_id": key.producer_id,
                        "batch_id": key.batch_id,
                    }),
                    true,
                )
                .await?;
            let row = rows.first().ok_or_else(|| {
                StorageError::Backend("ingest claim did not settle atomically".into())
            })?;
            return Ok(IngestCommitOutcome::Duplicate(decode_settled_ingest(row)?));
        }
        self.publish_batch(&invalidations).await;
        Ok(IngestCommitOutcome::Committed { records, settled })
    }
}

#[async_trait]
impl Storage for FalkorStorage {
    #[instrument(skip(self, m))]
    async fn upsert_memory(&self, m: &Memory) -> Result<CommitRecord, StorageError> {
        let lsn = self.next_lsn().await?;
        let now = Utc::now();
        let mt_label = self
            .ontology
            .memory_type_names
            .get(m.memory_type as usize)
            .ok_or_else(|| StorageError::Backend(format!("bad memory_type {}", m.memory_type)))?;
        let params = self.memory_params(m, lsn, mt_label);
        let rows = self.run_template("upsert_memory", &params, false).await?;
        let node_id = rows.first().and_then(|r| r.first()).and_then(|v| {
            if let FalkorValue::I64(i) = v {
                Some(*i as u64)
            } else {
                None
            }
        });
        self.publish(Invalidation::MemoryUpserted { id: m.id, lsn })
            .await;
        Ok(CommitRecord {
            lsn,
            committed_at: now,
            node_id,
            edge_id: None,
        })
    }

    async fn upsert_batch(
        &self,
        ms: &[Memory],
        rs: &[Relationship],
    ) -> Result<Vec<CommitRecord>, StorageError> {
        Ok(self
            .upsert_batch_inner(ms, rs, None, None, None, None)
            .await?
            .expect("ordinary batch cannot take the idempotent no-op path")
            .records)
    }

    async fn import_batch_once(
        &self,
        import_key: &str,
        ms: &[Memory],
        rs: &[Relationship],
    ) -> Result<bool, StorageError> {
        self.upsert_batch_once(import_key, ms, rs).await
    }

    async fn upsert_batch_once(
        &self,
        operation_key: &str,
        ms: &[Memory],
        rs: &[Relationship],
    ) -> Result<bool, StorageError> {
        let claim_token = format!(
            "{}:{}:{}",
            self.node_id,
            Utc::now().timestamp_micros(),
            self.publication_claim_seq
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        );
        let committed = self
            .upsert_batch_inner(ms, rs, None, None, Some(operation_key), Some(&claim_token))
            .await?
            .is_some();
        let pending_rows = self
            .run_template(
                "idempotent_batch_publication_claim",
                &serde_json::json!({
                    "operation_key": operation_key,
                    "claim_token": claim_token,
                    "lease_ms": 30_000,
                }),
                false,
            )
            .await?;
        if !committed && !pending_rows.is_empty() {
            // A prior attempt may have committed the marker and graph rows but
            // failed publication. Replay the exact payloads atomically stored
            // with that marker, independent of the retry's now-empty write set.
            let Some(FalkorValue::String(publication_json)) =
                pending_rows.first().and_then(|row| row.first())
            else {
                return Err(StorageError::CorruptMetadata {
                    key: "idempotent_batch_publication",
                    detail: "pending marker lacks publication JSON".into(),
                });
            };
            let invalidations: Vec<Invalidation> =
                serde_json::from_str(publication_json).map_err(|error| {
                    StorageError::CorruptMetadata {
                        key: "idempotent_batch_publication",
                        detail: error.to_string(),
                    }
                })?;
            self.publish_idempotent_batch(operation_key, &claim_token, &invalidations)
                .await?;
        } else if !committed
            && !self
                .run_template(
                    "idempotent_batch_publication_is_pending",
                    &serde_json::json!({ "operation_key": operation_key }),
                    true,
                )
                .await?
                .is_empty()
        {
            return Err(StorageError::Backend(
                "idempotent batch publication is owned by another worker".into(),
            ));
        }
        Ok(committed)
    }

    async fn commit_ingest_batch(
        &self,
        key: &IngestBatchKey,
        memories: &[Memory],
        relationships: &[Relationship],
        accepted: u32,
    ) -> Result<IngestCommitOutcome, StorageError> {
        self.commit_ingest_batch_inner(key, memories, relationships, accepted, None)
            .await
    }

    async fn commit_ingest_batch_with_effect(
        &self,
        key: &IngestBatchKey,
        memories: &[Memory],
        relationships: &[Relationship],
        accepted: u32,
        effect: &PostIngestEffect,
    ) -> Result<IngestCommitOutcome, StorageError> {
        self.commit_ingest_batch_inner(key, memories, relationships, accepted, Some(effect))
            .await
    }

    async fn pending_ingest_effects(
        &self,
        limit: u32,
    ) -> Result<Vec<PostIngestEffect>, StorageError> {
        let rows = self
            .run_template(
                "ingest_effects_pending",
                &serde_json::json!({ "limit": limit }),
                true,
            )
            .await?;
        rows.iter()
            .map(|row| match row.first() {
                Some(FalkorValue::String(json)) => {
                    serde_json::from_str(json).map_err(|error| StorageError::CorruptMetadata {
                        key: "ingest_effect",
                        detail: error.to_string(),
                    })
                }
                other => Err(StorageError::CorruptMetadata {
                    key: "ingest_effect",
                    detail: format!("expected string, found {other:?}"),
                }),
            })
            .collect()
    }

    async fn claim_ingest_effect(
        &self,
        claim_token: &str,
        lease_ms: i64,
    ) -> Result<Option<PostIngestEffect>, StorageError> {
        let rows = self
            .run_template(
                "ingest_effect_claim",
                &serde_json::json!({
                    "claim_token": claim_token,
                    "lease_ms": lease_ms,
                }),
                false,
            )
            .await?;
        match rows.first().and_then(|row| row.first()) {
            None => Ok(None),
            Some(FalkorValue::String(json)) => {
                serde_json::from_str(json).map(Some).map_err(|error| {
                    StorageError::CorruptMetadata {
                        key: "ingest_effect",
                        detail: error.to_string(),
                    }
                })
            }
            other => Err(StorageError::CorruptMetadata {
                key: "ingest_effect",
                detail: format!("expected string, found {other:?}"),
            }),
        }
    }

    async fn renew_ingest_effect_claim(
        &self,
        effect_id: &str,
        claim_token: &str,
        lease_ms: i64,
    ) -> Result<bool, StorageError> {
        Ok(!self
            .run_template(
                "ingest_effect_claim_renew",
                &serde_json::json!({
                    "effect_id": effect_id,
                    "claim_token": claim_token,
                    "lease_ms": lease_ms,
                }),
                false,
            )
            .await?
            .is_empty())
    }

    async fn acknowledge_ingest_effect(
        &self,
        effect_id: &str,
        claim_token: &str,
    ) -> Result<bool, StorageError> {
        Ok(!self
            .run_template(
                "ingest_effect_acknowledge",
                &serde_json::json!({
                    "effect_id": effect_id,
                    "claim_token": claim_token,
                }),
                false,
            )
            .await?
            .is_empty())
    }

    async fn delete_memory(&self, id: &MemoryId) -> Result<CommitRecord, StorageError> {
        // Soft delete: set `valid_until = now()`; do not remove the node.
        let lsn = self.next_lsn().await?;
        let now = Utc::now();
        // ST1 (audit): rewrite the serialized row too — every read path
        // reconstructs the Memory from props_json, so the node properties
        // and the row can never disagree.
        let props_json = match self.get_memory(id).await? {
            Some(mut m) => {
                m.valid_until = Some(now);
                m.recorded_at = now;
                FalkorStorage::props_json(&m, lsn)
            }
            None => String::new(),
        };
        let params = serde_json::json!({
            "id": hex(&id.0), "now": now.to_rfc3339(), "lsn": lsn,
            "props_json": props_json,
        });
        self.run_template("soft_delete_memory", &params, false)
            .await?;
        self.publish(Invalidation::MemoryDeleted { id: *id, lsn })
            .await;
        Ok(CommitRecord {
            lsn,
            committed_at: now,
            node_id: None,
            edge_id: None,
        })
    }

    async fn upsert_relationship(&self, r: &Relationship) -> Result<CommitRecord, StorageError> {
        let rec = self.upsert_relationship_row(r).await?;
        // R-T4: write `k'(b,a)` in the same operation. The companion row
        // write is terminal (no further materialization) so R-T4 never
        // recurses; DELETE-then-CREATE makes re-writes idempotent.
        if let Some(inv) = exocortex_kernel::materialize_inverse(&self.ontology, r) {
            self.upsert_relationship_row(&inv).await?;
        }
        Ok(rec)
    }

    async fn delete_relationship(&self, id: &RelationshipId) -> Result<CommitRecord, StorageError> {
        let lsn = self.next_lsn().await?;
        let now = Utc::now();
        // ST9 (audit): fetch by id (untyped match — the adapter never
        // creates `:RELATES` edges), patch the serialized row, and error
        // when no live row matched instead of reporting a successful no-op.
        let params = serde_json::json!({
            "rel_id": hex(&id.0),
        });
        let rows = self
            .run_template("get_relationship_by_id", &params, true)
            .await?;
        let props = rows.first().and_then(|r| r.first()).and_then(|v| match v {
            FalkorValue::Edge(e) => e.properties.get("props_json").cloned(),
            _ => None,
        });
        let live = props
            .as_ref()
            .map(|p| !matches!(p, FalkorValue::None))
            .unwrap_or(false);
        if !live {
            return Err(StorageError::Backend(format!(
                "delete_relationship: {} not found",
                hex(&id.0)
            )));
        }
        let FalkorValue::String(json_str) = props.expect("live implies string props_json") else {
            return Err(StorageError::Backend("bad rel props_json".into()));
        };
        let mut r: Relationship = serde_json::from_str(&json_str)
            .map_err(|e| StorageError::Backend(format!("bad rel props_json: {e}")))?;
        if r.valid_until.is_some() {
            return Err(StorageError::Backend(format!(
                "delete_relationship: {} already closed",
                hex(&id.0)
            )));
        }
        r.valid_until = Some(now);
        r.recorded_at = now;
        let params = serde_json::json!({
            "rel_id": hex(&id.0), "now": now.to_rfc3339(), "lsn": lsn,
            "props_json": FalkorStorage::props_json(&r, lsn),
        });
        let rows = self
            .run_template("soft_delete_relationship", &params, false)
            .await?;
        if rows.is_empty() {
            return Err(StorageError::Backend(format!(
                "delete_relationship: {} not found",
                hex(&id.0)
            )));
        }
        self.publish(Invalidation::RelationshipDeleted { id: *id, lsn })
            .await;
        Ok(CommitRecord {
            lsn,
            committed_at: now,
            node_id: None,
            edge_id: None,
        })
    }

    async fn promote_memory_visibility_audited(
        &self,
        memory: &Memory,
        audit: &AuditEvent,
    ) -> Result<CommitRecord, StorageError> {
        let lsn = self.next_lsn().await?;
        let now = Utc::now();
        let label = self
            .ontology
            .memory_type_names
            .get(memory.memory_type as usize)
            .ok_or_else(|| {
                StorageError::Backend(format!("bad memory_type {}", memory.memory_type))
            })?;
        let parts = [
            (
                "batch_promote_visibility_guard",
                serde_json::json!({
                    "id": hex(&memory.id.0),
                    "visibility": memory.visibility as u8,
                }),
            ),
            (
                "batch_upsert_memory",
                self.memory_params(memory, lsn, label),
            ),
            ("batch_audit_append", Self::audit_params(audit, lsn)),
        ];
        if !self.run_atomic_parts(&parts).await? {
            return Err(StorageError::Backend(
                "promotion would narrow current visibility or target disappeared".into(),
            ));
        }
        self.publish(Invalidation::MemoryUpserted { id: memory.id, lsn })
            .await;
        Ok(CommitRecord {
            lsn,
            committed_at: now,
            node_id: None,
            edge_id: None,
        })
    }

    async fn create_discovery_proposal(
        &self,
        proposal: &DiscoveryProposal,
    ) -> Result<(), StorageError> {
        if proposal.region.org != proposal.caller_scope.org_id
            || proposal.proposed_visibility > proposal.caller_scope.max_visibility
            || (!proposal.region.project.is_empty()
                && proposal.region.project != "*"
                && !proposal
                    .caller_scope
                    .project_ids
                    .contains(&proposal.region.project))
        {
            return Err(StorageError::ProposalMismatch);
        }
        if let Some(existing) = self.get_discovery_proposal(&proposal.discovery_id).await? {
            return if existing == *proposal {
                Ok(())
            } else {
                Err(StorageError::ProposalMismatch)
            };
        }
        let rows = self
            .run_template(
                "discovery_proposal_create",
                &Self::proposal_params(proposal)?,
                false,
            )
            .await?;
        if rows.is_empty() {
            match self.get_discovery_proposal(&proposal.discovery_id).await? {
                Some(existing) if existing == *proposal => Ok(()),
                Some(_) => Err(StorageError::ProposalMismatch),
                None => Err(StorageError::ProposalNotFound),
            }
        } else {
            Ok(())
        }
    }

    async fn store_discovery(&self, discovery: &DiscoveryRecord) -> Result<(), StorageError> {
        let existing = self
            .run_template(
                "discovery_record_state",
                &serde_json::json!({ "discovery_id": discovery.discovery_id }),
                true,
            )
            .await?;
        if let Some(row) = existing.first() {
            let stored: DiscoveryRecord = match row.first() {
                Some(FalkorValue::String(value)) => {
                    serde_json::from_str(value).map_err(|error| StorageError::CorruptMetadata {
                        key: "discovery_record",
                        detail: error.to_string(),
                    })?
                }
                other => {
                    return Err(StorageError::CorruptMetadata {
                        key: "discovery_record",
                        detail: format!("expected string, found {other:?}"),
                    })
                }
            };
            if stored != *discovery {
                return Err(StorageError::ProposalMismatch);
            }
            let stored_lsn = match row.get(1) {
                Some(FalkorValue::I64(value)) if *value >= 0 => *value as u64,
                other => {
                    return Err(StorageError::CorruptMetadata {
                        key: "discovery_outbox_lsn",
                        detail: format!("expected non-negative integer, found {other:?}"),
                    })
                }
            };
            if !matches!(row.get(2), Some(FalkorValue::Bool(true))) {
                self.publish_checked(&Invalidation::DiscoveryAvailable {
                    record: discovery.clone(),
                    lsn: stored_lsn,
                })
                .await?;
                self.run_template(
                    "discovery_outbox_mark_published",
                    &serde_json::json!({
                        "discovery_id": discovery.discovery_id,
                        "lsn": stored_lsn,
                    }),
                    false,
                )
                .await?;
            }
            return Ok(());
        }
        let lsn = self.next_lsn().await?;
        let create_params = Self::discovery_create_params(discovery, lsn)?;
        if !self
            .run_atomic_parts(&[("batch_discovery_record_store", create_params)])
            .await?
        {
            return Err(StorageError::ProposalMismatch);
        }
        let rows = self
            .run_template(
                "discovery_record_state",
                &serde_json::json!({ "discovery_id": discovery.discovery_id }),
                true,
            )
            .await?;
        let stored: DiscoveryRecord = match rows.first().and_then(|row| row.first()) {
            Some(FalkorValue::String(value)) => {
                serde_json::from_str(value).map_err(|error| StorageError::CorruptMetadata {
                    key: "discovery_record",
                    detail: error.to_string(),
                })?
            }
            other => {
                return Err(StorageError::CorruptMetadata {
                    key: "discovery_record",
                    detail: format!("expected string, found {other:?}"),
                })
            }
        };
        if stored != *discovery {
            return Err(StorageError::ProposalMismatch);
        }
        let stored_lsn = match rows.first().and_then(|row| row.get(1)) {
            Some(FalkorValue::I64(stored_lsn)) if *stored_lsn >= 0 => *stored_lsn as u64,
            other => {
                return Err(StorageError::CorruptMetadata {
                    key: "discovery_outbox_lsn",
                    detail: format!("expected non-negative integer, found {other:?}"),
                })
            }
        };
        let published = matches!(
            rows.first().and_then(|row| row.get(2)),
            Some(FalkorValue::Bool(true))
        );
        if !published {
            self.publish_checked(&Invalidation::DiscoveryAvailable {
                record: discovery.clone(),
                lsn: stored_lsn,
            })
            .await?;
            let marked = self
                .run_template(
                    "discovery_outbox_mark_published",
                    &serde_json::json!({
                        "discovery_id": discovery.discovery_id,
                        "lsn": stored_lsn,
                    }),
                    false,
                )
                .await?;
            if marked.is_empty() {
                return Err(StorageError::Backend(
                    "discovery outbox acknowledgement lost durable row".into(),
                ));
            }
        }
        Ok(())
    }

    async fn store_discovery_fenced(
        &self,
        discovery: &DiscoveryRecord,
        lease: &OwnerLease,
    ) -> Result<(), StorageError> {
        let lsn = self.next_lsn().await?;
        let mut params = Self::discovery_create_params(discovery, lsn)?;
        let serde_json::Value::Object(ref mut values) = params else {
            unreachable!("discovery params are an object")
        };
        values.insert(
            "lease_key".into(),
            serde_json::to_string(&lease.key)
                .map_err(|error| StorageError::Backend(error.to_string()))?
                .into(),
        );
        values.insert("token".into(), lease.fencing_token.as_str().into());
        values.insert("epoch".into(), lease.epoch.into());
        values.insert("now_ms".into(), Utc::now().timestamp_millis().into());
        let rows = self
            .run_template("fenced_discovery_record_store", &params, false)
            .await?;
        if rows.is_empty() {
            return Err(StorageError::FencedWriteRejected {
                lease_epoch: lease.epoch,
            });
        }
        match self.store_discovery(discovery).await {
            Ok(()) => Ok(()),
            Err(error) if self.discovery_is_durably_pending(discovery).await? => {
                tracing::warn!(
                    discovery_id = %discovery.discovery_id,
                    ?error,
                    "discovery committed; publication remains in the durable outbox"
                );
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    async fn repair_discovery_outbox(&self) -> Result<(), StorageError> {
        self.drain_discovery_outbox().await
    }

    async fn get_discovery(
        &self,
        discovery_id: &str,
    ) -> Result<Option<DiscoveryRecord>, StorageError> {
        let rows = self
            .run_template(
                "discovery_record_get",
                &serde_json::json!({ "discovery_id": discovery_id }),
                true,
            )
            .await?;
        match rows.first().and_then(|row| row.first()) {
            None | Some(FalkorValue::None) => Ok(None),
            Some(FalkorValue::String(json)) => {
                serde_json::from_str(json).map(Some).map_err(|error| {
                    StorageError::CorruptMetadata {
                        key: "discovery_record",
                        detail: error.to_string(),
                    }
                })
            }
            Some(other) => Err(StorageError::CorruptMetadata {
                key: "discovery_record",
                detail: format!("expected JSON string, found {other:?}"),
            }),
        }
    }

    async fn list_discoveries(
        &self,
        org_id: &str,
        limit: u32,
    ) -> Result<Vec<DiscoveryRecord>, StorageError> {
        let rows = self
            .run_template(
                "discovery_record_list",
                &serde_json::json!({ "org_id": org_id, "limit": limit.min(100) }),
                true,
            )
            .await?;
        rows.into_iter()
            .map(|row| match row.into_iter().next() {
                Some(FalkorValue::String(json)) => {
                    serde_json::from_str(&json).map_err(|error| StorageError::CorruptMetadata {
                        key: "discovery_record",
                        detail: error.to_string(),
                    })
                }
                other => Err(StorageError::CorruptMetadata {
                    key: "discovery_record",
                    detail: format!("expected JSON string, found {other:?}"),
                }),
            })
            .collect()
    }

    async fn get_discovery_proposal(
        &self,
        discovery_id: &str,
    ) -> Result<Option<DiscoveryProposal>, StorageError> {
        let rows = self
            .run_template(
                "discovery_proposal_get",
                &serde_json::json!({ "discovery_id": discovery_id }),
                true,
            )
            .await?;
        match rows.first().and_then(|row| row.first()) {
            None | Some(FalkorValue::None) => Ok(None),
            Some(FalkorValue::String(json)) => {
                serde_json::from_str(json)
                    .map(Some)
                    .map_err(|e| StorageError::CorruptMetadata {
                        key: "discovery_proposal",
                        detail: e.to_string(),
                    })
            }
            Some(other) => Err(StorageError::CorruptMetadata {
                key: "discovery_proposal",
                detail: format!("expected JSON string, found {other:?}"),
            }),
        }
    }

    async fn accept_discovery(
        &self,
        acceptance: &DiscoveryAcceptance,
    ) -> Result<CommitRecord, StorageError> {
        let relationship = &acceptance.relationship;
        let Some(proposal) = self
            .get_discovery_proposal(&acceptance.discovery_id)
            .await?
        else {
            return Err(StorageError::ProposalMismatch);
        };
        if proposal.region != acceptance.region
            || proposal.caller_scope != acceptance.caller_scope
            || proposal.from != relationship.from
            || proposal.to != relationship.to
            || proposal.kind != relationship.kind
            || proposal.proposed_visibility != relationship.visibility
            || acceptance.region.org != acceptance.caller_scope.org_id
            || acceptance.audit.org_id != acceptance.caller_scope.org_id
            || acceptance.audit.actor != acceptance.caller_scope.user_id
            || relationship.visibility > acceptance.caller_scope.max_visibility
        {
            return Err(StorageError::ProposalMismatch);
        }
        let mut relationships = vec![relationship.clone()];
        if let Some(inverse) = exocortex_kernel::materialize_inverse(&self.ontology, relationship) {
            relationships.push(inverse);
        }
        let block = self.next_lsn_block(relationships.len()).await?;
        let now = Utc::now();
        let mut parts = Vec::with_capacity(relationships.len() + 3);
        parts.push((
            "discovery_accept_guard",
            serde_json::json!({
                "discovery_id": acceptance.discovery_id,
                "org_id": acceptance.region.org,
                "region_project": acceptance.region.project,
                "region_memory_type": acceptance.region.memory_type,
                "from": hex(&relationship.from.0),
                "to": hex(&relationship.to.0),
                "kind": relationship.kind.0,
                "visibility": relationship.visibility as u8,
                "caller_scope_json": serde_json::to_string(&acceptance.caller_scope)
                    .map_err(|e| StorageError::Backend(e.to_string()))?,
                "proposal_json": serde_json::to_string(&proposal)
                    .map_err(|e| StorageError::Backend(e.to_string()))?,
            }),
        ));
        for (offset, row) in relationships.iter().enumerate() {
            let lsn = block.start + offset as u64;
            parts.push((
                "batch_upsert_relationship",
                serde_json::json!({
                    "rel_id": hex(&row.id.0), "from": hex(&row.from.0), "to": hex(&row.to.0),
                    "kind_label": self.kind_label(row.kind)?,
                    "props_json": FalkorStorage::props_json(row, lsn),
                    "visibility": row.visibility as u8,
                    "valid_from": row.valid_from.to_rfc3339(),
                    "valid_until": row.valid_until.map(|t| t.to_rfc3339()),
                    "invalidated_by": row.invalidated_by.map(|id| hex(&id.0)),
                    "recorded_at": row.recorded_at.to_rfc3339(), "lsn": lsn,
                }),
            ));
        }
        parts.push((
            "discovery_proposal_consume",
            serde_json::json!({
                "discovery_id": acceptance.discovery_id,
                "consumed_at": now.to_rfc3339(),
            }),
        ));
        parts.push((
            "batch_audit_append",
            Self::audit_params(&acceptance.audit, block.start),
        ));
        if !self.run_atomic_parts(&parts).await? {
            return Err(StorageError::ProposalMismatch);
        }
        for (offset, row) in relationships.iter().enumerate() {
            self.publish(Invalidation::RelationshipUpserted {
                id: row.id,
                from: row.from,
                to: row.to,
                kind: row.kind,
                lsn: block.start + offset as u64,
            })
            .await;
        }
        Ok(CommitRecord {
            lsn: block.start,
            committed_at: now,
            node_id: None,
            edge_id: None,
        })
    }

    async fn audit_range(
        &self,
        org_id: &str,
        since_lsn: u64,
        limit: u32,
    ) -> Result<Vec<serde_json::Value>, StorageError> {
        let rows = self
            .run_template(
                "audit_range",
                &serde_json::json!({
                    "org_id": org_id, "since_lsn": since_lsn, "limit": limit,
                }),
                true,
            )
            .await?;
        Ok(rows
            .iter()
            .filter_map(|row| row.first().map(falkor_value_to_json))
            .collect())
    }

    async fn get_memory(&self, id: &MemoryId) -> Result<Option<Memory>, StorageError> {
        // R-T11 (audit ST3): `Public` is treated as `Org` on v1 read paths —
        // fetch at the widest internal ceiling so a Public row is readable
        // instead of invisible.
        let params = serde_json::json!({
            "id": hex(&id.0),
            "max_visibility": Visibility::Public as u8,
        });
        let rows = self.run_template("get_memory_by_id", &params, true).await?;
        match rows.first().and_then(|r| r.first()) {
            Some(v) if !matches!(v, FalkorValue::None) => Ok(Some(memory_from_value(v)?)),
            _ => Ok(None),
        }
    }
    async fn get_memory_for(
        &self,
        id: &MemoryId,
        vc: &crate::VisibilityContext,
    ) -> Result<Option<Memory>, StorageError> {
        // ST4 (audit): fetch at the widest internal ceiling and apply the
        // visibility decision in Rust — an existing-but-forbidden row is
        // PermissionDenied (R-MT4), never a silent None.
        let params = serde_json::json!({
            "id": hex(&id.0),
            "max_visibility": Visibility::Public as u8,
        });
        let rows = self.run_template("get_memory_by_id", &params, true).await?;
        match rows.first().and_then(|r| r.first()) {
            Some(v) if !matches!(v, FalkorValue::None) => {
                let m = memory_from_value(v)?;
                if !crate::memory_visible(&m, vc) {
                    return Err(StorageError::PermissionDenied);
                }
                Ok(Some(m))
            }
            _ => Ok(None),
        }
    }

    async fn get_memories(&self, ids: &[MemoryId]) -> Result<Vec<Memory>, StorageError> {
        let rows = self
            .run_template(
                "get_memories_by_ids",
                &serde_json::json!({
                    "ids": ids.iter().map(|id| hex(&id.0)).collect::<Vec<_>>(),
                    "max_visibility": Visibility::Public as u8,
                }),
                true,
            )
            .await?;
        let by_id = memories_by_id(&rows)?;
        Ok(ids.iter().filter_map(|id| by_id.get(id).cloned()).collect())
    }

    async fn get_visible_memories(
        &self,
        ids: &[MemoryId],
        vc: &crate::VisibilityContext,
    ) -> Result<Vec<Memory>, StorageError> {
        let rows = self
            .run_template(
                "get_visible_memories_by_ids",
                &serde_json::json!({
                    "ids": ids.iter().map(|id| hex(&id.0)).collect::<Vec<_>>(),
                    "max_visibility": fetch_ceiling(vc.max_visibility),
                    "org_id": vc.org_id,
                    "user_id": vc.user_id,
                    "project_ids": vc.project_ids,
                    "team_ids": vc.team_ids,
                }),
                true,
            )
            .await?;
        let by_id = memories_by_id(&rows)?;
        Ok(ids
            .iter()
            .filter_map(|id| by_id.get(id))
            .filter(|memory| crate::memory_visible(memory, vc))
            .cloned()
            .collect())
    }

    async fn get_relationship(
        &self,
        id: &RelationshipId,
    ) -> Result<Option<Relationship>, StorageError> {
        let rows = self
            .run_template(
                "get_relationship_by_id",
                &serde_json::json!({ "rel_id": hex(&id.0) }),
                true,
            )
            .await?;
        match rows.first().and_then(|row| row.first()) {
            Some(value) if !matches!(value, FalkorValue::None) => {
                Ok(Some(relationship_from_value(value)?))
            }
            _ => Ok(None),
        }
    }

    async fn get_relationships(
        &self,
        ids: &[RelationshipId],
    ) -> Result<Vec<Relationship>, StorageError> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let rows = self
            .run_template(
                "get_relationships_by_ids",
                &serde_json::json!({
                    "rel_ids": ids.iter().map(|id| hex(&id.0)).collect::<Vec<_>>()
                }),
                true,
            )
            .await?;
        rows.iter()
            .filter_map(|row| row.first())
            .map(relationship_from_value)
            .collect()
    }

    async fn relationships_touching(
        &self,
        frontier: &[MemoryId],
        limit: u32,
    ) -> Result<Vec<Relationship>, StorageError> {
        let frontier: Vec<_> = frontier.iter().map(|id| hex(&id.0)).collect();
        let rows = self
            .run_template(
                "relationships_touching",
                &serde_json::json!({ "frontier": frontier, "limit": limit }),
                true,
            )
            .await?;
        rows.iter()
            .filter_map(|row| row.first())
            .map(relationship_from_value)
            .collect()
    }

    async fn relationships_in_region(
        &self,
        region: &RegionKey,
        limit: u32,
    ) -> Result<Vec<Relationship>, StorageError> {
        let query_limit = limit.saturating_add(1);
        let rows = self
            .run_template(
                "relationships_in_region",
                &serde_json::json!({
                    "org_id": region.org,
                    "project_id": region.project,
                    "memory_type": region.memory_type,
                    "limit": query_limit,
                }),
                true,
            )
            .await?;
        if rows.len() > limit as usize {
            return Err(StorageError::Backend(format!(
                "region relationship budget exceeded: more than {limit} rows"
            )));
        }
        let mut relationships: Vec<_> = rows
            .iter()
            .filter_map(|row| row.first())
            .map(relationship_from_value)
            .collect::<Result<_, _>>()?;
        relationships.sort_by_key(|relationship| {
            (
                relationship.from,
                relationship.to,
                relationship.kind,
                relationship.id,
            )
        });
        Ok(relationships)
    }

    async fn memories_in_region(
        &self,
        region: &RegionKey,
        limit: u32,
    ) -> Result<Vec<Memory>, StorageError> {
        let rows = self
            .run_template(
                "memories_in_region",
                &serde_json::json!({
                    "org_id": region.org,
                    "project_id": region.project,
                    "memory_type": region.memory_type,
                    "limit": limit.saturating_add(1),
                }),
                true,
            )
            .await?;
        if rows.len() > limit as usize {
            return Err(StorageError::Backend(format!(
                "region memory budget exceeded: more than {limit} rows"
            )));
        }
        rows.iter()
            .filter_map(|row| row.first())
            .map(memory_from_value)
            .collect()
    }

    async fn current_relationships_in_region(
        &self,
        region: &RegionKey,
        limit: u32,
    ) -> Result<Vec<Relationship>, StorageError> {
        let rows = self
            .run_template(
                "current_relationships_in_region",
                &serde_json::json!({
                    "org_id": region.org,
                    "project_id": region.project,
                    "memory_type": region.memory_type,
                    "limit": limit.saturating_add(1),
                }),
                true,
            )
            .await?;
        if rows.len() > limit as usize {
            return Err(StorageError::Backend(format!(
                "region relationship budget exceeded: more than {limit} rows"
            )));
        }
        rows.iter()
            .filter_map(|row| row.first())
            .map(relationship_from_value)
            .collect()
    }

    async fn memories_sharing_attributes(
        &self,
        tags: &[smol_str::SmolStr],
        entities: &[EntityId],
        limit: u32,
    ) -> Result<Vec<Memory>, StorageError> {
        let rows = self
            .run_template(
                "memories_sharing_attributes",
                &serde_json::json!({
                    "attribute_keys": tags
                        .iter()
                        .map(|tag| format!("t:{tag}"))
                        .chain(entities.iter().map(|id| format!("e:{}", hex(&id.0))))
                        .collect::<Vec<_>>(),
                    "limit": limit,
                }),
                true,
            )
            .await?;
        rows.iter()
            .filter_map(|row| row.first())
            .map(memory_from_value)
            .collect()
    }

    async fn traverse(
        &self,
        from: &MemoryId,
        spec: &TraversalSpec,
    ) -> Result<Vec<Memory>, StorageError> {
        // CR-6 hard caps before touching Cypher.
        if spec.max_depth > 4 {
            return Err(StorageError::Backend("max_depth > 4".into()));
        }
        if spec.max_nodes > 2048 {
            return Err(StorageError::Backend("max_nodes > 2048".into()));
        }
        let kinds: Vec<&str> = spec
            .kinds
            .iter()
            .map(|k| self.kind_label(*k))
            .collect::<Result<_, _>>()?;
        if spec.max_depth == 0 || spec.max_nodes == 0 {
            return Ok(Vec::new());
        }
        if self
            .get_visible_memories(&[*from], &spec.visibility_ctx)
            .await?
            .is_empty()
        {
            return Ok(Vec::new());
        }
        let mut frontier = vec![*from];
        let mut seen = std::collections::HashSet::from([*from]);
        let mut out = Vec::new();
        for _ in 0..spec.max_depth {
            let remaining = spec.max_nodes as usize - out.len();
            if remaining == 0 || frontier.is_empty() {
                break;
            }
            let params = serde_json::json!({
            "frontier": frontier.iter().map(|id| hex(&id.0)).collect::<Vec<_>>(),
            "kind_labels": kinds, "max_nodes": remaining,
            // R-T11: Public reads as Org, so an Org-scoped traversal fetches
            // at the widest internal ceiling (ST3 parity with the double).
            "max_visibility": fetch_ceiling(spec.visibility_ctx.max_visibility),
            "org_id": spec.visibility_ctx.org_id,
            "user_id": spec.visibility_ctx.user_id,
            "project_ids": spec.visibility_ctx.project_ids,
            "team_ids": spec.visibility_ctx.team_ids,
            });
            let mut rows = Vec::new();
            if matches!(spec.direction, Direction::Out | Direction::Both) {
                rows.extend(
                    self.run_template("traverse_one_hop_out", &params, true)
                        .await?,
                );
            }
            if matches!(spec.direction, Direction::In | Direction::Both) {
                rows.extend(
                    self.run_template("traverse_one_hop_in", &params, true)
                        .await?,
                );
            }
            let mut next = Vec::new();
            for row in &rows {
                if let Some(value) = row.first() {
                    let memory = memory_from_value(value)?;
                    if crate::memory_visible(&memory, &spec.visibility_ctx)
                        && seen.insert(memory.id)
                    {
                        next.push(memory);
                    }
                }
            }
            next.sort_by_key(|memory| memory.id);
            next.truncate(remaining);
            frontier = next.iter().map(|memory| memory.id).collect();
            out.extend(next);
        }
        Ok(out)
    }

    async fn find_by_entity(
        &self,
        entity: &EntityId,
        filter: &MemoryFilter,
    ) -> Result<Vec<Memory>, StorageError> {
        if filter.limit > 500 {
            return Err(StorageError::Backend("limit > 500".into()));
        }
        let project_id = filter.project_id.as_deref().unwrap_or_default();
        let valid_at = filter.valid_at.unwrap_or_else(Utc::now);
        let params = serde_json::json!({
            "entity_id": hex(&entity.0), "limit": filter.limit,
            "max_visibility": fetch_ceiling(filter.visibility_ctx.max_visibility),
            "org_id": filter.visibility_ctx.org_id,
            "user_id": filter.visibility_ctx.user_id,
            "project_ids": filter.visibility_ctx.project_ids,
            "team_ids": filter.visibility_ctx.team_ids,
            "memory_types": filter.memory_types,
            "project_id": project_id,
            "has_project": filter.project_id.is_some(),
            "valid_at": valid_at.to_rfc3339(),
            "has_valid_at": filter.valid_at.is_some(),
        });
        let rows = self.run_template("find_by_entity", &params, true).await?;
        let mut out = Vec::new();
        for row in &rows {
            if let Some(v) = row.first() {
                let m = memory_from_value(v)?;
                if (filter.memory_types.is_empty() || filter.memory_types.contains(&m.memory_type))
                    && filter
                        .project_id
                        .as_ref()
                        .is_none_or(|project| m.context.project_id.as_ref() == Some(project))
                    && filter.valid_at.is_none_or(|at| {
                        m.valid_from <= at && m.valid_until.is_none_or(|until| until > at)
                    })
                    && crate::memory_visible(&m, &filter.visibility_ctx)
                {
                    out.push(m);
                }
            }
        }
        Ok(out)
    }

    async fn get_state_at(&self, t: DateTime<Utc>) -> Result<GraphSnapshot, StorageError> {
        let params = serde_json::json!({
            "at": t.to_rfc3339(), "max_visibility": Visibility::Public as u8,
        });
        let mem_rows = self.run_template("count_state_at", &params, true).await?;
        let rel_rows = self
            .run_template("count_state_at_rels", &params, true)
            .await?;
        let n = |rows: &[Vec<FalkorValue>], key: &'static str| match rows
            .first()
            .and_then(|row| row.first())
        {
            Some(FalkorValue::I64(value)) if *value >= 0 => Ok(*value as u64),
            other => Err(StorageError::CorruptMetadata {
                key,
                detail: format!("expected one non-negative integer count, got {other:?}"),
            }),
        };
        Ok(GraphSnapshot {
            as_of: t,
            backend_lsn: self.last_backend_lsn().await?,
            memory_count: n(&mem_rows, "snapshot_memory_count")?,
            relationship_count: n(&rel_rows, "snapshot_relationship_count")?,
        })
    }

    async fn valid_at(
        &self,
        id: &MemoryId,
        at: DateTime<Utc>,
    ) -> Result<Option<Memory>, StorageError> {
        let params = serde_json::json!({
            "id": hex(&id.0), "at": at.to_rfc3339(),
            "max_visibility": Visibility::Public as u8,
        });
        let rows = self.run_template("valid_at", &params, true).await?;
        match rows.first().and_then(|r| r.first()) {
            Some(v) if !matches!(v, FalkorValue::None) => Ok(Some(memory_from_value(v)?)),
            _ => Ok(None),
        }
    }

    async fn query_cypher(&self, q: &CypherQuery) -> Result<ResultSet, StorageError> {
        let t = cypher::validate(q)?;
        let rows = self.run_template(t.id, &q.params, q.read_only).await?;
        let scanned = rows.len() as u64;
        let json_rows = rows
            .iter()
            .map(|row| serde_json::Value::Array(row.iter().map(falkor_value_to_json).collect()))
            .collect();
        Ok(ResultSet {
            rows: json_rows,
            scanned_rows: scanned,
        })
    }

    async fn stream_all_memories(&self) -> BoxStream<'_, Result<Memory, StorageError>> {
        let stream = futures::stream::unfold(
            (self, 0_u64, true, std::collections::VecDeque::new(), false),
            |(storage, mut cursor, mut first_page, mut buffered, mut done)| async move {
                loop {
                    if let Some(item) = buffered.pop_front() {
                        return Some((item, (storage, cursor, first_page, buffered, done)));
                    }
                    if done {
                        return None;
                    }
                    let params = serde_json::json!({
                        "after_lsn": cursor, "first_page": first_page, "limit": 500_u32
                    });
                    #[cfg(feature = "integration")]
                    storage.stream_memory_pages.fetch_add(1, Ordering::Relaxed);
                    let rows = match storage.run_template("stream_memories", &params, true).await {
                        Ok(rows) => rows,
                        Err(error) => {
                            done = true;
                            buffered.push_back(Err(error));
                            continue;
                        }
                    };
                    if rows.is_empty() {
                        done = true;
                        continue;
                    }
                    let mut decoded = std::collections::VecDeque::new();
                    let mut page_cursor = cursor;
                    for row in &rows {
                        let result = (|| {
                            page_cursor = decode_stream_lsn(row, page_cursor, "memory")?;
                            let value =
                                row.first().ok_or_else(|| StorageError::CorruptMetadata {
                                    key: "stream_cursor",
                                    detail: "memory stream row is empty".into(),
                                })?;
                            memory_from_value(value)
                        })();
                        match result {
                            Ok(memory) => decoded.push_back(Ok(memory)),
                            Err(error) => {
                                done = true;
                                buffered.clear();
                                buffered.push_back(Err(error));
                                break;
                            }
                        }
                    }
                    if !done {
                        cursor = page_cursor;
                        first_page = false;
                        buffered = decoded;
                    }
                }
            },
        );
        Box::pin(stream)
    }

    async fn stream_all_relationships(&self) -> BoxStream<'_, Result<Relationship, StorageError>> {
        let stream = futures::stream::unfold(
            (self, 0_u64, true, std::collections::VecDeque::new(), false),
            |(storage, mut cursor, mut first_page, mut buffered, mut done)| async move {
                loop {
                    if let Some(item) = buffered.pop_front() {
                        return Some((item, (storage, cursor, first_page, buffered, done)));
                    }
                    if done {
                        return None;
                    }
                    let params = serde_json::json!({
                        "after_lsn": cursor, "first_page": first_page, "limit": 500_u32
                    });
                    #[cfg(feature = "integration")]
                    storage
                        .stream_relationship_pages
                        .fetch_add(1, Ordering::Relaxed);
                    let rows = match storage
                        .run_template("stream_relationships", &params, true)
                        .await
                    {
                        Ok(rows) => rows,
                        Err(error) => {
                            done = true;
                            buffered.push_back(Err(error));
                            continue;
                        }
                    };
                    if rows.is_empty() {
                        done = true;
                        continue;
                    }
                    let mut decoded = std::collections::VecDeque::new();
                    let mut page_cursor = cursor;
                    for row in &rows {
                        let result = (|| {
                            page_cursor = decode_stream_lsn(row, page_cursor, "relationship")?;
                            let value =
                                row.first().ok_or_else(|| StorageError::CorruptMetadata {
                                    key: "stream_cursor",
                                    detail: "relationship stream row is empty".into(),
                                })?;
                            relationship_from_value(value)
                        })();
                        match result {
                            Ok(relationship) => decoded.push_back(Ok(relationship)),
                            Err(error) => {
                                done = true;
                                buffered.clear();
                                buffered.push_back(Err(error));
                                break;
                            }
                        }
                    }
                    if !done {
                        cursor = page_cursor;
                        first_page = false;
                        buffered = decoded;
                    }
                }
            },
        );
        Box::pin(stream)
    }

    async fn find_similar_offline(
        &self,
        _query: &Embedding,
        _k: usize,
        _filter: &MemoryFilter,
    ) -> Result<Vec<(MemoryId, f32)>, StorageError> {
        // Embeddings + the offline vector table land with ingest enrichment
        // (§7.2) and Dreams (§11); the interactive path never calls this
        // (R-Mcr4).
        Err(StorageError::Backend(
            "find_similar_offline: embeddings not yet stored (M6/M8)".into(),
        ))
    }

    async fn acquire_lease(
        &self,
        key: &LeaseKey,
        ttl: std::time::Duration,
    ) -> Result<OwnerLease, StorageError> {
        let key_str = serde_json::to_string(key)
            .map_err(|e| StorageError::Backend(format!("serialize lease key: {e}")))?;
        let now = Utc::now();
        let ttl = chrono::Duration::from_std(ttl)
            .map_err(|e| StorageError::Backend(format!("invalid lease ttl: {e}")))?;
        let expires_at = now + ttl;
        let token = format!(
            "{}:{}",
            self.node_id,
            now.timestamp_nanos_opt()
                .unwrap_or(now.timestamp_micros() * 1_000)
        );
        let rows = self
            .run_template(
                "lease_acquire",
                &serde_json::json!({
                    "lease_key": key_str,
                    "token": token,
                    "now_ms": now.timestamp_millis(),
                    "expires_at_ms": expires_at.timestamp_millis(),
                }),
                false,
            )
            .await?;
        let Some(epoch) = rows.first().and_then(|row| row.first()).and_then(|v| {
            if let FalkorValue::I64(epoch) = v {
                u64::try_from(*epoch).ok()
            } else {
                None
            }
        }) else {
            return Err(StorageError::Backend("lease held by another node".into()));
        };
        Ok(OwnerLease {
            key: key.clone(),
            owner_node_id: self.node_id.clone(),
            epoch,
            acquired_at: now,
            expires_at,
            grace_period: crate::trait_::grace_duration(),
            fencing_token: token.into(),
        })
    }

    async fn renew_lease(&self, lease: &OwnerLease) -> Result<OwnerLease, StorageError> {
        let now = Utc::now();
        let ttl = lease.expires_at - lease.acquired_at;
        let expires_at = now + ttl;
        let rows = self
            .run_template(
                "lease_renew",
                &serde_json::json!({
                    "lease_key": serde_json::to_string(&lease.key)
                        .map_err(|e| StorageError::Backend(e.to_string()))?,
                    "token": lease.fencing_token.as_str(),
                    "epoch": lease.epoch,
                    "now_ms": now.timestamp_millis(),
                    "expires_at_ms": expires_at.timestamp_millis(),
                }),
                false,
            )
            .await?;
        if rows.is_empty() {
            return Err(StorageError::Backend("lease lost (token mismatch)".into()));
        }
        Ok(OwnerLease {
            acquired_at: now,
            expires_at,
            ..lease.clone()
        })
    }

    async fn release_lease(&self, lease: OwnerLease) -> Result<(), StorageError> {
        let _ = self
            .run_template(
                "lease_release",
                &serde_json::json!({
                    "lease_key": serde_json::to_string(&lease.key)
                        .map_err(|e| StorageError::Backend(e.to_string()))?,
                    "token": lease.fencing_token.as_str(),
                    "epoch": lease.epoch,
                }),
                false,
            )
            .await?;
        Ok(())
    }

    async fn upsert_batch_fenced(
        &self,
        ms: &[Memory],
        rs: &[Relationship],
        lease: &OwnerLease,
    ) -> Result<FencedBatchCommit, StorageError> {
        self.upsert_batch_inner(ms, rs, Some(lease), None, None, None)
            .await?
            .ok_or_else(|| StorageError::Backend("fenced batch unexpectedly became a no-op".into()))
    }

    async fn upsert_batch_fenced_journaled(
        &self,
        ms: &[Memory],
        rs: &[Relationship],
        prepared_restore: &FencedRestore,
        cycle_id: &str,
        lease: &OwnerLease,
    ) -> Result<FencedBatchCommit, StorageError> {
        self.upsert_batch_inner(
            ms,
            rs,
            Some(lease),
            Some((prepared_restore, cycle_id)),
            None,
            None,
        )
        .await?
        .ok_or_else(|| StorageError::Backend("journaled batch unexpectedly became a no-op".into()))
    }

    async fn get_active_cycle_journal(
        &self,
        key: &LeaseKey,
    ) -> Result<Option<CycleJournalRecord>, StorageError> {
        let lease_key =
            serde_json::to_string(key).map_err(|error| StorageError::Backend(error.to_string()))?;
        let rows = self
            .run_template(
                "active_cycle_journal",
                &serde_json::json!({ "lease_key": lease_key }),
                true,
            )
            .await?;
        if rows.is_empty() {
            return Ok(None);
        }
        let cycle_id = match rows[0].first() {
            Some(FalkorValue::String(value)) => value.clone(),
            other => {
                return Err(StorageError::Backend(format!(
                    "corrupt cycle journal id: {other:?}"
                )))
            }
        };
        let lease_epoch = match rows[0].get(1) {
            Some(FalkorValue::I64(value)) if *value >= 0 => *value as u64,
            other => {
                return Err(StorageError::Backend(format!(
                    "corrupt cycle journal epoch: {other:?}"
                )))
            }
        };
        let mut restore = FencedRestore::default();
        for row in rows {
            let fragment = match row.get(2) {
                Some(FalkorValue::String(value)) => serde_json::from_str(value)
                    .map_err(|error| StorageError::Backend(error.to_string()))?,
                other => {
                    return Err(StorageError::Backend(format!(
                        "corrupt cycle journal fragment: {other:?}"
                    )))
                }
            };
            restore.merge(&fragment);
        }
        Ok(Some(CycleJournalRecord {
            cycle_id: cycle_id.into(),
            lease_key: key.clone(),
            lease_epoch,
            restore,
            state: CycleJournalState::Active,
        }))
    }

    async fn complete_cycle_journal_fenced(
        &self,
        cycle_id: &str,
        lease: &OwnerLease,
    ) -> Result<(), StorageError> {
        let rows = self
            .run_template(
                "cycle_journal_complete_fenced",
                &serde_json::json!({
                    "lease_key": serde_json::to_string(&lease.key)
                        .map_err(|error| StorageError::Backend(error.to_string()))?,
                    "cycle_id": cycle_id,
                    "token": lease.fencing_token.as_str(),
                    "epoch": lease.epoch,
                    "now_ms": Utc::now().timestamp_millis(),
                }),
                false,
            )
            .await?;
        if rows.is_empty() {
            return Err(StorageError::FencedWriteRejected {
                lease_epoch: lease.epoch,
            });
        }
        Ok(())
    }

    async fn cycle_succeeded(&self, key: &LeaseKey, cycle_id: &str) -> Result<bool, StorageError> {
        let rows = self
            .run_template(
                "cycle_journal_succeeded",
                &serde_json::json!({
                    "lease_key": serde_json::to_string(key)
                        .map_err(|error| StorageError::Backend(error.to_string()))?,
                    "cycle_id": cycle_id,
                }),
                true,
            )
            .await?;
        match rows.first().and_then(|row| row.first()) {
            Some(FalkorValue::I64(0)) => Ok(false),
            Some(FalkorValue::I64(1)) => Ok(true),
            other => Err(StorageError::CorruptMetadata {
                key: "cycle journal success count",
                detail: format!("expected 0 or 1, got {other:?}"),
            }),
        }
    }

    async fn settle_dreams_cycle_fenced(
        &self,
        cycle_id: &str,
        discoveries: &[DiscoveryRecord],
        lease: &OwnerLease,
    ) -> Result<(), StorageError> {
        let mut discovery_ids = Vec::with_capacity(discoveries.len());
        let mut org_ids = Vec::with_capacity(discoveries.len());
        let mut region_projects = Vec::with_capacity(discoveries.len());
        let mut region_memory_types = Vec::with_capacity(discoveries.len());
        let mut from_ids = Vec::with_capacity(discoveries.len());
        let mut to_ids = Vec::with_capacity(discoveries.len());
        let mut discovered_ats = Vec::with_capacity(discoveries.len());
        let mut discovery_props = Vec::with_capacity(discoveries.len());
        let mut discovery_lsns = Vec::with_capacity(discoveries.len());
        let mut max_lsn = 0u64;
        for discovery in discoveries {
            let lsn = self.next_lsn().await?;
            max_lsn = max_lsn.max(lsn);
            discovery_ids.push(discovery.discovery_id.to_string());
            org_ids.push(discovery.region.org.to_string());
            region_projects.push(discovery.region.project.to_string());
            region_memory_types.push(discovery.region.memory_type);
            from_ids.push(hex(&discovery.from.0));
            to_ids.push(hex(&discovery.to.0));
            discovered_ats.push(discovery.discovered_at.to_rfc3339());
            discovery_props.push(
                serde_json::to_string(discovery)
                    .map_err(|error| StorageError::Backend(error.to_string()))?,
            );
            discovery_lsns.push(lsn);
        }
        let discovery_indexes = (0..discoveries.len() as u64).collect::<Vec<_>>();
        let settled = self
            .run_template(
                "dreams_cycle_settle_fenced",
                &serde_json::json!({
                    "lease_key": serde_json::to_string(&lease.key)
                        .map_err(|error| StorageError::Backend(error.to_string()))?,
                    "cycle_id": cycle_id,
                    "token": lease.fencing_token.as_str(),
                    "epoch": lease.epoch,
                    "now_ms": Utc::now().timestamp_millis(),
                    "discovery_indexes": discovery_indexes,
                    "discovery_ids": discovery_ids,
                    "org_ids": org_ids,
                    "region_projects": region_projects,
                    "region_memory_types": region_memory_types,
                    "from_ids": from_ids,
                    "to_ids": to_ids,
                    "discovered_ats": discovered_ats,
                    "discovery_props": discovery_props,
                    "discovery_lsns": discovery_lsns,
                    "max_lsn": max_lsn,
                }),
                false,
            )
            .await?;
        if settled.is_empty() {
            return Err(StorageError::FencedWriteRejected {
                lease_epoch: lease.epoch,
            });
        }
        match self.drain_discovery_outbox().await {
            Ok(()) => Ok(()),
            Err(error) => {
                for discovery in discoveries {
                    if !self.discovery_is_durably_pending(discovery).await? {
                        return Err(error);
                    }
                }
                tracing::warn!(
                    count = discoveries.len(),
                    ?error,
                    "Dreams cycle settled; discovery publication remains in durable outbox"
                );
                Ok(())
            }
        }
    }

    async fn delete_memory_fenced(
        &self,
        id: &MemoryId,
        lease: &OwnerLease,
    ) -> Result<CommitRecord, StorageError> {
        let lsn = self.next_lsn().await?;
        let now = Utc::now();
        let props_json = match self.get_memory(id).await? {
            Some(mut m) => {
                m.valid_until = Some(now);
                FalkorStorage::props_json(&m, lsn)
            }
            None => String::new(),
        };
        let parts = [
            ("mutation_lsn_guard", serde_json::json!({ "lsn": lsn })),
            (
                "lease_fence_guard",
                serde_json::json!({
                    "lease_key": serde_json::to_string(&lease.key)
                        .map_err(|e| StorageError::Backend(e.to_string()))?,
                    "token": lease.fencing_token.as_str(),
                    "epoch": lease.epoch,
                    "now_ms": Utc::now().timestamp_millis(),
                }),
            ),
            (
                "batch_soft_delete_memory",
                serde_json::json!({
                    "id": hex(&id.0),
                    "now": now.to_rfc3339(),
                    "lsn": lsn,
                    "props_json": props_json,
                }),
            ),
        ];
        let mut params = Vec::new();
        let mut bodies = Vec::new();
        for (index, (template, values)) in parts.iter().enumerate() {
            let (body, mut bound) =
                self.namespaced_template(template, values, &format!("d{index}"))?;
            bodies.push(body);
            params.append(&mut bound);
        }
        params.sort_by(|a, b| a.0.cmp(&b.0));
        let prefix = params
            .into_iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect::<Vec<_>>()
            .join(" ");
        let text = format!(
            "CYPHER {prefix} WITH 1 AS __delete_step\n{}\nRETURN 1 AS committed",
            bodies.join("\nWITH 1 AS __delete_step\n")
        );
        let mut graph = self.client.select_graph(self.graph.clone());
        let result = graph
            .query(text.as_str())
            .execute()
            .await
            .map_err(|e| StorageError::Backend(format!("atomic fenced delete failed: {e}")))?;
        if result.data.count() == 0 {
            return Err(StorageError::FencedWriteRejected {
                lease_epoch: lease.epoch,
            });
        }
        self.publish(Invalidation::MemoryDeleted { id: *id, lsn })
            .await;
        Ok(CommitRecord {
            lsn,
            committed_at: now,
            node_id: None,
            edge_id: None,
        })
    }

    async fn restore_fenced(
        &self,
        restore: &FencedRestore,
        lease: &OwnerLease,
    ) -> Result<Vec<CommitRecord>, StorageError> {
        let total = restore
            .created_relationships
            .iter()
            .filter(|relationship| {
                restore
                    .owned_relationship_lsns
                    .contains_key(&relationship.id)
            })
            .count()
            + restore
                .created_memories
                .iter()
                .filter(|memory| restore.owned_memory_lsns.contains_key(&memory.id))
                .count()
            + restore
                .memories
                .iter()
                .filter(|memory| restore.owned_memory_lsns.contains_key(&memory.id))
                .count()
            + restore
                .relationships
                .iter()
                .filter(|relationship| {
                    restore
                        .owned_relationship_lsns
                        .contains_key(&relationship.id)
                })
                .count();
        let block = self.next_lsn_block(total).await?;
        let now = Utc::now();
        let mut next = block.start;
        let mut parts = vec![(
            "lease_fence_guard",
            serde_json::json!({
                "lease_key": serde_json::to_string(&lease.key)
                    .map_err(|error| StorageError::Backend(error.to_string()))?,
                "token": lease.fencing_token.as_str(),
                "epoch": lease.epoch,
                "now_ms": now.timestamp_millis(),
            }),
        )];
        let mut records = Vec::with_capacity(total);
        let mut invalidations = Vec::with_capacity(total);
        let mut push_record = |lsn| {
            records.push(CommitRecord {
                lsn,
                committed_at: now,
                node_id: None,
                edge_id: None,
            });
        };

        for relationship in &restore.created_relationships {
            let Some(owned_lsns) = restore.owned_relationship_lsns.get(&relationship.id) else {
                continue;
            };
            parts.push((
                "batch_purge_relationship_if_current",
                serde_json::json!({
                    "rel_id": hex(&relationship.id.0),
                    "kind_label": self.kind_label(relationship.kind)?,
                    "owned_lsns": owned_lsns,
                }),
            ));
            push_record(next);
            // The exact-owned purge may expose an interleaved concurrent
            // assertion rather than remove the identity. Force an
            // authoritative point fetch; a genuinely absent row triggers
            // the cache bridge's fail-closed reseed path.
            invalidations.push(Invalidation::RelationshipUpserted {
                id: relationship.id,
                from: relationship.from,
                to: relationship.to,
                kind: relationship.kind,
                lsn: next,
            });
            next += 1;
        }
        for memory in &restore.created_memories {
            let Some(owned_lsns) = restore.owned_memory_lsns.get(&memory.id) else {
                continue;
            };
            parts.push((
                "batch_purge_memory_if_current",
                serde_json::json!({ "id": hex(&memory.id.0), "owned_lsns": owned_lsns }),
            ));
            parts.push((
                "refresh_memory_attribute_index",
                serde_json::json!({ "id": hex(&memory.id.0) }),
            ));
            push_record(next);
            invalidations.push(Invalidation::MemoryUpserted {
                id: memory.id,
                lsn: next,
            });
            next += 1;
        }
        for memory in &restore.memories {
            let Some(owned_lsns) = restore.owned_memory_lsns.get(&memory.id) else {
                continue;
            };
            let label = self
                .ontology
                .memory_type_names
                .get(memory.memory_type as usize)
                .ok_or_else(|| {
                    StorageError::Backend(format!("bad memory_type {}", memory.memory_type))
                })?;
            let mut params = self.memory_params(memory, next, label);
            params["owned_lsns"] = serde_json::json!(owned_lsns);
            params["preimage_lsn"] = serde_json::json!(memory.lsn.value);
            parts.push(("batch_restore_memory_if_current", params));
            parts.push((
                "refresh_memory_attribute_index",
                serde_json::json!({ "id": hex(&memory.id.0) }),
            ));
            push_record(next);
            invalidations.push(Invalidation::MemoryUpserted {
                id: memory.id,
                lsn: next,
            });
            next += 1;
        }
        for relationship in &restore.relationships {
            let Some(owned_lsns) = restore.owned_relationship_lsns.get(&relationship.id) else {
                continue;
            };
            parts.push((
                "batch_restore_relationship_if_current",
                serde_json::json!({
                    "rel_id": hex(&relationship.id.0),
                    "owned_lsns": owned_lsns,
                    "preimage_lsn": relationship.lsn.value,
                    "from": hex(&relationship.from.0),
                    "to": hex(&relationship.to.0),
                    "kind_label": self.kind_label(relationship.kind)?,
                    "props_json": FalkorStorage::props_json(relationship, next),
                    "visibility": relationship.visibility as u8,
                    "valid_from": relationship.valid_from.to_rfc3339(),
                    "valid_until": relationship.valid_until.map(|time| time.to_rfc3339()),
                    "invalidated_by": relationship.invalidated_by.map(|id| hex(&id.0)),
                    "recorded_at": relationship.recorded_at.to_rfc3339(),
                    "lsn": next,
                }),
            ));
            push_record(next);
            invalidations.push(Invalidation::RelationshipUpserted {
                id: relationship.id,
                from: relationship.from,
                to: relationship.to,
                kind: relationship.kind,
                lsn: next,
            });
            next += 1;
        }
        if !self.run_atomic_parts(&parts).await? {
            return Err(StorageError::FencedWriteRejected {
                lease_epoch: lease.epoch,
            });
        }
        self.chaos_record_successor_restore(&block, lease).await?;
        self.publish_batch(&invalidations).await;
        Ok(records)
    }

    async fn ping(&self) -> Result<(), StorageError> {
        // R-O4 liveness: the Redis PING backing LSNs/leases answers.
        let pong: String = redis::cmd("PING")
            .query_async(&mut self.redis.clone())
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        if pong == "PONG" {
            Ok(())
        } else {
            Err(StorageError::Backend("unexpected ping reply".into()))
        }
    }

    async fn subscribe_invalidations(
        &self,
        _region: &RegionKey,
    ) -> Result<BoxStream<'_, Result<Invalidation, StorageError>>, StorageError> {
        let mut pubsub = self
            .redis_client
            .get_async_pubsub()
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        pubsub
            .subscribe(&self.channel)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        self.drain_discovery_outbox().await?;
        let msgs = futures::stream::unfold(pubsub, |mut ps| async move {
            use futures::StreamExt;
            let outcome = {
                let mut ms = ps.on_message();
                ms.next().await
            };
            outcome.map(|msg| (msg, ps))
        });
        let stream = msgs.filter_map(|msg| async move {
            match msg.get_payload::<String>() {
                Ok(payload) => match crate::types::decode_feed_invalidation(&payload) {
                    Ok(inv) => Some(Ok(inv)),
                    Err(e) => Some(Err(StorageError::Backend(format!("bad invalidation: {e}")))),
                },
                Err(e) => Some(Err(StorageError::Backend(format!(
                    "bad pubsub payload: {e}"
                )))),
            }
        });
        Ok(Box::pin(stream))
    }

    fn capabilities(&self) -> StorageCapabilities {
        StorageCapabilities {
            bi_temporal: true,
            streaming: true,
            leases: true,
            change_feed: true,
            max_traversal_depth: 4,
        }
    }
    fn backend_id(&self) -> StorageBackendId {
        StorageBackendId::FalkorDB
    }
    fn ontology_fingerprint(&self) -> [u8; 32] {
        self.ontology.fingerprint.0
    }
}

impl FalkorStorage {
    /// Serialize `m` for the `props_json` node property with the freshly
    /// assigned backend LSN stamped in, so rows read back from storage carry
    /// the storage-assigned LSN (§6.6 parity with `InMemoryStorage`).
    fn props_json<T: serde::Serialize>(value: &T, lsn: u64) -> String {
        let mut v = serde_json::to_value(value).expect("row serializes");
        if let serde_json::Value::Object(map) = &mut v {
            map.insert(
                "lsn".into(),
                serde_json::json!({
                    "space": "Backend",
                    "value": lsn,
                }),
            );
        }
        serde_json::to_string(&v).expect("patched row serializes")
    }

    /// Graph name accessor for tests.
    pub fn graph_name_clone(&self) -> String {
        self.graph.clone()
    }
    /// Reset and return `(memory pages, relationship pages)` fetched by the
    /// lazy bulk streams. Exposed for backend paging acceptance tests.
    #[doc(hidden)]
    #[cfg(feature = "integration")]
    pub fn take_stream_page_counts(&self) -> (u64, u64) {
        (
            self.stream_memory_pages.swap(0, Ordering::Relaxed),
            self.stream_relationship_pages.swap(0, Ordering::Relaxed),
        )
    }
    /// Reset and return compatibility-repair template executions.
    #[doc(hidden)]
    #[cfg(feature = "integration")]
    pub fn take_legacy_repair_query_count(&self) -> u64 {
        self.legacy_repair_queries.swap(0, Ordering::Relaxed)
    }
    /// Maximum number of decoded legacy rows retained together by migration.
    #[doc(hidden)]
    #[cfg(feature = "integration")]
    pub fn migration_peak_rows_for_testing(&self) -> u64 {
        self.migration_peak_rows.load(Ordering::Relaxed)
    }
    /// Reset and return Redis publication network requests. One pipelined
    /// batch counts once regardless of its number of compatible feed frames.
    #[doc(hidden)]
    #[cfg(feature = "integration")]
    pub fn take_publish_round_trips_for_testing(&self) -> u64 {
        self.publish_round_trips.swap(0, Ordering::Relaxed)
    }
    /// Fail the next Redis publication after its durable graph mutation.
    #[cfg(feature = "integration")]
    #[doc(hidden)]
    pub fn fail_next_publish_for_testing(&self) {
        self.fail_next_publish
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }
    /// Pause one Redis publication after its claim renewal.
    #[cfg(feature = "integration")]
    #[doc(hidden)]
    pub fn pause_next_publish_for_testing(&self) {
        self.pause_next_publish
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }
    /// Wait until the injected publication pause is reached.
    #[cfg(feature = "integration")]
    #[doc(hidden)]
    pub async fn wait_for_paused_publish_for_testing(&self) {
        self.publish_paused.notified().await;
    }
    /// Release the injected publication pause.
    #[cfg(feature = "integration")]
    #[doc(hidden)]
    pub fn release_paused_publish_for_testing(&self) {
        self.publish_release.notify_one();
    }
    /// Pause the next mutation after Redis allocates its LSN but before Falkor commits it.
    #[cfg(feature = "integration")]
    #[doc(hidden)]
    pub fn pause_next_lsn_for_testing(&self) {
        self.pause_next_lsn
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }
    /// Wait until the armed mutation has allocated its LSN.
    #[cfg(feature = "integration")]
    #[doc(hidden)]
    pub async fn wait_for_paused_lsn_for_testing(&self) {
        self.lsn_paused.notified().await;
    }
    /// Release a mutation paused after LSN allocation.
    #[cfg(feature = "integration")]
    #[doc(hidden)]
    pub fn release_paused_lsn_for_testing(&self) {
        self.lsn_release.notify_one();
    }
    /// Inject a Redis frontier failure before the next snapshot read.
    #[cfg(feature = "integration")]
    #[doc(hidden)]
    pub fn fail_next_backend_lsn_for_testing(&self) {
        self.fail_next_backend_lsn.store(true, Ordering::SeqCst);
    }
    /// Current backend LSN frontier (Redis GET).
    async fn last_backend_lsn(&self) -> Result<u64, StorageError> {
        #[cfg(feature = "integration")]
        if self.fail_next_backend_lsn.swap(false, Ordering::SeqCst) {
            return Err(StorageError::Backend(
                "injected backend LSN frontier failure".into(),
            ));
        }
        let value: Option<i64> = self
            .redis
            .clone()
            .get(&self.lsn_key)
            .await
            .map_err(|error| StorageError::Backend(error.to_string()))?;
        match value {
            None => Ok(0),
            Some(value) if value >= 0 => Ok(value as u64),
            Some(value) => Err(StorageError::CorruptMetadata {
                key: "backend_lsn",
                detail: format!("expected a non-negative Redis integer, got {value}"),
            }),
        }
    }
}

#[cfg(test)]
mod fingerprint_decode_tests {
    use super::*;

    #[test]
    fn malformed_persisted_fingerprint_is_not_missing() {
        assert_eq!(decode_persisted_fingerprint(&[]).unwrap(), None);
        for value in [
            FalkorValue::String("0".repeat(63)),
            FalkorValue::String(format!("{}zz", "0".repeat(62))),
            FalkorValue::I64(7),
        ] {
            assert!(matches!(
                decode_persisted_fingerprint(&[vec![value]]),
                Err(StorageError::CorruptMetadata {
                    key: "ontology_fingerprint",
                    ..
                })
            ));
        }
    }

    #[test]
    fn future_schema_version_fails_closed() {
        assert!(
            !schema_needs_migration(&[vec![FalkorValue::I64(STORAGE_SCHEMA_VERSION,)]]).unwrap()
        );
        assert!(schema_needs_migration(&[]).unwrap());
        assert!(matches!(
            schema_needs_migration(&[vec![FalkorValue::I64(STORAGE_SCHEMA_VERSION + 1)]]),
            Err(StorageError::CorruptMetadata {
                key: "schema_version",
                ..
            })
        ));
    }
}

#[cfg(test)]
mod row_decode_tests {
    use super::*;

    #[test]
    fn batched_memory_decode_propagates_corrupt_rows() {
        let rows = vec![vec![FalkorValue::String("not-a-node".into())]];
        assert!(memories_by_id(&rows).is_err());
    }

    #[test]
    fn persisted_ingest_settlement_rejects_wrong_types_negative_and_overflow() {
        let valid = vec![
            FalkorValue::I64(1),
            FalkorValue::I64(0),
            FalkorValue::I64(9),
        ];
        assert_eq!(
            decode_settled_ingest(&valid).unwrap(),
            SettledIngestBatch {
                accepted: 1,
                rejected: 0,
                assigned_lsn: 9,
            }
        );

        for corrupt in [
            vec![
                FalkorValue::String("1".into()),
                FalkorValue::I64(0),
                FalkorValue::I64(9),
            ],
            vec![
                FalkorValue::I64(1),
                FalkorValue::I64(-1),
                FalkorValue::I64(9),
            ],
            vec![
                FalkorValue::I64(i64::from(u32::MAX) + 1),
                FalkorValue::I64(0),
                FalkorValue::I64(9),
            ],
            vec![
                FalkorValue::I64(1),
                FalkorValue::I64(0),
                FalkorValue::I64(-1),
            ],
        ] {
            assert!(matches!(
                decode_settled_ingest(&corrupt),
                Err(StorageError::CorruptMetadata {
                    key: "ingest_settlement",
                    ..
                })
            ));
        }
    }

    #[test]
    fn stream_cursor_rejects_missing_wrong_negative_and_non_monotonic_lsns() {
        for row in [
            vec![FalkorValue::None],
            vec![FalkorValue::None, FalkorValue::String("1".into())],
            vec![FalkorValue::None, FalkorValue::I64(-1)],
            vec![FalkorValue::None, FalkorValue::I64(7)],
        ] {
            assert!(matches!(
                decode_stream_lsn(&row, 7, "memory"),
                Err(StorageError::CorruptMetadata {
                    key: "stream_cursor",
                    ..
                })
            ));
        }
        assert_eq!(
            decode_stream_lsn(&[FalkorValue::None, FalkorValue::I64(8)], 7, "memory").unwrap(),
            8
        );
    }
}
