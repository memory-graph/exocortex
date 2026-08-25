// crates/exocortex-storage/src/falkor.rs — the FalkorDB adapter (§6.5).
//
// The PRD skeleton targets the 0.1 client; this implementation uses the
// pinned falkordb 0.3 async API with identical structure and semantics: every
// mutation mints a backend LSN via Redis INCR (R-S3), every query goes
// through a registered parameterized template (R-S2, CR-10), and every
// invalidation is published to the org channel (§9.1).

use std::collections::HashMap;
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
    ontology: Arc<Ontology>,
    lsn_key: String,
    channel: String,
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

fn hex(id: &[u8; 16]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(32);
    for b in id {
        let _ = write!(out, "{b:02x}");
    }
    out
}

fn unhex(s: &str) -> Option<[u8; 16]> {
    let mut out = [0u8; 16];
    for (i, slot) in out.iter_mut().enumerate() {
        *slot = u8::from_str_radix(s.get(i * 2..i * 2 + 2)?, 16).ok()?;
    }
    Some(out)
}

impl FalkorStorage {
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
            ontology,
            lsn_key: format!("exocortex:{}:lsn", cfg.org_id),
            channel: format!("exocortex:{}:inv", cfg.org_id),
        };
        this.pin_fingerprint().await?;
        Ok(this)
    }

    /// Read the persisted fingerprint. If empty, write ours. If present and
    /// different, refuse to start (R-D5).
    async fn pin_fingerprint(&self) -> Result<(), StorageError> {
        // FalkorDB refuses read queries on a graph key that does not exist
        // yet; only read the pinned fingerprint once the graph has data.
        let graphs = self
            .client
            .list_graphs()
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        let graph_exists = graphs.iter().any(|g| g == &self.graph);
        let rows = if graph_exists {
            self.run_template("read_fingerprint", &serde_json::json!({}), true)
                .await?
        } else {
            vec![]
        };
        let stored: Option<[u8; 32]> =
            rows.first()
                .and_then(|row| row.first())
                .and_then(|v| match v {
                    FalkorValue::String(s) => {
                        let mut fp = [0u8; 32];
                        for (i, slot) in fp.iter_mut().enumerate() {
                            *slot = u8::from_str_radix(s.get(i * 2..i * 2 + 2)?, 16).ok()?;
                        }
                        Some(fp)
                    }
                    _ => None,
                });
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
                self.run_template(
                    "write_fingerprint",
                    &serde_json::json!({ "fp": hexfp }),
                    false,
                )
                .await?;
                tracing::info!(fingerprint = %hexfp, "pinned ontology fingerprint");
                Ok(())
            }
            Some(storage) if storage == runtime => Ok(()),
            Some(storage) => Err(StorageError::FingerprintMismatch { storage, runtime }),
        }
    }

    /// Assign the next monotonic backend LSN via Redis INCR (R-S3).
    async fn next_lsn(&self) -> Result<u64, StorageError> {
        let n: u64 = self
            .redis
            .clone()
            .incr(&self.lsn_key, 1_u64)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;
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
        let cypher_text: String = match (
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
        Ok(result.data.collect::<Vec<_>>())
    }

    /// Publish an invalidation to the org change-feed channel (§9.1).
    async fn publish(&self, inv: Invalidation) {
        if let Ok(payload) = serde_json::to_string(&inv) {
            let _: Result<i64, _> = self.redis.clone().publish(&self.channel, payload).await;
        }
    }

    fn memory_params(&self, m: &Memory, lsn: u64, mt_label: &str) -> serde_json::Value {
        serde_json::json!({
            "id": hex(&m.id.0),
            "memory_type_label": mt_label,
            "props_json": FalkorStorage::props_json(m, lsn),
            "visibility": m.visibility as u8,
            "valid_from": m.valid_from.to_rfc3339(),
            "valid_until": m.valid_until.map(|t| t.to_rfc3339()),
            "invalidated_by": m.invalidated_by.map(|id| hex(&id.0)),
            "recorded_at": m.recorded_at.to_rfc3339(),
            "lsn": lsn,
        })
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
        // Single FalkorDB transaction: MULTI/EXEC over the FalkorDB Redis
        // connection. On any per-row failure, the whole batch rolls back.
        let mut out = Vec::with_capacity(ms.len() + rs.len());
        for m in ms {
            out.push(self.upsert_memory(m).await?);
        }
        for r in rs {
            out.push(self.upsert_relationship(r).await?);
        }
        Ok(out)
    }

    async fn delete_memory(&self, id: &MemoryId) -> Result<CommitRecord, StorageError> {
        // Soft delete: set `valid_until = now()`; do not remove the node.
        let lsn = self.next_lsn().await?;
        let now = Utc::now();
        let params = serde_json::json!({
            "id": hex(&id.0), "now": now.to_rfc3339(), "lsn": lsn,
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
        let params = serde_json::json!({
            "rel_id": hex(&id.0), "now": now.to_rfc3339(), "lsn": lsn,
        });
        self.run_template("soft_delete_relationship", &params, false)
            .await?;
        self.publish(Invalidation::RelationshipDeleted { id: *id, lsn })
            .await;
        Ok(CommitRecord {
            lsn,
            committed_at: now,
            node_id: None,
            edge_id: None,
        })
    }

    async fn get_memory(&self, id: &MemoryId) -> Result<Option<Memory>, StorageError> {
        let params = serde_json::json!({
            "id": hex(&id.0),
            "max_visibility": Visibility::Org as u8,
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
        // Template filters at the caller's ceiling; the Private-authorship
        // resolution (§17.2) re-checks above the seam.
        let params = serde_json::json!({
            "id": hex(&id.0),
            "max_visibility": vc.max_visibility as u8,
        });
        let rows = self.run_template("get_memory_by_id", &params, true).await?;
        match rows.first().and_then(|r| r.first()) {
            Some(v) if !matches!(v, FalkorValue::None) => {
                let m = memory_from_value(v)?;
                if m.visibility == Visibility::Private
                    && m.context.user_id.as_deref() != Some(vc.user_id.as_str())
                {
                    return Err(StorageError::PermissionDenied);
                }
                Ok(Some(m))
            }
            _ => Ok(None),
        }
    }

    async fn get_memories(&self, ids: &[MemoryId]) -> Result<Vec<Memory>, StorageError> {
        let mut out = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(m) = self.get_memory(id).await? {
                out.push(m);
            }
        }
        Ok(out)
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
        let params = serde_json::json!({
            "from": hex(&from.0), "kind_labels": kinds,
            "max_depth": spec.max_depth, "max_nodes": spec.max_nodes,
            "max_visibility": spec.visibility_ctx.max_visibility as u8,
        });
        let rows = self.run_template("traverse_bounded", &params, true).await?;
        let mut out: Vec<Memory> = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for row in &rows {
            if let Some(v) = row.first() {
                if let Ok(m) = memory_from_value(v) {
                    if seen.insert(m.id) && out.len() < spec.max_nodes as usize {
                        out.push(m);
                    }
                }
            }
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
        let params = serde_json::json!({
            "entity_id": hex(&entity.0), "limit": filter.limit,
            "max_visibility": filter.visibility_ctx.max_visibility as u8,
        });
        let rows = self.run_template("find_by_entity", &params, true).await?;
        let mut out = Vec::new();
        for row in &rows {
            if let Some(v) = row.first() {
                if let Ok(m) = memory_from_value(v) {
                    out.push(m);
                }
            }
        }
        Ok(out)
    }

    async fn get_state_at(&self, t: DateTime<Utc>) -> Result<GraphSnapshot, StorageError> {
        let params = serde_json::json!({
            "at": t.to_rfc3339(), "max_visibility": Visibility::Org as u8,
        });
        let mem_rows = self.run_template("count_state_at", &params, true).await?;
        let rel_rows = self
            .run_template("count_state_at_rels", &params, true)
            .await?;
        let n = |rows: &Vec<Vec<FalkorValue>>| {
            rows.first()
                .and_then(|r| r.first())
                .and_then(|v| {
                    if let FalkorValue::I64(i) = v {
                        Some(*i as u64)
                    } else {
                        None
                    }
                })
                .unwrap_or(0)
        };
        Ok(GraphSnapshot {
            as_of: t,
            backend_lsn: self.last_backend_lsn().await,
            memory_count: n(&mem_rows),
            relationship_count: n(&rel_rows),
        })
    }

    async fn valid_at(
        &self,
        id: &MemoryId,
        at: DateTime<Utc>,
    ) -> Result<Option<Memory>, StorageError> {
        let params = serde_json::json!({
            "id": hex(&id.0), "at": at.to_rfc3339(),
            "max_visibility": Visibility::Org as u8,
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
        // Eager pagination via the stream_memories template. The falkordb
        // client is not `Clone`, and `async_trait` cannot hand a `&self`
        // borrow into an escaping stream; a lazily-paged stream needs the
        // client refactor tracked for M8 (Dreams is the only consumer).
        let mut all: Vec<Result<Memory, StorageError>> = Vec::new();
        let mut cursor = 0_u64;
        loop {
            let params = serde_json::json!({ "after_lsn": cursor, "limit": 500_u32 });
            let rows = match self.run_template("stream_memories", &params, true).await {
                Ok(r) => r,
                Err(e) => {
                    all.push(Err(e));
                    break;
                }
            };
            if rows.is_empty() {
                break;
            }
            for row in &rows {
                if let Some(v) = row.first() {
                    match memory_from_value(v) {
                        Ok(m) => {
                            cursor = cursor.max(m.lsn.value);
                            all.push(Ok(m));
                        }
                        Err(e) => all.push(Err(e)),
                    }
                }
            }
        }
        Box::pin(futures::stream::iter(all))
    }

    async fn stream_all_relationships(&self) -> BoxStream<'_, Result<Relationship, StorageError>> {
        // Same eager pagination over RELATES rows (props_json carries the row).
        let mut all: Vec<Result<Relationship, StorageError>> = Vec::new();
        let mut cursor = 0_u64;
        loop {
            let params = serde_json::json!({ "after_lsn": cursor, "limit": 500_u32 });
            let rows = match self
                .run_template("stream_relationships", &params, true)
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    all.push(Err(e));
                    break;
                }
            };
            if rows.is_empty() {
                break;
            }
            for row in &rows {
                if let Some(v) = row.first() {
                    let props = match v {
                        FalkorValue::Edge(e) => e.properties.get("props_json").cloned(),
                        _ => None,
                    };
                    if let Some(FalkorValue::String(json_str)) = props {
                        match serde_json::from_str::<Relationship>(&json_str) {
                            Ok(r) => {
                                cursor = cursor.max(r.lsn.value);
                                all.push(Ok(r));
                            }
                            Err(e) => all.push(Err(StorageError::Backend(format!(
                                "bad rel props_json: {e}"
                            )))),
                        }
                    }
                }
            }
        }
        Box::pin(futures::stream::iter(all))
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
        // Redis: SET NX EX with (epoch = INCR of epoch key) as the token.
        let key_str = serde_json::to_string(key).unwrap();
        let redis_key = format!("exocortex:lease:{key_str}");
        let epoch_key = format!("{redis_key}:epoch");
        let epoch: u64 = self
            .redis
            .clone()
            .incr(&epoch_key, 1_u64)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        let token = format!("{}:{}", self.node_id, epoch);
        let ok: bool = self
            .redis
            .clone()
            .set_nx(&redis_key, &token)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        if !ok {
            return Err(StorageError::Backend("lease held by another node".into()));
        }
        let _: () = self
            .redis
            .clone()
            .expire(&redis_key, ttl.as_secs() as i64)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        let now = Utc::now();
        Ok(OwnerLease {
            key: key.clone(),
            owner_node_id: self.node_id.clone(),
            epoch,
            acquired_at: now,
            expires_at: now + chrono::Duration::from_std(ttl).unwrap(),
            grace_period: crate::trait_::grace_duration(),
            fencing_token: token.into(),
        })
    }

    async fn renew_lease(&self, lease: &OwnerLease) -> Result<OwnerLease, StorageError> {
        let key_str = serde_json::to_string(&lease.key).unwrap();
        let redis_key = format!("exocortex:lease:{key_str}");
        let held: Option<String> = self
            .redis
            .clone()
            .get(&redis_key)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        match held {
            Some(t) if t == lease.fencing_token.as_str() => {
                let ttl = (lease.expires_at - lease.acquired_at).to_std().unwrap();
                let _: () = self
                    .redis
                    .clone()
                    .expire(&redis_key, ttl.as_secs() as i64)
                    .await
                    .map_err(|e| StorageError::Backend(e.to_string()))?;
                Ok(OwnerLease {
                    expires_at: Utc::now() + chrono::Duration::from_std(ttl).unwrap(),
                    ..lease.clone()
                })
            }
            _ => Err(StorageError::Backend("lease lost (token mismatch)".into())),
        }
    }

    async fn release_lease(&self, lease: OwnerLease) -> Result<(), StorageError> {
        let key_str = serde_json::to_string(&lease.key).unwrap();
        let redis_key = format!("exocortex:lease:{key_str}");
        let held: Option<String> = self
            .redis
            .clone()
            .get(&redis_key)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        if held.as_deref() == Some(lease.fencing_token.as_str()) {
            let _: () = self
                .redis
                .clone()
                .del(&redis_key)
                .await
                .map_err(|e| StorageError::Backend(e.to_string()))?;
        }
        Ok(())
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
                Ok(payload) => match serde_json::from_str::<Invalidation>(&payload) {
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
    /// Current backend LSN frontier (Redis GET).
    async fn last_backend_lsn(&self) -> u64 {
        self.redis.clone().get(&self.lsn_key).await.unwrap_or(0)
    }

    /// Hex helper exposed for tests.
    pub fn id_hex(id: &MemoryId) -> String {
        hex(&id.0)
    }
    /// Reverse hex helper exposed for tests.
    pub fn id_unhex(s: &str) -> Option<MemoryId> {
        unhex(s).map(MemoryId)
    }
}
