// crates/exocortex-ingest/src/service.rs
//! The `IngestService` tonic implementation (§18.7): HMAC first (R-I8),
//! then the §7.13 pipeline; batches are atomic and the ack names the first
//! offending draft_key with its RejectCode.

use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tonic::{Request, Response, Status, Streaming};

use exocortex_kernel::{
    Memory, MemoryContext, MemoryId, Ontology, Provenance, Relationship, RelationshipId,
    Visibility, LSN,
};
use exocortex_storage::Storage;
use exocortex_wire::ingest::v1::{
    ingest_service_server::IngestService, FingerprintRequest, FingerprintResponse, IngestAck,
    IngestBatch, RegisterSourceRequest, RegisterSourceResponse, RejectCode, RejectRow, SubmitAck,
    SubmitOne,
};

use crate::embedding::EmbedderRef;
use crate::entities::EntityExtractor;

/// §18.8.5: the idempotency + source registries keep their last 1000
/// entries (LRU), so a churning producer set cannot grow them unboundedly.
const REGISTRY_LRU_CAP: usize = 1000;

/// Kinds produced exclusively by backend computation (R-T14): Dreams is
/// the only legitimate producer, so the ingest boundary refuses them.
const COMPUTED_ONLY_KIND: &str = "SimilarTo";

/// The source-ceiling registry: (org, source_uri, producer_id) -> ceiling.
pub type SourceRegistry = lru::LruCache<(String, String, String), Visibility>;
/// The idempotency store: (producer_id, batch_id) -> original ack.
pub type SeenBatchRegistry = lru::LruCache<(String, String), IngestAck>;

/// The Ingestion Protocol server over any Storage backend.
pub struct IngestServer<S: Storage> {
    /// Durable storage (commit target).
    pub storage: Arc<S>,
    /// The effective ontology (fingerprint gate + triple validation).
    pub ontology: Arc<Ontology>,
    /// Producer authentication key (R-I8).
    pub hmac_key: [u8; 32],
    /// Registered source ceilings: (org, source_uri, producer_id) -> ceiling
    /// (R-I3 / R-T11a). LRU-bounded (§18.8.5) and Arc-shared across clones.
    /// std::Mutex: critical sections are pure map ops, never held across awaits.
    pub sources: Arc<Mutex<SourceRegistry>>,
    /// Where the ceiling registry persists (M6.5); `None` = ephemeral.
    pub sources_file: Option<std::path::PathBuf>,
    /// Idempotency LRU: (producer_id, batch_id) -> original ack (§18.8.5).
    pub seen_batches: Arc<Mutex<SeenBatchRegistry>>,
    /// Backend-assigned embeddings (§7.5); `None` disables the embedding
    /// step (backend config flag, R-Lat3).
    pub embedder: Option<EmbedderRef>,
    /// Entity extraction (server-side only, R-T18).
    pub extractor: EntityExtractor,
    /// Reasoning enrichment: after a successful commit, enqueue
    /// `SessionWrapup` work (§10.7 step 8).
    pub reasoning: Option<Arc<exocortex_reasoning::ReasoningEngine<S>>>,
}

impl<S: Storage> Clone for IngestServer<S> {
    fn clone(&self) -> Self {
        // Cheap handle clone: storage, registries, and ontology are shared;
        // commits through any clone hit the same registries (no
        // `blocking_lock` under async contention).
        Self {
            storage: self.storage.clone(),
            ontology: self.ontology.clone(),
            hmac_key: self.hmac_key,
            sources: self.sources.clone(),
            sources_file: self.sources_file.clone(),
            seen_batches: self.seen_batches.clone(),
            embedder: self.embedder.clone(),
            extractor: self.extractor.clone(),
            reasoning: self.reasoning.clone(),
        }
    }
}

impl<S: Storage> IngestServer<S> {
    /// Clone handle (streaming handlers run on owned copies).
    pub fn clone_via_arc(&self) -> IngestServer<S> {
        self.clone()
    }

    /// Build the server.
    pub fn new(storage: Arc<S>, ontology: Arc<Ontology>, hmac_key: [u8; 32]) -> Self {
        let org = "org".to_string();
        Self {
            storage,
            ontology,
            hmac_key,
            sources: Arc::new(Mutex::new(lru::LruCache::new(
                std::num::NonZeroUsize::new(REGISTRY_LRU_CAP).unwrap(),
            ))),
            sources_file: None,
            seen_batches: Arc::new(Mutex::new(lru::LruCache::new(
                std::num::NonZeroUsize::new(REGISTRY_LRU_CAP).unwrap(),
            ))),
            embedder: None,
            extractor: EntityExtractor::new(&org),
            reasoning: None,
        }
    }

    /// Backend config flag: enable the embedding step (§7.5).
    pub fn with_embedder(mut self, embedder: EmbedderRef) -> Self {
        self.embedder = Some(embedder);
        self
    }

    /// Wire the reasoning engine for post-commit `SessionWrapup` enrichment
    /// (§10.7 step 8). The caller owns the engine's `run` loop.
    pub fn with_reasoning(mut self, engine: Arc<exocortex_reasoning::ReasoningEngine<S>>) -> Self {
        self.reasoning = Some(engine);
        self
    }

    /// Persist the ceiling registry to `path` on every registration, and
    /// load it now (M6.5). Failures are logged, never fatal: an unreadable
    /// registry degrades to re-registration, not an outage.
    pub fn with_sources_file(mut self, path: std::path::PathBuf) -> Self {
        if let Ok(raw) = std::fs::read_to_string(&path) {
            match serde_json::from_str::<Vec<((String, String, String), u8)>>(&raw) {
                Ok(rows) => {
                    let mut sources = self.sources.lock().unwrap();
                    for ((org, uri, producer), vis) in rows {
                        sources.put((org, uri, producer), Self::vis_from_i32(vis as i32));
                    }
                }
                Err(e) => tracing::warn!(?e, "source registry unreadable; starting empty"),
            }
        }
        self.sources_file = Some(path);
        self
    }

    /// Flush the ceiling registry to disk (best effort).
    fn persist_sources(&self, sources: &SourceRegistry) {
        let Some(path) = &self.sources_file else {
            return;
        };
        let rows: Vec<((String, String, String), u8)> = sources
            .iter()
            .map(|((o, u, p), v)| ((o.clone(), u.clone(), p.clone()), *v as u8))
            .collect();
        if let Err(e) = std::fs::write(path, serde_json::to_vec(&rows).unwrap_or_default()) {
            tracing::warn!(?e, "source registry persist failed");
        }
    }

    fn verify_hmac(&self, b: &IngestBatch) -> Result<(), Status> {
        let Some(producer) = &b.producer else {
            return Err(Status::unauthenticated("no producer"));
        };
        if producer.hmac_signature.is_empty() {
            return Err(Status::unauthenticated("missing hmac"));
        }
        let mut unsigned = b.clone();
        if let Some(p) = unsigned.producer.as_mut() {
            p.hmac_signature = vec![];
        }
        let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(&self.hmac_key)
            .expect("HMAC accepts any key length");
        mac.update(&prost::Message::encode_to_vec(&unsigned));
        let expected = mac.finalize().into_bytes();
        if expected.len() != producer.hmac_signature.len()
            || !bool::from(subtle::ConstantTimeEq::ct_eq(
                expected.as_slice(),
                producer.hmac_signature.as_slice(),
            ))
        {
            return Err(Status::unauthenticated("hmac verification failed"));
        }
        Ok(())
    }

    fn ontology_matches(&self, b: &IngestBatch) -> bool {
        b.ontology_fingerprint.as_slice() == self.ontology.fingerprint.0.as_slice()
    }

    fn vis_from_i32(v: i32) -> Visibility {
        match v {
            0 => Visibility::Private,
            1 => Visibility::Project,
            2 => Visibility::Team,
            3 => Visibility::Org,
            _ => Visibility::Public,
        }
    }

    /// Kernel-side validation of one memory draft (§7.13 steps 3-7).
    fn validate_memory(
        &self,
        batch: &IngestBatch,
        m: &exocortex_wire::ingest::v1::MemoryDraft,
        ceiling: Visibility,
        snapshot: bool,
    ) -> Result<Memory, RejectCode> {
        let Some(mt) = self
            .ontology
            .memory_type_by_name
            .get(m.memory_type.as_str())
        else {
            return Err(RejectCode::UnknownMemoryType);
        };
        if m.title.is_empty() || m.title.len() > 200 || m.content.is_empty() {
            return Err(RejectCode::Unknown);
        }
        let vis = Self::vis_from_i32(m.visibility);
        if !vis.within(ceiling) {
            return Err(RejectCode::VisibilityWidening);
        }
        if snapshot && m.external_key.is_none() {
            return Err(RejectCode::MissingExternalKey);
        }
        let now = chrono::Utc::now();
        let mut mem = Memory {
            id: MemoryId::new_v7(),
            memory_type: *mt,
            title: m.title.clone().into(),
            content: m.content.clone(),
            summary: None,
            // §2.6.1: tags are lowercased/trimmed/deduped at draft→memory.
            tags: exocortex_kernel::normalize_tags(m.tags.iter().map(|t| t.as_str())),
            visibility: vis,
            provenance: if snapshot {
                Provenance::ExternalSnapshot(exocortex_kernel::ExternalSnapshot {
                    source_uri: batch.source_uri.clone().into(),
                    snapshot_id: batch
                        .snapshot
                        .as_ref()
                        .map(|s| s.snapshot_id.clone())
                        .unwrap_or_default()
                        .into(),
                    schema_hash: [0u8; 32],
                    observed_at: now,
                    external_key: exocortex_kernel::ExternalKey {
                        table_uuid: m
                            .external_key
                            .as_ref()
                            .map(|k| String::from_utf8_lossy(&k.table_uuid).to_string())
                            .unwrap_or_default()
                            .into(),
                        logical_pk: m
                            .external_key
                            .as_ref()
                            .map(|k| k.logical_pk.as_bytes().to_vec())
                            .unwrap_or_default(),
                        mapping_version: m
                            .external_key
                            .as_ref()
                            .map(|k| k.mapping_version)
                            .unwrap_or(0),
                    },
                    producer_id: batch.producer_id.clone().into(),
                })
            } else {
                Provenance::Asserted {
                    author: batch.producer_id.clone().into(),
                }
            },
            context: MemoryContext {
                timestamp: now,
                project_id: None,
                project_path: None,
                team_id: None,
                tenant_id: None,
                session_id: None,
                user_id: None,
                created_by: None,
                files_involved: Default::default(),
                languages: Default::default(),
                frameworks: Default::default(),
                technologies: Default::default(),
                git_commit: None,
                git_branch: None,
                working_directory: None,
                entities: Default::default(),
                additional_metadata: serde_json::Value::Null,
            },
            importance: exocortex_kernel::memory::F01::new(0.5).unwrap(),
            confidence: exocortex_kernel::memory::F01::new(0.8).unwrap(),
            effectiveness: None,
            usage_count: 0,
            valid_from: m
                .valid_from
                .map(|t| chrono::DateTime::from_timestamp(t.seconds, t.nanos as u32).unwrap_or(now))
                .unwrap_or(now),
            valid_until: None,
            recorded_at: now,
            invalidated_by: None,
            embedding: None,
            lsn: LSN::new_local(0),
        };
        if let Some(k) = &m.external_key {
            mem.id = MemoryId::from_external(
                &batch.org_id,
                &batch.source_uri,
                String::from_utf8_lossy(&k.table_uuid).as_ref(),
                k.logical_pk.as_bytes(),
                k.mapping_version,
            );
        }
        // Server-side entity extraction (R-T18).
        crate::entities::attach_entities(&mut mem, &self.extractor);
        // Backend-assigned embedding (§7.5): embed `title + content` after
        // entity extraction, on the commit path only (R-Lat3). Failures
        // degrade to `embedding: None` — Dreams skips the row, ingest never
        // rejects on embedder health.
        if let Some(embedder) = &self.embedder {
            if let Ok(v) = embedder.embed(&format!("{}\n{}", m.title, m.content)) {
                metrics::counter!("exocortex_ingest_embeddings_total",
                    "model" => embedder.model_id())
                .increment(1);
                mem.embedding = Some(v);
            }
        }
        Ok(mem)
    }

    fn validate_relationship(
        &self,
        r: &exocortex_wire::ingest::v1::RelationshipDraft,
        draft_ids: &HashMap<String, Memory>,
        ceiling: Visibility,
    ) -> Result<Relationship, RejectCode> {
        let Some(from_mem) = draft_ids.get(&r.from_draft_key) else {
            return Err(RejectCode::InvalidTypeTriple);
        };
        let Some(to_mem) = draft_ids.get(&r.to_draft_key) else {
            return Err(RejectCode::InvalidTypeTriple);
        };
        let Some(kind) = self.ontology.kind_id(&r.kind) else {
            return Err(RejectCode::UnknownKind);
        };
        // R-T14: computed-only kinds land exclusively via the Dreams cycle
        // (Computed/SimilarityHnsw provenance, §12.1 step 5). A producer
        // asserting one through the batch path would forge the invariant,
        // so the boundary rejects it outright.
        if r.kind == COMPUTED_ONLY_KIND {
            return Err(RejectCode::ComputedKindRejected);
        }
        let vis = Self::vis_from_i32(r.visibility);
        if !vis.within(ceiling) {
            return Err(RejectCode::VisibilityWidening);
        }
        // R-T17: the (from.type, kind, to.type) triple must be permitted.
        let triples = self
            .ontology
            .triples_by_kind
            .get(&kind)
            .ok_or(RejectCode::UnknownKind)?;
        let ok = triples.iter().any(|t| {
            let from_ok = t
                .from_types
                .as_deref()
                .is_none_or(|xs| xs.contains(&from_mem.memory_type));
            let to_ok = t
                .to_types
                .as_deref()
                .is_none_or(|xs| xs.contains(&to_mem.memory_type));
            from_ok && to_ok
        });
        if !ok {
            return Err(RejectCode::InvalidTypeTriple);
        }
        let now = chrono::Utc::now();
        Ok(Relationship {
            id: RelationshipId::derive(from_mem.id, kind, to_mem.id, None),
            kind,
            from: from_mem.id,
            to: to_mem.id,
            visibility: vis,
            provenance: Provenance::Asserted {
                author: "ingest".into(),
            },
            properties: exocortex_kernel::RelationshipProperties {
                strength: if r.strength == 0.0 {
                    self.ontology
                        .kinds_by_id
                        .get(&kind)
                        .map(|m| m.default_strength)
                        .unwrap_or(0.5)
                } else {
                    r.strength.clamp(0.0, 1.0)
                },
                confidence: if r.confidence == 0.0 {
                    0.8
                } else {
                    r.confidence.clamp(0.0, 1.0)
                },
                context: if r.context.is_empty() {
                    None
                } else {
                    Some(r.context.clone().into())
                },
                evidence_count: 1,
                success_rate: None,
                validation_count: 0,
                counter_evidence_count: 0,
                last_validated: now,
            },
            description: None,
            bidirectional: false,
            valid_from: now,
            valid_until: None,
            recorded_at: now,
            invalidated_by: None,
            lsn: LSN::new_local(0),
        })
    }
}

fn ack_reject_all(batch: &IngestBatch, code: RejectCode, detail: &str) -> IngestAck {
    let rows = batch
        .memories
        .iter()
        .map(|m| RejectRow {
            draft_key: m.draft_key.clone(),
            code: code as i32,
            detail: detail.to_string(),
        })
        .chain(batch.relationships.iter().map(|r| RejectRow {
            draft_key: format!("{}->{}", r.from_draft_key, r.to_draft_key),
            code: code as i32,
            detail: detail.to_string(),
        }))
        .collect();
    IngestAck {
        batch_id: batch.batch_id.clone(),
        accepted: 0,
        rejected: (batch.memories.len() + batch.relationships.len()) as u32,
        rejections: rows,
        assigned_lsn: 0,
    }
}

#[tonic::async_trait]
impl<S: Storage + 'static> IngestService for IngestServer<S> {
    async fn submit(&self, req: Request<IngestBatch>) -> Result<Response<IngestAck>, Status> {
        let batch = req.into_inner();

        // R-I8: HMAC before any validation.
        if let Err(e) = self.verify_hmac(&batch) {
            return Ok(Response::new(ack_reject_all(
                &batch,
                RejectCode::Unauthorized,
                e.message(),
            )));
        }
        // Step 1: ontology fingerprint.
        if !self.ontology_matches(&batch) {
            return Ok(Response::new(ack_reject_all(
                &batch,
                RejectCode::IncompatibleOntology,
                "ontology fingerprint mismatch",
            )));
        }
        // Idempotency: replay returns the original ack.
        {
            let mut seen = self.seen_batches.lock().unwrap();
            if let Some(original) = seen.get(&(batch.producer_id.clone(), batch.batch_id.clone())) {
                let mut replay = original.clone();
                replay.rejections = vec![RejectRow {
                    draft_key: String::new(),
                    code: RejectCode::DuplicateBatch as i32,
                    detail: "idempotent replay".into(),
                }];
                return Ok(Response::new(replay));
            }
        }
        // Step 2: source admission + ceiling equality (R-I3).
        let registered_ceiling = {
            let mut sources = self.sources.lock().unwrap();
            sources
                .get(&(
                    batch.org_id.clone(),
                    batch.source_uri.clone(),
                    batch.producer_id.clone(),
                ))
                .copied()
        };
        let Some(ceiling) = registered_ceiling else {
            return Ok(Response::new(ack_reject_all(
                &batch,
                RejectCode::UnknownSource,
                "producer not registered",
            )));
        };
        if Self::vis_from_i32(batch.ceiling) != ceiling {
            return Ok(Response::new(ack_reject_all(
                &batch,
                RejectCode::UnknownSource,
                "ceiling mismatch (R-I3)",
            )));
        }

        // Step 3-4: validate every draft; the first row-level violation
        // rejects the whole batch (atomic, R-T17).
        let snapshot = batch.snapshot.is_some();
        let mut ok_mem = Vec::with_capacity(batch.memories.len());
        let mut rejections: Vec<RejectRow> = Vec::new();
        let mut draft_ids: HashMap<String, Memory> = HashMap::new();
        for m in &batch.memories {
            match self.validate_memory(&batch, m, ceiling, snapshot) {
                Ok(mem) => {
                    draft_ids.insert(m.draft_key.clone(), mem.clone());
                    ok_mem.push(mem);
                }
                Err(code) => rejections.push(RejectRow {
                    draft_key: m.draft_key.clone(),
                    code: code as i32,
                    detail: format!("{code:?}"),
                }),
            }
        }
        if !rejections.is_empty() {
            let mut ack = ack_reject_all(&batch, RejectCode::Unknown, "atomic batch rejected");
            ack.rejections = rejections;
            return Ok(Response::new(ack));
        }

        let mut ok_rel = Vec::with_capacity(batch.relationships.len());
        for r in &batch.relationships {
            match self.validate_relationship(r, &draft_ids, ceiling) {
                Ok(rel) => ok_rel.push(rel),
                Err(code) => rejections.push(RejectRow {
                    draft_key: format!("{}->{}", r.from_draft_key, r.to_draft_key),
                    code: code as i32,
                    detail: format!("{code:?}"),
                }),
            }
        }
        if !rejections.is_empty() {
            let mut ack = ack_reject_all(&batch, RejectCode::Unknown, "atomic batch rejected");
            ack.rejections = rejections;
            return Ok(Response::new(ack));
        }

        // R-T4: writing `k(a,b)` writes `k'(b,a)` in the same batch. The
        // companion mirrors provenance/visibility; deterministic ids make
        // re-materialization idempotent.
        let mut batch_ids: std::collections::HashSet<RelationshipId> =
            ok_rel.iter().map(|r| r.id).collect();
        for r in ok_rel.clone() {
            if let Some(inv) = exocortex_kernel::materialize_inverse(&self.ontology, &r) {
                if batch_ids.insert(inv.id) {
                    ok_rel.push(inv);
                }
            }
        }

        // Step: persist accepted rows in one transactional batch.
        let commit = self
            .storage
            .upsert_batch(&ok_mem, &ok_rel)
            .await
            .map_err(|e| Status::internal(format!("storage: {e}")))?;
        let assigned_lsn = commit.last().map(|c| c.lsn).unwrap_or(0);

        // §10.7 step 8: on every session-wrapup submit, enqueue
        // `SessionWrapup { memories }` after the storage commit so the
        // reasoning engine derives edges off the interactive path.
        if batch.source_uri.starts_with("session://") {
            if let Some(engine) = &self.reasoning {
                engine
                    .enqueue(exocortex_reasoning::ReasoningWork::SessionWrapup {
                        memories: ok_mem.iter().map(|m| m.id).collect(),
                    })
                    .await;
            }
        }
        let ack = IngestAck {
            batch_id: batch.batch_id.clone(),
            accepted: (ok_mem.len() + ok_rel.len()) as u32,
            rejected: 0,
            rejections: vec![],
            assigned_lsn,
        };
        self.seen_batches.lock().unwrap().put(
            (batch.producer_id.clone(), batch.batch_id.clone()),
            ack.clone(),
        );
        Ok(Response::new(ack))
    }

    type SubmitStreamStream = futures::stream::BoxStream<'static, Result<SubmitAck, Status>>;

    async fn submit_stream(
        &self,
        req: Request<Streaming<SubmitOne>>,
    ) -> Result<Response<Self::SubmitStreamStream>, Status> {
        // Fan-in to `submit`, one row at a time, streaming acks back.
        // The stream forwards each body through the batched path.
        use futures::StreamExt;
        let mut inbound = req.into_inner();
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<SubmitAck, Status>>(32);
        let self_arc = self.clone_via_arc();
        tokio::spawn(async move {
            while let Some(Ok(one)) = inbound.next().await {
                if let Some(b) = one.body.map(|body| match body {
                    exocortex_wire::ingest::v1::submit_one::Body::Batch(b) => b,
                }) {
                    let _ = tx
                        .send(self_arc.submit(Request::new(b)).await.map(|r| SubmitAck {
                            ack: Some(r.into_inner()),
                        }))
                        .await;
                }
            }
        });
        Ok(Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        )))
    }

    async fn fingerprint(
        &self,
        _req: Request<FingerprintRequest>,
    ) -> Result<Response<FingerprintResponse>, Status> {
        Ok(Response::new(FingerprintResponse {
            fingerprint: self.ontology.fingerprint.0.to_vec(),
            kernel_version: env!("CARGO_PKG_VERSION").into(),
            packs: self
                .ontology
                .packs
                .iter()
                .map(|p| p.name.to_string())
                .collect(),
        }))
    }

    async fn register_source(
        &self,
        req: Request<RegisterSourceRequest>,
    ) -> Result<Response<RegisterSourceResponse>, Status> {
        let r = req.into_inner();
        let ceiling = Self::vis_from_i32(r.ceiling);
        {
            let mut sources = self.sources.lock().unwrap();
            sources.put((r.org_id, r.source_uri, r.producer_id), ceiling);
            self.persist_sources(&sources);
        }
        Ok(Response::new(RegisterSourceResponse {
            ceiling: ceiling as i32,
        }))
    }
}
