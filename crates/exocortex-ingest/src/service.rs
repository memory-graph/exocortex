// crates/exocortex-ingest/src/service.rs
//! The `IngestService` tonic implementation (§18.7): HMAC first (R-I8),
//! then the §7.13 pipeline; batches are atomic and the ack names the first
//! offending draft_key with its RejectCode.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tonic::{Request, Response, Status, Streaming};

use exocortex_kernel::{
    Memory, MemoryContext, MemoryId, Ontology, Provenance, Relationship, RelationshipId,
    Visibility, LSN,
};
use exocortex_storage::{RegionKey, Storage};
use exocortex_wire::ingest::v1::{
    ingest_service_server::IngestService, FingerprintRequest, FingerprintResponse, IngestAck,
    IngestBatch, RegisterSourceRequest, RegisterSourceResponse, RejectCode, RejectRow, SubmitAck,
    SubmitOne,
};

use crate::embedding::EmbedderRef;

/// See `IngestServer::org_guard` (SmolStr without importing it here).
type SmolStrLike = smol_str::SmolStr;
use crate::entities::EntityExtractor;

/// §18.8.5: the idempotency + source registries keep their last 1000
/// entries (LRU), so a churning producer set cannot grow them unboundedly.
const REGISTRY_LRU_CAP: usize = 1000;

// W6 (audit): the computed-only marker rides the ontology (R-T14) — the
// pack declares it (`computed_only_kinds!`), the boundary reads it. No
// string literal here to drift from the pack.

/// The registered source: its ceiling (R-I3) and its declared producer
/// kind (D8). The kind is stored ONCE at first registration and rides
/// every provenance row the source asserts — retrofitting producer
/// identity onto committed rows is guessing, so it lands before the
/// second producer exists.
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
pub struct SourceEntry {
    /// The effective ceiling (admin value wins, audit WS2).
    pub ceiling: Visibility,
    /// D8: the declared producer kind. Old persisted rows read back as
    /// `Custom` (PRD §3.8: "existing rows read back as custom").
    #[serde(default = "default_producer_kind")]
    pub kind: exocortex_kernel::ProducerKind,
}

fn default_producer_kind() -> exocortex_kernel::ProducerKind {
    exocortex_kernel::ProducerKind::Custom
}

/// The source registry: (org, source_uri, producer_id) -> SourceEntry.
pub type SourceRegistry = lru::LruCache<(String, String, String), SourceEntry>;
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
    /// This server's owning org (round-3 C4): a backend node serves ONE
    /// org; batches or source registrations naming any other org are
    /// rejected before validation. `None` disables the guard (tests,
    /// library embedding).
    pub org_guard: Option<SmolStrLike>,
    /// Registered source ceilings: (org, source_uri, producer_id) -> ceiling
    /// (R-I3 / R-T11a). LRU-bounded (§18.8.5) and Arc-shared across clones.
    /// std::Mutex: critical sections are pure map ops, never held across awaits.
    pub sources: Arc<Mutex<SourceRegistry>>,
    /// Where the ceiling registry persists (M6.5); `None` = ephemeral.
    pub sources_file: Option<std::path::PathBuf>,
    /// W7 (audit): where the idempotency registry persists (JSONL of
    /// (producer_id, batch_id, ack)). `None` = in-memory only (the old
    /// behavior: a restart re-committed a replayed batch).
    pub batches_file: Option<std::path::PathBuf>,
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
    /// IN4 (audit): the Dreams write-counter trigger. When wired, every
    /// committed memory notifies the region's counters so the §12.2
    /// predicate can actually fire (the trigger previously had no
    /// non-test caller).
    pub dreams: Option<Arc<exocortex_dreams::DreamsEngine<S>>>,
    /// Admin-configured ceilings (§18.2 / audit WS2): out-of-band, immutable
    /// through the RPC surface. Empty = self-registration stands (dev/tests).
    pub admin_ceilings: HashMap<(String, String, String), Visibility>,
    /// Production policy mode: an unknown source cannot self-register.
    pub require_admin_ceiling: bool,
    /// D10c (§4.10b): bounded recent-acceptance ring for near-duplicate
    /// hints — (org, id, type, title, content-hash, embedding) for the
    /// last [`RECENT_RING_LEN`] committed memories. Hints compare each
    /// accepted draft's embedding against this ring (0.92, the Dreams
    /// merge threshold); the ring is rebuilt from nothing on restart, so
    /// hints degrade to none — never wrong.
    pub recent: Arc<Mutex<std::collections::VecDeque<RecentEmbedding>>>,
}

/// One ring entry.
#[derive(Clone)]
pub struct RecentEmbedding {
    /// Owning org (hints never cross orgs).
    pub org: String,
    /// Memory id (hex in the hint).
    pub id: exocortex_kernel::MemoryId,
    /// Memory type (same-type vs cross-type distinguishes replaces from
    /// contradicts).
    pub memory_type: u8,
    /// Title (surfaced in the hint).
    pub title: String,
    /// Exact-content marker (title + content equality ⇒ duplicate).
    pub content_exact: String,
    /// The embedding (None rows are not ringed).
    pub embedding: Vec<f32>,
}

/// Ring bound: hints are a recency heuristic; beyond this the Dreams
/// cycle's own harvest is the mechanism.
pub const RECENT_RING_LEN: usize = 512;
/// The hint threshold: Dreams' merge threshold (§12.5 step 5) — the same
/// cosine bar means "near-duplicate" means the same thing everywhere.
pub const SIMILAR_HINT_THRESHOLD: f32 = 0.92;

impl<S: Storage> Clone for IngestServer<S> {
    fn clone(&self) -> Self {
        // Cheap handle clone: storage, registries, and ontology are shared;
        // commits through any clone hit the same registries (no
        // `blocking_lock` under async contention).
        Self {
            org_guard: self.org_guard.clone(),
            storage: self.storage.clone(),
            ontology: self.ontology.clone(),
            hmac_key: self.hmac_key,
            sources: self.sources.clone(),
            sources_file: self.sources_file.clone(),
            batches_file: self.batches_file.clone(),
            seen_batches: self.seen_batches.clone(),
            embedder: self.embedder.clone(),
            extractor: self.extractor.clone(),
            reasoning: self.reasoning.clone(),
            dreams: self.dreams.clone(),
            admin_ceilings: self.admin_ceilings.clone(),
            require_admin_ceiling: self.require_admin_ceiling,
            recent: self.recent.clone(),
        }
    }
}

impl<S: Storage> IngestServer<S> {
    /// Clone handle (streaming handlers run on owned copies).
    pub fn clone_via_arc(&self) -> IngestServer<S> {
        self.clone()
    }

    /// Build the server (unpinned org — tests, library embedding).
    pub fn new(storage: Arc<S>, ontology: Arc<Ontology>, hmac_key: [u8; 32]) -> Self {
        let org = "org".to_string();
        Self {
            org_guard: None,
            storage,
            ontology,
            hmac_key,
            sources: Arc::new(Mutex::new(lru::LruCache::new(
                std::num::NonZeroUsize::new(REGISTRY_LRU_CAP).unwrap(),
            ))),
            sources_file: None,
            batches_file: None,
            seen_batches: Arc::new(Mutex::new(lru::LruCache::new(
                std::num::NonZeroUsize::new(REGISTRY_LRU_CAP).unwrap(),
            ))),
            embedder: None,
            extractor: EntityExtractor::new(&org),
            reasoning: None,
            dreams: None,
            admin_ceilings: HashMap::new(),
            require_admin_ceiling: false,
            recent: Arc::new(Mutex::new(std::collections::VecDeque::new())),
        }
    }

    /// Backend config flag: enable the embedding step (§7.5).
    pub fn with_embedder(mut self, embedder: EmbedderRef) -> Self {
        self.embedder = Some(embedder);
        self
    }

    /// Pin the owning org (round-3 C4): a backend node serves ONE org;
    /// submit and register_source reject foreign org ids before any
    /// validation, so cross-org writes can neither commit nor publish
    /// invalidations the cache bridge would misapply.
    pub fn with_org(mut self, org: &str) -> Self {
        // IN6 (audit): the org scopes EntityId derivation — the extractor
        // must be rebuilt with the node's ACTUAL org, not left hashing the
        // literal "org" into every entity id.
        self.extractor = crate::entities::EntityExtractor::new(org);
        self.org_guard = Some(org.into());
        self
    }

    /// Wire the reasoning engine for post-commit `SessionWrapup` enrichment
    /// (§10.7 step 8). The caller owns the engine's `run` loop.
    pub fn with_reasoning(mut self, engine: Arc<exocortex_reasoning::ReasoningEngine<S>>) -> Self {
        self.reasoning = Some(engine);
        self
    }

    /// Wire the Dreams engine so commits feed the §12.2 write-counter
    /// trigger (IN4). The caller owns the engine's `run` loop.
    pub fn with_dreams(mut self, engine: Arc<exocortex_dreams::DreamsEngine<S>>) -> Self {
        self.dreams = Some(engine);
        self
    }

    /// Admin-side ceiling provisioning (§18.2 / audit WS2): ceilings keyed
    /// by `(org, source_uri, producer_id)` that the producer cannot write.
    /// A registration for a provisioned key may not EXCEED the configured
    /// ceiling, and the configured value is authoritative — it is what
    /// RegisterSource returns and what R-I3 compares batches against, so a
    /// producer configured narrower than it believes still fails the SDK's
    /// `CeilingMismatch` check instead of widening itself.
    pub fn with_admin_ceilings(
        mut self,
        ceilings: impl IntoIterator<Item = ((String, String, String), Visibility)>,
    ) -> Self {
        self.admin_ceilings = ceilings.into_iter().collect();
        self
    }

    /// Fail closed for registrations not provisioned by an administrator.
    pub fn require_admin_ceilings(mut self) -> Self {
        self.require_admin_ceiling = true;
        self
    }

    /// Persist the ceiling registry to `path` on every registration, and
    /// load it now (M6.5). Failures are logged, never fatal: an unreadable
    /// registry degrades to re-registration, not an outage.
    pub fn with_sources_file(mut self, path: std::path::PathBuf) -> Self {
        if let Ok(raw) = std::fs::read_to_string(&path) {
            match serde_json::from_str::<Vec<((String, String, String), SourceEntry)>>(&raw) {
                Ok(rows) => {
                    let mut sources = self.sources.lock().unwrap();
                    for ((org, uri, producer), entry) in rows {
                        sources.put((org, uri, producer), entry);
                    }
                }
                Err(e) => tracing::warn!(?e, "source registry unreadable (pre-D8 rows re-register on first use); starting empty"),
            }
        }
        self.sources_file = Some(path);
        self
    }

    /// W7 (audit): persist the idempotency registry to a JSONL file, and
    /// load it at boot — a replayed batch after a restart answers
    /// `DuplicateBatch` instead of re-committing.
    pub fn with_batches_file(mut self, path: std::path::PathBuf) -> Self {
        if let Ok(raw) = std::fs::read_to_string(&path) {
            let mut seen = self.seen_batches.lock().unwrap();
            for line in raw.lines().filter(|l| !l.trim().is_empty()) {
                if let Ok(row) = serde_json::from_str::<PersistedBatch>(line) {
                    seen.put(
                        (row.producer_id.clone(), row.batch_id.clone()),
                        IngestAck {
                            batch_id: row.batch_id,
                            accepted: row.accepted,
                            rejected: row.rejected,
                            rejections: vec![],
                            assigned_lsn: row.assigned_lsn,
                            similar_to: vec![],
                        },
                    );
                }
            }
        }
        self.batches_file = Some(path);
        self
    }

    /// Append one settled batch to the JSONL log (best effort).
    fn persist_batch(&self, producer_id: &str, ack: &IngestAck) {
        use std::io::Write as _;
        let Some(path) = &self.batches_file else {
            return;
        };
        let row = PersistedBatch {
            producer_id: producer_id.to_string(),
            batch_id: ack.batch_id.clone(),
            accepted: ack.accepted,
            rejected: ack.rejected,
            assigned_lsn: ack.assigned_lsn,
        };
        if let Ok(mut f) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
        {
            let _ = writeln!(f, "{}", serde_json::to_string(&row).unwrap_or_default());
        }
    }

    /// Flush the ceiling registry to disk (best effort).
    fn persist_sources(&self, sources: &SourceRegistry) {
        let Some(path) = &self.sources_file else {
            return;
        };
        let rows: Vec<((String, String, String), SourceEntry)> = sources
            .iter()
            .map(|((o, u, p), v)| ((o.clone(), u.clone(), p.clone()), *v))
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
        if !exocortex_wire::signing::verify_signature(&self.hmac_key, b) {
            return Err(Status::unauthenticated("hmac verification failed"));
        }
        Ok(())
    }

    fn ontology_matches(&self, b: &IngestBatch) -> bool {
        b.ontology_fingerprint.as_slice() == self.ontology.fingerprint.0.as_slice()
    }

    /// WS5 (audit): every discriminant outside 0..=4 is REJECTED — the old
    /// fall-through coerced unknown values (5, 99, -1) to PUBLIC, the
    /// widest scope, entirely silently. Fail closed, never open.
    fn vis_from_i32(v: i32) -> Result<Visibility, RejectCode> {
        match v {
            0 => Ok(Visibility::Private),
            1 => Ok(Visibility::Project),
            2 => Ok(Visibility::Team),
            3 => Ok(Visibility::Org),
            4 => Ok(Visibility::Public),
            _ => Err(RejectCode::VisibilityWidening),
        }
    }

    /// Kernel-side validation of one memory draft (§7.13 steps 3-7).
    fn validate_memory(
        &self,
        batch: &IngestBatch,
        m: &exocortex_wire::ingest::v1::MemoryDraft,
        ceiling: Visibility,
        snapshot: bool,
        producer_kind: exocortex_kernel::ProducerKind,
    ) -> Result<Memory, RejectCode> {
        let Some(mt) = self
            .ontology
            .memory_type_by_name
            .get(m.memory_type.as_str())
        else {
            return Err(RejectCode::UnknownMemoryType);
        };
        // W2 (audit): the KERNEL owns the rulebook — one validator for
        // the online path, the offline path, and any future producer.
        let vis = Self::vis_from_i32(m.visibility)?;
        let kernel_draft = exocortex_kernel::MemoryDraft {
            memory_type: *mt,
            title: m.title.clone().into(),
            content: m.content.clone(),
            summary: None,
            visibility: vis,
            context: exocortex_kernel::MemoryContext {
                timestamp: chrono::Utc::now(),
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
            edge_hints: Default::default(),
            external_key: None,
        };
        if let Err(e) = exocortex_kernel::validator::validate_draft(
            &self.ontology,
            &kernel_draft,
            exocortex_kernel::validator::SourceCeiling {
                source: "ingest",
                ceiling,
            },
        ) {
            return Err(kernel_error_to_reject(&e));
        }
        if snapshot && m.external_key.is_none() {
            return Err(RejectCode::MissingExternalKey);
        }
        // B8/B9: §18.6 fixes the widths — table_uuid is 16 bytes,
        // snapshot schema_hash is 32. Malformed coordinates are rejected,
        // never lossy-coerced (a truncated UUID silently forks/collides
        // identity; a short hash silently zeroes provenance).
        if let Some(k) = &m.external_key {
            if k.table_uuid.len() != 16 {
                return Err(RejectCode::InvalidExternalKey);
            }
        }
        if let Some(s) = &batch.snapshot {
            if s.schema_hash.len() != 32 {
                return Err(RejectCode::InvalidExternalKey);
            }
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
                    schema_hash: batch
                        .snapshot
                        .as_ref()
                        .map(|s| {
                            let mut h = [0u8; 32];
                            h.copy_from_slice(&s.schema_hash);
                            h
                        })
                        .unwrap_or([0u8; 32]),
                    observed_at: now,
                    external_key: exocortex_kernel::ExternalKey {
                        // B8: raw UUID bytes are stored as their hex
                        // rendering — lossless and human-readable — never
                        // a from_utf8_lossy string (distinct invalid-UTF8
                        // UUIDs must never normalize together).
                        table_uuid: m
                            .external_key
                            .as_ref()
                            .map(|k| hex32(&k.table_uuid))
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
                // D8: the registered producer kind rides every assertion.
                Provenance::Asserted {
                    author: batch.producer_id.clone().into(),
                    producer_kind: Some(producer_kind),
                }
            },
            context: MemoryContext {
                timestamp: now,
                // W3 (audit): session-scoped sources stamp the session id
                // (parsed from `session://<id>`); without this the same
                // conversation was split by transport — offline-written
                // memories carried context online ones did not.
                session_id: session_id_of(&batch.source_uri, &batch.producer_id),
                project_id: None,
                project_path: None,
                team_id: None,
                tenant_id: None,
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
            // B8: identity rides the RAW uuid bytes — hex here would
            // match the stored rendering but hashing bytes keeps one
            // canonical input shape for R-T18a.
            mem.id = MemoryId::from_external(
                &batch.org_id,
                &batch.source_uri,
                &k.table_uuid,
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
                metrics::counter!("exocortex_ingest_embeddings_total").increment(1);
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
        // §4.5: the target is a within-batch draft_key OR an existing
        // memory by 32-hex id (the submit loop pre-resolves those into
        // `draft_ids` under their hex id as the pseudo-key). Exactly one
        // of the two fields is set — both or neither is a reject.
        let to_key = if r.to_memory_id.is_empty() {
            if r.to_draft_key.is_empty() {
                return Err(RejectCode::InvalidTypeTriple);
            }
            r.to_draft_key.as_str()
        } else {
            if !r.to_draft_key.is_empty() {
                return Err(RejectCode::InvalidTypeTriple);
            }
            r.to_memory_id.as_str()
        };
        let Some(to_mem) = draft_ids.get(to_key) else {
            return Err(RejectCode::InvalidTypeTriple);
        };
        let Some(kind) = self.ontology.kind_id(&r.kind) else {
            return Err(RejectCode::UnknownKind);
        };
        // R-T14: computed-only kinds land exclusively via the Dreams cycle
        // (Computed/SimilarityHnsw provenance, §12.1 step 5). A producer
        // asserting one through the batch path would forge the invariant,
        // so the boundary rejects it outright.
        if self
            .ontology
            .kinds_by_id
            .get(&kind)
            .is_some_and(|m| m.computed_only)
        {
            return Err(RejectCode::ComputedKindRejected);
        }
        // W5 + §4.5: an edge is never more visible than the narrower
        // endpoint. Within-batch, the client derives this; for a
        // to_memory_id target the client CANNOT (it does not know the
        // stored visibility), so the server derives it authoritatively.
        let vis = if r.to_memory_id.is_empty() {
            let v = Self::vis_from_i32(r.visibility)?;
            if !v.within(ceiling) {
                return Err(RejectCode::VisibilityWidening);
            }
            v
        } else {
            exocortex_kernel::relationship_visibility(from_mem.visibility, to_mem.visibility)
        };
        // R-T17 via the kernel's one triple check (W2): both sides
        // required, no local copy.
        if let Err(e) = exocortex_kernel::validator::validate_triple(
            &self.ontology,
            from_mem.memory_type,
            kind,
            to_mem.memory_type,
        ) {
            return Err(kernel_error_to_reject(&e));
        }
        // WS4 (audit): non-finite or out-of-range values are REJECTED,
        // never coerced — NaN.clamp() is NaN, which serde renders as null
        // and makes the row unreadable on read-back.
        if !r.strength.is_finite() || !(0.0..=1.0).contains(&r.strength) {
            return Err(RejectCode::Unknown);
        }
        if !r.confidence.is_finite() || !(0.0..=1.0).contains(&r.confidence) {
            return Err(RejectCode::Unknown);
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
                producer_kind: None,
            },
            properties: exocortex_kernel::RelationshipProperties {
                strength: if r.strength == 0.0 {
                    self.ontology
                        .kinds_by_id
                        .get(&kind)
                        .map(|m| m.default_strength)
                        .unwrap_or(0.5)
                } else {
                    r.strength
                },
                confidence: if r.confidence == 0.0 {
                    0.8
                } else {
                    r.confidence
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

impl<S: Storage> IngestServer<S> {
    /// D10c (§4.10b): compute the advisory near-duplicate hints for a
    /// batch's accepted memories against the recent ring, then admit the
    /// batch's own rows into the ring. Deterministic: cosine ≥ 0.92 (the
    /// Dreams merge bar), same type ⇒ `replaces`, cross type ⇒
    /// `contradicts`, exact title+content ⇒ `duplicate`. Non-blocking by
    /// construction — the caller has already committed.
    fn similar_to_hints(
        &self,
        org: &str,
        committed: &[(String, Memory)],
    ) -> Vec<exocortex_wire::ingest::v1::SimilarToHint> {
        let mut hints = Vec::new();
        {
            let ring = self.recent.lock().unwrap();
            for (draft_key, m) in committed {
                let Some(emb) = &m.embedding else { continue };
                let mut best: Option<(f32, &RecentEmbedding)> = None;
                for e in ring.iter() {
                    if e.org != org || e.embedding.len() != emb.len() {
                        continue;
                    }
                    let c = exocortex_dreams::mcr2::cosine(emb, &e.embedding);
                    if c >= SIMILAR_HINT_THRESHOLD && best.is_none_or(|(bc, _)| c > bc) {
                        best = Some((c, e));
                    }
                }
                if let Some((_, e)) = best {
                    // Classification (§4.10b): cross-type near-duplicates
                    // are refutations regardless of text overlap — the
                    // type disagreement IS the signal.
                    let suggestion = if e.memory_type != m.memory_type {
                        "contradicts"
                    } else if e.content_exact
                        == format!(
                            "{}
{}",
                            m.title, m.content
                        )
                    {
                        "duplicate"
                    } else {
                        "replaces"
                    };
                    hints.push(exocortex_wire::ingest::v1::SimilarToHint {
                        draft_key: draft_key.clone(),
                        existing_memory_id: hex32(&e.id.0),
                        existing_title: e.title.clone(),
                        suggestion: suggestion.into(),
                    });
                }
            }
        }
        // Admit this batch's embedded rows into the ring (hints compare
        // against PRIOR commits; a batch never hints against itself).
        {
            let mut ring = self.recent.lock().unwrap();
            for (_, m) in committed {
                let Some(emb) = &m.embedding else { continue };
                ring.push_back(RecentEmbedding {
                    org: org.to_string(),
                    id: m.id,
                    memory_type: m.memory_type,
                    title: m.title.to_string(),
                    content_exact: format!(
                        "{}
{}",
                        m.title, m.content
                    ),
                    embedding: emb.clone(),
                });
            }
            while ring.len() > RECENT_RING_LEN {
                ring.pop_front();
            }
        }
        hints
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
        similar_to: vec![],
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
        // Step 1a (R-I8.5 / PRD R5): canonical checksum. An empty or
        // wrong checksum is a mismatch, never a bypass — the field is a
        // §18.1 integrity obligation, not decoration.
        if batch.checksum != exocortex_wire::signing::canonical_checksum(&batch) {
            return Ok(Response::new(ack_reject_all(
                &batch,
                RejectCode::BadChecksum,
                "checksum mismatch",
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
        // C4: single-org node — foreign orgs are rejected outright.
        if let Some(guard) = &self.org_guard {
            if batch.org_id != guard.as_str() {
                return Ok(Response::new(ack_reject_all(
                    &batch,
                    RejectCode::UnknownSource,
                    "org does not match this node",
                )));
            }
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
        // Step 2: source admission + ceiling equality (R-I3). D8: the
        // registered producer kind rides the entry and stamps every
        // provenance row below.
        let registered = {
            let mut sources = self.sources.lock().unwrap();
            sources
                .get(&(
                    batch.org_id.clone(),
                    batch.source_uri.clone(),
                    batch.producer_id.clone(),
                ))
                .copied()
        };
        let Some(source) = registered else {
            return Ok(Response::new(ack_reject_all(
                &batch,
                RejectCode::UnknownSource,
                "producer not registered",
            )));
        };
        let ceiling = source.ceiling;
        if Self::vis_from_i32(batch.ceiling).unwrap_or(Visibility::Public) != ceiling {
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
            match self.validate_memory(&batch, m, ceiling, snapshot, source.kind) {
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

        // §4.5: resolve cross-batch edge targets BEFORE relationship
        // validation — the stored type is needed for the same R-T17 check
        // a within-batch edge gets, and a missing/malformed id rejects
        // with the id named in the detail (no new code).
        for r in &batch.relationships {
            if !r.to_memory_id.is_empty() {
                let id = match parse_hex_id(&r.to_memory_id) {
                    Some(id) => id,
                    None => {
                        rejections.push(RejectRow {
                            draft_key: format!("{}->#{}", r.from_draft_key, r.to_memory_id),
                            code: RejectCode::InvalidTypeTriple as i32,
                            detail: format!(
                                "to_memory_id `{}` is not a 32-hex memory id",
                                r.to_memory_id
                            ),
                        });
                        continue;
                    }
                };
                match self.storage.get_memory(&id).await {
                    Ok(Some(target)) => {
                        draft_ids.insert(r.to_memory_id.clone(), target);
                    }
                    Ok(None) => rejections.push(RejectRow {
                        draft_key: format!("{}->#{}", r.from_draft_key, r.to_memory_id),
                        code: RejectCode::InvalidTypeTriple as i32,
                        detail: format!("to_memory_id `{}` does not exist", r.to_memory_id),
                    }),
                    Err(e) => {
                        return Err(Status::internal(format!("storage: {e}")));
                    }
                }
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
                    draft_key: if r.to_memory_id.is_empty() {
                        format!("{}->{}", r.from_draft_key, r.to_draft_key)
                    } else {
                        format!("{}->#{}", r.from_draft_key, r.to_memory_id)
                    },
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

        // D6: backend write grouping. One node per (org, flavor, key) under
        // a deterministic id, one member edge per accepted memory, both
        // `Derived` provenance. Idempotent across replays and restarts.
        // `committed_memories` stays PRODUCER rows only — reasoning,
        // Dreams counters, telemetry, and hints exclude structural rows.
        let committed_memories = ok_mem.clone();
        let mut grouping_nodes_created = 0u32;
        if let Some((rule, key)) = crate::grouping::grouping_key(&batch) {
            let now = chrono::Utc::now();
            if let Some(node) =
                crate::grouping::grouping_node(&self.ontology, &batch.org_id, rule, &key, now)
            {
                let mut edges = Vec::with_capacity(ok_mem.len());
                for m in &ok_mem {
                    if let Some(e) =
                        crate::grouping::grouping_edge(&self.ontology, rule, m, &node, now)
                    {
                        edges.push(e);
                    }
                }
                // R-T4 for the structural edge: HasMember rides along.
                let mut seen_ids: std::collections::HashSet<RelationshipId> =
                    edges.iter().map(|e| e.id).collect();
                for e in edges.clone() {
                    if let Some(inv) = exocortex_kernel::materialize_inverse(&self.ontology, &e) {
                        if seen_ids.insert(inv.id) {
                            edges.push(inv);
                        }
                    }
                }
                ok_rel.extend(edges);
                ok_mem.push(node);
                grouping_nodes_created = 1;
            }
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
                        memories: committed_memories.iter().map(|m| m.id).collect(),
                    })
                    .await;
            }
        }
        // IN4 (audit): every committed memory feeds its region's
        // write-counter so the Dreams trigger can fire (§12.2).
        if let Some(dreams) = &self.dreams {
            for m in &committed_memories {
                let region = RegionKey {
                    org: batch.org_id.clone().into(),
                    project: m.context.project_id.clone().unwrap_or_else(|| "*".into()),
                    memory_type: m.memory_type,
                };
                dreams.on_write(region).await;
            }
        }
        // D10a (§4.9/§4.10): derived confidence — a live Replaces/
        // Contradicts edge pointing AT a memory floors that memory's
        // confidence, so stale beliefs rank below their successors the
        // moment the supersession lands (not when some later cycle
        // notices). Best effort: a failure degrades to the old value and
        // is logged, never fatal to the ack.
        let supersession_targets: Vec<exocortex_kernel::MemoryId> = ok_rel
            .iter()
            .filter(|r| {
                self.ontology.kinds_by_id.get(&r.kind).is_some_and(|m| {
                    m.display_name == "Replaces" || m.display_name == "Contradicts"
                })
            })
            .map(|r| r.to)
            .collect();
        for target in supersession_targets {
            match self.storage.get_memory(&target).await {
                Ok(Some(mut stale)) => {
                    let floor = exocortex_kernel::memory::derived_confidence(true, 0, 0);
                    if stale.confidence.partial_cmp_score(&floor) == std::cmp::Ordering::Greater {
                        stale.confidence = floor;
                        if let Err(e) = self.storage.upsert_batch(&[stale], &[]).await {
                            tracing::warn!(?e, "supersession floor re-stamp failed");
                        }
                    }
                }
                Ok(None) => {}
                Err(e) => tracing::warn!(?e, "supersession floor lookup failed"),
            }
        }

        // D10c (§4.10b): advisory near-duplicate hints. Deterministic
        // embedding cosine against the bounded recent-acceptance ring —
        // the same mechanism Dreams uses for merge candidates (0.92), no
        // LLM, non-blocking. Computed against PRIOR commits, so a batch
        // never hints against itself.
        // Producer draft keys parallel the committed producer rows
        // (validation preserves order).
        let keyed: Vec<(String, Memory)> = batch
            .memories
            .iter()
            .zip(committed_memories.iter())
            .map(|(m, mem)| (m.draft_key.clone(), mem.clone()))
            .collect();
        let similar_to = self.similar_to_hints(&batch.org_id, &keyed);

        // D9 (§3.9): write telemetry — S2/S5's counters. Labels from the
        // registered source and the signed client metadata (§4.4);
        // unknown/absent telemetry degrades to "unknown", never drops the
        // row.
        metrics::counter!("exocortex_ingest_batches_total").increment(1);
        metrics::counter!("exocortex_ingest_memories_accepted_total")
            .increment(committed_memories.len() as u64);
        if grouping_nodes_created > 0 {
            metrics::counter!("exocortex_grouping_nodes_created_total").increment(1);
        }

        let ack = IngestAck {
            batch_id: batch.batch_id.clone(),
            accepted: (committed_memories.len() + ok_rel.len()) as u32,
            rejected: 0,
            rejections: vec![],
            assigned_lsn,
            similar_to,
        };
        self.seen_batches.lock().unwrap().put(
            (batch.producer_id.clone(), batch.batch_id.clone()),
            ack.clone(),
        );
        self.persist_batch(&batch.producer_id, &ack);
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
            while let Some(item) = inbound.next().await {
                // IN13 (audit): every submitted row is accounted for in
                // exactly one ack — a body-less SubmitOne gets a reject
                // ack, and an inbound error terminates the stream with an
                // explicit status instead of ending it silently.
                match item {
                    Ok(one) => {
                        let b = match one.body {
                            Some(exocortex_wire::ingest::v1::submit_one::Body::Batch(b)) => b,
                            None => {
                                let ack = IngestAck {
                                    batch_id: String::new(),
                                    accepted: 0,
                                    rejected: 0,
                                    rejections: vec![RejectRow {
                                        draft_key: String::new(),
                                        code: RejectCode::Unknown as i32,
                                        detail: "submit_one carried no body".into(),
                                    }],
                                    assigned_lsn: 0,
                                    similar_to: vec![],
                                };
                                if tx.send(Ok(SubmitAck { ack: Some(ack) })).await.is_err() {
                                    break;
                                }
                                continue;
                            }
                        };
                        if tx
                            .send(self_arc.submit(Request::new(b)).await.map(|r| SubmitAck {
                                ack: Some(r.into_inner()),
                            }))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    Err(e) => {
                        tracing::warn!(%e, "submit_stream inbound error; ending stream");
                        let _ = tx.send(Err(Status::internal("inbound stream error"))).await;
                        break;
                    }
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
        // Audit WS1: RegisterSource mutates the registry Submit authorizes
        // against, so it carries the same producer identity + HMAC (R-I8).
        // An unauthenticated RPC must not be able to overwrite or LRU-evict
        // a registered producer.
        if !exocortex_wire::signing::verify_registration(&self.hmac_key, &r) {
            return Err(Status::unauthenticated(
                "registration requires a valid producer HMAC (R-I8)",
            ));
        }
        if let Some(guard) = &self.org_guard {
            if r.org_id != guard.as_str() {
                return Err(Status::invalid_argument("org does not match this node"));
            }
        }
        let requested = Self::vis_from_i32(r.ceiling).unwrap_or(Visibility::Public);
        // D8: the closed enum, enforced at the boundary. A free string
        // would persist typos into append-only provenance forever;
        // UNSPECIFIED is a refusal to declare, and a typo'd discriminant
        // fails closed here.
        let declared_kind = wire_kind_to_kernel(r.producer_kind).ok_or_else(|| {
            Status::invalid_argument("producer_kind must be a declared ProducerKind (D8)")
        })?;
        if declared_kind == exocortex_kernel::ProducerKind::Unspecified {
            return Err(Status::invalid_argument(
                "producer_kind UNSPECIFIED is rejected: every producer declares what it is (D8)",
            ));
        }
        let key = (r.org_id, r.source_uri, r.producer_id);
        // Audit WS2: an admin-configured ceiling is authoritative — the
        // producer cannot register above it, and the configured value is
        // what gets registered and echoed. Without one, an EXISTING
        // registration stands: re-registration never silently overwrites a
        // different ceiling (the echo lets the SDK's R-I3 equality check
        // fire `CeilingMismatch` instead). The producer KIND follows the
        // same first-registration-wins rule for the same reason.
        let effective = {
            let mut sources = self.sources.lock().unwrap();
            let admin = self.admin_ceilings.get(&key).copied();
            let existing = sources.get(&key).copied();
            let ceiling = match (admin, existing) {
                (Some(a), _) => {
                    if requested > a {
                        return Err(Status::permission_denied(
                            "requested ceiling exceeds the admin-configured value (R-I3)",
                        ));
                    }
                    a
                }
                (None, existing) => {
                    if self.require_admin_ceiling {
                        return Err(Status::permission_denied(
                            "source has no administrator-provisioned ceiling (R-I3)",
                        ));
                    }
                    existing.map(|entry| entry.ceiling).unwrap_or(requested)
                }
            };
            let kind = existing.map(|e| e.kind).unwrap_or(declared_kind);
            let entry = SourceEntry { ceiling, kind };
            sources.put(key, entry);
            self.persist_sources(&sources);
            entry
        };
        Ok(Response::new(RegisterSourceResponse {
            ceiling: effective.ceiling as i32,
        }))
    }
}

/// Lowercase hex over raw uuid bytes — the lossless rendering stored on
/// `kernel::ExternalKey.table_uuid` (B8: never a lossy UTF-8 string).
fn hex32(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
    out
}

/// §4.5: 32-hex id parsing (the same shape preflight uses).
fn parse_hex_id(s: &str) -> Option<exocortex_kernel::MemoryId> {
    let b = s.as_bytes();
    if b.len() != 32 || !s.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    let mut out = [0u8; 16];
    for i in 0..16 {
        out[i] = u8::from_str_radix(std::str::from_utf8(&b[i * 2..i * 2 + 2]).ok()?, 16).ok()?;
    }
    Some(exocortex_kernel::MemoryId(out))
}

/// D8: the wire enum value -> the kernel's stored enum. Unknown
/// discriminants (a client compiled against a NEWER enum than this
/// server) fail closed to None — registration rejects.
fn wire_kind_to_kernel(v: i32) -> Option<exocortex_kernel::ProducerKind> {
    use exocortex_kernel::ProducerKind;
    match v {
        1 => Some(ProducerKind::CodingAgent),
        2 => Some(ProducerKind::ResearchAgent),
        3 => Some(ProducerKind::DocsAdapter),
        4 => Some(ProducerKind::AnalyticsAdapter),
        5 => Some(ProducerKind::Custom),
        _ => None,
    }
}

/// W3: the session id for `session://<id>` sources (None otherwise).
fn session_id_of(source_uri: &str, producer_id: &str) -> Option<smol_str::SmolStr> {
    if producer_id != "session-wrapup" {
        return None;
    }
    source_uri
        .strip_prefix("session://")
        .filter(|id| !id.is_empty())
        .map(Into::into)
}

/// W2: the exhaustive KernelError -> RejectCode mapping, compile-checked.
fn kernel_error_to_reject(e: &exocortex_kernel::KernelError) -> RejectCode {
    use exocortex_kernel::KernelError;
    match e {
        KernelError::TitleBounds
        | KernelError::EmptyContent
        | KernelError::SummaryBounds
        | KernelError::MetadataTooLarge => RejectCode::Unknown,
        KernelError::VisibilityWidening { .. } => RejectCode::VisibilityWidening,
        KernelError::UnknownKind(_) => RejectCode::UnknownKind,
        KernelError::InvalidTypeTriple { .. } => RejectCode::InvalidTypeTriple,
        KernelError::ScoreOutOfRange(_) => RejectCode::Unknown,
        KernelError::DuplicatePack(_)
        | KernelError::DuplicateKind(_)
        | KernelError::DuplicateTypeName(_)
        | KernelError::UnboundKernelConstant(_) => RejectCode::Unknown,
    }
}

/// W7: the persisted idempotency row (JSONL).
#[derive(serde::Serialize, serde::Deserialize)]
struct PersistedBatch {
    producer_id: String,
    batch_id: String,
    accepted: u32,
    rejected: u32,
    assigned_lsn: u64,
}
