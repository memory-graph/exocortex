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
use exocortex_storage::{
    IngestBatchKey, IngestCommitOutcome, IngestRegionDelta, PostIngestEffect, RegionKey, Storage,
    VisibilityContext,
};
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

/// D21-a: the projection stored against a registered source (the
/// persisted form of the wire `ProjectionDescriptor`). `schema_hash` is
/// the canonical digest over the declared column set — the value every
/// batch's `ExternalSnapshot.schema_hash` must match (D21-d).
#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct StoredProjection {
    /// What subset of the source is in scope, in the source's own terms.
    pub selector: String,
    /// (source_field, memory_type, kind) — the mapping `mapping_version` versions.
    pub fields: Vec<(String, String, String)>,
    /// (column, data type) at mapping time.
    pub columns: Vec<(String, String)>,
    /// Canonical digest over `columns` (hex, 32 bytes).
    pub schema_hash: String,
    /// Bumped on every deliberate selector/field/schema change.
    pub mapping_version: u32,
    /// Rows one submit window may carry (server enforces per batch).
    pub max_rows_per_window: u64,
    /// Declared graph-share bound (percent); Dreams-side evaluation is
    /// the recorded deferral.
    pub max_graph_share_percent: u32,
}

/// Canonical digest over a declared column set (D21-d). The formula
/// lives in `exocortex_wire::projection::schema_hash_hex` so every
/// table-flavored adapter derives the identical value kernel-free;
/// this is the server-side call of the one implementation.
pub fn projection_schema_hash(columns: &[(String, String)]) -> String {
    exocortex_wire::projection::schema_hash_hex(columns)
}

/// Table-shaped flavors whose registrations REQUIRE a declared projection
/// (D21-a). `session`, `custom`, and the shipped docs flavor are exempt
/// in v1 — the Mintlify adapter's compliance migration is tracked work.
pub fn projection_required(flavor: &str) -> bool {
    matches!(flavor, "iceberg" | "delta" | "parquet-dir" | "cdc-postgres")
}

/// The registered source: its ceiling (R-I3) and its declared producer
/// kind (D8). The kind is stored ONCE at first registration and rides
/// every provenance row the source asserts — retrofitting producer
/// identity onto committed rows is guessing, so it lands before the
/// second producer exists.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct SourceEntry {
    /// The effective ceiling (admin value wins, audit WS2).
    pub ceiling: Visibility,
    /// D8: the declared producer kind. Old persisted rows read back as
    /// `Custom` (PRD §3.8: "existing rows read back as custom").
    #[serde(default = "default_producer_kind")]
    pub kind: exocortex_kernel::ProducerKind,
    /// D6: grouping is selected from the registered flavor, never inferred
    /// from a producer-controlled URI. Old registry rows intentionally load
    /// with no flavor and therefore receive no grouping.
    #[serde(default)]
    pub flavor: SmolStrLike,
    /// D21-a: the declared projection (table-shaped flavors only).
    #[serde(default)]
    pub projection: Option<StoredProjection>,
}

/// Immutable administrator policy for one exact producer identity.
#[derive(Clone, Copy, Debug)]
pub struct AdminSourcePolicy {
    /// Maximum visibility the producer may assert.
    pub ceiling: Visibility,
    /// Administrator-pinned producer provenance class. Production never
    /// derives this authority from restart-local registration state.
    pub kind: exocortex_kernel::ProducerKind,
    /// Producer-specific HMAC key (R-I8); never the cluster peer key.
    pub signing_key: [u8; 32],
}

/// Exact producer identity used by administrator policy.
pub type SourcePolicyKey = (String, String, String);

fn default_producer_kind() -> exocortex_kernel::ProducerKind {
    exocortex_kernel::ProducerKind::Custom
}

/// The source registry: (org, source_uri, producer_id) -> SourceEntry.
pub type SourceRegistry = lru::LruCache<(String, String, String), SourceEntry>;
/// Process-local replay accelerator. Durable authority uses the same
/// `(org_id, producer_id, batch_id)` identity in storage.
pub type SeenBatchRegistry = lru::LruCache<(String, String, String), IngestAck>;

/// The Ingestion Protocol server over any Storage backend.
pub struct IngestServer<S: Storage> {
    /// Durable storage (commit target).
    pub storage: Arc<S>,
    /// The effective ontology (fingerprint gate + triple validation).
    pub ontology: Arc<Ontology>,
    /// Producer authentication key (R-I8).
    default_producer_key: Option<[u8; 32]>,
    /// This server's owning org (round-3 C4): a backend node serves ONE
    /// org; batches or source registrations naming any other org are
    /// rejected before validation. `None` disables the guard (tests,
    /// library embedding).
    pub org_guard: Option<SmolStrLike>,
    /// Registered source ceilings: (org, source_uri, producer_id) -> ceiling
    /// (R-I3 / R-T11a). LRU-bounded (§18.8.5) and Arc-shared across clones.
    /// The mutex protects only map access and snapshot cloning; serialization
    /// and durable file replacement happen after the guard is released.
    pub sources: Arc<Mutex<SourceRegistry>>,
    /// Where the ceiling registry persists (M6.5); `None` = ephemeral.
    pub sources_file: Option<std::path::PathBuf>,
    /// Existing unreadable or malformed durable state blocks both registration
    /// and submit. Treating it as empty could widen a previously narrow source.
    source_registry_error: Option<Arc<str>>,
    /// Serializes mutation snapshots with their durable replacement without
    /// holding the registry map lock across filesystem I/O.
    source_persistence: Arc<Mutex<()>>,
    #[cfg(test)]
    source_persist_hook: Option<Arc<dyn Fn(usize) + Send + Sync>>,
    /// Idempotency LRU: (producer_id, batch_id) -> original ack (§18.8.5).
    pub seen_batches: Arc<Mutex<SeenBatchRegistry>>,
    /// Backend-assigned embeddings (§7.5); `None` disables the embedding
    /// step (backend config flag, R-Lat3).
    pub embedder: Option<EmbedderRef>,
    /// Admission for synchronous model work. Permits are held inside Tokio's
    /// dedicated blocking pool and match the configured model's concurrency.
    embedding_permits: Arc<tokio::sync::Semaphore>,
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
    admin_policies: HashMap<SourcePolicyKey, AdminSourcePolicy>,
    /// Production policy mode: an unknown source cannot self-register.
    require_admin_policy: bool,
    /// Production transport mode: every RPC must carry the authenticated
    /// principal installed by the ingress authorization layer.
    pub require_request_principal: bool,
    /// Personal single-user mode may name arbitrary project/team scopes: the
    /// authenticated user owns the entire local graph. Shared nodes never set
    /// this flag and retain exact membership checks.
    allow_personal_scopes: bool,
    /// D10c (§4.10b): bounded recent-acceptance ring for near-duplicate
    /// hints — (org, id, type, title, content-hash, embedding) for the
    /// last [`RECENT_RING_LEN`] committed memories. Hints compare each
    /// accepted draft's embedding against this ring (0.92, the Dreams
    /// merge threshold); the ring is rebuilt from nothing on restart, so
    /// hints degrade to none — never wrong.
    pub recent: Arc<Mutex<std::collections::VecDeque<RecentEmbedding>>>,
    /// Shared concurrency admission for expensive batch validation/commit.
    pub submit_permits: Arc<tokio::sync::Semaphore>,
    /// Wakes the durable-effect drainer after a newly committed outbox row.
    post_ingest_notify: Arc<tokio::sync::Notify>,
    /// D6 grouping rules selected by the registered source flavor.
    grouping_rules: Arc<Vec<crate::grouping::GroupingRule>>,
    /// D21-d: snapshot ids observed per source, newest last, bounded —
    /// a batch naming an already-superseded snapshot is a rewind. In
    /// memory by design; the durable cursor/snapshot record is D20's
    /// open question 3.
    #[allow(clippy::type_complexity)]
    seen_snapshots: Arc<Mutex<HashMap<(String, String, String), Vec<String>>>>,
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
    pub embedding: exocortex_kernel::Embedding,
}

/// Ring bound: hints are a recency heuristic; beyond this the Dreams
/// cycle's own harvest is the mechanism.
pub const RECENT_RING_LEN: usize = 512;
/// The hint threshold: Dreams' merge threshold (§12.5 step 5) — the same
/// cosine bar means "near-duplicate" means the same thing everywhere.
pub const SIMILAR_HINT_THRESHOLD: f32 = 0.92;
const DEFAULT_CONCURRENT_SUBMITS: usize = 64;

impl<S: Storage> Clone for IngestServer<S> {
    fn clone(&self) -> Self {
        // Cheap handle clone: storage, registries, and ontology are shared;
        // commits through any clone hit the same registries (no
        // `blocking_lock` under async contention).
        Self {
            org_guard: self.org_guard.clone(),
            storage: self.storage.clone(),
            ontology: self.ontology.clone(),
            default_producer_key: self.default_producer_key,
            sources: self.sources.clone(),
            sources_file: self.sources_file.clone(),
            source_registry_error: self.source_registry_error.clone(),
            source_persistence: self.source_persistence.clone(),
            #[cfg(test)]
            source_persist_hook: self.source_persist_hook.clone(),
            seen_batches: self.seen_batches.clone(),
            embedder: self.embedder.clone(),
            embedding_permits: self.embedding_permits.clone(),
            extractor: self.extractor.clone(),
            reasoning: self.reasoning.clone(),
            dreams: self.dreams.clone(),
            admin_policies: self.admin_policies.clone(),
            require_admin_policy: self.require_admin_policy,
            require_request_principal: self.require_request_principal,
            allow_personal_scopes: self.allow_personal_scopes,
            recent: self.recent.clone(),
            submit_permits: self.submit_permits.clone(),
            post_ingest_notify: self.post_ingest_notify.clone(),
            grouping_rules: self.grouping_rules.clone(),
            seen_snapshots: self.seen_snapshots.clone(),
        }
    }
}

impl<S: Storage> IngestServer<S> {
    /// Build the server (unpinned org — tests, library embedding).
    pub fn new(storage: Arc<S>, ontology: Arc<Ontology>, hmac_key: [u8; 32]) -> Self {
        let org = "org".to_string();
        Self {
            org_guard: None,
            storage,
            ontology,
            default_producer_key: Some(hmac_key),
            sources: Arc::new(Mutex::new(lru::LruCache::new(
                std::num::NonZeroUsize::new(REGISTRY_LRU_CAP).unwrap(),
            ))),
            seen_snapshots: Arc::new(Mutex::new(HashMap::new())),
            sources_file: None,
            source_registry_error: None,
            source_persistence: Arc::new(Mutex::new(())),
            #[cfg(test)]
            source_persist_hook: None,
            seen_batches: Arc::new(Mutex::new(lru::LruCache::new(
                std::num::NonZeroUsize::new(REGISTRY_LRU_CAP).unwrap(),
            ))),
            embedder: None,
            embedding_permits: Arc::new(tokio::sync::Semaphore::new(1)),
            extractor: EntityExtractor::new(&org),
            reasoning: None,
            dreams: None,
            admin_policies: HashMap::new(),
            require_admin_policy: false,
            require_request_principal: false,
            allow_personal_scopes: false,
            recent: Arc::new(Mutex::new(std::collections::VecDeque::new())),
            submit_permits: Arc::new(tokio::sync::Semaphore::new(DEFAULT_CONCURRENT_SUBMITS)),
            post_ingest_notify: Arc::new(tokio::sync::Notify::new()),
            grouping_rules: Arc::new(crate::grouping::grouping_rules().to_vec()),
        }
    }

    /// Build a production server whose producer authentication is sourced
    /// exclusively from exact administrator policy. There is no fallback key.
    pub fn new_with_admin_policies(
        storage: Arc<S>,
        ontology: Arc<Ontology>,
        policies: impl IntoIterator<Item = (SourcePolicyKey, AdminSourcePolicy)>,
    ) -> Self {
        let mut server = Self::new(storage, ontology, [0; 32]);
        server.default_producer_key = None;
        server.admin_policies = policies.into_iter().collect();
        // Immutable policy is deliberately not copied into the bounded
        // runtime LRU: registration consults `admin_policies` directly, so
        // policy authority cannot be evicted or replaced by persisted state.
        server.require_admin_policy = true;
        server
    }

    /// Bound concurrent batch work. A saturated server returns the protocol's
    /// deterministic `RATE_LIMITED` rejection before ontology or storage work.
    pub fn with_submit_concurrency_limit(mut self, limit: usize) -> Self {
        self.submit_permits = Arc::new(tokio::sync::Semaphore::new(limit));
        self
    }

    /// Install a complete grouping-rule table. Production uses the v1 table;
    /// adapters can extend it without changing commit orchestration.
    pub fn with_grouping_rules(mut self, rules: Vec<crate::grouping::GroupingRule>) -> Self {
        self.grouping_rules = Arc::new(rules);
        self
    }

    /// Backend config flag: enable the embedding step (§7.5).
    pub fn with_embedder(mut self, embedder: EmbedderRef) -> Self {
        self.embedding_permits = Arc::new(tokio::sync::Semaphore::new(
            embedder.max_concurrency().max(1),
        ));
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

    /// Require an ingress-authenticated principal on every gRPC request.
    pub fn require_request_principal(mut self) -> Self {
        self.require_request_principal = true;
        self
    }

    /// Permit the one authenticated personal principal to author project and
    /// team scoped rows without a pre-provisioned membership catalogue.
    pub fn allow_personal_scopes(mut self) -> Self {
        self.allow_personal_scopes = true;
        self
    }

    /// Persist the ceiling registry to `path` on every registration, and load
    /// it now (M6.5). Existing unreadable or malformed state fails closed.
    pub fn with_sources_file(mut self, path: std::path::PathBuf) -> Self {
        match std::fs::read_to_string(&path) {
            Ok(raw) => {
                match serde_json::from_str::<Vec<((String, String, String), SourceEntry)>>(&raw) {
                    Ok(rows) => {
                        let mut sources = self.sources.lock().unwrap();
                        for ((org, uri, producer), entry) in rows {
                            sources.put((org, uri, producer), entry);
                        }
                    }
                    Err(error) => {
                        tracing::error!(
                            ?error,
                            "source registry is malformed; registration disabled"
                        );
                        self.source_registry_error = Some("source registry is malformed".into());
                    }
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                tracing::error!(
                    ?error,
                    "source registry is unreadable; registration disabled"
                );
                self.source_registry_error = Some("source registry is unreadable".into());
            }
        }
        self.sources_file = Some(path);
        self
    }

    fn source_rows(sources: &SourceRegistry) -> Vec<((String, String, String), SourceEntry)> {
        sources
            .iter()
            .map(|((o, u, p), v)| ((o.clone(), u.clone(), p.clone()), v.clone()))
            .collect()
    }

    /// Flush a lock-free snapshot of the ceiling registry to disk (best effort).
    fn persist_source_rows(
        &self,
        rows: &[((String, String, String), SourceEntry)],
    ) -> std::io::Result<()> {
        let Some(path) = &self.sources_file else {
            return Ok(());
        };
        #[cfg(test)]
        if let Some(hook) = &self.source_persist_hook {
            hook(rows.len());
        }
        let bytes = serde_json::to_vec(rows).map_err(std::io::Error::other)?;
        exocortex_storage::bounded_io::atomic_write_private(path, &bytes, "source registry")
    }

    fn producer_key(&self, org: &str, source: &str, producer: &str) -> Result<[u8; 32], Status> {
        self.admin_policies
            .get(&(org.to_owned(), source.to_owned(), producer.to_owned()))
            .map(|policy| policy.signing_key)
            .or(self.default_producer_key)
            .ok_or_else(|| {
                Status::permission_denied("producer has no administrator signing policy")
            })
    }

    fn producer_auth_key(&self, b: &IngestBatch) -> Result<[u8; 32], Status> {
        let Some(producer) = &b.producer else {
            return Err(Status::unauthenticated("no producer"));
        };
        if producer.hmac_signature.is_empty() {
            return Err(Status::unauthenticated("missing hmac"));
        }
        self.producer_key(&b.org_id, &b.source_uri, &b.producer_id)
    }

    fn ontology_matches(&self, b: &IngestBatch) -> Result<(), String> {
        // OC-PRD D2 (ingest row): the producer's compatibility
        // fingerprint must be current or recognized from the pinned
        // record's rolling-upgrade history — a producer may know less
        // than the server, never more. The error text is the legible
        // rejection a subset node owes a superset producer (D3).
        exocortex_kernel::admit_producer_batch(
            &b.ontology_fingerprint,
            &self.ontology,
            &self.storage.recognized_ontology_fingerprints(),
        )
        .map_err(|e| e.to_string())
    }

    fn request_principal<T>(
        &self,
        request: &Request<T>,
    ) -> Result<Option<VisibilityContext>, Status> {
        let principal = request.extensions().get::<VisibilityContext>().cloned();
        if self.require_request_principal && principal.is_none() {
            return Err(Status::unauthenticated(
                "request has no authenticated principal",
            ));
        }
        Ok(principal)
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
        snapshot: bool,
        source: &SourceEntry,
        times: BatchTimes,
        principal: Option<&VisibilityContext>,
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
        let (tenant_id, user_id, project_id, team_id) = if let Some(principal) = principal {
            if vis > principal.max_visibility {
                return Err(RejectCode::VisibilityWidening);
            }
            let metadata = batch
                .producer
                .as_ref()
                .and_then(|producer| producer.client_metadata.as_ref());
            let requested_project = metadata
                .map(|metadata| metadata.project_id.as_str())
                .filter(|id| !id.is_empty());
            let requested_team = metadata
                .map(|metadata| metadata.team_id.as_str())
                .filter(|id| !id.is_empty());
            if !self.allow_personal_scopes
                && vis == Visibility::Project
                && !requested_project.is_some_and(|id| {
                    principal
                        .project_ids
                        .iter()
                        .any(|allowed| allowed.as_str() == id)
                })
            {
                return Err(RejectCode::VisibilityWidening);
            }
            if !self.allow_personal_scopes
                && vis == Visibility::Team
                && !requested_team.is_some_and(|id| {
                    principal
                        .team_ids
                        .iter()
                        .any(|allowed| allowed.as_str() == id)
                })
            {
                return Err(RejectCode::VisibilityWidening);
            }
            (
                Some(principal.org_id.clone()),
                Some(principal.user_id.clone()),
                requested_project.map(Into::into),
                requested_team.map(Into::into),
            )
        } else {
            (None, None, None, None)
        };
        let kernel_draft = exocortex_kernel::MemoryDraft {
            memory_type: *mt,
            title: m.title.clone().into(),
            content: m.content.clone(),
            summary: None,
            visibility: vis,
            context: exocortex_kernel::MemoryContext {
                timestamp: chrono::Utc::now(),
                project_id: project_id.clone(),
                project_path: None,
                team_id: team_id.clone(),
                tenant_id: tenant_id.clone(),
                session_id: None,
                user_id: user_id.clone(),
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
                ceiling: source.ceiling,
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
        let valid_from = match m.valid_from.as_ref() {
            Some(value) => checked_timestamp(value.seconds, value.nanos)?,
            None => times.recorded_at,
        };
        let valid_until = m
            .valid_until
            .as_ref()
            .map(|value| checked_timestamp(value.seconds, value.nanos))
            .transpose()?;
        if valid_until.is_some_and(|until| until < valid_from) {
            return Err(RejectCode::Unknown);
        }
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
                    observed_at: times.observed_at,
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
                    producer_kind: Some(source.kind),
                }
            },
            context: MemoryContext {
                timestamp: times.observed_at,
                // W3 (audit): session-scoped sources stamp the session id
                // (parsed from `session://<id>`); without this the same
                // conversation was split by transport — offline-written
                // memories carried context online ones did not.
                session_id: session_id_of(&batch.source_uri, &source.flavor),
                project_id,
                project_path: None,
                team_id,
                tenant_id,
                user_id,
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
            valid_from,
            valid_until,
            recorded_at: times.recorded_at,
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
        Ok(mem)
    }

    fn validate_relationship(
        &self,
        batch: &IngestBatch,
        r: &exocortex_wire::ingest::v1::RelationshipDraft,
        draft_ids: &HashMap<String, Memory>,
        ceiling: Visibility,
        producer_kind: exocortex_kernel::ProducerKind,
        times: BatchTimes,
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
        // W5 + §4.5: the server derives edge visibility authoritatively for
        // both within-batch and existing-memory targets. A signed producer is
        // still untrusted input and cannot widen or narrow an edge away from
        // the endpoint-owned visibility contract.
        Self::vis_from_i32(r.visibility)?;
        let vis = exocortex_kernel::relationship_visibility(from_mem.visibility, to_mem.visibility);
        if !vis.within(ceiling) {
            return Err(RejectCode::VisibilityWidening);
        }
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
        let provenance = if batch.snapshot.is_some() {
            // RelationshipDraft has no independent ExternalKey coordinate.
            // An external edge is an assertion emitted by its source draft,
            // so it inherits that authenticated row's snapshot coordinates.
            from_mem.provenance.clone()
        } else {
            Provenance::Asserted {
                author: batch.producer_id.clone().into(),
                producer_kind: Some(producer_kind),
            }
        };
        Ok(Relationship {
            id: RelationshipId::derive(from_mem.id, kind, to_mem.id, None),
            kind,
            from: from_mem.id,
            to: to_mem.id,
            visibility: vis,
            provenance,
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
                last_validated: times.recorded_at,
            },
            description: None,
            bidirectional: false,
            valid_from: times.recorded_at,
            valid_until: None,
            recorded_at: times.recorded_at,
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
                    if e.org != org
                        || e.embedding.model != emb.model
                        || e.embedding.vector.len() != emb.vector.len()
                    {
                        continue;
                    }
                    let c = exocortex_dreams::mcr2::cosine(&emb.vector, &e.embedding.vector);
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

struct ValidatedBatch {
    memories: Vec<Memory>,
    relationships: Vec<Relationship>,
    loaded: HashMap<MemoryId, Memory>,
}

struct CommitRows {
    memories: Vec<Memory>,
    relationships: Vec<Relationship>,
    producer_memories: Vec<Memory>,
    grouping_nodes_created: u32,
    accepted: u32,
}

#[derive(Clone, Copy)]
struct BatchTimes {
    observed_at: chrono::DateTime<chrono::Utc>,
    recorded_at: chrono::DateTime<chrono::Utc>,
}

fn checked_timestamp(
    seconds: i64,
    nanos: i32,
) -> Result<chrono::DateTime<chrono::Utc>, RejectCode> {
    let nanos = u32::try_from(nanos).map_err(|_| RejectCode::Unknown)?;
    if nanos >= 1_000_000_000 {
        return Err(RejectCode::Unknown);
    }
    chrono::DateTime::from_timestamp(seconds, nanos).ok_or(RejectCode::Unknown)
}

fn batch_times(batch: &IngestBatch) -> Result<BatchTimes, RejectCode> {
    let now = chrono::Utc::now();
    let recorded_at = match batch.recorded_at.as_ref() {
        Some(value) => checked_timestamp(value.seconds, value.nanos)?,
        None => now,
    };
    let observed_at = match batch.observed_at.as_ref() {
        Some(value) => checked_timestamp(value.seconds, value.nanos)?,
        None => recorded_at,
    };
    Ok(BatchTimes {
        observed_at,
        recorded_at,
    })
}

fn internal_storage_status(
    operation: &'static str,
    error: exocortex_storage::StorageError,
) -> Status {
    tracing::error!(operation, ?error, "ingest storage operation failed");
    Status::internal("internal storage error")
}

impl<S: Storage + 'static> IngestServer<S> {
    fn reject_rows(batch: &IngestBatch, rejections: Vec<RejectRow>) -> IngestAck {
        let mut ack = ack_reject_all(batch, RejectCode::Unknown, "atomic batch rejected");
        ack.rejections = rejections;
        ack
    }

    fn admit_batch(
        &self,
        batch: &IngestBatch,
    ) -> Result<tokio::sync::OwnedSemaphorePermit, IngestAck> {
        self.admit_batch_with(
            batch,
            |key, candidate| {
                if exocortex_wire::signing::verify_signature(&key, candidate) {
                    Ok(())
                } else {
                    Err(Status::unauthenticated("hmac verification failed"))
                }
            },
            exocortex_wire::signing::canonical_checksum,
        )
    }

    fn admit_batch_with<V, C>(
        &self,
        batch: &IngestBatch,
        verify_hmac: V,
        canonical_checksum: C,
    ) -> Result<tokio::sync::OwnedSemaphorePermit, IngestAck>
    where
        V: FnOnce([u8; 32], &IngestBatch) -> Result<(), Status>,
        C: FnOnce(&IngestBatch) -> String,
    {
        let producer_key = self
            .producer_auth_key(batch)
            .map_err(|error| ack_reject_all(batch, RejectCode::Unauthorized, error.message()))?;
        if let Err(detail) =
            exocortex_wire::limits::validate_batch_resources(&batch.memories, &batch.relationships)
        {
            return Err(ack_reject_all(
                batch,
                RejectCode::ResourceLimitExceeded,
                detail,
            ));
        }
        let permit = self
            .submit_permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| {
                ack_reject_all(
                    batch,
                    RejectCode::RateLimited,
                    "concurrent ingestion limit reached",
                )
            })?;
        if let Err(error) = verify_hmac(producer_key, batch) {
            return Err(ack_reject_all(
                batch,
                RejectCode::Unauthorized,
                error.message(),
            ));
        }
        if batch.checksum != canonical_checksum(batch) {
            return Err(ack_reject_all(
                batch,
                RejectCode::BadChecksum,
                "checksum mismatch",
            ));
        }
        if let Err(detail) = self.ontology_matches(batch) {
            return Err(ack_reject_all(
                batch,
                RejectCode::IncompatibleOntology,
                &detail,
            ));
        }
        if self
            .org_guard
            .as_ref()
            .is_some_and(|guard| batch.org_id != guard.as_str())
        {
            return Err(ack_reject_all(
                batch,
                RejectCode::UnknownSource,
                "org does not match this node",
            ));
        }
        Ok(permit)
    }

    fn registered_source(&self, batch: &IngestBatch) -> Result<SourceEntry, IngestAck> {
        if self.source_registry_error.is_some() {
            return Err(ack_reject_all(
                batch,
                RejectCode::UnknownSource,
                "source registry unavailable",
            ));
        }
        let source = self
            .sources
            .lock()
            .unwrap()
            .get(&(
                batch.org_id.clone(),
                batch.source_uri.clone(),
                batch.producer_id.clone(),
            ))
            .cloned()
            .ok_or_else(|| {
                ack_reject_all(batch, RejectCode::UnknownSource, "producer not registered")
            })?;
        let requested_ceiling = Self::vis_from_i32(batch.ceiling).map_err(|_| {
            ack_reject_all(
                batch,
                RejectCode::UnknownSource,
                "unknown source ceiling discriminant",
            )
        })?;
        if requested_ceiling != source.ceiling {
            return Err(ack_reject_all(
                batch,
                RejectCode::UnknownSource,
                "ceiling mismatch (R-I3)",
            ));
        }
        if batch
            .snapshot
            .as_ref()
            .is_some_and(|snapshot| snapshot.source_flavor.as_str() != source.flavor.as_str())
        {
            return Err(ack_reject_all(
                batch,
                RejectCode::UnknownSource,
                "snapshot source_flavor differs from the registered source",
            ));
        }
        // D21-a/d: the declared projection governs the submission. Bound,
        // schema-hash, and rewind verdicts are cheap registry checks —
        // they run with the other pre-validation admission rules.
        if let Some(projection) = &source.projection {
            let rows = batch.memories.len() as u64;
            if projection.max_rows_per_window > 0 && rows > projection.max_rows_per_window {
                return Err(ack_reject_all(
                    batch,
                    RejectCode::ProjectionBoundExceeded,
                    &format!(
                        "batch carries {rows} rows; the declared max_rows_per_window is {}",
                        projection.max_rows_per_window
                    ),
                ));
            }
            if let Some(snapshot) = &batch.snapshot {
                let mut hex = String::with_capacity(64);
                for byte in &snapshot.schema_hash {
                    use std::fmt::Write as _;
                    let _ = write!(hex, "{byte:02x}");
                }
                if !hex.eq_ignore_ascii_case(&projection.schema_hash) {
                    return Err(ack_reject_all(
                        batch,
                        RejectCode::SchemaDrift,
                        "snapshot schema_hash differs from the registered mapping — re-register the projection deliberately with a bumped mapping_version",
                    ));
                }
                let key = (
                    batch.org_id.clone(),
                    batch.source_uri.clone(),
                    batch.producer_id.clone(),
                );
                let seen = self.seen_snapshots.lock().unwrap();
                if let Some(history) = seen.get(&key) {
                    if let Some(last) = history.last() {
                        if &snapshot.snapshot_id != last
                            && history.iter().any(|id| id == &snapshot.snapshot_id)
                        {
                            return Err(ack_reject_all(
                                batch,
                                RejectCode::SourceRewound,
                                &format!(
                                    "snapshot {} was already superseded by {} — the source was rewound and needs an operator",
                                    snapshot.snapshot_id, last
                                ),
                            ));
                        }
                    }
                }
            }
        }
        Ok(source)
    }

    async fn embed_memories(&self, memories: &mut [Memory]) {
        let Some(embedder) = self.embedder.clone() else {
            return;
        };
        if memories.is_empty() {
            return;
        }
        let texts = memories
            .iter()
            .map(|memory| format!("{}\n{}", memory.title, memory.content))
            .collect::<Vec<_>>();
        let model_id = embedder.model_id();
        let model_version = embedder.model_version();
        let Ok(permit) = self.embedding_permits.clone().acquire_owned().await else {
            return;
        };
        let result = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            embedder.embed_batch(&texts)
        })
        .await;
        let vectors = match result {
            Ok(Ok(vectors)) if vectors.len() == memories.len() => vectors,
            Ok(Ok(vectors)) => {
                tracing::warn!(
                    expected = memories.len(),
                    actual = vectors.len(),
                    "embedder returned the wrong batch cardinality"
                );
                return;
            }
            Ok(Err(error)) => {
                tracing::warn!(%error, "embedding batch failed; committing without vectors");
                return;
            }
            Err(error) => {
                tracing::warn!(%error, "embedding blocking task failed; committing without vectors");
                return;
            }
        };
        for (memory, vector) in memories.iter_mut().zip(vectors) {
            memory.embedding = Some(exocortex_kernel::Embedding {
                model: exocortex_kernel::EmbeddingModel {
                    name: model_id.into(),
                    version: model_version.into(),
                },
                vector,
            });
        }
        metrics::counter!("exocortex_ingest_embeddings_total").increment(memories.len() as u64);
    }

    fn replay_ack(&self, batch: &IngestBatch) -> Option<IngestAck> {
        self.seen_batches
            .lock()
            .unwrap()
            .get(&(
                batch.org_id.clone(),
                batch.producer_id.clone(),
                batch.batch_id.clone(),
            ))
            .cloned()
            .map(|mut replay| {
                replay.rejections = vec![RejectRow {
                    draft_key: String::new(),
                    code: RejectCode::DuplicateBatch as i32,
                    detail: "idempotent replay".into(),
                }];
                replay
            })
    }

    async fn validate_batch(
        &self,
        batch: &IngestBatch,
        source: &SourceEntry,
        principal: Option<&VisibilityContext>,
    ) -> Result<Result<ValidatedBatch, IngestAck>, Status> {
        let times = match batch_times(batch) {
            Ok(times) => times,
            Err(code) => {
                return Ok(Err(ack_reject_all(
                    batch,
                    code,
                    "malformed or out-of-range temporal coordinate",
                )))
            }
        };
        let mut memories = Vec::with_capacity(batch.memories.len());
        let mut rejections = Vec::new();
        let mut draft_ids = HashMap::new();
        for draft in &batch.memories {
            match self.validate_memory(
                batch,
                draft,
                batch.snapshot.is_some(),
                source,
                times,
                principal,
            ) {
                Ok(memory) => {
                    draft_ids.insert(draft.draft_key.clone(), memory.clone());
                    memories.push(memory);
                }
                Err(code) => rejections.push(RejectRow {
                    draft_key: draft.draft_key.clone(),
                    code: code as i32,
                    detail: format!("{code:?}"),
                }),
            }
        }
        if !rejections.is_empty() {
            return Ok(Err(Self::reject_rows(batch, rejections)));
        }

        let mut external_targets = Vec::new();
        for relationship in &batch.relationships {
            if relationship.to_memory_id.is_empty() {
                continue;
            }
            if let Some(id) = parse_hex_id(&relationship.to_memory_id) {
                external_targets.push((
                    relationship.from_draft_key.clone(),
                    relationship.to_memory_id.clone(),
                    id,
                ));
            } else {
                rejections.push(RejectRow {
                    draft_key: format!(
                        "{}->#{}",
                        relationship.from_draft_key, relationship.to_memory_id
                    ),
                    code: RejectCode::InvalidTypeTriple as i32,
                    detail: format!(
                        "to_memory_id `{}` is not a 32-hex memory id",
                        relationship.to_memory_id
                    ),
                });
            }
        }
        let unique_external = external_targets
            .iter()
            .map(|(_, _, id)| *id)
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let loaded = self
            .storage
            .get_memories(&unique_external)
            .await
            .map_err(|error| internal_storage_status("load relationship targets", error))?
            .into_iter()
            .map(|memory| (memory.id, memory))
            .collect::<HashMap<_, _>>();
        for (from_draft_key, encoded_id, id) in external_targets {
            match loaded.get(&id) {
                Some(target)
                    if principal.is_some_and(|context| {
                        !exocortex_storage::memory_visible(target, context)
                    }) =>
                {
                    rejections.push(RejectRow {
                        draft_key: format!("{from_draft_key}->#{encoded_id}"),
                        code: RejectCode::VisibilityWidening as i32,
                        detail: format!(
                            "to_memory_id `{encoded_id}` is outside the authenticated membership"
                        ),
                    });
                }
                Some(target) => {
                    draft_ids.insert(encoded_id, target.clone());
                }
                None => rejections.push(RejectRow {
                    draft_key: format!("{from_draft_key}->#{encoded_id}"),
                    code: RejectCode::InvalidTypeTriple as i32,
                    detail: format!("to_memory_id `{encoded_id}` does not exist"),
                }),
            }
        }
        if !rejections.is_empty() {
            return Ok(Err(Self::reject_rows(batch, rejections)));
        }

        let mut relationships = Vec::with_capacity(batch.relationships.len());
        for draft in &batch.relationships {
            match self.validate_relationship(
                batch,
                draft,
                &draft_ids,
                source.ceiling,
                source.kind,
                times,
            ) {
                Ok(relationship) => relationships.push(relationship),
                Err(code) => rejections.push(RejectRow {
                    draft_key: if draft.to_memory_id.is_empty() {
                        format!("{}->{}", draft.from_draft_key, draft.to_draft_key)
                    } else {
                        format!("{}->#{}", draft.from_draft_key, draft.to_memory_id)
                    },
                    code: code as i32,
                    detail: format!("{code:?}"),
                }),
            }
        }
        if !rejections.is_empty() {
            return Ok(Err(Self::reject_rows(batch, rejections)));
        }
        // Embedding is deliberately OUTSIDE validate_batch (D21-b): a
        // preflight runs the identical verdict path and embedding has no
        // effect on any verdict — committing it to validation would burn
        // the embedder on every dry run. Submit embeds after validation.
        Ok(Ok(ValidatedBatch {
            memories,
            relationships,
            loaded,
        }))
    }

    /// D21-b (adapter-contract PRD D2): the Submit pipeline through
    /// validation, committing nothing. The SAME admission, registration,
    /// and verdict code a real submission runs — `admit_batch`,
    /// `registered_source`, `validate_batch` — stopped before
    /// materialization: no LSN, no audit row, no idempotency claim, no
    /// replay consult (replay is a transport property, not a data
    /// verdict), no `similar_to` (commit-side). Verdicts are
    /// submit-identical by construction; `assigned_lsn` is always 0.
    pub async fn preflight_batch(
        &self,
        principal: Option<&VisibilityContext>,
        batch: &IngestBatch,
    ) -> Result<IngestAck, Status> {
        let _submit_permit = match self.admit_batch(batch) {
            Ok(permit) => permit,
            Err(ack) => return Ok(ack),
        };
        let source = match self.registered_source(batch) {
            Ok(source) => source,
            Err(ack) => return Ok(ack),
        };
        match self.validate_batch(batch, &source, principal).await? {
            Ok(validated) => Ok(IngestAck {
                batch_id: batch.batch_id.clone(),
                accepted: (validated.memories.len() + validated.relationships.len()) as u32,
                rejected: 0,
                rejections: vec![],
                assigned_lsn: 0,
                similar_to: vec![],
            }),
            Err(ack) => Ok(ack),
        }
    }

    /// D21-b: the registered source entry for dry-run stamping. The
    /// registration is authoritative — a preflight runs under the REAL
    /// ceiling and kind, never caller-declared ones.
    pub fn source_entry(
        &self,
        org: &str,
        source_uri: &str,
        producer_id: &str,
    ) -> Result<SourceEntry, String> {
        if let Some(error) = &self.source_registry_error {
            return Err(format!("source registry unavailable: {error}"));
        }
        self.sources
            .lock()
            .unwrap()
            .get(&(
                org.to_owned(),
                source_uri.to_owned(),
                producer_id.to_owned(),
            ))
            .cloned()
            .ok_or_else(|| "producer not registered".to_string())
    }

    /// D21-c (adapter-contract PRD D3): compile the rulebook as data —
    /// the ontology's type/kind/triple tables plus the requesting
    /// source's REGISTERED ceiling, stamped with the compatibility
    /// fingerprint. The registration lookup is best-effort BY DESIGN: an
    /// unknown source yields `registered_ceiling: None` and the adapter
    /// degrades to server-side validation rather than trusting a ceiling
    /// it could not confirm (A3).
    pub fn validation_manifest(
        &self,
        org: &str,
        source_uri: &str,
        producer_id: &str,
    ) -> Result<exocortex_wire::manifest::ValidationManifest, Status> {
        let summary = &self.ontology.summary;
        let registered_ceiling =
            self.source_entry(org, source_uri, producer_id)
                .ok()
                .map(|entry| match entry.ceiling {
                    exocortex_kernel::Visibility::Private => 0,
                    exocortex_kernel::Visibility::Project => 1,
                    exocortex_kernel::Visibility::Team => 2,
                    exocortex_kernel::Visibility::Org | exocortex_kernel::Visibility::Public => 3,
                });
        let mut manifest = exocortex_wire::manifest::ValidationManifest {
            manifest_version: exocortex_wire::manifest::MANIFEST_VERSION,
            compatibility_fingerprint: hex32(&self.ontology.fingerprint.0),
            memory_types: summary
                .memory_types
                .iter()
                .enumerate()
                .map(|(id, name)| exocortex_wire::manifest::ManifestMemoryType {
                    name: name.to_string(),
                    id: id as u8,
                })
                .collect(),
            kinds: summary
                .kinds
                .iter()
                .map(|kind| exocortex_wire::manifest::ManifestKind {
                    id: kind.id,
                    name: kind.name.clone(),
                    computed_only: kind.computed_only,
                    default_strength: kind.default_strength,
                })
                .collect(),
            type_triples: summary
                .type_triples
                .iter()
                .map(|triple| exocortex_wire::manifest::ManifestTriple {
                    kind: triple.kind,
                    from_types: triple.from_types.clone(),
                    to_types: triple.to_types.clone(),
                })
                .collect(),
            // R-T5/KP3: title bounds are char counts, not bytes.
            title_min_chars: 1,
            title_max_chars: 200,
            registered_ceiling,
        };
        manifest.memory_types.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(manifest)
    }

    /// D21-b: the producer's signing key, for server-side signing of a
    /// principal-initiated dry run (the caller is an authenticated
    /// principal impersonating the registered producer; nothing commits).
    pub fn producer_signing_key(
        &self,
        org: &str,
        source_uri: &str,
        producer_id: &str,
    ) -> Result<[u8; 32], String> {
        self.producer_key(org, source_uri, producer_id)
            .map_err(|error| error.message().to_string())
    }

    fn materialize_commit_rows(
        &self,
        batch: &IngestBatch,
        source_flavor: &str,
        mut validated: ValidatedBatch,
    ) -> CommitRows {
        let supersession_targets = validated
            .relationships
            .iter()
            .filter(|relationship| {
                self.ontology
                    .kinds_by_id
                    .get(&relationship.kind)
                    .is_some_and(|kind| {
                        kind.display_name == "Replaces" || kind.display_name == "Contradicts"
                    })
            })
            .map(|relationship| relationship.to)
            .collect::<std::collections::HashSet<_>>();
        let confidence_floor = exocortex_kernel::memory::derived_confidence(true, 0, 0);
        let producer_count = validated.memories.len();
        let accepted = (producer_count + validated.relationships.len()) as u32;
        for memory in &mut validated.memories {
            if supersession_targets.contains(&memory.id)
                && memory.confidence.partial_cmp_score(&confidence_floor)
                    == std::cmp::Ordering::Greater
            {
                memory.confidence = confidence_floor;
            }
        }
        for target in supersession_targets {
            if validated.memories.iter().any(|memory| memory.id == target) {
                continue;
            }
            if let Some(mut stale) = validated.loaded.get(&target).cloned() {
                if stale.confidence.partial_cmp_score(&confidence_floor)
                    == std::cmp::Ordering::Greater
                {
                    stale.confidence = confidence_floor;
                    validated.memories.push(stale);
                }
            }
        }

        let producer_memories = validated.memories[..producer_count].to_vec();
        let grouping_nodes_created = self.materialize_grouping(
            batch,
            source_flavor,
            &producer_memories,
            &mut validated.memories,
            &mut validated.relationships,
        );
        self.materialize_inverses(&mut validated.relationships);
        CommitRows {
            memories: validated.memories,
            relationships: validated.relationships,
            producer_memories,
            grouping_nodes_created,
            accepted,
        }
    }

    fn materialize_grouping(
        &self,
        batch: &IngestBatch,
        source_flavor: &str,
        producer_memories: &[Memory],
        memories: &mut Vec<Memory>,
        relationships: &mut Vec<Relationship>,
    ) -> u32 {
        let Some((rule, key)) =
            crate::grouping::grouping_key(batch, source_flavor, &self.grouping_rules)
        else {
            return 0;
        };
        let now = chrono::Utc::now();
        let Some(mut node) =
            crate::grouping::grouping_node(&self.ontology, &batch.org_id, rule, &key, now)
        else {
            return 0;
        };
        if let Some(scope) = producer_memories.first() {
            node.visibility = producer_memories
                .iter()
                .map(|memory| memory.visibility)
                .min()
                .unwrap_or(scope.visibility);
            node.context.tenant_id = scope.context.tenant_id.clone();
            node.context.user_id = scope.context.user_id.clone();
            node.context.project_id = scope.context.project_id.clone();
            node.context.team_id = scope.context.team_id.clone();
        }
        for memory in producer_memories {
            if let Some(edge) =
                crate::grouping::grouping_edge(&self.ontology, rule, memory, &node, now)
            {
                relationships.push(edge);
            }
        }
        memories.push(node);
        1
    }

    fn materialize_inverses(&self, relationships: &mut Vec<Relationship>) {
        let mut ids = relationships
            .iter()
            .map(|relationship| relationship.id)
            .collect::<std::collections::HashSet<_>>();
        for relationship in relationships.clone() {
            if let Some(inverse) =
                exocortex_kernel::materialize_inverse(&self.ontology, &relationship)
            {
                if ids.insert(inverse.id) {
                    relationships.push(inverse);
                }
            }
        }
    }

    async fn commit_rows(
        &self,
        batch: &IngestBatch,
        rows: &CommitRows,
    ) -> Result<Result<(u64, u32), IngestAck>, Status> {
        let accepted = rows.accepted;
        let key = IngestBatchKey {
            org_id: batch.org_id.clone().into(),
            producer_id: batch.producer_id.clone().into(),
            batch_id: batch.batch_id.clone().into(),
        };
        let effect = self.post_ingest_effect(batch, rows, &key);
        let outcome = self
            .storage
            .commit_ingest_batch_with_effect(
                &key,
                &rows.memories,
                &rows.relationships,
                accepted,
                &effect,
            )
            .await
            .map_err(|error| internal_storage_status("commit ingest batch", error))?;
        match outcome {
            IngestCommitOutcome::Committed { settled, .. } => {
                self.post_ingest_notify.notify_one();
                Ok(Ok((settled.assigned_lsn, accepted)))
            }
            IngestCommitOutcome::Duplicate(settled) => Ok(Err(IngestAck {
                batch_id: batch.batch_id.clone(),
                accepted: settled.accepted,
                rejected: settled.rejected,
                rejections: vec![RejectRow {
                    draft_key: String::new(),
                    code: RejectCode::DuplicateBatch as i32,
                    detail: "idempotent replay".into(),
                }],
                assigned_lsn: settled.assigned_lsn,
                similar_to: vec![],
            })),
        }
    }

    fn post_ingest_effect(
        &self,
        batch: &IngestBatch,
        rows: &CommitRows,
        key: &IngestBatchKey,
    ) -> PostIngestEffect {
        let mut regions = HashMap::<RegionKey, (u32, u32)>::new();
        let mut memory_regions = HashMap::<MemoryId, RegionKey>::new();
        for memory in &rows.producer_memories {
            let region = RegionKey {
                org: batch.org_id.clone().into(),
                project: memory
                    .context
                    .project_id
                    .clone()
                    .unwrap_or_else(|| "*".into()),
                memory_type: memory.memory_type,
            };
            regions.entry(region.clone()).or_default().0 += 1;
            memory_regions.insert(memory.id, region);
        }
        for relationship in &rows.relationships {
            if let Some(region) = memory_regions.get(&relationship.from) {
                regions.entry(region.clone()).or_default().1 += 1;
            }
        }
        let mut region_deltas = regions
            .into_iter()
            .map(|(region, (memories, relationships))| IngestRegionDelta {
                region,
                memories,
                relationships,
            })
            .collect::<Vec<_>>();
        region_deltas.sort_by(|left, right| {
            (
                left.region.org.as_str(),
                left.region.project.as_str(),
                left.region.memory_type,
            )
                .cmp(&(
                    right.region.org.as_str(),
                    right.region.project.as_str(),
                    right.region.memory_type,
                ))
        });
        PostIngestEffect {
            effect_id: serde_json::to_string(key)
                .expect("ingest batch identity is infallibly serializable")
                .into(),
            session_memory_ids: if batch.source_uri.starts_with("session://") {
                rows.producer_memories
                    .iter()
                    .map(|memory| memory.id)
                    .collect()
            } else {
                Vec::new()
            },
            region_deltas,
        }
    }

    async fn deliver_post_ingest_effect(
        &self,
        effect: &PostIngestEffect,
        delivery_generation: u64,
        claim_token: &str,
        retain_legacy_identity: bool,
    ) -> Result<(), String> {
        if let Some(dreams) = &self.dreams {
            for delta in &effect.region_deltas {
                dreams
                    .on_writes_once(
                        effect.effect_id.as_str(),
                        delivery_generation,
                        delta.region.clone(),
                        delta.memories,
                        delta.relationships,
                    )
                    .await
                    .map_err(|error| error.to_string())?;
            }
        }
        if !effect.session_memory_ids.is_empty() {
            if let Some(engine) = &self.reasoning {
                engine
                    .process_durable_session_wrapup(
                        format!("reasoning:{}", effect.effect_id).into(),
                        effect.session_memory_ids.clone(),
                    )
                    .await
                    .map_err(|error| error.to_string())?;
            }
        }
        let acknowledged = self
            .storage
            .acknowledge_ingest_effect(effect.effect_id.as_str(), claim_token)
            .await
            .map_err(|error| error.to_string())?;
        if !acknowledged {
            return Err("post-ingest outbox acknowledgement lost its pending effect".into());
        }
        self.reclaim_ingest_effect(effect, delivery_generation, retain_legacy_identity)
            .await
    }

    async fn reclaim_ingest_effect(
        &self,
        effect: &PostIngestEffect,
        delivery_generation: u64,
        retain_legacy_identity: bool,
    ) -> Result<(), String> {
        if let Some(dreams) = &self.dreams {
            dreams
                .settle_writes_once(
                    effect.effect_id.as_str(),
                    delivery_generation,
                    retain_legacy_identity,
                    effect
                        .region_deltas
                        .iter()
                        .map(|delta| delta.region.clone())
                        .collect(),
                )
                .await
                .map_err(|error| {
                    format!(
                        "post-ingest effect was acknowledged but Dreams identity reclamation failed: {error}"
                    )
                })?;
        }
        let completed = self
            .storage
            .complete_ingest_effect_cleanup(effect.effect_id.as_str())
            .await
            .map_err(|error| error.to_string())?;
        if !completed {
            return Err("post-ingest cleanup lost its acknowledged effect".into());
        }
        Ok(())
    }

    /// Drain durable post-ingest work forever. Backend assembly retains this
    /// task so dependency outages delay effects without retaining submit
    /// permits or losing crash-recovery state.
    pub async fn run_post_ingest_effects(self: Arc<Self>) {
        const CLAIM_LEASE_MS: i64 = 30_000;
        let mut delay = std::time::Duration::from_millis(25);
        loop {
            match self.storage.pending_ingest_effect_cleanups(1).await {
                Ok(cleanups) if !cleanups.is_empty() => {
                    let claimed = &cleanups[0];
                    let effect = &claimed.effect;
                    match self
                        .reclaim_ingest_effect(
                            effect,
                            claimed.delivery_generation,
                            claimed.retain_legacy_identity,
                        )
                        .await
                    {
                        Ok(()) => delay = std::time::Duration::from_millis(25),
                        Err(error) => {
                            tracing::warn!(%error, effect_id = %effect.effect_id, "post-ingest cleanup retrying");
                            tokio::time::sleep(delay).await;
                            delay = (delay * 2).min(std::time::Duration::from_secs(5));
                        }
                    }
                    continue;
                }
                Ok(_) => {}
                Err(error) => {
                    tracing::warn!(%error, "post-ingest cleanup scan retrying");
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(std::time::Duration::from_secs(5));
                    continue;
                }
            }
            let claim_token = uuid::Uuid::now_v7().to_string();
            match self
                .storage
                .claim_ingest_effect(&claim_token, CLAIM_LEASE_MS)
                .await
            {
                Ok(None) => {
                    tokio::select! {
                        _ = tokio::time::sleep(delay) => {}
                        _ = self.post_ingest_notify.notified() => {
                            delay = std::time::Duration::from_millis(25);
                            continue;
                        }
                    }
                    delay = (delay * 2).min(std::time::Duration::from_secs(1));
                }
                Ok(Some(claimed)) => {
                    let effect = claimed.effect;
                    let delivery_generation = claimed.delivery_generation;
                    let mut renewal = tokio::time::interval(std::time::Duration::from_secs(10));
                    renewal.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                    renewal.tick().await;
                    let delivery = self.deliver_post_ingest_effect(
                        &effect,
                        delivery_generation,
                        &claim_token,
                        claimed.retain_legacy_identity,
                    );
                    tokio::pin!(delivery);
                    let result = loop {
                        tokio::select! {
                            biased;
                            _ = renewal.tick() => {
                                match self.storage.renew_ingest_effect_claim(
                                    effect.effect_id.as_str(),
                                    &claim_token,
                                    CLAIM_LEASE_MS,
                                ).await {
                                    Ok(true) => {}
                                    Ok(false) => break Err("post-ingest claim ownership was lost".into()),
                                    Err(error) => break Err(format!("post-ingest claim renewal failed: {error}")),
                                }
                            }
                            delivered = &mut delivery => break delivered,
                        }
                    };
                    match result {
                        Ok(()) => delay = std::time::Duration::from_millis(25),
                        Err(error) => {
                            tracing::warn!(%error, effect_id = %effect.effect_id, "post-ingest effect delivery retrying after claim expiry");
                            tokio::time::sleep(delay).await;
                            delay = (delay * 2).min(std::time::Duration::from_secs(5));
                        }
                    }
                }
                Err(error) => {
                    tracing::warn!(%error, "post-ingest outbox read retrying");
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(std::time::Duration::from_secs(5));
                }
            }
        }
    }

    fn remember_ack(&self, batch: &IngestBatch, ack: IngestAck) -> IngestAck {
        self.seen_batches.lock().unwrap().put(
            (
                batch.org_id.clone(),
                batch.producer_id.clone(),
                batch.batch_id.clone(),
            ),
            ack.clone(),
        );
        ack
    }

    fn finish_commit(
        &self,
        batch: &IngestBatch,
        rows: &CommitRows,
        assigned_lsn: u64,
        accepted: u32,
    ) -> IngestAck {
        // D21-d: remember the settled snapshot so a later batch naming an
        // older one is a rewind, not a replay.
        if let Some(snapshot) = &batch.snapshot {
            let key = (
                batch.org_id.clone(),
                batch.source_uri.clone(),
                batch.producer_id.clone(),
            );
            let mut seen = self.seen_snapshots.lock().unwrap();
            let history = seen.entry(key).or_default();
            if history.last() != Some(&snapshot.snapshot_id) {
                history.push(snapshot.snapshot_id.clone());
                if history.len() > 16 {
                    history.remove(0);
                }
            }
        }
        let keyed = batch
            .memories
            .iter()
            .zip(rows.producer_memories.iter())
            .map(|(draft, memory)| (draft.draft_key.clone(), memory.clone()))
            .collect::<Vec<_>>();
        let similar_to = self.similar_to_hints(&batch.org_id, &keyed);
        metrics::counter!("exocortex_ingest_batches_total").increment(1);
        metrics::counter!("exocortex_ingest_memories_accepted_total")
            .increment(rows.producer_memories.len() as u64);
        if rows.grouping_nodes_created > 0 {
            metrics::counter!("exocortex_grouping_nodes_created_total").increment(1);
        }
        IngestAck {
            batch_id: batch.batch_id.clone(),
            accepted,
            rejected: 0,
            rejections: vec![],
            assigned_lsn,
            similar_to,
        }
    }
}

#[tonic::async_trait]
impl<S: Storage + 'static> IngestService for IngestServer<S> {
    async fn submit(&self, req: Request<IngestBatch>) -> Result<Response<IngestAck>, Status> {
        let principal = self.request_principal(&req)?;
        let batch = req.into_inner();
        if principal
            .as_ref()
            .is_some_and(|context| context.org_id.as_str() != batch.org_id)
        {
            return Err(Status::permission_denied(
                "authenticated principal cannot write another org",
            ));
        }

        let _submit_permit = match self.admit_batch(&batch) {
            Ok(permit) => permit,
            Err(ack) => return Ok(Response::new(ack)),
        };
        if let Some(replay) = self.replay_ack(&batch) {
            return Ok(Response::new(replay));
        }
        let source = match self.registered_source(&batch) {
            Ok(source) => source,
            Err(ack) => return Ok(Response::new(ack)),
        };
        let mut validated = match self
            .validate_batch(&batch, &source, principal.as_ref())
            .await?
        {
            Ok(validated) => validated,
            Err(ack) => return Ok(Response::new(ack)),
        };
        self.embed_memories(&mut validated.memories).await;
        let rows = self.materialize_commit_rows(&batch, &source.flavor, validated);
        let (assigned_lsn, accepted) = match self.commit_rows(&batch, &rows).await? {
            Ok(commit) => commit,
            Err(ack) => return Ok(Response::new(self.remember_ack(&batch, ack))),
        };
        let ack = self.finish_commit(&batch, &rows, assigned_lsn, accepted);
        Ok(Response::new(self.remember_ack(&batch, ack)))
    }

    type SubmitStreamStream = futures::stream::BoxStream<'static, Result<SubmitAck, Status>>;

    /// D21-b: dry-run Submit — the caller's own HMAC, the full verdict
    /// path, zero commit.
    async fn preflight(&self, req: Request<IngestBatch>) -> Result<Response<IngestAck>, Status> {
        let principal = self.request_principal(&req)?;
        let batch = req.into_inner();
        if principal
            .as_ref()
            .is_some_and(|context| context.org_id.as_str() != batch.org_id)
        {
            return Err(Status::permission_denied(
                "authenticated principal cannot preflight another org",
            ));
        }
        let ack = self.preflight_batch(principal.as_ref(), &batch).await?;
        Ok(Response::new(ack))
    }

    async fn submit_stream(
        &self,
        req: Request<Streaming<SubmitOne>>,
    ) -> Result<Response<Self::SubmitStreamStream>, Status> {
        let principal = self.request_principal(&req)?;
        // Fan-in to `submit`, one row at a time, streaming acks back.
        // The stream forwards each body through the batched path.
        use futures::StreamExt;
        let mut inbound = req.into_inner();
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<SubmitAck, Status>>(32);
        let self_arc = self.clone();
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
                        let mut request = Request::new(b);
                        if let Some(principal) = principal.clone() {
                            request.extensions_mut().insert(principal);
                        }
                        if tx
                            .send(self_arc.submit(request).await.map(|r| SubmitAck {
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
        req: Request<FingerprintRequest>,
    ) -> Result<Response<FingerprintResponse>, Status> {
        self.request_principal(&req)?;
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

    /// D21-c: pull the rulebook as data (see `validation_manifest`).
    async fn get_validation_manifest(
        &self,
        req: Request<exocortex_wire::ingest::v1::ManifestRequest>,
    ) -> Result<Response<exocortex_wire::ingest::v1::ManifestResponse>, Status> {
        self.request_principal(&req)?;
        let r = req.into_inner();
        if self
            .org_guard
            .as_ref()
            .is_some_and(|guard| guard.as_str() != r.org_id)
        {
            return Err(Status::permission_denied("org does not match this node"));
        }
        let manifest = self.validation_manifest(&r.org_id, &r.source_uri, &r.producer_id)?;
        let manifest_json =
            serde_json::to_string(&manifest).map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(
            exocortex_wire::ingest::v1::ManifestResponse {
                manifest_json,
                compatibility_fingerprint: self.ontology.fingerprint.0.to_vec(),
            },
        ))
    }

    async fn register_source(
        &self,
        req: Request<RegisterSourceRequest>,
    ) -> Result<Response<RegisterSourceResponse>, Status> {
        let principal = self.request_principal(&req)?;
        let r = req.into_inner();
        // Audit WS1: RegisterSource mutates the registry Submit authorizes
        // against, so it carries the same producer identity + HMAC (R-I8).
        // An unauthenticated RPC must not be able to overwrite or LRU-evict
        // a registered producer.
        let producer_key = self.producer_key(&r.org_id, &r.source_uri, &r.producer_id)?;
        if !exocortex_wire::signing::verify_registration(&producer_key, &r) {
            return Err(Status::unauthenticated(
                "registration requires a valid producer HMAC (R-I8)",
            ));
        }
        if let Some(guard) = &self.org_guard {
            if r.org_id != guard.as_str() {
                return Err(Status::invalid_argument("org does not match this node"));
            }
        }
        if principal
            .as_ref()
            .is_some_and(|principal| principal.org_id.as_str() != r.org_id)
        {
            return Err(Status::permission_denied(
                "authenticated principal cannot register another org",
            ));
        }
        if self.source_registry_error.is_some() {
            return Err(Status::failed_precondition("source registry unavailable"));
        }
        let requested = Self::vis_from_i32(r.ceiling)
            .map_err(|_| Status::invalid_argument("unknown source ceiling discriminant"))?;
        if principal
            .as_ref()
            .is_some_and(|principal| requested > principal.max_visibility)
        {
            return Err(Status::permission_denied(
                "requested source ceiling exceeds authenticated principal",
            ));
        }
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
        let declared_flavor: SmolStrLike = r.source_flavor.as_str().into();
        let key = (r.org_id, r.source_uri, r.producer_id);
        // Audit WS2: an admin-configured ceiling is authoritative — the
        // producer cannot register above it, and the configured value is
        // what gets registered and echoed. Without one, an EXISTING
        // registration stands: re-registration never silently overwrites a
        // different ceiling (the echo lets the SDK's R-I3 equality check
        // fire `CeilingMismatch` instead). The producer KIND follows the
        // same first-registration-wins rule for the same reason.
        // Acquire the persistence sequencer before changing the map. Snapshot
        // completion order therefore cannot differ from durable replacement
        // order, while the map guard is still released before filesystem I/O.
        let _persistence = self.source_persistence.lock().unwrap();
        // D21-d: set when a deliberate re-registration extends the schema
        // with additions only — one audit row is written below.
        let mut schema_extended: Option<StoredProjection> = None;
        let (effective, candidate, source_rows) = {
            let sources = self.sources.lock().unwrap();
            let mut candidate = sources.clone();
            let admin = self.admin_policies.get(&key).copied();
            let existing = candidate.get(&key).cloned();
            let ceiling = match (admin, existing.as_ref()) {
                (Some(a), _) => {
                    if requested > a.ceiling {
                        return Err(Status::permission_denied(
                            "requested ceiling exceeds the admin-configured value (R-I3)",
                        ));
                    }
                    a.ceiling
                }
                (None, existing) => {
                    if self.require_admin_policy {
                        return Err(Status::permission_denied(
                            "source has no administrator-provisioned ceiling (R-I3)",
                        ));
                    }
                    existing.map(|entry| entry.ceiling).unwrap_or(requested)
                }
            };
            let kind = admin
                .map(|policy| policy.kind)
                .or_else(|| existing.as_ref().map(|entry| entry.kind))
                .unwrap_or(declared_kind);
            let flavor: SmolStrLike = existing
                .as_ref()
                .map(|entry| entry.flavor.clone())
                .unwrap_or_else(|| declared_flavor.clone());
            if existing
                .as_ref()
                .is_some_and(|entry| entry.flavor != declared_flavor)
            {
                return Err(Status::failed_precondition(
                    "source_flavor differs from the existing registration",
                ));
            }
            // D21-a: table-shaped sources declare their projection at
            // registration — an undeclared adapter cannot submit.
            let stored_projection = if projection_required(&declared_flavor) {
                let Some(descriptor) = &r.projection else {
                    return Err(Status::invalid_argument(
                        "table-shaped source requires a declared projection: selector, field mapping, source schema, bounds (adapter-contract D21-a)",
                    ));
                };
                let columns: Vec<(String, String)> = descriptor
                    .source_schema
                    .iter()
                    .map(|c| (c.name.clone(), c.data_type.clone()))
                    .collect();
                let fields: Vec<(String, String, String)> = descriptor
                    .fields
                    .iter()
                    .map(|f| {
                        (
                            f.source_field.clone(),
                            f.memory_type.clone(),
                            f.kind.clone(),
                        )
                    })
                    .collect();
                for (source_field, _, _) in &fields {
                    if !columns.iter().any(|(name, _)| name == source_field) {
                        return Err(Status::failed_precondition(
                            "projection maps a field absent from the declared source schema (indistinguishable from a removed or renamed column — fail closed)",
                        ));
                    }
                }
                let incoming = StoredProjection {
                    selector: descriptor.selector.clone(),
                    fields,
                    columns,
                    schema_hash: projection_schema_hash(
                        &descriptor
                            .source_schema
                            .iter()
                            .map(|c| (c.name.clone(), c.data_type.clone()))
                            .collect::<Vec<_>>(),
                    ),
                    mapping_version: descriptor.mapping_version,
                    max_rows_per_window: descriptor
                        .bounds
                        .as_ref()
                        .map(|b| b.max_rows_per_window)
                        .unwrap_or(0),
                    max_graph_share_percent: descriptor
                        .bounds
                        .as_ref()
                        .map(|b| b.max_graph_share_percent)
                        .unwrap_or(0),
                };
                match &existing.as_ref().and_then(|entry| entry.projection.clone()) {
                    None => Some(incoming),
                    Some(previous) => {
                        if &incoming != previous {
                            if incoming.mapping_version <= previous.mapping_version {
                                return Err(Status::failed_precondition(
                                    "projection change requires a mapping_version bump (D21-a)",
                                ));
                            }
                            // D21-d verdict table, evaluated over the
                            // deliberate re-registration's column diff.
                            for (name, data_type) in &previous.columns {
                                match incoming
                                    .columns
                                    .iter()
                                    .find(|(new_name, _)| new_name == name)
                                {
                                    None => {
                                        return Err(Status::failed_precondition(format!(
                                            "mapped-adjacent column `{name}` was removed or renamed — fail closed until the mapping is re-authored"
                                        )))
                                    }
                                    Some((_, new_type)) if new_type != data_type => {
                                        return Err(Status::failed_precondition(format!(
                                            "column `{name}` was retyped ({data_type} -> {new_type}) — fail closed until the mapping is re-authored"
                                        )))
                                    }
                                    Some(_) => {}
                                }
                            }
                            // Only additions reached here: accept and
                            // audit (one row, below).
                            schema_extended = Some(incoming.clone());
                        }
                        Some(incoming)
                    }
                }
            } else {
                // Exempt flavors ignore any supplied projection (it is
                // not stored, not audited, and not enforced).
                None
            };
            let entry = SourceEntry {
                ceiling,
                kind,
                flavor,
                projection: stored_projection,
            };
            candidate.put(key.clone(), entry.clone());
            let rows = Self::source_rows(&candidate);
            (entry, candidate, rows)
        };
        self.persist_source_rows(&source_rows)
            .map_err(|_| Status::internal("source registry persistence failed"))?;
        *self.sources.lock().unwrap() = candidate;
        // The persistence sequencer must not be held across the audit
        // await below (the guard is not Send).
        drop(_persistence);
        // D21-d: an accepted extension (columns added, nothing mapped
        // touched) writes EXACTLY ONE audit row — the record that the
        // schema grew without a mapping bump.
        if let Some(projection) = schema_extended {
            let audit = exocortex_storage::AuditEvent {
                action: "schema_extended".into(),
                actor: key.2.clone().into(),
                org_id: key.0.clone().into(),
                input_digest: *blake3::hash(
                    format!(
                        "{}|{}|{}",
                        key.1, projection.schema_hash, projection.mapping_version
                    )
                    .as_bytes(),
                )
                .as_bytes(),
                output_ids: Default::default(),
                fingerprint: self.storage.ontology_fingerprint(),
                lease_epoch: None,
                recorded_at: chrono::Utc::now(),
            };
            self.storage
                .append_audit(&audit)
                .await
                .map_err(|_| Status::internal("schema-extension audit append failed"))?;
        }
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
    exocortex_kernel::MemoryId::parse_hex(s)
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
        6 => Some(ProducerKind::Extracted),
        _ => None,
    }
}

/// W3: the session id for `session://<id>` sources (None otherwise).
fn session_id_of(source_uri: &str, source_flavor: &str) -> Option<smol_str::SmolStr> {
    if source_flavor != "session" {
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
        KernelError::InvalidActionInput(_) => RejectCode::Unknown,
        KernelError::DuplicatePack(_)
        | KernelError::DuplicateKind(_)
        | KernelError::DuplicateTypeName(_)
        | KernelError::UnboundKernelConstant(_) => RejectCode::Unknown,
    }
}

#[cfg(test)]
#[path = "service_admission_tests.rs"]
mod admission_order_tests;
