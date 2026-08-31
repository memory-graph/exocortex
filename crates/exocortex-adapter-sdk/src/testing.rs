//! A scriptable `IngestService` mock (R18) for adapter authors' own
//! tests. Serves the real gRPC surface on a random local port and lets a
//! test script: rate-limit, force transport failure, drift the
//! fingerprint, mismatch the ceiling, and record the call order.
//!
//! Scripts are queues: each `Submit` pops one entry. "Rate-limit three
//! times, then accept" is `[Fail(unavailable), Fail(unavailable),
//! Fail(unavailable), Accept]` or a `RejectRows(RATE_LIMITED)` sequence.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use exocortex_wire::ingest::v1::ingest_service_server::{IngestService, IngestServiceServer};
use exocortex_wire::ingest::v1::{
    FingerprintRequest, FingerprintResponse, IngestAck, IngestBatch, RegisterSourceRequest,
    RegisterSourceResponse, RejectRow, SubmitAck, SubmitOne,
};
use tonic::{Request, Response, Status, Streaming};

/// One scripted submit outcome.
#[derive(Clone, Debug)]
pub enum MockSubmit {
    /// Accept every draft in the batch.
    Accept,
    /// Reject every draft with this code/detail.
    RejectRows(i32, &'static str),
    /// Fail the RPC itself with a transport-level status.
    Fail(tonic::Code, &'static str),
}

/// `RATE_LIMITED` shorthand (wire code 11).
pub fn rate_limited() -> MockSubmit {
    MockSubmit::RejectRows(11, "rate limited (mock)")
}

/// `UNKNOWN_MEMORY_TYPE` shorthand (wire code 3).
pub fn unknown_memory_type() -> MockSubmit {
    MockSubmit::RejectRows(3, "unknown memory_type (mock)")
}

/// Shared mock state, inspectable by the test.
#[derive(Clone, Default)]
pub struct MockState {
    calls: Arc<Mutex<Vec<&'static str>>>,
    script: Arc<Mutex<VecDeque<MockSubmit>>>,
    ceiling: Arc<Mutex<i32>>,
    fingerprint: Arc<Mutex<[u8; 32]>>,
    submitted: Arc<Mutex<Vec<IngestBatch>>>,
    registrations: Arc<Mutex<Vec<RegisterSourceRequest>>>,
    /// D21-b: batches received through Preflight (never committed).
    preflighted: Arc<Mutex<Vec<IngestBatch>>>,
    /// D21-c: serve the manifest envelope with a STALE fingerprint (the
    /// degrade-path test).
    stale_manifest_fingerprint: Arc<std::sync::atomic::AtomicBool>,
    /// D21-c: serve manifests at all. OFF by default — most adapter tests
    /// exercise the no-manifest degrade path, and a canned rulebook that
    /// omits a real adapter's types would reject its drafts locally.
    serve_manifest: Arc<std::sync::atomic::AtomicBool>,
}

/// A running mock server.
pub struct MockServer {
    /// Inspectable state: calls, submitted batches.
    pub state: MockState,
    addr: std::net::SocketAddr,
    handle: tokio::task::JoinHandle<()>,
}

impl MockServer {
    /// Start on 127.0.0.1:0 accepting everything.
    pub async fn start() -> Self {
        Self::start_with(vec![], 3, [7u8; 32]).await
    }

    /// Start with an explicit script, ceiling, and fingerprint.
    pub async fn start_with(script: Vec<MockSubmit>, ceiling: i32, fingerprint: [u8; 32]) -> Self {
        let state = MockState {
            calls: Arc::new(Mutex::new(Vec::new())),
            script: Arc::new(Mutex::new(script.into())),
            ceiling: Arc::new(Mutex::new(ceiling)),
            fingerprint: Arc::new(Mutex::new(fingerprint)),
            submitted: Arc::new(Mutex::new(Vec::new())),
            registrations: Arc::new(Mutex::new(Vec::new())),
            preflighted: Arc::new(Mutex::new(Vec::new())),
            stale_manifest_fingerprint: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            serve_manifest: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };
        let svc = IngestServiceServer::new(MockService(state.clone()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let router = tonic::service::Routes::new(svc).into_axum_router();
            let _ = axum::serve(listener, router).await;
        });
        Self {
            state,
            addr,
            handle,
        }
    }

    /// Backend URL for `AdapterConfig::backend_url`.
    pub fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// Recorded call order, e.g. `["fingerprint", "register", "submit"]`.
    pub fn calls(&self) -> Vec<String> {
        self.state
            .calls
            .lock()
            .unwrap()
            .iter()
            .map(|s| s.to_string())
            .collect()
    }

    /// Every submitted batch, in order.
    pub fn submitted(&self) -> Vec<IngestBatch> {
        self.state.submitted.lock().unwrap().clone()
    }

    /// Every registration request, in order (D21-a: the projection
    /// arrives here).
    pub fn registrations(&self) -> Vec<RegisterSourceRequest> {
        self.state.registrations.lock().unwrap().clone()
    }

    /// Every preflighted batch, in order (D21-b — dry runs only).
    pub fn preflighted(&self) -> Vec<IngestBatch> {
        self.state.preflighted.lock().unwrap().clone()
    }

    /// Queue more script entries (mid-run additions).
    pub fn push_script(&self, entries: Vec<MockSubmit>) {
        self.state.script.lock().unwrap().extend(entries);
    }

    /// Change the reported fingerprint (drift mid-run, R8).
    pub fn drift_fingerprint(&self, fp: [u8; 32]) {
        *self.state.fingerprint.lock().unwrap() = fp;
    }

    /// Change the registration ceiling (mismatch test, R7).
    pub fn set_ceiling(&self, ceiling: i32) {
        *self.state.ceiling.lock().unwrap() = ceiling;
    }

    /// Stop the server.
    pub fn stop(self) {
        self.handle.abort();
    }

    /// D21-c: serve validation manifests (a small canned rulebook). Off
    /// by default; tests of the manifest path enable it explicitly.
    pub fn enable_manifest(&self) {
        self.state
            .serve_manifest
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// D21-c: serve the manifest envelope with a fingerprint that does
    /// NOT match the one `Fingerprint` reports — the SDK must degrade to
    /// server-side validation rather than trust it.
    pub fn serve_stale_manifest_fingerprint(&self) {
        self.state
            .serve_manifest
            .store(true, std::sync::atomic::Ordering::SeqCst);
        self.state
            .stale_manifest_fingerprint
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

struct MockService(MockState);

#[tonic::async_trait]
impl IngestService for MockService {
    type SubmitStreamStream = futures::stream::BoxStream<'static, Result<SubmitAck, Status>>;

    async fn fingerprint(
        &self,
        _req: Request<FingerprintRequest>,
    ) -> Result<Response<FingerprintResponse>, Status> {
        self.0.calls.lock().unwrap().push("fingerprint");
        Ok(Response::new(FingerprintResponse {
            fingerprint: self.0.fingerprint.lock().unwrap().to_vec(),
            kernel_version: "mock".into(),
            packs: vec![],
        }))
    }

    async fn register_source(
        &self,
        req: Request<RegisterSourceRequest>,
    ) -> Result<Response<RegisterSourceResponse>, Status> {
        self.0.calls.lock().unwrap().push("register");
        self.0.registrations.lock().unwrap().push(req.into_inner());
        Ok(Response::new(RegisterSourceResponse {
            ceiling: *self.0.ceiling.lock().unwrap(),
        }))
    }

    async fn submit(&self, req: Request<IngestBatch>) -> Result<Response<IngestAck>, Status> {
        self.0.calls.lock().unwrap().push("submit");
        let batch = req.into_inner();
        self.0.submitted.lock().unwrap().push(batch.clone());
        let action = self
            .0
            .script
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(MockSubmit::Accept);
        match action {
            MockSubmit::Accept => Ok(Response::new(IngestAck {
                batch_id: batch.batch_id,
                accepted: batch.memories.len() as u32,
                rejected: 0,
                rejections: vec![],
                assigned_lsn: 1,
                similar_to: vec![],
            })),
            MockSubmit::RejectRows(code, detail) => Ok(Response::new(IngestAck {
                batch_id: batch.batch_id,
                accepted: 0,
                rejected: batch.memories.len() as u32,
                similar_to: vec![],
                rejections: batch
                    .memories
                    .iter()
                    .map(|m| RejectRow {
                        draft_key: m.draft_key.clone(),
                        code,
                        detail: detail.into(),
                    })
                    .collect(),
                assigned_lsn: 0,
            })),
            MockSubmit::Fail(code, msg) => Err(Status::new(code, msg)),
        }
    }

    async fn submit_stream(
        &self,
        _req: Request<Streaming<SubmitOne>>,
    ) -> Result<Response<Self::SubmitStreamStream>, Status> {
        Err(Status::unimplemented("streaming not mocked"))
    }

    /// D21-b: record the dry run and answer with the accept-all verdict
    /// (`assigned_lsn` 0 — preflight assigns nothing). Script-driven
    /// rejection rows apply to Submit only; a dry run's rejections come
    /// from real validation, which the mock does not perform.
    async fn preflight(&self, req: Request<IngestBatch>) -> Result<Response<IngestAck>, Status> {
        self.0.calls.lock().unwrap().push("preflight");
        let batch = req.into_inner();
        self.0.preflighted.lock().unwrap().push(batch.clone());
        Ok(Response::new(IngestAck {
            batch_id: batch.batch_id,
            accepted: (batch.memories.len() + batch.relationships.len()) as u32,
            rejected: 0,
            rejections: vec![],
            assigned_lsn: 0,
            similar_to: vec![],
        }))
    }

    /// D21-c: serve a small canned rulebook stamped with the mock's
    /// fingerprint (or a stale one when the knob is set). The canned
    /// content is enough for the interpreter tests: one type, one kind,
    /// one triple, a title bound.
    async fn get_validation_manifest(
        &self,
        _req: Request<exocortex_wire::ingest::v1::ManifestRequest>,
    ) -> Result<Response<exocortex_wire::ingest::v1::ManifestResponse>, Status> {
        if !self
            .0
            .serve_manifest
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return Err(Status::unimplemented(
                "manifest not configured on this mock (enable_manifest)",
            ));
        }
        self.0.calls.lock().unwrap().push("manifest");
        let mut fingerprint = *self.0.fingerprint.lock().unwrap();
        if self
            .0
            .stale_manifest_fingerprint
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            fingerprint[0] = fingerprint[0].wrapping_add(1);
        }
        let manifest = exocortex_wire::manifest::ValidationManifest {
            manifest_version: exocortex_wire::manifest::MANIFEST_VERSION,
            compatibility_fingerprint: {
                let mut hex = String::with_capacity(64);
                for byte in fingerprint {
                    use std::fmt::Write as _;
                    let _ = write!(hex, "{byte:02x}");
                }
                hex
            },
            memory_types: vec![exocortex_wire::manifest::ManifestMemoryType {
                name: "General".into(),
                id: 0,
            }],
            kinds: vec![exocortex_wire::manifest::ManifestKind {
                id: 1,
                name: "RelatedTo".into(),
                computed_only: false,
                default_strength: 0.3,
            }],
            type_triples: vec![exocortex_wire::manifest::ManifestTriple {
                kind: 1,
                from_types: None,
                to_types: None,
            }],
            title_min_chars: 1,
            title_max_chars: 200,
            registered_ceiling: Some(3),
        };
        Ok(Response::new(
            exocortex_wire::ingest::v1::ManifestResponse {
                manifest_json: serde_json::to_string(&manifest).unwrap(),
                compatibility_fingerprint: fingerprint.to_vec(),
            },
        ))
    }
}
