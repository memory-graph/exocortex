// crates/exocortex-adapter-sdk/src/testing.rs
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
        _req: Request<RegisterSourceRequest>,
    ) -> Result<Response<RegisterSourceResponse>, Status> {
        self.0.calls.lock().unwrap().push("register");
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
            })),
            MockSubmit::RejectRows(code, detail) => Ok(Response::new(IngestAck {
                batch_id: batch.batch_id,
                accepted: 0,
                rejected: batch.memories.len() as u32,
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
}
