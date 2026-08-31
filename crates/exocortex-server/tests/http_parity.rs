//! W6/H4/H5 acceptance: every registered operation answers over HTTP on a
//! running server (CR-9 parity, §21.3.6 over the wire), outputs are
//! byte-identical to the typed handler's, bearer auth is enforced (R-Sec7),
//! and the observability endpoints mount (R-O2/R-O4).

use std::sync::Arc;

use exocortex_cache::{GraphSnapshot, LocalCache};
use exocortex_kernel::{Memory, MemoryContext, MemoryId, Provenance, Visibility, LSN};
use exocortex_ops::operations::ops_vc;
use exocortex_ops::{entries, OpContext};
use exocortex_server::http_bind::HttpBind;
use exocortex_storage::{DiscoveryProposal, DiscoveryRecord, InMemoryStorage, RegionKey, Storage};

fn ontology() -> Arc<exocortex_kernel::Ontology> {
    Arc::new(
        exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()]).unwrap(),
    )
}

fn mem(title: &str, n: u8) -> Memory {
    Memory {
        id: MemoryId([n; 16]),
        memory_type: 3,
        title: title.into(),
        content: format!("c {title}"),
        summary: None,
        tags: Default::default(),
        visibility: Visibility::Org,
        provenance: Provenance::Asserted {
            author: "t".into(),
            producer_kind: None,
        },
        context: MemoryContext {
            timestamp: chrono::Utc::now(),
            project_id: None,
            project_path: None,
            team_id: None,
            tenant_id: Some("org".into()),
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
        valid_from: chrono::Utc::now(),
        valid_until: None,
        recorded_at: chrono::Utc::now(),
        invalidated_by: None,
        embedding: None,
        lsn: LSN::new_local(0),
    }
}

fn hex(id: &MemoryId) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(32);
    for b in id.0 {
        let _ = write!(out, "{b:02x}");
    }
    out
}

/// Minimal HTTP/1.1 client over a TCP socket (no new dependency).
async fn http(
    addr: std::net::SocketAddr,
    method: &str,
    path: &str,
    bearer: Option<&str>,
    body: Option<&serde_json::Value>,
) -> (u16, serde_json::Value, String) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
    let payload = body.map(|b| b.to_string()).unwrap_or_default();
    let auth = bearer
        .map(|t| format!("Authorization: Bearer {t}\r\n"))
        .unwrap_or_default();
    let req = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\n{auth}Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{payload}",
        payload.len()
    );
    sock.write_all(req.as_bytes()).await.unwrap();
    let mut raw = Vec::new();
    sock.read_to_end(&mut raw).await.unwrap();
    let text = String::from_utf8_lossy(&raw).into_owned();
    let status: u16 = text
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let body_str = text.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
    let json = serde_json::from_str(&body_str).unwrap_or(serde_json::Value::Null);
    (status, json, body_str)
}

#[tokio::test(flavor = "multi_thread")]
async fn every_operation_answers_over_http_with_auth() {
    let onto = ontology();
    let storage = Arc::new(InMemoryStorage::new(onto.clone()));
    let (cache, _rx) = LocalCache::new(64 * 1024 * 1024);
    let a = mem("parity-target", 1);
    let b = mem("parity-other", 2);
    let mut snap = GraphSnapshot::empty();
    snap.push_test_memory(a.clone());
    snap.push_test_memory(b.clone());
    cache.publish("org", Arc::new(snap));
    storage.upsert_memory(&a).await.unwrap();
    storage.upsert_memory(&b).await.unwrap();
    // PX6: one derived edge so `explain_edge` answers successfully.
    let derived_edge = exocortex_kernel::Relationship {
        id: exocortex_kernel::RelationshipId::derive(
            a.id,
            exocortex_kernel::kinds::SOLVES,
            b.id,
            None,
        ),
        kind: exocortex_kernel::kinds::SOLVES,
        from: a.id,
        to: b.id,
        visibility: Visibility::Org,
        provenance: exocortex_kernel::Provenance::Derived {
            rule_id: "R1".into(),
            evidence: vec![],
        },
        properties: exocortex_kernel::relationship::RelationshipProperties {
            strength: 0.8,
            confidence: 0.8,
            context: None,
            evidence_count: 1,
            success_rate: None,
            validation_count: 0,
            counter_evidence_count: 0,
            last_validated: chrono::Utc::now(),
        },
        description: None,
        bidirectional: false,
        valid_from: chrono::Utc::now(),
        valid_until: None,
        recorded_at: chrono::Utc::now(),
        invalidated_by: None,
        lsn: exocortex_kernel::LSN::new_local(0),
    };
    storage.upsert_relationship(&derived_edge).await.unwrap();
    // D7: an open Contradicts edge for resolve_contradiction's parity pass.
    let contradiction_edge = {
        let mut edge = derived_edge.clone();
        edge.kind = onto.kind_id("Contradicts").expect("Contradicts registered");
        edge.id = exocortex_kernel::RelationshipId::derive(a.id, edge.kind, b.id, None);
        edge
    };
    storage
        .upsert_relationship(&contradiction_edge)
        .await
        .unwrap();
    let contradiction_edge_hex = {
        use std::fmt::Write as _;
        let mut hex = String::with_capacity(32);
        for byte in contradiction_edge.id.0 {
            let _ = write!(hex, "{byte:02x}");
        }
        hex
    };
    let derived_edge_hex = {
        use std::fmt::Write as _;
        let mut hex = String::with_capacity(32);
        for byte in derived_edge.id.0 {
            let _ = write!(hex, "{byte:02x}");
        }
        hex
    };
    let discovery_at = chrono::Utc::now();
    storage
        .store_discovery(&DiscoveryRecord {
            discovery_id: "http-parity-discovery".into(),
            region: RegionKey {
                org: "org".into(),
                project: "*".into(),
                memory_type: a.memory_type,
            },
            from: a.id,
            to: b.id,
            discovery_type: "transitive".into(),
            quality: 0.6,
            via_types: [1, 2],
            discovery_cycle_id: "http-parity-cycle".into(),
            discovered_at: discovery_at,
        })
        .await
        .unwrap();
    let ctx = Arc::new(OpContext {
        visibility_ctx: ops_vc("org", "alice", Visibility::Org),
        audit_admin: true,
        storage: storage.clone() as Arc<dyn exocortex_storage::Storage>,
        cache: Arc::new(cache),
        deadline: chrono::Utc::now() + chrono::Duration::seconds(5),
        // D2: preflight needs the effective rulebook.
        ontology: Some(ontology()),
        ingest_preflight: None,
    });

    let principals = Arc::new(
        exocortex_server::principal::PrincipalRegistry::single_with_audit_admin(
            "test-only-secret-bearer-token-00000000".into(),
            ctx.visibility_ctx.clone(),
            true,
        )
        .unwrap(),
    );
    let bind = HttpBind::with_principals(ctx.clone(), principals);
    let app = bind.router(None);
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let input_for = |name: &str| -> serde_json::Value {
        match name {
            "find_related" => serde_json::json!({ "anchor": hex(&a.id), "k": 2 }),
            "get_memory" => serde_json::json!({ "id": hex(&a.id) }),
            "search_memories" => serde_json::json!({ "query": "parity", "limit": 5 }),
            // D2: a small valid batch (Problem + Fix + Fixes edge).
            "preflight_wrapup" => serde_json::json!({
                "project_id": "p",
                "memories": [
                    { "draft_key": "p", "memory_type": "Problem", "title": "Pool exhausted",
                      "content": "c", "visibility": "project", "tags": [] },
                    { "draft_key": "f", "memory_type": "Fix", "title": "Fixed pool exhaustion",
                      "content": "c", "visibility": "project", "tags": [] }
                ],
                "edges": [
                    { "from_draft_key": "f", "to_draft_key": "p", "to_memory_id": "",
                      "kind": "Fixes", "strength": 0.9 }
                ]
            }),
            "promote_visibility" => {
                serde_json::json!({ "memory_id": hex(&b.id), "to": "org" })
            }
            "list_discoveries" => serde_json::json!({ "limit": 20 }),
            "issue_discovery" => serde_json::json!({
                "discovery_id": "http-parity-discovery",
                "kind": "RelatedTo",
            }),
            "accept_discovery" => serde_json::json!({
                "discovery_id": "22222222-2222-2222-2222-222222222222",
                "from": hex(&a.id),
                "to": hex(&b.id),
                "kind": "RelatedTo",
            }),
            "list_audit_records" => serde_json::json!({ "since_lsn": 0 }),
            "retract_edge" => {
                serde_json::json!({
                    "edge_id": derived_edge_hex,
                    "reason": "http parity probe",
                })
            }
            "resolve_contradiction" => serde_json::json!({
                "edge_id": contradiction_edge_hex,
                "resolution": "neither",
                "note": "http parity probe",
            }),
            "get_chain" => serde_json::json!({ "memory": hex(&a.id), "max_depth": 2 }),
            "explain_edge" => serde_json::json!({ "edge": derived_edge_hex }),
            // PX2: a dry-run whose typed verdict is deterministic under the
            // Org-scoped admin ctx (the Project-ceiling verb refuses it).
            "preflight_action" => serde_json::json!({
                "pack": "exocortex-pack-mortgage-v1",
                "verb": "AttachRuleFinding",
                "input": {
                    "loan": "0".repeat(32),
                    "rule": "0".repeat(32),
                    "finding_title": "t",
                    "finding_content": "c",
                },
            }),
            other => panic!("no test input crafted for op {other}"),
        }
    };

    // CR-9: for each registered op, HTTP output == typed-handler output.
    let all = entries();
    assert!(
        all.len() >= 6,
        "expected the registered op set, got {}",
        all.len()
    );
    for entry in &all {
        // Pack verbs are covered by the dedicated Project-scoped parity
        // block below: the shared admin ctx is Org-scoped, deliberately
        // outside these verbs' declared ceilings.
        if entry.pack.is_some() {
            continue;
        }
        // D21-b: `preflight_batch` needs a real ingest registration; it is
        // covered by its own parity test below with the backend handle.
        if entry.name == "preflight_batch" {
            continue;
        }
        let input = input_for(entry.name);
        let proposal = (entry.name == "accept_discovery").then(|| DiscoveryProposal {
            discovery_id: "22222222-2222-2222-2222-222222222222".into(),
            region: RegionKey {
                org: "org".into(),
                project: "*".into(),
                memory_type: a.memory_type,
            },
            from: a.id,
            to: b.id,
            kind: onto.kind_id("RelatedTo").expect("RelatedTo registered"),
            proposed_visibility: Visibility::Org,
            caller_scope: ctx.visibility_ctx.clone(),
            issued_at: chrono::Utc::now(),
        });
        if let Some(proposal) = &proposal {
            storage
                .store_discovery(&DiscoveryRecord {
                    discovery_id: proposal.discovery_id.clone(),
                    region: proposal.region.clone(),
                    from: proposal.from,
                    to: proposal.to,
                    discovery_type: "transitive".into(),
                    quality: 0.6,
                    via_types: [1, 2],
                    discovery_cycle_id: "typed-acceptance".into(),
                    discovered_at: proposal.issued_at,
                })
                .await
                .unwrap();
            storage.create_discovery_proposal(proposal).await.unwrap();
        }
        let expected = (entry.handler)(entry, &ctx, input.clone())
            .await
            .unwrap_or_else(|e| panic!("{}: handler: {e}", entry.name));

        // Stateful parity executes the same operation through both surfaces.
        // Issue a second immutable proposal because a successful acceptance
        // permanently consumes the first id. Relationship identity depends on
        // endpoints and kind, so the stable outputs remain directly comparable.
        let mut http_input = input.clone();
        if entry.name == "resolve_contradiction" {
            // Resolution permanently closes its edge: give HTTP a second
            // open Contradicts edge with identical shape.
            let mut http_edge = derived_edge.clone();
            http_edge.kind = onto.kind_id("Contradicts").unwrap();
            http_edge.id =
                exocortex_kernel::RelationshipId::derive(a.id, http_edge.kind, b.id, Some("http"));
            storage.upsert_relationship(&http_edge).await.unwrap();
            let mut hex = String::with_capacity(32);
            use std::fmt::Write as _;
            for byte in http_edge.id.0 {
                let _ = write!(hex, "{byte:02x}");
            }
            http_input["edge_id"] = serde_json::Value::String(hex);
        }
        if entry.name == "retract_edge" {
            // Retraction permanently closes its edge: give HTTP a
            // second open edge with identical shape so both surfaces
            // answer successfully and outputs stay comparable.
            let mut http_edge = derived_edge.clone();
            http_edge.kind = exocortex_kernel::kinds::CAUSES;
            http_edge.id = exocortex_kernel::RelationshipId::derive(
                a.id,
                exocortex_kernel::kinds::CAUSES,
                b.id,
                None,
            );
            storage.upsert_relationship(&http_edge).await.unwrap();
            let mut hex = String::with_capacity(32);
            use std::fmt::Write as _;
            for byte in http_edge.id.0 {
                let _ = write!(hex, "{byte:02x}");
            }
            http_input["edge_id"] = serde_json::Value::String(hex);
        }
        if entry.name == "issue_discovery" {
            // Issuance atomically retires presentation state and leaves an
            // immutable proposal behind. Give HTTP an independent discovery
            // id with otherwise identical content.
            let http_discovery_id = "http-parity-discovery-http";
            http_input["discovery_id"] = serde_json::Value::String(http_discovery_id.to_owned());
            storage
                .store_discovery(&DiscoveryRecord {
                    discovery_id: http_discovery_id.into(),
                    region: RegionKey {
                        org: "org".into(),
                        project: "*".into(),
                        memory_type: a.memory_type,
                    },
                    from: a.id,
                    to: b.id,
                    discovery_type: "transitive".into(),
                    quality: 0.6,
                    via_types: [1, 2],
                    discovery_cycle_id: "http-parity-cycle".into(),
                    discovered_at: discovery_at,
                })
                .await
                .unwrap();
        }
        if let Some(proposal) = &proposal {
            let mut http_proposal = proposal.clone();
            http_proposal.discovery_id = "33333333-3333-3333-3333-333333333333".into();
            http_input["discovery_id"] =
                serde_json::Value::String(http_proposal.discovery_id.to_string());
            storage
                .store_discovery(&DiscoveryRecord {
                    discovery_id: http_proposal.discovery_id.clone(),
                    region: http_proposal.region.clone(),
                    from: http_proposal.from,
                    to: http_proposal.to,
                    discovery_type: "transitive".into(),
                    quality: 0.6,
                    via_types: [1, 2],
                    discovery_cycle_id: "http-acceptance".into(),
                    discovered_at: http_proposal.issued_at,
                })
                .await
                .unwrap();
            storage
                .create_discovery_proposal(&http_proposal)
                .await
                .unwrap();
        }

        let (status, actual, _) = if (entry.http_method)() == axum::http::Method::GET {
            let qs = query_of(&http_input);
            http(
                addr,
                "GET",
                &format!("{}?{}", entry.http_path, qs),
                Some("test-only-secret-bearer-token-00000000"),
                None,
            )
            .await
        } else {
            http(
                addr,
                "POST",
                entry.http_path,
                Some("test-only-secret-bearer-token-00000000"),
                Some(&http_input),
            )
            .await
        };
        assert_eq!(status, 200, "{}: http status", entry.name);
        parity_check(entry.name, &actual, &expected);
    }

    // R-Sec7: no bearer -> 401 on every op route.
    let (status, _, _) = http(
        addr,
        "POST",
        "/v1/get_memory",
        None,
        Some(&serde_json::json!({ "id": hex(&a.id) })),
    )
    .await;
    assert_eq!(status, 401, "auth enforced");
    let (status, _, _) = http(
        addr,
        "POST",
        "/v1/get_memory",
        Some("wrong-token"),
        Some(&serde_json::json!({ "id": hex(&a.id) })),
    )
    .await;
    assert_eq!(status, 401, "wrong bearer rejected");

    // H4: observability endpoints.
    let (status, body, text) = http(
        addr,
        "GET",
        "/metrics",
        Some("test-only-secret-bearer-token-00000000"),
        None,
    )
    .await;
    assert_eq!(status, 200);
    assert!(body.is_null(), "metrics is text, not JSON");
    assert!(text.contains("exocortex"), "prometheus text format: {text}");
    let (status, body, _) = http(addr, "GET", "/health/ready", None, None).await;
    assert_eq!(status, 200);
    assert_eq!(body["status"], "ready");
    for path in ["/health/cluster", "/health/sync", "/health/hydration"] {
        let (status, _, _) = http(addr, "GET", path, None, None).await;
        assert_eq!(status, 401, "{path} rejects unauthenticated callers");
        let (status, body, _) = http(
            addr,
            "GET",
            path,
            Some("test-only-secret-bearer-token-00000000"),
            None,
        )
        .await;
        assert_eq!(status, 200, "{path} answers");
        assert!(body.is_object(), "{path} returns JSON");
    }
}

/// CR-9 parity per op: read-only ops must match byte-for-byte; stateful
/// ops append a fresh audit record per invocation, so their comparison
/// covers every stable field and requires a live audit LSN on both sides.
fn parity_check(name: &str, http_out: &serde_json::Value, direct: &serde_json::Value) {
    match name {
        "issue_discovery" => {
            // Discovery ids are immutable and single-use, so the direct and
            // HTTP halves receive independent ids over identical endpoints.
            assert_ne!(http_out["discovery_id"], direct["discovery_id"]);
            for field in ["from", "to", "kind", "visibility"] {
                assert_eq!(http_out[field], direct[field], "{name}: {field}");
            }
        }
        "resolve_contradiction" => {
            // The two surfaces resolve two independently created open
            // Contradicts edges, so ids and audit LSNs differ by
            // construction; both must record the same resolution shape
            // with a concrete audit commit.
            for output in [http_out, direct] {
                assert_eq!(
                    output["resolution"].as_str(),
                    Some("neither"),
                    "{name}: resolution echoed"
                );
                assert_eq!(output["superseded"], serde_json::Value::Null, "{name}");
                assert_eq!(
                    output["edge_id"].as_str().map(str::len),
                    Some(32),
                    "{name}: edge id"
                );
                assert!(output["audit_lsn"].as_u64().unwrap_or(0) > 0);
            }
        }
        "retract_edge" => {
            // The two surfaces close two independently created open
            // edges, so ids and audit LSNs differ by construction; both
            // must name a concrete 32-hex id and an audit commit.
            for output in [http_out, direct] {
                assert_eq!(
                    output["edge_id"].as_str().map(str::len),
                    Some(32),
                    "{name}: edge id"
                );
                assert!(output["audit_lsn"].as_u64().unwrap_or(0) > 0);
            }
        }
        "accept_discovery" => {
            // Discovery ids are single-use and deliberately participate in
            // relationship identity, so the two independently authorized
            // invocations cannot have byte-identical edge ids. Both surfaces
            // must still return a concrete 128-bit identity and audit commit.
            for output in [http_out, direct] {
                assert_eq!(
                    output["edge_id"].as_str().map(str::len),
                    Some(32),
                    "{name}: edge id"
                );
            }
            assert!(http_out["audit_lsn"].as_u64().unwrap_or(0) > 0);
            assert!(direct["audit_lsn"].as_u64().unwrap_or(0) > 0);
        }
        "promote_visibility" => {
            assert_eq!(http_out["memory_id"], direct["memory_id"], "{name}");
            assert_eq!(http_out["visibility"], direct["visibility"], "{name}");
            assert!(http_out["audit_lsn"].as_u64().unwrap_or(0) > 0);
        }
        "list_audit_records" => {
            // The audit log only grows; every direct record must appear in
            // the HTTP view.
            let direct_records = direct["records"].as_array().expect("records");
            let http_records = http_out["records"].as_array().expect("records");
            assert!(http_records.len() >= direct_records.len(), "{name}");
            for r in direct_records {
                assert!(
                    http_records.contains(r),
                    "{name}: direct record {r} present over HTTP"
                );
            }
        }
        other => {
            assert_eq!(
                http_out, direct,
                "{other}: HTTP output matches the typed handler (CR-9)"
            );
        }
    }
}

fn query_of(v: &serde_json::Value) -> String {
    let obj = v.as_object().expect("object input");
    obj.iter()
        .map(|(k, val)| match val {
            serde_json::Value::String(s) => format!("{k}={s}"),
            other => format!("{k}={other}"),
        })
        .collect::<Vec<_>>()
        .join("&")
}

/// D21-b (adapter-contract PRD D2): `preflight_batch` — the registry dry
/// run over a REAL registration answers byte-identically over HTTP and
/// the typed handler (CR-9), reports Submit's own verdicts with the
/// shared correction vocabulary, and commits nothing.
#[tokio::test(flavor = "multi_thread")]
async fn preflight_batch_answers_identically_over_http_and_the_registry() {
    use exocortex_wire::ingest::v1::ingest_service_server::IngestService as _;

    let onto = ontology();
    let storage = Arc::new(InMemoryStorage::new(onto.clone()));
    let (cache, _writer) = LocalCache::new(16 * 1024 * 1024);

    // A REAL registration on a real ingest server over the same storage.
    let ingest = Arc::new(exocortex_ingest::IngestServer::new(
        storage.clone(),
        onto.clone(),
        [7u8; 32],
    ));
    let mut registration = exocortex_wire::ingest::v1::RegisterSourceRequest {
        org_id: "org".into(),
        source_uri: "custom://parity-probe".into(),
        producer_id: "parity-probe".into(),
        ceiling: 1,
        source_flavor: "custom".into(),
        producer_kind: 5,
        producer: Some(exocortex_wire::ingest::v1::ProducerIdentity {
            node_id: "node".into(),
            agent_id: String::new(),
            adapter_id: "adapter".into(),
            hmac_signature: vec![],
            client_metadata: None,
        }),
        projection: None,
    };
    exocortex_wire::signing::sign_registration(&[7u8; 32], &mut registration);
    ingest
        .register_source(tonic::Request::new(registration))
        .await
        .unwrap();

    let mut scope = ops_vc("org", "alice", Visibility::Org);
    scope.project_ids = std::iter::once("p1".into()).collect();
    let ctx = Arc::new(OpContext {
        visibility_ctx: scope,
        audit_admin: true,
        storage: storage.clone() as Arc<dyn exocortex_storage::Storage>,
        cache: Arc::new(cache),
        deadline: chrono::Utc::now() + chrono::Duration::seconds(5),
        ontology: Some(onto.clone()),
        ingest_preflight: Some(exocortex_server::backend::preflight_handle(ingest.clone())),
    });
    let principals = Arc::new(
        exocortex_server::principal::PrincipalRegistry::single_with_audit_admin(
            "test-only-preflight-bearer-token-000".into(),
            ctx.visibility_ctx.clone(),
            true,
        )
        .unwrap(),
    );
    let bind = HttpBind::with_principals(ctx.clone(), principals);
    let app = bind.router(None);
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let entry = entries()
        .into_iter()
        .find(|e| e.name == "preflight_batch")
        .expect("preflight_batch registered");
    // A sample with one clean row and one mapping error: the verdict must
    // name the bad row with the shared vocabulary and a correction.
    let input = serde_json::json!({
        "org_id": "org",
        "source_uri": "custom://parity-probe",
        "producer_id": "parity-probe",
        "project_id": "p1",
        "memories": [
            { "draft_key": "ok", "memory_type": "Fix", "title": "Fixed the parity gap",
              "content": "c", "tags": [], "visibility": "project" },
            { "draft_key": "broken", "memory_type": "NotAType", "title": "mapping typo",
              "content": "c", "tags": [], "visibility": "project" }
        ],
        "relationships": []
    });
    let audit_before = storage.audit_range("org", 0, 1000).await.unwrap().len();
    let typed = (entry.handler)(entry, &ctx, input.clone())
        .await
        .expect("typed surface");
    let (status, over_http, raw) = http(
        addr,
        "POST",
        "/v1/preflight_batch",
        Some("test-only-preflight-bearer-token-000"),
        Some(&input),
    )
    .await;
    assert_eq!(status, 200, "{raw}");
    assert_eq!(typed, over_http, "byte-identical across surfaces");
    assert_eq!(typed["committed"], false, "preflight commits nothing");
    // Batch-atomic like Submit: one bad row rejects the batch, so both
    // rows count as would-reject while the named rejection is the cause.
    assert_eq!(typed["would_reject"], 2);
    let rejections = typed["rejections"].as_array().unwrap();
    assert_eq!(rejections.len(), 1);
    assert_eq!(rejections[0]["draft_key"], "broken");
    assert_eq!(rejections[0]["code"], "UnknownMemoryType");
    assert!(!rejections[0]["correction"].as_str().unwrap().is_empty());
    assert_eq!(
        storage.audit_range("org", 0, 1000).await.unwrap().len(),
        audit_before,
        "no audit row from the dry run"
    );

    // The org guard: a principal may not dry-run another org.
    let mut foreign = input.clone();
    foreign["org_id"] = serde_json::Value::String("other-org".into());
    let err = (entry.handler)(entry, &ctx, foreign)
        .await
        .expect_err("cross-org preflight refused");
    assert!(err.to_string().contains("another org"), "{err}");
}

/// PX2 acceptance: pack Actions and Functions ride the SAME parity walk
/// in the same shape as kernel ops — HTTP output byte-identical to the
/// typed handler's. Pack verbs declare their own ceilings
/// (`AttachRuleFinding` is Project), so this block runs against a
/// Project-scoped principal with Project-visible fixtures.
#[tokio::test]
async fn pack_verbs_answer_identically_over_http_and_the_registry() {
    use exocortex_storage::Storage as _;

    let onto = Arc::new(
        exocortex_kernel::Ontology::from_packs(vec![
            exocortex_pack_dev_v1::pack_def(),
            exocortex_pack_mortgage_v1::pack_def(),
        ])
        .unwrap(),
    );
    let storage = Arc::new(InMemoryStorage::new(onto.clone()));
    let (cache, _writer) = LocalCache::new(16 * 1024 * 1024);

    // Project-visible fixtures: a loan file and a rule for the finding.
    let mut loan = mem("loan file 7", 7);
    loan.memory_type = onto.memory_type_id("LoanApplication").unwrap();
    loan.visibility = Visibility::Private;
    loan.context.user_id = Some("alice".into());
    let mut rule = mem("DTI rule", 8);
    rule.memory_type = onto.memory_type_id("RuleDefinition").unwrap();
    rule.visibility = Visibility::Private;
    rule.context.user_id = Some("alice".into());
    storage.upsert_memory(&loan).await.unwrap();
    storage.upsert_memory(&rule).await.unwrap();

    let ctx = Arc::new(OpContext {
        visibility_ctx: ops_vc("org", "alice", Visibility::Project),
        audit_admin: false,
        storage: storage.clone() as Arc<dyn exocortex_storage::Storage>,
        cache: Arc::new(cache),
        deadline: chrono::Utc::now() + chrono::Duration::seconds(5),
        ontology: Some(onto.clone()),
        ingest_preflight: None,
    });
    let principals = Arc::new(
        exocortex_server::principal::PrincipalRegistry::single_with_audit_admin(
            "test-only-pack-verb-bearer-token-0000".into(),
            ctx.visibility_ctx.clone(),
            true,
        )
        .unwrap(),
    );
    let bind = HttpBind::with_principals(ctx.clone(), principals);
    let app = bind.router(None);
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let pack_entries: Vec<_> = entries().into_iter().filter(|e| e.pack.is_some()).collect();
    assert!(
        pack_entries.len() >= 2,
        "pack verbs joined the registry, got {}",
        pack_entries.len()
    );
    for entry in pack_entries {
        let input = match entry.name {
            "exocortex-pack-mortgage-v1.AttachRuleFinding" => serde_json::json!({
                "loan": hex(&loan.id),
                "rule": hex(&rule.id),
                "finding_title": "DTI over ceiling",
                "finding_content": "41% against a 43% policy",
            }),
            "exocortex-pack-mortgage-v1.IsCategoricallyEligible" => serde_json::json!({
                "income_verified": true,
                "categorical_kind": "categorical",
            }),
            other => panic!("no parity input crafted for pack verb {other}"),
        };
        let typed = (entry.handler)(entry, &ctx, input.clone())
            .await
            .unwrap_or_else(|e| panic!("{}: typed: {e}", entry.name));
        // Stateful parity: the action commits on its first execution, so
        // the HTTP run re-runs the body fresh — the output SHAPE is
        // compared (verb/counts), and the audit row is asserted directly.
        let (status, over_http, raw) = http(
            addr,
            "POST",
            entry.http_path,
            Some("test-only-pack-verb-bearer-token-0000"),
            Some(&input),
        )
        .await;
        assert_eq!(status, 200, "{}: {raw}", entry.name);
        if entry.name.ends_with("AttachRuleFinding") {
            assert_eq!(typed["verb"], over_http["verb"]);
            assert_eq!(
                typed["memories"].as_array().map(Vec::len),
                over_http["memories"].as_array().map(Vec::len),
                "committed row count matches across surfaces"
            );
            assert_eq!(
                typed["edges"].as_array().map(Vec::len),
                over_http["edges"].as_array().map(Vec::len)
            );
            let rows = storage.audit_range("org", 0, 100).await.unwrap();
            assert!(
                rows.iter()
                    .any(|r| r["action"] == "exocortex-pack-mortgage-v1.AttachRuleFinding"),
                "one audit row per call, keyed pack.verb"
            );
        } else {
            assert_eq!(
                typed, over_http,
                "{}: byte-identical function output",
                entry.name
            );
        }
    }
}
