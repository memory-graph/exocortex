//! H11 / §23 #18: the full session-wrapup chain over the wire — a producer
//! submits a batch to the backend node via gRPC, storage commits publish
//! invalidations through the cluster hub, and a sibling client's SSE
//! subscriber observes the memory in its local cache within 500ms.
//! R-T16a: a second sync of the same source with a bumped snapshot_id
//! produces ADDITIONAL assertions, never overwrites.

use std::sync::Arc;

use exocortex_kernel::Ontology;
use exocortex_storage::{InMemoryStorage, Storage};
use exocortex_wire::ingest::v1::{
    ingest_service_client::IngestServiceClient, ExternalKey, ExternalSnapshotInfo, IngestBatch,
    MemoryDraft, ProducerIdentity,
};

use exocortex_cache::LocalCache;
use exocortex_client::sync::{run_sse_sync, SseSyncConfig};

const HMAC_KEY: [u8; 32] = [7u8; 32];

fn authed<T>(message: T) -> tonic::Request<T> {
    let mut request = tonic::Request::new(message);
    request
        .metadata_mut()
        .insert("authorization", "Bearer e2e-bearer".parse().unwrap());
    request
}

async fn boot() -> (
    exocortex_server::backend::BackendNode,
    Arc<InMemoryStorage>,
    Arc<Ontology>,
    std::net::SocketAddr,
) {
    let onto = Arc::new(Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()]).unwrap());
    let storage = Arc::new(InMemoryStorage::new(onto.clone()));
    let node = exocortex_server::backend::run_backend_node(
        storage.clone(),
        onto.clone(),
        exocortex_server::backend::BackendNodeArgs {
            bind: "127.0.0.1:0".into(),
            transport: exocortex_server::backend::TransportSecurity::PlaintextLoopback,
            node_id: "e2e-node".into(),
            cluster_secret: HMAC_KEY,
            principals: Arc::new(
                exocortex_server::principal::PrincipalRegistry::single(
                    "e2e-bearer".into(),
                    exocortex_ops::operations::ops_vc(
                        "org",
                        "e2e",
                        exocortex_kernel::Visibility::Org,
                    ),
                )
                .unwrap(),
            ),
            gossip_listen: "127.0.0.1:0".parse().unwrap(),
            seed_nodes: vec![],
            redis_url: None,
            quiet_hours: exocortex_dreams::fire::QuietHours::none(),
            admin_ceilings: vec![
                (
                    (
                        "org".into(),
                        "iceberg://warehouse/orders".into(),
                        "external-sync".into(),
                    ),
                    exocortex_kernel::Visibility::Org,
                ),
                (
                    (
                        "org".into(),
                        "session://e2e".into(),
                        "session-wrapup".into(),
                    ),
                    exocortex_kernel::Visibility::Org,
                ),
            ],
        },
    )
    .await
    .unwrap();
    let addr = node.local_addr;
    (node, storage, onto, addr)
}

fn signed(mut b: IngestBatch) -> IngestBatch {
    exocortex_wire::signing::prepare_batch(&HMAC_KEY, &mut b);
    b
}

fn ext_batch(fp: [u8; 32], snapshot: &str, key: &str, title: &str) -> IngestBatch {
    signed(IngestBatch {
        org_id: "org".into(),
        source_uri: "iceberg://warehouse/orders".into(),
        producer_id: "external-sync".into(),
        batch_id: format!("b-{snapshot}-{key}"),
        mapping_version: "orders:1.0.0".into(),
        ontology_fingerprint: fp.to_vec(),
        ceiling: 3,
        checksum: String::new(),
        observed_at: None,
        recorded_at: None,
        snapshot: Some(ExternalSnapshotInfo {
            snapshot_id: snapshot.into(),
            schema_hash: [0u8; 32].to_vec(),
            source_flavor: "custom".into(),
        }),
        memories: vec![MemoryDraft {
            draft_key: key.into(),
            id: String::new(),
            memory_type: "General".into(),
            title: title.into(),
            content: "orders row".into(),
            tags: vec![],
            visibility: 3,
            valid_from: None,
            valid_until: None,
            external_key: Some(ExternalKey {
                table_uuid: [9u8; 16].to_vec(),
                logical_pk: key.into(),
                mapping_version: 1,
            }),
        }],
        relationships: vec![],
        producer: Some(ProducerIdentity {
            node_id: "n".into(),
            agent_id: String::new(),
            adapter_id: String::new(),
            hmac_signature: vec![],

            client_metadata: None,
        }),
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn wrapup_chain_grpc_to_sse_to_sibling_client() {
    let (node, storage, onto, addr) = boot().await;
    let keepalive = node;

    // Sibling client: cache + writer over the same storage (visibility of
    // the committed rows), SSE subscriber against the node's HTTP surface.
    let (cache, rx) = LocalCache::new(64 * 1024 * 1024);
    let cache = Arc::new(cache);
    {
        let cache = cache.clone();
        let storage = storage.clone();
        tokio::spawn(async move { cache.run(storage, rx).await });
    }
    let seed = test_mem("seed", 1);
    storage.upsert_memory(&seed).await.unwrap();
    cache.reseed_from_storage(&*storage, &"org".into()).await;
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let mut cfg = SseSyncConfig::new(format!("http://{addr}"), HMAC_KEY, onto.fingerprint.0);
    cfg.backoff = std::time::Duration::from_millis(50);
    // CS1: /v1/changes sits behind the same bearer layer as the op surface.
    cfg.bearer = Some("e2e-bearer".into());
    cfg.client_token = Some("e2e-bearer".into());
    cfg.client_key = Some(exocortex_server::sse::derive_client_sse_key(
        &HMAC_KEY,
        "e2e-bearer",
    ));
    let connection_ready = Arc::new(tokio::sync::Notify::new());
    cfg.connection_ready = Some(connection_ready.clone());
    let sync = tokio::spawn(run_sse_sync(cfg, cache.clone(), 0, None));
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        connection_ready.notified(),
    )
    .await
    .expect("SSE subscriber establishes its live stream");

    // Producer: register + submit over real gRPC.
    let mut client = IngestServiceClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    client
        .register_source(authed(exocortex_wire::signing::registration(
            &HMAC_KEY,
            "org",
            "session://e2e",
            "session-wrapup",
            3,
            "session",
            "n",
            exocortex_wire::ingest::v1::ProducerKind::CodingAgent,
        )))
        .await
        .unwrap();

    let target = test_mem("chained-target", 9);
    let b = IngestBatch {
        org_id: "org".into(),
        source_uri: "session://e2e".into(),
        producer_id: "session-wrapup".into(),
        batch_id: "chain-1".into(),
        mapping_version: "session-wrapup:1.0.0".into(),
        ontology_fingerprint: onto.fingerprint.0.to_vec(),
        ceiling: 3,
        checksum: String::new(),
        observed_at: None,
        recorded_at: None,
        snapshot: None,
        memories: vec![MemoryDraft {
            draft_key: "k1".into(),
            id: String::new(),
            memory_type: "General".into(),
            title: target.title.to_string(),
            content: "chained".into(),
            tags: vec![],
            visibility: 3,
            valid_from: None,
            valid_until: None,
            external_key: None,
        }],
        relationships: vec![],
        producer: Some(ProducerIdentity {
            node_id: "n".into(),
            agent_id: "a".into(),
            adapter_id: String::new(),
            hmac_signature: vec![],

            client_metadata: None,
        }),
    };
    let ack = client
        .submit(authed(signed(b.clone())))
        .await
        .unwrap()
        .into_inner();
    // D6: memory + InSession + companion.
    assert_eq!(ack.accepted, 3, "batch accepted over gRPC: {ack:?}");

    // The committed id: derive from the storage stream (single memory).
    let committed = {
        use futures::StreamExt;
        let mut ms = storage.stream_all_memories().await;
        let mut found = None;
        while let Some(Ok(m)) = ms.next().await {
            if m.title == target.title {
                found = Some(m.id);
            }
        }
        found.expect("committed row present")
    };

    let subscriber_visibility =
        exocortex_ops::operations::ops_vc("org", "e2e", exocortex_kernel::Visibility::Org);
    let stored = storage
        .get_memory(&committed)
        .await
        .unwrap()
        .expect("committed row remains readable");
    assert!(
        exocortex_storage::memory_visible(&stored, &subscriber_visibility),
        "the authenticated subscriber principal can see the committed row"
    );

    // §23 #18: the sibling observes it through the feed within 500ms.
    let vc = exocortex_ops::VisibilityContext {
        user_id: "u".into(),
        org_id: "org".into(),
        project_ids: Default::default(),
        team_ids: Default::default(),
        max_visibility: exocortex_kernel::Visibility::Org,
    };
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(500);
    let mut seen = false;
    while tokio::time::Instant::now() < deadline {
        if cache.get_memory("org", &committed, &vc).is_some() {
            seen = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    sync.abort();
    assert!(
        seen,
        "sibling client observed the commit via SSE within 500ms (version={:?}, node_sync_lsn={})",
        cache.version("org"),
        keepalive.health.load().sync_lsn,
    );
}

/// R-T16a under the shipped identity contract (B8/B9: external identity
/// is (org, source, table_uuid, logical_pk, mapping_version); snapshot id
/// and hash ride PROVENANCE, not identity). A re-sync of the same row
/// under a new snapshot upserts the SAME id with the new snapshot stamped
/// — one current row, never a duplicate — while a different logical_pk is
/// a genuinely new assertion that appends. (The old "two rows" assertion
/// passed only against the double's version stack, a model the Falkor
/// backend does not have — audit ST7/ST8.)
#[tokio::test(flavor = "multi_thread")]
async fn two_sync_snapshot_bump_upserts_same_row_new_pk_appends() {
    let (node, storage, onto, addr) = boot().await;
    let _keepalive = node;
    let mut client = IngestServiceClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    client
        .register_source(authed(exocortex_wire::signing::registration(
            &HMAC_KEY,
            "org",
            "iceberg://warehouse/orders",
            "external-sync",
            3,
            "external",
            "n",
            exocortex_wire::ingest::v1::ProducerKind::CodingAgent,
        )))
        .await
        .unwrap();

    let s1 = ext_batch(
        onto.fingerprint.0,
        "s1",
        "order-7",
        "payments owned by team-payments",
    );
    let ack1 = client.submit(authed(s1)).await.unwrap().into_inner();
    assert_eq!(ack1.accepted, 1, "first sync lands: {ack1:?}");

    let s2 = ext_batch(
        onto.fingerprint.0,
        "s2",
        "order-7",
        "payments owned by team-platform",
    );
    let ack2 = client.submit(authed(s2)).await.unwrap().into_inner();
    assert_eq!(ack2.accepted, 1, "second sync lands: {ack2:?}");

    // A different row is a genuinely new assertion.
    let s3 = ext_batch(
        onto.fingerprint.0,
        "s2",
        "order-8",
        "payments owned by team-platform",
    );
    let ack3 = client.submit(authed(s3)).await.unwrap().into_inner();
    assert_eq!(ack3.accepted, 1, "new logical_pk appends: {ack3:?}");

    use futures::StreamExt;
    let mut ms = storage.stream_all_memories().await;
    let mut rows = Vec::new();
    while let Some(Ok(m)) = ms.next().await {
        if m.title.contains("payments owned by") {
            rows.push(m);
        }
    }
    // order-7 exists ONCE (upserted in place, newest content) and order-8
    // exists as its own row.
    assert_eq!(rows.len(), 2, "one row per external key: {rows:?}");
    let order7 = rows
        .iter()
        .find(|m| matches!(&m.provenance, exocortex_kernel::Provenance::ExternalSnapshot(e) if e.external_key.logical_pk.as_slice() == b"order-7"))
        .expect("order-7 row present");
    assert_eq!(
        order7.title, "payments owned by team-platform",
        "same-key re-sync upserts the current content"
    );
    let snap = match &order7.provenance {
        exocortex_kernel::Provenance::ExternalSnapshot(e) => e,
        other => panic!("external provenance, got {other:?}"),
    };
    assert_eq!(
        snap.snapshot_id.as_str(),
        "s2",
        "provenance carries the LATEST snapshot id"
    );
}

/// Deterministic test memory (fixed id from `n`).
fn test_mem(title: &str, n: u8) -> exocortex_kernel::Memory {
    use exocortex_kernel::{Memory, MemoryContext, Provenance, Visibility, LSN};
    Memory {
        id: exocortex_kernel::MemoryId([n; 16]),
        memory_type: 3,
        title: title.into(),
        content: format!("content {title}"),
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
        valid_from: chrono::Utc::now(),
        valid_until: None,
        recorded_at: chrono::Utc::now(),
        invalidated_by: None,
        embedding: None,
        lsn: LSN::new_local(0),
    }
}
