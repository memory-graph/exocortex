//! W7 acceptance (in-process shape): backend nodes serve HTTP + SSE + gRPC
//! on one listener, the lease re-election loop populates `/health/cluster`,
//! and chitchat gossip converges membership carrying wire-version +
//! ontology fingerprint. The docker-compose kill test (lease handover <2s
//! against live FalkorDB) runs via `--features integration`.

use std::sync::Arc;
use std::time::Duration;

use exocortex_server::backend::{run_backend_node, BackendNodeArgs};
use exocortex_storage::InMemoryStorage;
use exocortex_storage::Storage;

fn args(bind: &str, gossip: u16, seeds: Vec<String>) -> BackendNodeArgs {
    BackendNodeArgs {
        org: "org".into(),
        bind: bind.into(),
        transport: exocortex_server::backend::TransportSecurity::PlaintextLoopback,
        node_id: format!("node-{gossip}"),
        cluster_secret: [7u8; 32],
        principals: Arc::new(
            exocortex_server::principal::PrincipalRegistry::single_with_audit_admin(
                "test-only-backend-bearer-token-00000000".into(),
                exocortex_ops::operations::ops_vc("org", "test", exocortex_kernel::Visibility::Org),
                true,
            )
            .unwrap(),
        ),
        gossip_listen: format!("127.0.0.1:{gossip}").parse().unwrap(),
        seed_nodes: seeds,
        redis_url: None,
        quiet_hours: exocortex_dreams::fire::QuietHours::none(),
        admin_source_policies: vec![],
    }
}

async fn http_get(addr: std::net::SocketAddr, path: &str, bearer: Option<&str>) -> (u16, String) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
    let auth = bearer
        .map(|t| format!("Authorization: Bearer {t}\r\n"))
        .unwrap_or_default();
    let req = format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\n{auth}Connection: close\r\n\r\n");
    sock.write_all(req.as_bytes()).await.unwrap();
    let mut raw = Vec::new();
    sock.read_to_end(&mut raw).await.unwrap();
    let text = String::from_utf8_lossy(&raw).into_owned();
    let status: u16 = text
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    (
        status,
        text.split("\r\n\r\n").nth(1).unwrap_or("").to_string(),
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn dropping_backend_node_stops_ingress_and_releases_its_port() {
    let onto = Arc::new(
        exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()]).unwrap(),
    );
    let storage = Arc::new(InMemoryStorage::new(onto.clone()));
    let storage_lifetime = Arc::downgrade(&storage);
    let mut node = run_backend_node(storage.clone(), onto, args("127.0.0.1:0", 41991, vec![]))
        .await
        .unwrap();
    drop(storage);
    let addr = node.local_addr;
    assert!(std::net::TcpStream::connect(addr).is_ok());
    assert!(
        tokio::time::timeout(Duration::from_millis(10), node.wait_for_ingress())
            .await
            .is_err(),
        "cancelling a healthy ingress wait must leave its task owned by the node"
    );
    drop(node);

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        if let Ok(listener) = std::net::TcpListener::bind(addr) {
            drop(listener);
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "dropping BackendNode must release {addr}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while storage_lifetime.upgrade().is_some() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "dropping BackendNode must release storage retained by background tasks"
        );
        tokio::task::yield_now().await;
    }
}

#[tokio::test]
async fn cache_bridge_reseeds_past_an_upsert_whose_row_was_already_deleted() {
    use exocortex_kernel::{Provenance, Relationship, RelationshipId, RelationshipProperties};
    use exocortex_storage::{Direction, Invalidation, TraversalSpec};

    let onto = Arc::new(
        exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()]).unwrap(),
    );
    let storage = Arc::new(InMemoryStorage::new(onto));
    let from = acceptance_memory(1, false);
    let to = acceptance_memory(2, false);
    storage
        .upsert_batch(&[from.clone(), to.clone()], &[])
        .await
        .unwrap();
    let (cache, writer_rx) = exocortex_cache::LocalCache::new(64 * 1024 * 1024);
    let cache = Arc::new(cache);
    let writer = tokio::spawn({
        let cache = cache.clone();
        let storage = storage.clone();
        async move { cache.run(storage, writer_rx).await }
    });
    cache
        .reseed_from_storage(&*storage, &"org".into())
        .await
        .unwrap();

    let relationship = Relationship {
        id: RelationshipId([0x54; 16]),
        kind: exocortex_kernel::kinds::SOLVES,
        from: from.id,
        to: to.id,
        visibility: exocortex_kernel::Visibility::Org,
        provenance: Provenance::Asserted {
            author: "bridge-test".into(),
            producer_kind: None,
        },
        properties: RelationshipProperties {
            strength: 0.5,
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
    let health = Arc::new(arc_swap::ArcSwap::from_pointee(
        exocortex_server::http_bind::HealthSnapshot::default(),
    ));
    let invalidation = Invalidation::RelationshipUpserted {
        id: relationship.id,
        from: relationship.from,
        to: relationship.to,
        kind: relationship.kind,
        lsn: 54,
    };
    tokio::time::timeout(
        Duration::from_millis(250),
        exocortex_server::backend::apply_cache_invalidation_with_retry(
            &cache,
            &*storage,
            "org",
            &health,
            invalidation,
            Duration::from_millis(1),
        ),
    )
    .await
    .expect("authoritative reseed lets the bridge pass a stale upsert");
    assert_eq!(health.load().sync_lsn, 54);
    exocortex_server::backend::apply_cache_invalidation_with_retry(
        &cache,
        &*storage,
        "org",
        &health,
        Invalidation::RelationshipDeleted {
            id: relationship.id,
            lsn: 55,
        },
        Duration::from_millis(1),
    )
    .await;
    assert_eq!(health.load().sync_lsn, 55);
    let visible =
        exocortex_ops::operations::ops_vc("org", "test", exocortex_kernel::Visibility::Org);
    let reached = cache.traverse(
        "org",
        &from.id,
        &TraversalSpec {
            max_depth: 1,
            direction: Direction::Out,
            kinds: vec![relationship.kind].into(),
            max_nodes: 8,
            visibility_ctx: visible,
            as_of: None,
        },
    );
    assert_eq!(
        reached.iter().map(|memory| memory.id).collect::<Vec<_>>(),
        Vec::<exocortex_kernel::MemoryId>::new()
    );
    writer.abort();
}

#[tokio::test]
async fn cache_bridge_stream_discontinuities_reseed_before_later_progress() {
    use exocortex_storage::{Invalidation, StorageError};
    use futures::stream;

    for terminates_cleanly in [false, true] {
        let onto = Arc::new(
            exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()])
                .unwrap(),
        );
        let storage = Arc::new(InMemoryStorage::new(onto));
        let authoritative = acceptance_memory(if terminates_cleanly { 61 } else { 60 }, false);
        storage.upsert_memory(&authoritative).await.unwrap();
        let (cache, writer_rx) = exocortex_cache::LocalCache::new(64 * 1024 * 1024);
        let cache = Arc::new(cache);
        let writer = tokio::spawn({
            let cache = cache.clone();
            let storage = storage.clone();
            async move { cache.run(storage, writer_rx).await }
        });
        cache.seed_local("org", &[], &[], 0);
        let health = Arc::new(arc_swap::ArcSwap::from_pointee(
            exocortex_server::http_bind::HealthSnapshot::default(),
        ));
        let later = Invalidation::MemoryDeleted {
            id: authoritative.id,
            lsn: 999,
        };
        let items = if terminates_cleanly {
            Vec::new()
        } else {
            vec![
                Err(StorageError::Backend("injected stream fault".into())),
                Ok(later),
            ]
        };

        exocortex_server::backend::consume_cache_subscription(
            &cache,
            &*storage,
            "org",
            &health,
            stream::iter(items),
            Duration::from_millis(1),
            Duration::from_millis(4),
        )
        .await;

        let visible =
            exocortex_ops::operations::ops_vc("org", "test", exocortex_kernel::Visibility::Org);
        assert!(
            cache
                .get_memory("org", &authoritative.id, &visible)
                .is_some(),
            "an authoritative reseed repairs a {} stream",
            if terminates_cleanly {
                "terminated"
            } else {
                "failed"
            }
        );
        assert!(
            health.load().backend_lsn < 999,
            "the post-fault feed item must not progress before reseed/resubscribe"
        );
        assert_eq!(health.load().sync_lsn, health.load().backend_lsn);
        writer.abort();
    }
}

#[tokio::test]
async fn cache_bridge_burst_has_one_acknowledged_atomic_publication() {
    use exocortex_storage::Invalidation;

    let onto = Arc::new(
        exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()]).unwrap(),
    );
    let storage = Arc::new(InMemoryStorage::new(onto));
    let first = acceptance_memory(62, false);
    let second = acceptance_memory(63, false);
    let (cache, writer_rx) = exocortex_cache::LocalCache::new(64 * 1024 * 1024);
    let cache = Arc::new(cache);
    let writer = tokio::spawn({
        let cache = cache.clone();
        let storage = storage.clone();
        async move { cache.run(storage, writer_rx).await }
    });
    cache.seed_local("org", &[], &[], 0);
    storage
        .upsert_batch(&[first.clone(), second.clone()], &[])
        .await
        .unwrap();
    let before = cache.snapshot_publications();
    let health = Arc::new(arc_swap::ArcSwap::from_pointee(
        exocortex_server::http_bind::HealthSnapshot::default(),
    ));

    exocortex_server::backend::apply_cache_invalidations_with_retry(
        &cache,
        &*storage,
        "org",
        &health,
        vec![
            Invalidation::MemoryUpserted {
                id: first.id,
                lsn: 70,
            },
            Invalidation::MemoryUpserted {
                id: second.id,
                lsn: 71,
            },
        ],
        Duration::from_millis(1),
        Duration::from_millis(4),
    )
    .await;

    assert_eq!(cache.snapshot_publications(), before + 1);
    assert_eq!(health.load().sync_lsn, 71);
    let visible =
        exocortex_ops::operations::ops_vc("org", "test", exocortex_kernel::Visibility::Org);
    assert!(cache.get_memory("org", &first.id, &visible).is_some());
    assert!(cache.get_memory("org", &second.id, &visible).is_some());
    writer.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn backend_nodes_serve_http_grpc_and_gossip_converges() {
    let onto = Arc::new(
        exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()]).unwrap(),
    );
    let storage = Arc::new(InMemoryStorage::new(onto.clone()));

    // Two backend nodes over the shared storage; node-b seeds from node-a's
    // gossip port.
    let node_a = run_backend_node(
        storage.clone(),
        onto.clone(),
        args("127.0.0.1:0", 41001, vec![]),
    )
    .await
    .expect("node-a boots");
    let node_b = run_backend_node(
        storage.clone(),
        onto.clone(),
        args("127.0.0.1:0", 41002, vec!["127.0.0.1:41001".to_string()]),
    )
    .await
    .expect("node-b boots");

    let expected_wire = exocortex_wire::WIRE_VERSION.to_string();
    let gossip_deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        let a = node_a.gossip.with_chitchat(|c| c.state_snapshot()).await;
        let b = node_b.gossip.with_chitchat(|c| c.state_snapshot()).await;
        let converged = [&a, &b].iter().all(|snapshot| {
            snapshot.node_states.len() == 2
                && snapshot.node_states.iter().all(|state| {
                    state.get("wire_version") == Some(expected_wire.as_str())
                        && state
                            .get("ontology_fingerprint")
                            .is_some_and(|fingerprint| fingerprint.len() == 64)
                })
        });
        if converged {
            break;
        }
        assert!(
            tokio::time::Instant::now() < gossip_deadline,
            "both production gossip handles converge on two compatible members"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // HTTP parity surface answers with auth on both nodes.
    for (addr, name) in [(node_a.local_addr, "a"), (node_b.local_addr, "b")] {
        let (status, _) = http_get(
            addr,
            "/v1/audit?since_lsn=0",
            Some("test-only-backend-bearer-token-00000000"),
        )
        .await;
        assert_eq!(status, 200, "node {name} serves ops over HTTP");
        let (status, _) = http_get(addr, "/v1/audit?since_lsn=0", None).await;
        assert_eq!(status, 401, "node {name} enforces bearer auth");
        let (status, body) = http_get(addr, "/health/ready", None).await;
        assert_eq!(status, 200);
        assert!(body.contains("ready"));
    }

    // The lease loop populates cluster health within 2s (M5 shape; live
    // handover semantics ride the FalkorDB compose harness).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    let mut leader_seen = false;
    while tokio::time::Instant::now() < deadline {
        for node in [&node_a, &node_b] {
            let h = node.health.load_full();
            if h.leader_node_id.is_some() && h.lease_epoch >= 1 {
                leader_seen = true;
            }
        }
        if leader_seen {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(leader_seen, "lease re-election loop stamps /health/cluster");

    // SSE router mounted: the feed answers on the same listener. Read only
    // the first bytes — the stream is open-ended, so `read_to_end` would
    // block forever by design. CS1 (audit): the route sits behind the
    // bearer layer like every other op, so the header must ride along.
    {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut sock = tokio::net::TcpStream::connect(node_a.local_addr)
            .await
            .unwrap();
        let req = format!(
            "GET /v1/changes?token=test-only-backend-bearer-token-00000000 HTTP/1.1\r\nHost: {}\r\nAuthorization: Bearer test-only-backend-bearer-token-00000000\r\nConnection: close\r\n\r\n",
            node_a.local_addr
        );
        sock.write_all(req.as_bytes()).await.unwrap();
        let mut head = vec![0u8; 1024];
        let n = tokio::time::timeout(Duration::from_secs(2), sock.read(&mut head))
            .await
            .expect("sse responds within 2s")
            .expect("read");
        let text = String::from_utf8_lossy(&head[..n]).into_owned();
        assert!(
            text.starts_with("HTTP/1.1 200"),
            "SSE route answers: {text}"
        );
        assert!(
            text.contains("text/event-stream"),
            "SSE content type: {text}"
        );
        assert!(text.contains("exocortex"), "initial anchor comment");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn backend_node_threads_a_non_default_org_through_its_runtime() {
    let onto = Arc::new(
        exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()]).unwrap(),
    );
    let storage = Arc::new(InMemoryStorage::new(onto.clone()));
    let mut memory = acceptance_memory(88, false);
    memory.context.tenant_id = Some("acme".into());
    storage.upsert_memory(&memory).await.unwrap();

    let mut config = args("127.0.0.1:0", 41008, vec![]);
    config.org = "acme".into();
    config.principals = Arc::new(
        exocortex_server::principal::PrincipalRegistry::single_with_audit_admin(
            "test-only-acme-bearer-token-00000000".into(),
            exocortex_ops::operations::ops_vc("acme", "reader", exocortex_kernel::Visibility::Org),
            true,
        )
        .unwrap(),
    );
    let node = run_backend_node(storage, onto, config)
        .await
        .expect("a configured non-default organization boots");
    let acme =
        exocortex_ops::operations::ops_vc("acme", "reader", exocortex_kernel::Visibility::Org);
    assert!(node.cache.get_memory("acme", &memory.id, &acme).is_some());
    assert!(node.cache.get_memory("org", &memory.id, &acme).is_none());
    let (status, _) = http_get(
        node.local_addr,
        "/v1/audit?since_lsn=0",
        Some("test-only-acme-bearer-token-00000000"),
    )
    .await;
    assert_eq!(
        status, 200,
        "the configured org principal reaches operations"
    );
}

fn acceptance_memory(seed: u8, embedding: bool) -> exocortex_kernel::Memory {
    use exocortex_kernel::{Memory, MemoryContext, MemoryId, Provenance, Visibility, LSN};
    Memory {
        id: MemoryId([seed; 16]),
        memory_type: 3,
        title: format!("cluster-{seed}").into(),
        content: format!("cluster acceptance {seed}"),
        summary: None,
        tags: Default::default(),
        visibility: Visibility::Org,
        provenance: Provenance::Asserted {
            author: "acceptance".into(),
            producer_kind: None,
        },
        context: MemoryContext {
            timestamp: chrono::Utc::now(),
            project_id: Some("p".into()),
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
        embedding: embedding.then(|| acceptance_embedding(vec![1.0; 64])),
        lsn: LSN::new_local(0),
    }
}

fn acceptance_embedding(vector: Vec<f32>) -> exocortex_kernel::Embedding {
    exocortex_kernel::Embedding {
        model: exocortex_kernel::EmbeddingModel {
            name: "fake-deterministic".into(),
            version: "v1".into(),
        },
        vector,
    }
}

/// §23 #15 direct three-node acceptance: measured cache convergence, actual
/// leader-task loss and takeover, one consolidation owner, and stale fencing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_p95_handoff_no_duplicate_and_stale_fence() {
    let onto = Arc::new(
        exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()]).unwrap(),
    );
    let storage = Arc::new(InMemoryStorage::new(onto.clone()));
    let mut nodes = Vec::new();
    for offset in 0..3u16 {
        nodes.push(
            run_backend_node(
                storage.clone(),
                onto.clone(),
                args("127.0.0.1:0", 44001 + offset, vec![]),
            )
            .await
            .unwrap(),
        );
    }

    tokio::time::timeout(Duration::from_secs(2), async {
        while nodes
            .iter()
            .filter(|node| node.leader_gate.load(std::sync::atomic::Ordering::SeqCst))
            .count()
            != 1
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("exactly one initial owner");

    let visibility =
        exocortex_ops::operations::ops_vc("org", "test", exocortex_kernel::Visibility::Org);
    let mut samples = Vec::new();
    for seed in 20..52u8 {
        let memory = acceptance_memory(seed, false);
        let started = tokio::time::Instant::now();
        let commit = storage.upsert_memory(&memory).await.unwrap();
        tokio::time::timeout(Duration::from_millis(500), async {
            loop {
                let ready = nodes.iter().all(|node| {
                    node.health.load().sync_lsn >= commit.lsn
                        && node
                            .cache
                            .get_memory("org", &memory.id, &visibility)
                            .is_some()
                });
                if ready {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("all three node-local caches converge within 500ms");
        samples.push(started.elapsed());
    }
    samples.sort_unstable();
    let p95 = samples[(samples.len() * 95).div_ceil(100) - 1];
    assert!(p95 < Duration::from_millis(500), "p95={p95:?}");

    let old_owner = nodes
        .iter()
        .position(|node| node.leader_gate.load(std::sync::atomic::Ordering::SeqCst))
        .unwrap();
    let old_epoch = nodes[old_owner].health.load().lease_epoch;
    nodes[old_owner].stop_leader_election();
    let new_owner = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let owners: Vec<_> = nodes
                .iter()
                .enumerate()
                .filter(|(_, node)| node.leader_gate.load(std::sync::atomic::Ordering::SeqCst))
                .map(|(index, _)| index)
                .collect();
            if owners.len() == 1
                && owners[0] != old_owner
                && nodes[owners[0]].health.load().lease_epoch > old_epoch
            {
                break owners[0];
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("a surviving node takes ownership after leader loss");
    assert!(nodes[new_owner].health.load().lease_epoch > old_epoch);

    storage
        .upsert_memory(&acceptance_memory(70, true))
        .await
        .unwrap();
    storage
        .upsert_memory(&acceptance_memory(71, true))
        .await
        .unwrap();
    let region = exocortex_storage::RegionKey {
        org: "org".into(),
        project: "p".into(),
        memory_type: 3,
    };
    let engines: Vec<_> = nodes
        .iter()
        .enumerate()
        .map(|(index, node)| {
            exocortex_dreams::DreamsEngine::new(
                storage.clone(),
                exocortex_dreams::trigger::DreamsTrigger::default(),
                0.01,
                0.05,
                false,
                format!("acceptance-{index}").into(),
            )
            .with_leader_gate(node.leader_gate.clone())
        })
        .collect();
    let (a, b, c) = tokio::join!(
        engines[0].try_consolidate(&region),
        engines[1].try_consolidate(&region),
        engines[2].try_consolidate(&region),
    );
    let outcomes = [a, b, c];
    let successes: Vec<_> = outcomes.into_iter().filter_map(Result::ok).collect();
    assert_eq!(successes.len(), 1, "only the elected owner consolidates");
    assert!(
        !successes[0].merged.is_empty(),
        "the elected owner performs a real consolidation"
    );
    let merged: std::collections::HashSet<_> = successes[0].merged.iter().collect();
    assert_eq!(
        merged.len(),
        successes[0].merged.len(),
        "no duplicate consolidation"
    );

    let key = exocortex_storage::LeaseKey::Dreams {
        org: "org".into(),
        region: "stale-proof".into(),
    };
    let stale = storage.acquire_lease(&key, Duration::ZERO).await.unwrap();
    let current = storage
        .acquire_lease(&key, Duration::from_secs(1))
        .await
        .unwrap();
    assert!(current.epoch > stale.epoch);
    let rejected = storage
        .upsert_batch_fenced(&[acceptance_memory(90, false)], &[], &stale)
        .await;
    assert!(matches!(
        rejected,
        Err(exocortex_storage::StorageError::FencedWriteRejected { .. })
    ));
}

fn acceptance_relationship(
    from: exocortex_kernel::MemoryId,
    to: exocortex_kernel::MemoryId,
) -> exocortex_kernel::Relationship {
    use exocortex_kernel::{Provenance, RelKindId, Relationship, RelationshipId};
    let kind = RelKindId(5);
    Relationship {
        id: RelationshipId::derive(from, kind, to, None),
        kind,
        from,
        to,
        visibility: exocortex_kernel::Visibility::Org,
        provenance: Provenance::Asserted {
            author: "lifecycle-acceptance".into(),
            producer_kind: None,
        },
        properties: exocortex_kernel::RelationshipProperties {
            strength: 0.5,
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
    }
}

/// §23 #20: one event burst enters the production owner-gated run loop on a
/// three-node backend and yields consolidation, prune, discovery, and reset.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_owner_runs_full_event_driven_dreams_lifecycle() {
    let onto = Arc::new(
        exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()]).unwrap(),
    );
    let storage = Arc::new(InMemoryStorage::new(onto.clone()));
    let mut nodes = Vec::new();
    for offset in 0..3u16 {
        nodes.push(
            run_backend_node(
                storage.clone(),
                onto.clone(),
                args("127.0.0.1:0", 44101 + offset, vec![]),
            )
            .await
            .unwrap(),
        );
    }
    let owner = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let owners: Vec<_> = nodes
                .iter()
                .enumerate()
                .filter(|(_, node)| node.leader_gate.load(std::sync::atomic::Ordering::SeqCst))
                .map(|(index, _)| index)
                .collect();
            if owners.len() == 1 {
                break owners[0];
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("exactly one Dreams owner is ready");

    let mut duplicate_a = acceptance_memory(100, true);
    let mut duplicate_b = acceptance_memory(101, true);
    duplicate_a.embedding = Some(acceptance_embedding(vec![1.0; 64]));
    duplicate_b.embedding = Some(acceptance_embedding(vec![1.0; 64]));
    let mut chain_a = acceptance_memory(102, true);
    let mut chain_b = acceptance_memory(103, true);
    let mut chain_c = acceptance_memory(104, true);
    chain_a.embedding = Some(acceptance_embedding({
        let mut value = vec![0.0; 64];
        value[0] = 1.0;
        value
    }));
    chain_b.embedding = Some(acceptance_embedding({
        let mut value = vec![0.0; 64];
        value[1] = 1.0;
        value
    }));
    chain_c.embedding = Some(acceptance_embedding({
        let mut value = vec![0.0; 64];
        value[2] = 1.0;
        value
    }));
    let mut already_closed = acceptance_memory(105, false);
    already_closed.valid_until = Some(chrono::Utc::now());
    storage
        .upsert_batch(
            &[
                duplicate_a,
                duplicate_b,
                chain_a.clone(),
                chain_b.clone(),
                chain_c.clone(),
                already_closed.clone(),
            ],
            &[
                acceptance_relationship(chain_a.id, chain_b.id),
                acceptance_relationship(chain_b.id, chain_c.id),
            ],
        )
        .await
        .unwrap();

    let region = exocortex_storage::RegionKey {
        org: "org".into(),
        project: "p".into(),
        memory_type: 3,
    };
    nodes[owner].dreams.last_cycle_at.insert(
        region.clone(),
        chrono::Utc::now() - chrono::Duration::hours(7),
    );
    for _ in 0..1000 {
        nodes[owner].dreams.on_write(region.clone()).await.unwrap();
    }

    let result = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let result = nodes[owner].dreams.last_result.read().await.clone();
            let discovered = nodes[owner]
                .dreams
                .pending_discoveries()
                .iter()
                .any(|proposal| proposal.endpoints == (chain_a.id, chain_c.id));
            let reset = nodes[owner]
                .dreams
                .counters
                .get(&region)
                .is_some_and(|counter| *counter == Default::default());
            if let Some(result) = result.filter(|_| discovered && reset) {
                break result;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("owner completes the full event-driven lifecycle and resets counters");

    assert_eq!(result.owner_node_id, format!("node-{}", 44101 + owner));
    assert!(!result.merged.is_empty(), "cycle consolidates duplicates");
    assert!(
        result
            .pruned
            .iter()
            .any(|(id, reason)| *id == already_closed.id
                && *reason == exocortex_dreams::PruneReason::Redundant),
        "cycle observes the closed row in its prune result"
    );
    for (index, node) in nodes.iter().enumerate() {
        if index != owner {
            assert!(
                node.dreams.last_result.read().await.is_none(),
                "followers never execute the owner-only cycle"
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn chitchat_state_carries_wire_version_and_fingerprint() {
    use chitchat::transport::UdpTransport;
    use chitchat::{ChitchatConfig, ChitchatId, FailureDetectorConfig};

    let onto =
        exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()]).unwrap();
    let fp_hex: String = {
        use std::fmt::Write as _;
        let mut out = String::with_capacity(64);
        for b in onto.fingerprint.0 {
            let _ = write!(out, "{b:02x}");
        }
        out
    };
    let port = 42001u16;
    let config = ChitchatConfig {
        chitchat_id: ChitchatId::new(
            "gossip-check".into(),
            1,
            format!("127.0.0.1:{port}").parse().unwrap(),
        ),
        cluster_id: "exocortex".into(),
        gossip_interval: Duration::from_millis(200),
        listen_addr: format!("127.0.0.1:{port}").parse().unwrap(),
        seed_nodes: vec![],
        failure_detector_config: FailureDetectorConfig::default(),
        marked_for_deletion_grace_period: Duration::from_secs(10),
        catchup_callback: None,
        extra_liveness_predicate: None,
    };
    let handle = chitchat::spawn_chitchat(
        config,
        vec![
            (
                "wire_version".into(),
                exocortex_wire::WIRE_VERSION.to_string(),
            ),
            ("ontology_fingerprint".into(), fp_hex),
        ],
        &UdpTransport,
    )
    .await
    .expect("gossip spawns");

    let state = handle.with_chitchat(|c| c.state_snapshot()).await;
    let self_state = state.node_states.first().expect("self node state");
    let kv = |k: &str| self_state.get(k).expect(k).to_string();
    assert_eq!(kv("wire_version"), "1");
    assert_eq!(kv("ontology_fingerprint").len(), 64);
    handle.shutdown().await.ok();
}

#[tokio::test(flavor = "multi_thread")]
async fn shared_listener_uses_tls_and_refuses_plaintext() {
    use exocortex_wire::ingest::v1::ingest_service_client::IngestServiceClient;
    use exocortex_wire::ingest::v1::FingerprintRequest;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tonic::transport::{Certificate, ClientTlsConfig, Endpoint};

    let onto = Arc::new(
        exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()]).unwrap(),
    );
    let storage = Arc::new(InMemoryStorage::new(onto.clone()));
    let mut tls_args = args("127.0.0.1:0", 43001, vec![]);
    tls_args.transport = exocortex_server::backend::TransportSecurity::Tls {
        certificate: "tests/fixtures/localhost-cert.pem".into(),
        private_key: "tests/fixtures/localhost-key.pem".into(),
    };
    let node = run_backend_node(storage, onto, tls_args)
        .await
        .expect("valid TLS listener boots");

    let ca = Certificate::from_pem(include_bytes!("fixtures/localhost-cert.pem"));
    let endpoint = Endpoint::from_shared(format!("https://localhost:{}", node.local_addr.port()))
        .unwrap()
        .tls_config(
            ClientTlsConfig::new()
                .ca_certificate(ca)
                .domain_name("localhost"),
        )
        .unwrap();
    let channel = endpoint.connect().await.expect("trusted TLS handshake");
    let mut client = IngestServiceClient::new(channel);
    let unauthenticated = client
        .fingerprint(FingerprintRequest {})
        .await
        .expect_err("gRPC cannot bypass bearer principal middleware");
    assert_eq!(unauthenticated.code(), tonic::Code::Unauthenticated);

    let mut request = tonic::Request::new(FingerprintRequest {});
    request.metadata_mut().insert(
        "authorization",
        "Bearer test-only-backend-bearer-token-00000000"
            .parse()
            .unwrap(),
    );
    let response = client
        .fingerprint(request)
        .await
        .expect("gRPC shares the TLS listener");
    assert_eq!(response.into_inner().fingerprint.len(), 32);

    let mut plaintext = tokio::net::TcpStream::connect(node.local_addr)
        .await
        .unwrap();
    plaintext
        .write_all(b"GET /health/ready HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut bytes = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(1), plaintext.read_to_end(&mut bytes)).await;
    assert!(
        !bytes.starts_with(b"HTTP/"),
        "TLS listener must not emit any plaintext HTTP response, including an auth rejection: {bytes:?}"
    );
}

#[tokio::test]
async fn malformed_tls_material_fails_before_listener_startup() {
    let onto = Arc::new(
        exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()]).unwrap(),
    );
    let storage = Arc::new(InMemoryStorage::new(onto.clone()));
    let mut bad = args("127.0.0.1:0", 43002, vec![]);
    bad.transport = exocortex_server::backend::TransportSecurity::Tls {
        certificate: "tests/fixtures/localhost-cert.pem".into(),
        private_key: "tests/fixtures/localhost-cert.pem".into(),
    };
    assert!(run_backend_node(storage, onto, bad).await.is_err());
}

#[tokio::test]
async fn plaintext_transport_rejects_non_loopback_library_bind() {
    let onto = Arc::new(
        exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()]).unwrap(),
    );
    let storage = Arc::new(InMemoryStorage::new(onto.clone()));
    let shared = args("0.0.0.0:0", 43003, vec![]);
    let error = match run_backend_node(storage, onto, shared).await {
        Ok(_) => panic!("library callers cannot bypass the loopback restriction"),
        Err(error) => error,
    };
    assert!(
        error.to_string().contains("restricted to loopback"),
        "unexpected startup error: {error:#}"
    );
}

/// R-O4: readiness is observational — when the storage probe fails and the
/// lease loop goes stale, `/health/ready` answers a minimal 503; healthy
/// maintainers restore 200.
#[tokio::test(flavor = "multi_thread")]
async fn health_ready_reflects_maintainer_truth() {
    let onto = std::sync::Arc::new(
        exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()]).unwrap(),
    );
    let storage = std::sync::Arc::new(InMemoryStorage::new(onto.clone()));
    let node = run_backend_node(storage, onto, args("127.0.0.1:0", 0, vec![]))
        .await
        .unwrap();

    // Healthy: maintainers (probe + lease loop) report green.
    let mut ok = false;
    for _ in 0..50 {
        let (status, body) = http_get(node.local_addr, "/health/ready", None).await;
        if status == 200 && body.contains("\"ready\"") {
            ok = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(ok, "ready turns 200 once maintainers report green");

    // Simulate maintainer failure: storage probe fails + lease tick stale.
    node.health.rcu(|h| {
        let mut next = h.as_ref().clone();
        next.storage_ok = false;
        next.last_lease_tick = Some(chrono::Utc::now() - chrono::Duration::seconds(60));
        std::sync::Arc::new(next)
    });
    let (status, body) = http_get(node.local_addr, "/health/ready", None).await;
    assert_eq!(status, 503, "unhealthy node must not answer ready");
    assert!(
        body.contains("\"not-ready\"") && !body.contains("storage_ok"),
        "public probe is minimal: {body}"
    );

    // A dead/reconnecting invalidation feed is independently not ready even
    // when storage and the lease maintainer remain healthy.
    node.health.rcu(|h| {
        let mut next = h.as_ref().clone();
        next.storage_ok = true;
        next.last_lease_tick = Some(chrono::Utc::now());
        next.cluster_feed_ready = false;
        std::sync::Arc::new(next)
    });
    let (status, body) = http_get(node.local_addr, "/health/ready", None).await;
    assert_eq!(status, 503, "dead invalidation feed must fail readiness");
    assert!(body.contains("\"not-ready\""));
}

#[tokio::test(flavor = "multi_thread")]
async fn production_startup_subscribes_before_authoritative_cache_image() {
    let onto = std::sync::Arc::new(
        exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()]).unwrap(),
    );
    let storage = std::sync::Arc::new(InMemoryStorage::new(onto.clone()));
    let (subscription_captured, release_subscription) =
        storage.pause_next_invalidation_subscription();
    let (snapshot_captured, release_snapshot) = storage.pause_next_memory_stream_after_snapshot();
    let starting = tokio::spawn(run_backend_node(
        storage.clone(),
        onto,
        args("127.0.0.1:0", 0, vec![]),
    ));

    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        subscription_captured.notified(),
    )
    .await
    .expect("production cache bridge captures its receiver before reseeding");
    release_subscription.notify_one();
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        snapshot_captured.notified(),
    )
    .await
    .expect("authoritative memory image is captured after subscription");
    let committed_during_startup = acceptance_memory(96, false);
    storage
        .upsert_memory(&committed_during_startup)
        .await
        .unwrap();
    release_snapshot.notify_one();

    let node = tokio::time::timeout(std::time::Duration::from_secs(5), starting)
        .await
        .expect("backend startup completes")
        .expect("startup task remains live")
        .expect("backend starts");
    let visibility =
        exocortex_ops::operations::ops_vc("org", "reader", exocortex_kernel::Visibility::Org);
    assert!(
        node.cache
            .get_memory("org", &committed_during_startup.id, &visibility)
            .is_some(),
        "a commit in the snapshot/subscription window is present at startup"
    );
}
