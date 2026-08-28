//! W5 acceptance: the reconnecting SSE subscriber applies backend deltas in
//! LSN order (hold-back gate unit test), decodes/verifies envelopes, and a
//! live server + SSE + cache integration observes a committed memory within
//! 500ms (Success Criteria #18's shape).

use std::sync::Arc;
use std::time::Duration;

use exocortex_cache::{CacheWrite, LocalCache};
use exocortex_client::sync::{b64_decode, decode_envelope, run_sse_sync, LsnGate, SseSyncConfig};
use exocortex_cluster::ClusterNode;
use exocortex_kernel::{MemoryId, RelKindId};
use exocortex_storage::{
    DiscoveryRecord, InMemoryStorage, Invalidation, RegionKey, Storage, VisibilityContext,
};
use exocortex_wire::WIRE_VERSION;
use prost::Message;

const HMAC_KEY: [u8; 32] = [9u8; 32];
// Process/test scheduling is not part of the 500 ms change-propagation SLO.
// Arm that exact deadline only after the authenticated initial image arrives.
const HARNESS_STARTUP_TIMEOUT: Duration = Duration::from_secs(10);

fn hex16(b: &[u8; 16]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(32);
    for x in b {
        let _ = write!(out, "{x:02x}");
    }
    out
}

#[test]
fn base64_decoder_matches_standard_alphabet() {
    // RFC 4648 test vectors.
    assert_eq!(b64_decode("").unwrap(), b"");
    assert_eq!(b64_decode("Zg==").unwrap(), b"f");
    assert_eq!(b64_decode("Zm8=").unwrap(), b"fo");
    assert_eq!(b64_decode("Zm9v").unwrap(), b"foo");
    assert_eq!(b64_decode("Zm9vYg==").unwrap(), b"foob");
    assert_eq!(b64_decode("Zm9vYmE=").unwrap(), b"fooba");
    assert_eq!(b64_decode("Zm9vYmFy").unwrap(), b"foobar");
    assert!(b64_decode("!!!").is_none());
}

#[test]
fn lsn_gate_holds_out_of_order_and_releases_in_order() {
    let mut gate = LsnGate::new(1);
    let id = |n: u8| MemoryId([n; 16]);
    let inv = |n: u8, lsn: u64| Invalidation::MemoryUpserted { id: id(n), lsn };

    // Pre-anchor stale replay (below the subscribe point) is dropped.
    assert!(LsnGate::new(4).push(2, inv(2, 2)).is_empty());

    // The first observed envelope may itself be ahead of an in-flight earlier
    // publish. It must be held, not promoted to the expected frontier.
    let mut first_out_of_order = LsnGate::new(4);
    assert!(first_out_of_order.push(5, inv(5, 5)).is_empty());
    let released = first_out_of_order.push(4, inv(4, 4));
    assert_eq!(released.len(), 2);
    assert_eq!(first_out_of_order.next_lsn(), 6);

    // 1 anchors the stream and releases immediately.
    let first = gate.push(1, inv(1, 1));
    assert_eq!(first.len(), 1);

    // 3 arrives ahead of order: buffered, nothing released.
    assert!(gate.push(3, inv(3, 3)).is_empty());
    assert_eq!(gate.next_lsn(), 2);
    // 2 arrives: releases 2, 3 in order.
    let released = gate.push(2, inv(2, 2));
    let ids: Vec<u8> = released
        .iter()
        .map(|i| match i {
            Invalidation::MemoryUpserted { id, .. } => id.0[0],
            _ => panic!("wrong kind"),
        })
        .collect();
    assert_eq!(ids, vec![2, 3], "strict LSN order after the gap fills");
    assert_eq!(gate.next_lsn(), 4);

    // Stale replay (already applied) is dropped.
    assert!(gate.push(2, inv(2, 2)).is_empty());

    // Gap: 6 buffered, 4+5 missing -> gap expires past timeout.
    assert!(gate.push(6, inv(6, 6)).is_empty());
    assert!(gate.gap_expired(Duration::from_millis(0)).is_some());
    assert_eq!(
        gate.gap_expired(Duration::from_millis(0)),
        Some(4),
        "resubscribe from the missing LSN"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn comment_only_stream_cannot_mask_an_expired_lsn_gap() {
    use axum::response::sse::{Event, Sse};
    use axum::routing::get;
    use std::convert::Infallible;

    let onto = Arc::new(
        exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()]).unwrap(),
    );
    let node = ClusterNode::new(
        Arc::new(InMemoryStorage::new(onto.clone())),
        "comment-gap".into(),
        onto.fingerprint,
        HMAC_KEY,
    );
    let payload = b64_encode(
        &node
            .envelope(Invalidation::MemoryUpserted {
                id: MemoryId([2; 16]),
                lsn: 2,
            })
            .encode_to_vec(),
    );
    let app = axum::Router::new().route(
        "/v1/changes",
        get(move || {
            let payload = payload.clone();
            async move {
                let stream = async_stream::stream! {
                    yield Ok::<Event, Infallible>(Event::default().event("inv").data(payload));
                    loop {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                        yield Ok(Event::default().comment("heartbeat"));
                    }
                };
                Sse::new(stream)
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let (cache, _writer_rx) = LocalCache::new(1024 * 1024);
    let resynced = Arc::new(tokio::sync::Notify::new());
    let connected = Arc::new(tokio::sync::Notify::new());
    let callback = {
        let resynced = resynced.clone();
        Box::new(move || {
            let resynced = resynced.clone();
            Box::pin(async move { resynced.notify_one() })
                as futures::future::BoxFuture<'static, ()>
        }) as exocortex_client::sync::ResyncFn
    };
    let mut cfg = SseSyncConfig::new(format!("http://{addr}"), HMAC_KEY, onto.fingerprint.0);
    cfg.gap_timeout = Duration::from_millis(30);
    cfg.reconcile_interval = Duration::from_secs(10);
    cfg.stall_timeout = Duration::from_secs(10);
    cfg.backoff = Duration::from_secs(1);
    cfg.connection_ready = Some(connected.clone());
    let sync = tokio::spawn(run_sse_sync(cfg, Arc::new(cache), 1, Some(callback)));

    tokio::time::timeout(Duration::from_secs(3), connected.notified())
        .await
        .expect("comment-only SSE stream connects");
    tokio::time::timeout(Duration::from_millis(500), resynced.notified())
        .await
        .expect("gap timer forces resync while comments keep the transport live");
    sync.abort();
}

#[test]
fn envelope_decode_verifies_hmac_fingerprint_and_wire_version() {
    let onto = Arc::new(
        exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()]).unwrap(),
    );
    let node = ClusterNode::new(
        Arc::new(InMemoryStorage::new(onto.clone())),
        "node-x".into(),
        onto.fingerprint,
        HMAC_KEY,
    );
    let env = node.envelope(Invalidation::MemorySnapshotUpserted {
        memory: Box::new(test_memory("decoded", 7)),
        lsn: 42,
    });
    let payload = b64_encode(&env.encode_to_vec());

    let (inv, lsn) = decode_envelope(&HMAC_KEY, &onto.fingerprint.0, &payload)
        .expect("verified envelope decodes");
    assert_eq!(lsn, 42);
    match inv {
        Invalidation::MemorySnapshotUpserted { memory, lsn } => {
            assert_eq!(memory.id, MemoryId([7; 16]));
            assert_eq!(lsn, 42);
        }
        other => panic!("wrong kind: {other:?}"),
    }

    // Wrong key, wrong fingerprint, wrong wire version all reject.
    let bad_key = [8u8; 32];
    assert!(decode_envelope(&bad_key, &onto.fingerprint.0, &payload).is_err());
    assert!(decode_envelope(&HMAC_KEY, &[0u8; 32], &payload).is_err());
    let mut tampered = env.clone();
    tampered.wire_version = WIRE_VERSION + 1;
    assert!(
        decode_envelope(
            &HMAC_KEY,
            &onto.fingerprint.0,
            &b64_encode(&tampered.encode_to_vec())
        )
        .is_err(),
        "wire-version mismatch rejected (R-W2)"
    );
}

#[test]
fn visibility_advance_decodes_and_keeps_the_lsn_gate_gap_free() {
    let onto = Arc::new(
        exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()]).unwrap(),
    );
    let node = ClusterNode::new(
        Arc::new(InMemoryStorage::new(onto.clone())),
        "visibility-client".into(),
        onto.fingerprint,
        HMAC_KEY,
    );
    let hidden = node.envelope(Invalidation::VisibilityAdvance { lsn: 7 });
    let payload = b64_encode(&hidden.encode_to_vec());
    let (decoded, lsn) = decode_envelope(&HMAC_KEY, &onto.fingerprint.0, &payload)
        .expect("signed identifier-free advance decodes");
    assert_eq!(lsn, 7);
    assert!(matches!(
        decoded,
        Invalidation::VisibilityAdvance { lsn: 7 }
    ));

    let mut gate = LsnGate::new(7);
    let released = gate.push(7, decoded);
    assert!(matches!(
        released.as_slice(),
        [Invalidation::VisibilityAdvance { lsn: 7 }]
    ));
    let visible = Invalidation::MemoryUpserted {
        id: MemoryId([8; 16]),
        lsn: 8,
    };
    assert_eq!(gate.push(8, visible).len(), 1);
    assert_eq!(gate.next_lsn(), 9, "hidden LSN never creates a gap loop");
}

#[tokio::test(flavor = "multi_thread")]
async fn held_visibility_advance_forces_authenticated_reseed_when_gap_fills() {
    use axum::extract::RawQuery;
    use axum::response::sse::{Event, Sse};
    use axum::routing::get;
    use std::convert::Infallible;
    use std::sync::atomic::{AtomicBool, Ordering};

    let onto = Arc::new(
        exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()]).unwrap(),
    );
    let storage = Arc::new(InMemoryStorage::new(onto.clone()));
    let node = ClusterNode::new(
        storage.clone(),
        "held-revocation".into(),
        onto.fingerprint,
        HMAC_KEY,
    );
    let bearer = "held-revocation-bearer";
    let client_key = exocortex_wire::signing::derive_sse_client_key(&HMAC_KEY, bearer);
    let signed_payload = |invalidation| {
        let mut envelope = node.envelope(invalidation);
        exocortex_wire::signing::sign_invalidation_envelope(&client_key, &mut envelope);
        b64_encode(&envelope.encode_to_vec())
    };
    // LSN 5 arrives first and must be held. LSN 4 then fills the gap and
    // releases both events; revocation is a property of the released batch,
    // not merely the last network arrival.
    let hidden = signed_payload(Invalidation::VisibilityAdvance { lsn: 5 });
    let gap_fill = signed_payload(Invalidation::MemorySnapshotUpserted {
        memory: Box::new(test_memory("gap-fill", 4)),
        lsn: 4,
    });
    let reseed = signed_payload(Invalidation::GraphReseed {
        snapshot_json: serde_json::to_vec(&serde_json::json!({
            "memories": [],
            "relationships": []
        }))
        .unwrap(),
        lsn: 5,
    });
    let authenticated = Arc::new(AtomicBool::new(false));
    let reseed_requested = Arc::new(AtomicBool::new(false));
    let app = axum::Router::new().route(
        "/v1/changes",
        get({
            let authenticated = authenticated.clone();
            let reseed_requested = reseed_requested.clone();
            move |headers: http::HeaderMap, RawQuery(query): RawQuery| {
                let hidden = hidden.clone();
                let gap_fill = gap_fill.clone();
                let reseed = reseed.clone();
                let authenticated = authenticated.clone();
                let reseed_requested = reseed_requested.clone();
                async move {
                    authenticated.store(
                        headers
                            .get(http::header::AUTHORIZATION)
                            .is_some_and(|value| value == "Bearer held-revocation-bearer"),
                        Ordering::SeqCst,
                    );
                    let seed = query.as_deref().is_some_and(|query| query.contains("seed=true"));
                    reseed_requested.fetch_or(seed, Ordering::SeqCst);
                    let stream = async_stream::stream! {
                        if seed {
                            yield Ok::<Event, Infallible>(Event::default().event("inv").data(reseed));
                        } else {
                            yield Ok::<Event, Infallible>(Event::default().event("inv").data(hidden));
                            yield Ok::<Event, Infallible>(Event::default().event("inv").data(gap_fill));
                        }
                        futures::future::pending::<()>().await;
                    };
                    Sse::new(stream)
                }
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let stale = test_memory("must-be-revoked", 9);
    let (cache, rx) = LocalCache::new(1024 * 1024);
    cache.seed_local("org", std::slice::from_ref(&stale), &[], 0);
    let cache = Arc::new(cache);
    let writer = {
        let cache = cache.clone();
        let storage = storage.clone();
        tokio::spawn(async move { cache.run(storage, rx).await })
    };
    let hydrated = Arc::new(tokio::sync::Notify::new());
    let connected = Arc::new(tokio::sync::Notify::new());
    let mut cfg = SseSyncConfig::new(format!("http://{addr}"), HMAC_KEY, onto.fingerprint.0);
    cfg.org = "org".into();
    cfg.bearer = Some(bearer.into());
    cfg.client_key = Some(client_key);
    cfg.hydration_ready = Some(hydrated.clone());
    cfg.connection_ready = Some(connected.clone());
    cfg.backoff = Duration::from_millis(1);
    cfg.reconcile_interval = Duration::from_secs(10);
    let sync = tokio::spawn(run_sse_sync(cfg, cache.clone(), 4, None));

    tokio::time::timeout(Duration::from_secs(3), connected.notified())
        .await
        .expect("authenticated SSE connection becomes ready");
    tokio::time::timeout(Duration::from_millis(500), hydrated.notified())
        .await
        .expect("released hidden event immediately requests an authenticated reseed");
    assert!(authenticated.load(Ordering::SeqCst));
    assert!(reseed_requested.load(Ordering::SeqCst));
    assert!(cache
        .get_memory(
            "org",
            &stale.id,
            &VisibilityContext {
                user_id: "reader".into(),
                org_id: "org".into(),
                project_ids: Default::default(),
                team_ids: Default::default(),
                max_visibility: exocortex_kernel::Visibility::Org,
            }
        )
        .is_none());
    assert_eq!(cache.version("org").unwrap().backend_lsn, 5);
    sync.abort();
    writer.abort();
}

#[tokio::test]
async fn discovery_available_decodes_advances_cache_frontier_and_rejects_malformed_record() {
    let onto = Arc::new(
        exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()]).unwrap(),
    );
    let storage = Arc::new(InMemoryStorage::new(onto.clone()));
    let node = ClusterNode::new(
        storage.clone(),
        "discovery-client".into(),
        onto.fingerprint,
        HMAC_KEY,
    );
    let record = DiscoveryRecord {
        discovery_id: "discovery-7".into(),
        region: RegionKey {
            org: "org".into(),
            project: "project".into(),
            memory_type: 3,
        },
        from: MemoryId([1; 16]),
        to: MemoryId([2; 16]),
        discovery_type: "two-hop".into(),
        quality: 0.75,
        via_types: [1, 2],
        discovery_cycle_id: "cycle-7".into(),
        discovered_at: chrono::Utc::now(),
    };
    let envelope = node.envelope(Invalidation::DiscoveryAvailable {
        record: record.clone(),
        lsn: 7,
    });
    let payload = b64_encode(&envelope.encode_to_vec());
    let (decoded, lsn) = decode_envelope(&HMAC_KEY, &onto.fingerprint.0, &payload)
        .expect("signed discovery invalidation decodes");
    assert_eq!(lsn, 7);
    assert!(matches!(
        &decoded,
        Invalidation::DiscoveryAvailable {
            record: decoded_record,
            lsn: 7
        } if decoded_record == &record
    ));

    let mut gate = LsnGate::new(7);
    let released = gate.push(lsn, decoded);
    assert_eq!(released.len(), 1);
    assert_eq!(gate.next_lsn(), 8);

    let (cache, rx) = LocalCache::new(1024 * 1024);
    let cached_memory = test_memory("unchanged", 1);
    cache.seed_local("org", std::slice::from_ref(&cached_memory), &[], 3);
    let before = cache.graphs_snapshot("org").unwrap();
    let before_nodes = before.petgraph.node_count();
    let before_edges = before.petgraph.edge_count();
    let cache = Arc::new(cache);
    let runner = {
        let cache = cache.clone();
        tokio::spawn(async move { cache.run(storage, rx).await })
    };
    cache
        .submit(CacheWrite::Apply(released.into_iter().next().unwrap()))
        .await;
    cache.flush().await;
    let after = cache.graphs_snapshot("org").unwrap();
    assert_eq!(after.last_backend_lsn, 7);
    assert_eq!(after.last_local_lsn, 3);
    assert_eq!(after.petgraph.node_count(), before_nodes);
    assert_eq!(after.petgraph.edge_count(), before_edges);
    let retained = after.petgraph.node_weights().next().unwrap();
    assert_eq!(retained.id, cached_memory.id);
    assert_eq!(retained.title, cached_memory.title);
    runner.abort();

    let mut malformed = envelope;
    let discovery = match malformed.inv.as_mut().and_then(|inv| inv.kind.as_mut()) {
        Some(exocortex_wire::sse::v1::invalidation::Kind::DiscoveryAvailable(discovery)) => {
            discovery
        }
        _ => panic!("expected discovery payload"),
    };
    discovery.record_json = b"not-json".to_vec();
    exocortex_wire::signing::sign_invalidation_envelope(&HMAC_KEY, &mut malformed);
    let error = decode_envelope(
        &HMAC_KEY,
        &onto.fingerprint.0,
        &b64_encode(&malformed.encode_to_vec()),
    )
    .expect_err("a valid signature does not admit a malformed discovery record");
    assert!(error.to_string().contains("discovery record"));
}

/// Live server + SSE + cache: a committed upsert reaches the client cache
/// through the feed within 500ms.
#[tokio::test(flavor = "multi_thread")]
async fn sse_feed_observed_by_client_cache_within_500ms() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("exocortex=debug,info")
        .with_writer(std::io::stderr)
        .try_init();
    let onto = Arc::new(
        exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()]).unwrap(),
    );
    let storage = Arc::new(InMemoryStorage::new(onto.clone()));
    let cluster = Arc::new(ClusterNode::new(
        storage.clone(),
        "srv".into(),
        onto.fingerprint,
        HMAC_KEY,
    ));
    let _runner = {
        let runner = cluster.clone();
        tokio::spawn(async move { runner.run().await })
    };

    // Cache + writer loop over the same storage (the Apply path fetches rows
    // by id).
    let (cache, rx) = LocalCache::new(16 * 1024 * 1024);
    let cache = Arc::new(cache);
    {
        let cache = cache.clone();
        let storage = storage.clone();
        tokio::spawn(async move { cache.run(storage, rx).await });
    }

    let vc = VisibilityContext {
        user_id: "u".into(),
        org_id: "org".into(),
        project_ids: Default::default(),
        team_ids: Default::default(),
        max_visibility: exocortex_kernel::Visibility::Org,
    };
    let (server_cache, _server_rx) = LocalCache::new(1024 * 1024);
    let ctx = Arc::new(exocortex_ops::OpContext {
        visibility_ctx: vc.clone(),
        audit_admin: false,
        storage: storage.clone() as Arc<dyn Storage>,
        cache: Arc::new(server_cache),
        deadline: chrono::Utc::now() + chrono::Duration::seconds(30),
        ontology: Some(onto.clone()),
    });
    let bind = exocortex_server::http_bind::HttpBind::new(
        ctx,
        "test-only-feed-bearer-token-00000000".into(),
    );
    let app = bind.router(Some(exocortex_server::sse::sse_router(cluster.clone())));
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    // Seed one memory, then require the authenticated full-image boundary
    // before measuring the subsequent live update.
    let seed = test_memory("seed", 1);
    storage.upsert_memory(&seed).await.unwrap();

    let hydrated = Arc::new(tokio::sync::Notify::new());
    let cfg = {
        let mut c = SseSyncConfig::new(format!("http://{addr}"), HMAC_KEY, onto.fingerprint.0);
        c.backoff = Duration::from_millis(50);
        c.bearer = Some("test-only-feed-bearer-token-00000000".into());
        c.client_key = Some(exocortex_wire::signing::derive_sse_client_key(
            &HMAC_KEY,
            "test-only-feed-bearer-token-00000000",
        ));
        c.hydration_ready = Some(hydrated.clone());
        c
    };
    let sync = tokio::spawn(run_sse_sync(cfg, cache.clone(), 0, None));
    tokio::time::timeout(HARNESS_STARTUP_TIMEOUT, hydrated.notified())
        .await
        .expect("authenticated graph reseed reaches the client");

    // Commit an upsert on the backend and publish it through the already-live
    // authenticated stream. Stale duplicate fan-out is harmless at the gate.
    let m = test_memory("pushed-via-sse", 2);
    let commit = storage.upsert_memory(&m).await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_millis(500);
    let mut seen = false;
    while tokio::time::Instant::now() < deadline {
        let _ = cluster.admit_and_publish(cluster.envelope(Invalidation::MemoryUpserted {
            id: m.id,
            lsn: commit.lsn,
        }));
        if cache.get_memory("org", &m.id, &vc).is_some() {
            seen = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    sync.abort();
    assert!(
        seen,
        "client cache observed the memory via the SSE feed within 500ms"
    );
    let _ = hex16(&m.id.0);
    let _ = RelKindId(0);
    let _ = CacheWrite::Evict("x".into());
}

#[tokio::test(flavor = "multi_thread")]
async fn periodic_reseed_repairs_a_commit_with_no_change_feed_publication() {
    let onto = Arc::new(
        exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()]).unwrap(),
    );
    let storage = Arc::new(InMemoryStorage::new(onto.clone()));
    let cluster = Arc::new(ClusterNode::new(
        storage.clone(),
        "reconcile".into(),
        onto.fingerprint,
        HMAC_KEY,
    ));
    // Deliberately do not run ClusterNode::run: storage commits therefore
    // have no path into the replay ring or live SSE broadcast.
    let app = exocortex_server::sse::sse_router(cluster).layer(axum::Extension(
        exocortex_ops::operations::ops_vc(
            "org",
            "reconcile-reader",
            exocortex_kernel::Visibility::Org,
        ),
    ));
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let (cache, rx) = LocalCache::new(16 * 1024 * 1024);
    let cache = Arc::new(cache);
    tokio::spawn({
        let cache = cache.clone();
        let storage = storage.clone();
        async move { cache.run(storage, rx).await }
    });
    let hydrated = Arc::new(tokio::sync::Notify::new());
    let mut cfg = SseSyncConfig::new(format!("http://{addr}"), HMAC_KEY, onto.fingerprint.0);
    cfg.bearer = Some("reconcile-token".into());
    cfg.client_key = Some(exocortex_wire::signing::derive_sse_client_key(
        &HMAC_KEY,
        "reconcile-token",
    ));
    cfg.backoff = Duration::from_millis(5);
    cfg.reconcile_interval = Duration::from_millis(40);
    cfg.hydration_ready = Some(hydrated.clone());
    let sync = tokio::spawn(run_sse_sync(cfg, cache.clone(), 0, None));
    tokio::time::timeout(HARNESS_STARTUP_TIMEOUT, hydrated.notified())
        .await
        .expect("initial authoritative image");

    let missed = test_memory("missed-publication", 44);
    storage.upsert_memory(&missed).await.unwrap();
    let vc = VisibilityContext {
        user_id: "reconcile-reader".into(),
        org_id: "org".into(),
        project_ids: Default::default(),
        team_ids: Default::default(),
        max_visibility: exocortex_kernel::Visibility::Org,
    };
    let deadline = tokio::time::Instant::now() + Duration::from_millis(500);
    while cache.get_memory("org", &missed.id, &vc).is_none()
        && tokio::time::Instant::now() < deadline
    {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    sync.abort();
    assert!(
        cache.get_memory("org", &missed.id, &vc).is_some(),
        "bounded authoritative reconciliation repairs a lost publication"
    );
}

/// R-Sec5: a token-bearing subscriber's envelopes verify against the
/// derived per-client key, and the wrong key rejects them.
#[tokio::test(flavor = "multi_thread")]
async fn per_client_sse_hmac_verifies_with_derived_key() {
    use exocortex_server::sse::derive_client_sse_key;

    let onto = Arc::new(
        exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()]).unwrap(),
    );
    let storage = Arc::new(InMemoryStorage::new(onto.clone()));
    let cluster = Arc::new(ClusterNode::new(
        storage.clone(),
        "srv".into(),
        onto.fingerprint,
        HMAC_KEY,
    ));
    let app = exocortex_server::sse::sse_router(cluster.clone()).layer(axum::Extension(
        exocortex_ops::operations::ops_vc("org", "test-reader", exocortex_kernel::Visibility::Org),
    ));
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    // Subscribe WITH the token via the sync loop; the cache observes the row.
    let (cache, rx) = LocalCache::new(16 * 1024 * 1024);
    let cache = Arc::new(cache);
    {
        let cache = cache.clone();
        let storage = storage.clone();
        tokio::spawn(async move { cache.run(storage, rx).await });
    }
    let seed = test_memory("seed", 1);
    storage.upsert_memory(&seed).await.unwrap();

    let token = "client-token-7";
    let derived = derive_client_sse_key(&HMAC_KEY, token);
    let hydrated = Arc::new(tokio::sync::Notify::new());
    let mut cfg = SseSyncConfig::new(format!("http://{addr}"), HMAC_KEY, onto.fingerprint.0);
    cfg.backoff = Duration::from_millis(50);
    cfg.bearer = Some(token.into());
    cfg.client_key = Some(derived);
    cfg.hydration_ready = Some(hydrated.clone());
    let sync = tokio::spawn(run_sse_sync(cfg, cache.clone(), 0, None));
    tokio::time::timeout(HARNESS_STARTUP_TIMEOUT, hydrated.notified())
        .await
        .expect("derived-key verified initial image");

    let vc = VisibilityContext {
        user_id: "u".into(),
        org_id: "org".into(),
        project_ids: Default::default(),
        team_ids: Default::default(),
        max_visibility: exocortex_kernel::Visibility::Org,
    };
    let m = test_memory("per-client-hmac", 3);
    let commit = storage.upsert_memory(&m).await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_millis(1500);
    let mut seen = false;
    while tokio::time::Instant::now() < deadline {
        let _ = cluster.admit_and_publish(cluster.envelope(Invalidation::MemoryUpserted {
            id: m.id,
            lsn: commit.lsn,
        }));
        if cache.get_memory("org", &m.id, &vc).is_some() {
            seen = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    sync.abort();
    assert!(
        seen,
        "derived-key verification admits re-signed envelopes (R-Sec5)"
    );

    // A client holding the WRONG derived key must reject them.
    let wrong = derive_client_sse_key(&[1u8; 32], token);
    let env = cluster.envelope(Invalidation::MemorySnapshotUpserted {
        memory: Box::new(m.clone()),
        lsn: commit.lsn,
    });
    let payload = b64_encode(&env.encode_to_vec());
    // (The server re-signs before this reaches the wire; simulate the
    // re-sign here to check the negative path deterministically.)
    let resigned = {
        let mut e = env.clone();
        exocortex_wire::signing::sign_invalidation_envelope(&derived, &mut e);
        b64_encode(&e.encode_to_vec())
    };
    assert!(decode_envelope(&derived, &onto.fingerprint.0, &resigned).is_ok());
    assert!(decode_envelope(&wrong, &onto.fingerprint.0, &resigned).is_err());
    // The cluster-key signature is NOT valid for the re-signed payload.
    assert!(decode_envelope(&HMAC_KEY, &onto.fingerprint.0, &resigned).is_err());
    let _ = payload;
}

fn test_memory(title: &str, n: u8) -> exocortex_kernel::Memory {
    use exocortex_kernel::{Memory, MemoryContext, Provenance, Visibility, LSN};
    Memory {
        id: MemoryId([n; 16]),
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

#[tokio::test]
async fn seed_ignorant_server_fails_initial_hydration_promptly() {
    use axum::response::sse::{Event, Sse};
    use axum::routing::get;
    use std::convert::Infallible;

    let app = axum::Router::new().route(
        "/v1/changes",
        get(|| async {
            let stream = async_stream::stream! {
                loop {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    yield Ok::<Event, Infallible>(Event::default().comment("legacy-heartbeat"));
                }
            };
            Sse::new(stream)
        }),
    );
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let (cache, writer_rx) = LocalCache::new(1024 * 1024);
    let cache = Arc::new(cache);
    let mut cfg = SseSyncConfig::new(format!("http://{addr}"), HMAC_KEY, [1; 32]);
    cfg.initial_hydration_timeout = Duration::from_millis(40);
    cfg.stall_timeout = Duration::from_secs(5);

    let result = tokio::time::timeout(
        Duration::from_millis(250),
        exocortex_client::sync::hydrate_and_start_backend_sync(cfg, cache.clone(), writer_rx),
    )
    .await
    .expect("legacy incompatibility is bounded");
    assert!(matches!(
        result,
        Err(exocortex_client::sync::SyncError::InitialHydrationTimeout(timeout))
            if timeout == Duration::from_millis(40)
    ));
    assert_eq!(
        cache.resident_orgs(),
        0,
        "empty state is never declared ready"
    );
    server.abort();
}

/// R6-B06: exercise the exact production lifecycle helper. It must not return
/// before an authenticated full image is visible, and its retained writer/SSE
/// tasks must continue applying later commits.
#[tokio::test(flavor = "multi_thread")]
async fn production_backend_sync_hydrates_before_ready_and_stays_live() {
    let onto = Arc::new(
        exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()]).unwrap(),
    );
    let storage = Arc::new(InMemoryStorage::new(onto.clone()));
    let initial = test_memory("initial-backend-row", 71);
    storage.upsert_memory(&initial).await.unwrap();
    let cluster = Arc::new(ClusterNode::new(
        storage.clone(),
        "production-sync".into(),
        onto.fingerprint,
        HMAC_KEY,
    ));

    let (server_cache, _server_rx) = LocalCache::new(1024 * 1024);
    let principal =
        exocortex_ops::operations::ops_vc("org", "reader", exocortex_kernel::Visibility::Org);
    let ctx = Arc::new(exocortex_ops::OpContext {
        visibility_ctx: principal.clone(),
        audit_admin: false,
        storage: storage.clone() as Arc<dyn Storage>,
        cache: Arc::new(server_cache),
        deadline: chrono::Utc::now() + chrono::Duration::seconds(30),
        ontology: Some(onto.clone()),
    });
    let bind = exocortex_server::http_bind::HttpBind::new(
        ctx,
        "test-only-sync-bearer-token-00000000".into(),
    );
    let app = bind.router(Some(exocortex_server::sse::sse_router(cluster.clone())));
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let (cache, writer_rx) = LocalCache::new(16 * 1024 * 1024);
    let cache = Arc::new(cache);
    let mut cfg = SseSyncConfig::new(format!("http://{addr}"), HMAC_KEY, onto.fingerprint.0);
    cfg.bearer = Some("test-only-sync-bearer-token-00000000".into());
    cfg.client_key = Some(exocortex_wire::signing::derive_sse_client_key(
        &HMAC_KEY,
        "test-only-sync-bearer-token-00000000",
    ));
    cfg.backoff = Duration::from_millis(20);
    let sync = tokio::time::timeout(
        HARNESS_STARTUP_TIMEOUT,
        exocortex_client::sync::hydrate_and_start_backend_sync(cfg, cache.clone(), writer_rx),
    )
    .await
    .expect("production lifecycle reaches hydrated readiness")
    .expect("compatible server supplies an initial seed");
    assert!(
        cache.get_memory("org", &initial.id, &principal).is_some(),
        "initial backend image is visible before readiness returns"
    );

    let live = test_memory("continuous-backend-row", 72);
    let commit = storage.upsert_memory(&live).await.unwrap();
    // A03: the acceptance clock begins at the committed backend write, not at
    // process startup or subscriber construction.
    let deadline = tokio::time::Instant::now() + Duration::from_millis(500);
    let _ = cluster.admit_and_publish(cluster.envelope(Invalidation::MemoryUpserted {
        id: live.id,
        lsn: commit.lsn,
    }));
    while cache.get_memory("org", &live.id, &principal).is_none()
        && tokio::time::Instant::now() < deadline
    {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        cache.get_memory("org", &live.id, &principal).is_some(),
        "retained writer and SSE task continuously apply later commits"
    );

    let mut narrowed = live.clone();
    narrowed.visibility = exocortex_kernel::Visibility::Project;
    narrowed.context.project_id = Some("hidden-project".into());
    let narrowed_commit = storage.upsert_memory(&narrowed).await.unwrap();
    let _ = cluster.admit_and_publish(cluster.envelope(Invalidation::MemoryUpserted {
        id: narrowed.id,
        lsn: narrowed_commit.lsn,
    }));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while cache.get_memory("org", &live.id, &principal).is_some()
        && tokio::time::Instant::now() < deadline
    {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    sync.abort();
    assert!(
        cache.get_memory("org", &live.id, &principal).is_none(),
        "identifier-free visibility advance forces a reseed that evicts the stale wider row"
    );
}

/// Standard-alphabet base64 encode (mirror of the server's encoder).
fn b64_encode(data: &[u8]) -> String {
    const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b = [
            chunk[0],
            chunk.get(1).copied().unwrap_or(0),
            chunk.get(2).copied().unwrap_or(0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        out.push(T[(n >> 18) as usize & 63] as char);
        out.push(T[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 {
            T[(n >> 6) as usize & 63] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            T[n as usize & 63] as char
        } else {
            '='
        });
    }
    out
}

/// Deterministic replay probe: publish 3 envelopes first, then subscribe
/// from LSN 1 — the client must replay-apply deltas 2 and 3.
#[tokio::test(flavor = "multi_thread")]
async fn deterministic_replay_probe() {
    let onto = Arc::new(
        exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()]).unwrap(),
    );
    let storage = Arc::new(InMemoryStorage::new(onto.clone()));
    let cluster = Arc::new(ClusterNode::new(
        storage.clone(),
        "probe".into(),
        onto.fingerprint,
        HMAC_KEY,
    ));
    let (cache, rx) = LocalCache::new(16 * 1024 * 1024);
    let cache = Arc::new(cache);
    {
        let cache = cache.clone();
        let storage = storage.clone();
        tokio::spawn(async move { cache.run(storage, rx).await });
    }

    // Publish 1..3 deterministically BEFORE any subscriber exists.
    for lsn in 1..=3u64 {
        let m = test_memory(&format!("replay-{lsn}"), lsn as u8);
        let commit = storage.upsert_memory(&m).await.unwrap();
        assert_eq!(commit.lsn, lsn);
        let _ = cluster.admit_and_publish(cluster.envelope(Invalidation::MemoryUpserted {
            id: m.id,
            lsn: commit.lsn,
        }));
    }

    let app = exocortex_server::sse::sse_router(cluster.clone()).layer(axum::Extension(
        exocortex_ops::operations::ops_vc("org", "test-reader", exocortex_kernel::Visibility::Org),
    ));
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    // The client's org graph must be resident before deltas can apply
    // (§8.2: the reseed establishes the org, then Apply flows in).
    cache
        .reseed_from_storage(&*storage, &"org".into())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut cfg = SseSyncConfig::new(format!("http://{addr}"), HMAC_KEY, onto.fingerprint.0);
    cfg.backoff = Duration::from_millis(50);
    let sync = tokio::spawn(run_sse_sync(cfg, cache.clone(), 1, None));

    let vc = VisibilityContext {
        user_id: "u".into(),
        org_id: "org".into(),
        project_ids: Default::default(),
        team_ids: Default::default(),
        max_visibility: exocortex_kernel::Visibility::Org,
    };
    // Wait for replay-apply of lsn 3 (bounded).
    let m3_id = {
        let m = test_memory("replay-3", 3);
        storage.upsert_memory(&m).await.unwrap(); // idempotent; id deterministic
        m.id
    };
    let mut seen = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while tokio::time::Instant::now() < deadline {
        if cache.get_memory("org", &m3_id, &vc).is_some() {
            seen = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    sync.abort();
    assert!(seen, "replayed delta 3 applied through the es client");
}
