//! Cross-node coherence over the live harness (§9.1 + R-C6): two nodes on
//! the same FalkorDB/Redis — node A commits, node B's cluster loop (Redis
//! pub-sub) surfaces the invalidation on its LOCAL SSE hub, and a
//! reconnecting subscriber replays the buffered window from B. Requires
//! `FALKOR_URL` (the docker-compose harness); skips otherwise.

#![cfg(feature = "integration")]

use std::sync::Arc;
use std::time::Duration;

use exocortex_cluster::ClusterNode;
use exocortex_kernel::{Memory, MemoryContext, MemoryId, Provenance, Visibility, LSN};
use exocortex_pack_dev_v1::pack_def;
use exocortex_storage::{FalkorConfig, FalkorStorage, Storage};
use futures::StreamExt;

fn falkor_url() -> Option<String> {
    std::env::var("FALKOR_URL").ok().filter(|u| !u.is_empty())
}

async fn node(node_id: &str, graph: &str) -> FalkorStorage {
    let url = falkor_url().expect("FALKOR_URL set (checked by the gate)");
    let redis = url.replacen("falkor://", "redis://", 1);
    let onto = Arc::new(exocortex_kernel::Ontology::from_packs(vec![pack_def()]).unwrap());
    FalkorStorage::connect(
        FalkorConfig {
            falkor_url: url,
            redis_url: redis,
            graph_name: graph.into(),
            org_id: "crossnode".into(),
            node_id: node_id.into(),
        },
        onto,
    )
    .await
    .expect("connect")
}

fn mem(title: &str, n: u8) -> Memory {
    Memory {
        id: MemoryId([n; 16]),
        memory_type: 3,
        title: title.into(),
        content: "c".into(),
        summary: None,
        tags: Default::default(),
        visibility: Visibility::Org,
        provenance: Provenance::Asserted {
            author: "cross".into(),
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

/// Node A commits; node B's local hub carries it (Redis pub-sub, §9.1);
/// an SSE subscriber on B observes the envelope within a bounded wait.
#[tokio::test(flavor = "multi_thread")]
async fn cross_node_commit_reaches_peer_hub() {
    if falkor_url().is_none() {
        eprintln!("skipping cross_node_commit_reaches_peer_hub: FALKOR_URL not set");
        return;
    }
    let graph = format!("crossnode_a_{}", std::process::id());
    let storage_a = Arc::new(node("node-a", &graph).await);
    let storage_b = Arc::new(node("node-b", &graph).await);

    let onto = Arc::new(exocortex_kernel::Ontology::from_packs(vec![pack_def()]).unwrap());
    let cluster_b = Arc::new(ClusterNode::new(
        storage_b.clone(),
        "node-b".into(),
        onto.fingerprint,
        [9u8; 32],
    ));
    {
        let runner = cluster_b.clone();
        tokio::spawn(async move { runner.run().await });
    }
    let mut subscriber = cluster_b.subscribe_local();

    // Node A commits two rows; B never touches storage itself.
    let c1 = storage_a.upsert_memory(&mem("via-a-1", 1)).await.unwrap();
    let c2 = storage_a.upsert_memory(&mem("via-a-2", 2)).await.unwrap();

    // B's hub delivers both envelopes (bounded: pub-sub is async).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut seen = Vec::new();
    while seen.len() < 2 && tokio::time::Instant::now() < deadline {
        if let Ok(Ok(env)) =
            tokio::time::timeout(Duration::from_millis(500), subscriber.recv()).await
        {
            assert!(
                cluster_b.admit(&env).is_ok(),
                "peer admission verifies A's signature"
            );
            seen.push(env);
        }
    }
    assert_eq!(seen.len(), 2, "both of A's commits reached B's hub");
    let lsns: Vec<_> = seen
        .iter()
        .map(|e| e.inv.as_ref().map(|i| i.backend_lsn).unwrap_or(0))
        .collect();
    assert!(lsns.contains(&c1.lsn) && lsns.contains(&c2.lsn));
}

/// R-C6 on the live path: after the commits above-shaped traffic, a
/// subscriber connecting to B with `?since_lsn=` receives the buffered
/// window replay — the ring is fed by the real storage pub-sub loop.
#[tokio::test(flavor = "multi_thread")]
async fn cross_node_replay_window_serves_reconnects() {
    use exocortex_server::sse::{sse_router, SseAuth};
    if falkor_url().is_none() {
        eprintln!("skipping cross_node_replay_window_serves_reconnects: FALKOR_URL not set");
        return;
    }
    let graph = format!("crossnode_b_{}", std::process::id());
    let storage_a = Arc::new(node("node-a", &graph).await);
    let storage_b = Arc::new(node("node-b", &graph).await);

    let onto = Arc::new(exocortex_kernel::Ontology::from_packs(vec![pack_def()]).unwrap());
    let cluster_b = Arc::new(ClusterNode::new(
        storage_b.clone(),
        "node-b".into(),
        onto.fingerprint,
        [9u8; 32],
    ));
    {
        let runner = cluster_b.clone();
        tokio::spawn(async move { runner.run().await });
    }
    // Wait for B's subscription to be live before committing (pub-sub has
    // no replay; early commits would only hit the ring via the hub).
    tokio::time::sleep(Duration::from_millis(500)).await;

    let c1 = storage_a.upsert_memory(&mem("replay-1", 1)).await.unwrap();
    let c2 = storage_a.upsert_memory(&mem("replay-2", 2)).await.unwrap();
    // Let B's loop ingest both into hub + replay ring.
    tokio::time::sleep(Duration::from_millis(800)).await;

    // Reconnect from before both: the ring replays 2 events, in LSN order.
    let replay = match cluster_b.replay_since(c1.lsn - 1) {
        exocortex_cluster::Replay::Fresh(envs) => envs,
        exocortex_cluster::Replay::TooOld => {
            panic!("live ring must bridge a 2-event window")
        }
    };
    assert!(
        replay.len() >= 2,
        "both commits buffered in B's replay ring: {:?}",
        replay.len()
    );
    let lsns: Vec<_> = replay
        .iter()
        .map(|e| e.inv.as_ref().map(|i| i.backend_lsn).unwrap_or(0))
        .collect();
    assert!(lsns.windows(2).all(|w| w[0] <= w[1]), "LSN order");
    assert!(lsns.contains(&c1.lsn) && lsns.contains(&c2.lsn));

    // And the router serves the same window over SSE.
    let app = sse_router(cluster_b.clone(), SseAuth::OptionalToken);
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    sock.write_all(
        format!(
            "GET /v1/changes?since_lsn={} HTTP/1.1\r\nHost: {addr}\r\nAccept: text/event-stream\r\nConnection: close\r\n\r\n",
            c1.lsn - 1
        )
        .as_bytes(),
    )
    .await
    .unwrap();
    let mut buf = Vec::new();
    let mut chunk = [0u8; 8192];
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(400), sock.read(&mut chunk)).await {
            Ok(Ok(0)) => break,
            Ok(Ok(n)) => buf.extend_from_slice(&chunk[..n]),
            _ => {}
        }
    }
    let text = String::from_utf8_lossy(&buf);
    assert!(text.starts_with("HTTP/1.1 200"), "replay connect: {text}");
    assert!(
        text.matches("event: inv").count() >= 2,
        "replayed events over SSE: {text}"
    );
}
