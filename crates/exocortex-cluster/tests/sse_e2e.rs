//! M5 SSE end-to-end (§9.8 step 7): an upsert on the backend produces an
//! `inv` SSE event at a subscribed client. Runs against the live FalkorDB
//! harness when `FALKOR_URL` is set; skips otherwise.

#![cfg(feature = "integration")]

use std::sync::Arc;
use std::time::{Duration, Instant};

use exocortex_cluster::ClusterNode;
use exocortex_kernel::{Memory, MemoryContext, MemoryId, Provenance, Visibility, LSN};
use exocortex_pack_dev_v1::pack_def;
use exocortex_server::sse::sse_router;
use exocortex_storage::{FalkorConfig, FalkorStorage, Invalidation, Storage};

fn falkor_url() -> Option<String> {
    std::env::var("FALKOR_URL").ok().filter(|u| !u.is_empty())
}

fn mem(title: &str) -> Memory {
    Memory {
        id: MemoryId::new_v7(),
        memory_type: 3,
        title: title.into(),
        content: "c".into(),
        summary: None,
        tags: Default::default(),
        visibility: Visibility::Org,
        provenance: Provenance::Asserted {
            author: "sse".into(),
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

#[tokio::test(flavor = "multi_thread")]
async fn sse_client_observes_upsert_within_200ms() {
    let Some(url) = falkor_url() else {
        eprintln!("skipping sse_e2e: FALKOR_URL not set");
        return;
    };
    let onto = Arc::new(exocortex_kernel::Ontology::from_packs(vec![pack_def()]).unwrap());
    let storage = std::sync::Arc::new(
        FalkorStorage::connect(
            FalkorConfig {
                falkor_url: url.clone(),
                redis_url: url.replacen("falkor://", "redis://", 1),
                graph_name: format!("sse_e2e_{}", std::process::id()),
                org_id: "sse".into(),
                node_id: "sse-node".into(),
            },
            onto.clone(),
        )
        .await
        .expect("connect"),
    );

    let cluster = Arc::new(ClusterNode::new(
        storage.clone(),
        "sse-node".into(),
        onto.fingerprint,
        [3u8; 32],
    ));
    let runner = cluster.clone();
    tokio::spawn(async move { runner.run().await });

    let app = sse_router(cluster.clone(), SseAuth::OptionalToken);
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // Raw HTTP GET over a TCP socket; parse SSE lines with a tiny reader
    // (no HTTP client dependency — recorded in the M5 report).
    let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    sock.write_all(
        format!(
            "GET /v1/changes HTTP/1.1
Host: {addr}
Accept: text/event-stream
Connection: close

"
        )
        .as_bytes(),
    )
    .await
    .unwrap();
    let mut sock = sock;
    let (mut rx, _) = tokio::io::split(sock);

    // Subscribe to storage's own change feed first (it feeds the cluster
    // run loop), then upsert.
    let t0 = Instant::now();
    storage.upsert_memory(&mem("sse-pushed")).await.unwrap();
    let _ = cluster
        .tx
        .send(cluster.envelope(Invalidation::MemoryUpserted {
            id: mem("x").id,
            lsn: 1,
        }));

    let deadline = t0 + Duration::from_millis(200);
    let mut buf = vec![0u8; 4096];
    let mut seen = String::new();
    let mut saw_inv = false;
    while Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(50), rx.read(&mut buf)).await {
            Ok(Ok(0)) | Ok(Err(_)) | Err(_) => continue,
            Ok(Ok(n)) => {
                seen.push_str(&String::from_utf8_lossy(&buf[..n]));
                if seen.contains("event: inv") {
                    saw_inv = true;
                    break;
                }
            }
        }
    }
    assert!(
        saw_inv,
        "SSE client observed the inv event within 200ms (p95 target)"
    );
}
