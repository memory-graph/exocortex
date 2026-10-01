//! D47: a degraded backend must surface on the read path. With the SSE
//! stream failing (store down), reads are served from a frozen snapshot —
//! silently before this fix. Now every read carries a `sync` health object
//! while degraded, healthy-path bytes stay registry-identical, and
//! `exocortex.backend_status` reports the diagnosis on demand.

use std::sync::Arc;

use exocortex_cache::LocalCache;
use exocortex_client::mcp::ExocortexMcp;
use exocortex_client::sync::StreamErrorCell;
use exocortex_storage::VisibilityContext;

fn server(cell: Option<StreamErrorCell>) -> ExocortexMcp {
    let onto = Arc::new(
        exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()]).unwrap(),
    );
    let (cache, _writer_rx) = LocalCache::new(1024 * 1024);
    let vc = VisibilityContext {
        user_id: "user".into(),
        org_id: "org".into(),
        project_ids: Default::default(),
        team_ids: Default::default(),
        max_visibility: exocortex_kernel::Visibility::Org,
    };
    let mut server = ExocortexMcp::new("org".into(), Arc::new(cache), vc, onto);
    if let Some(cell) = cell {
        server = server.with_sync_health(cell);
    }
    server
}

fn degraded_cell() -> StreamErrorCell {
    let cell = StreamErrorCell::default();
    *cell.lock().unwrap() = Some("SSE backend returned HTTP 503: storage unreachable".into());
    cell
}

#[tokio::test]
async fn degraded_reads_carry_the_sync_health_object() {
    let server = server(Some(degraded_cell()));
    let out = server
        .search_memories("anything".into(), None)
        .await
        .unwrap();
    let value: serde_json::Value = serde_json::from_str(&out).unwrap();
    let sync = value
        .get("sync")
        .expect("a degraded read must carry the sync health object");
    assert_eq!(sync["degraded"], serde_json::json!(true));
    assert!(
        sync["last_error"]
            .as_str()
            .expect("last_error string")
            .contains("503"),
        "the degradation note must name the failure, got: {out}"
    );

    let out = server
        .get_memory("deadbeefdeadbeefdeadbeefdeadbeef".into())
        .await
        .unwrap();
    assert!(
        out.contains("\"sync\"") && out.contains("503"),
        "get_memory must carry the degradation note too, got: {out}"
    );

    let out = server
        .find_related("deadbeefdeadbeefdeadbeefdeadbeef".into(), None)
        .await
        .unwrap();
    assert!(
        out.contains("\"sync\"") && out.contains("503"),
        "find_related must carry the degradation note too, got: {out}"
    );
}

#[tokio::test]
async fn healthy_reads_stay_registry_identical() {
    let server = server(Some(StreamErrorCell::default()));
    let out = server
        .search_memories("anything".into(), None)
        .await
        .unwrap();
    let value: serde_json::Value = serde_json::from_str(&out).unwrap();
    assert!(
        value.get("sync").is_none(),
        "a healthy stream must not stamp reads, got: {out}"
    );
}

#[tokio::test]
async fn backend_status_reports_mode_snapshot_and_failure() {
    let degraded = server(Some(degraded_cell()));
    let out = degraded.backend_status().await.unwrap();
    let value: serde_json::Value = serde_json::from_str(&out).unwrap();
    assert_eq!(value["mode"], serde_json::json!("backend"));
    assert_eq!(value["sync"]["degraded"], serde_json::json!(true));
    assert!(value["sync"]["last_error"]
        .as_str()
        .unwrap()
        .contains("503"));
    assert!(value["snapshot_version"].is_object());

    let standalone = server(None);
    let out = standalone.backend_status().await.unwrap();
    let value: serde_json::Value = serde_json::from_str(&out).unwrap();
    assert_eq!(value["mode"], serde_json::json!("standalone"));
    assert!(
        value.get("sync").is_none(),
        "standalone has no sync loop to report, got: {out}"
    );
}

#[tokio::test]
async fn recovery_clears_the_degradation_note() {
    let cell = degraded_cell();
    let server = server(Some(cell.clone()));
    *cell.lock().unwrap() = None;
    let out = server
        .search_memories("anything".into(), None)
        .await
        .unwrap();
    assert!(
        !out.contains("\"sync\""),
        "a recovered stream must read as live again, got: {out}"
    );
    let status = server.backend_status().await.unwrap();
    assert!(status.contains("\"degraded\":false"), "got: {status}");
}
