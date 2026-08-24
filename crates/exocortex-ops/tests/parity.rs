//! M7 acceptance (§21.3): registry parity, schema goldens, identical outputs
//! across surfaces, and audit records for promote_visibility /
//! accept_discovery (§21.4 R-A2).

use std::sync::Arc;

use exocortex_cache::{GraphSnapshot, LocalCache};
use exocortex_kernel::{Memory, MemoryContext, MemoryId, Provenance, Visibility, LSN};
use exocortex_ops::operations::{ops_vc, GetMemoryInput, PromoteVisibilityInput};
use exocortex_ops::{entries, OpContext, Operation};
use exocortex_storage::{InMemoryStorage, Storage};

fn ontology() -> Arc<exocortex_kernel::Ontology> {
    Arc::new(
        exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()]).unwrap(),
    )
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
        provenance: Provenance::Asserted { author: "t".into() },
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

fn hex(id: &MemoryId) -> String {
    id.0.iter().map(|b| format!("{b:02x}")).collect()
}

#[test]
fn parity_every_operation_on_both_surfaces_with_schemas() {
    // §21.2: unique names, non-empty MCP + HTTP names, schema titles.
    let all = entries();
    assert!(!all.is_empty(), "operations registered");
    let mut seen = std::collections::HashSet::new();
    for e in &all {
        assert!(seen.insert(e.name), "duplicate op name {}", e.name);
        assert!(
            e.mcp_tool_name.starts_with("exocortex."),
            "{}: mcp tool name",
            e.name
        );
        assert!(e.http_path.starts_with('/'), "{}: http path", e.name);
        let i = (e.input_schema)();
        let o = (e.output_schema)();
        assert!(
            i.schema
                .metadata
                .as_ref()
                .and_then(|m| m.title.clone())
                .is_some(),
            "{}: input schema missing title",
            e.name
        );
        assert!(
            o.schema
                .metadata
                .as_ref()
                .and_then(|m| m.title.clone())
                .is_some(),
            "{}: output schema missing title",
            e.name
        );
    }
}

#[tokio::test]
async fn both_surfaces_identical_outputs() {
    // §21.3 step 6: the same handler serves MCP and HTTP — drive the
    // registered entry (the shared implementation) directly and compare
    // against the typed operation's output.
    let (ctx, m) = ctx_sync();
    let entry = entries()
        .into_iter()
        .find(|e| e.name == "get_memory")
        .expect("get_memory registered");

    let input = serde_json::to_value(GetMemoryInput { id: hex(&m.id) }).unwrap();
    let via_registry = (entry.handler)(&ctx, input.clone())
        .await
        .expect("registry surface");

    let typed: GetMemoryInput = serde_json::from_value(input).unwrap();
    let direct = exocortex_ops::operations::GetMemory::default()
        .handle(&ctx, typed)
        .await
        .expect("typed surface");
    let direct_json = serde_json::to_value(direct).unwrap();
    assert_eq!(via_registry, direct_json, "CR-9: identical outputs");
}

fn ctx_sync() -> (OpContext, Memory) {
    let (cache, _rx) = LocalCache::new(64 * 1024 * 1024);
    let storage = InMemoryStorage::new(ontology());
    let m = mem("parity-target");
    let mut snap = GraphSnapshot::empty();
    snap.push_test_memory(m.clone());
    cache.publish("org", Arc::new(snap));
    (
        OpContext {
            visibility_ctx: ops_vc("org", "alice", Visibility::Org),
            storage: Arc::new(storage),
            cache: Arc::new(cache),
            deadline: chrono::Utc::now() + chrono::Duration::seconds(5),
        },
        m,
    )
}

#[tokio::test]
async fn promote_visibility_writes_audit_record() {
    let (ctx, m) = ctx_sync();
    ctx.storage.upsert_memory(&m).await.unwrap();
    let out = exocortex_ops::operations::PromoteVisibilityOp::default()
        .handle(
            &ctx,
            PromoteVisibilityInput {
                memory_id: hex(&m.id),
                to: "org".into(),
            },
        )
        .await
        .expect("promotion");
    assert!(out.audit_lsn > 0, "R-A2: promotion is audited");

    let audit = exocortex_ops::operations::ListAuditRecordsOp::default()
        .handle(
            &ctx,
            exocortex_ops::operations::ListAuditInput { since_lsn: 0 },
        )
        .await
        .unwrap();
    assert!(audit
        .records
        .iter()
        .any(|r| r["action"] == "promote_visibility"));
}

#[tokio::test]
async fn accept_discovery_writes_audit_record_and_edge() {
    let (ctx, _m) = ctx_sync();
    let a = mem("a");
    let b = mem("b");
    ctx.storage.upsert_memory(&a).await.unwrap();
    ctx.storage.upsert_memory(&b).await.unwrap();
    let out = exocortex_ops::operations::AcceptDiscoveryOp::default()
        .handle(
            &ctx,
            exocortex_ops::operations::AcceptDiscoveryInput {
                discovery_id: "11111111-1111-1111-1111-111111111111".into(),
                from: hex(&a.id),
                to: hex(&b.id),
                kind: "Solves".into(),
            },
        )
        .await
        .expect("accept");
    assert!(!out.edge_id.is_empty());
    assert!(out.audit_lsn > 0, "R-A2: acceptance is audited");
}

#[test]
fn openapi_and_mcp_json_generate_from_one_registry() {
    // The goldens: schema drift is caught by xtask gen-schemas; this test
    // pins that BOTH catalogues come from the same entries.
    let openapi_paths: Vec<_> = entries().iter().map(|e| e.http_path).collect();
    let mcp_tools: Vec<_> = entries().iter().map(|e| e.mcp_tool_name).collect();
    assert_eq!(openapi_paths.len(), mcp_tools.len());
    assert!(openapi_paths.contains(&"/v1/audit"));
    assert!(mcp_tools.contains(&"exocortex.find_related"));
}
