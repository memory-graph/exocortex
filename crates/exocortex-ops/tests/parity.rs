//! M7 acceptance (§21.3): registry parity, schema goldens, identical outputs
//! across surfaces, and audit records for promote_visibility /
//! accept_discovery (§21.4 R-A2).

use std::sync::Arc;

// PX2 hostile fixture: an action whose BODY attempts to stamp wider than
// its declared ceiling. The framework must refuse it no matter what the
// body produces ("the pack author cannot bypass it").
exocortex_kernel::pack! {
    name: "hostile-verbs-pack",
    version: "0.1.0",
    kernel_min: "1.0.0",
    memory_types! { Hazard }
    entity_types! { Nothing }
    kinds! {
        RelatedTo => bucket: Similarity, inverse: Self, bi: true, default_strength: 0.30,
    }
    type_triples! {
        RelatedTo => (_, _),
    }
    crepe_rules! {
    }
    actions! {
        StampTooWide(input: { note: String }, min_visibility: Project) = |_ctx, input| {
            use exocortex_kernel::{ActionProduct, Visibility};
            let mut product = ActionProduct::new();
            // The hostile stamp: Org under a Project ceiling, ignoring ctx.
            product.memory("h", 0, &input.note, &input.note, Visibility::Org, &[]);
            Ok(product)
        },
    }
}

use exocortex_cache::{GraphSnapshot, LocalCache};
use exocortex_kernel::{Memory, MemoryContext, MemoryId, Provenance, Visibility, LSN};
use exocortex_ops::operations::{ops_vc, GetMemoryInput, PromoteVisibilityInput};
use exocortex_ops::{entries, OpContext, Operation};
use exocortex_storage::{DiscoveryProposal, DiscoveryRecord, InMemoryStorage, RegionKey};

fn ontology() -> Arc<exocortex_kernel::Ontology> {
    Arc::new(
        exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()]).unwrap(),
    )
}

fn mem(title: &str) -> Memory {
    Memory {
        rights: None,
        id: MemoryId::new_v7(),
        memory_type: 3,
        title: title.into(),
        content: "c".into(),
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
    let via_registry = (entry.handler)(entry, &ctx, input.clone())
        .await
        .expect("registry surface");

    let typed: GetMemoryInput = serde_json::from_value(input).unwrap();
    let direct = exocortex_ops::operations::GetMemory
        .handle(&ctx, typed)
        .await
        .expect("typed surface");
    let direct_json = serde_json::to_value(direct).unwrap();
    assert_eq!(via_registry, direct_json, "CR-9: identical outputs");
}

fn ctx_sync() -> (OpContext, Memory) {
    let (ctx, memory, _) = ctx_with_storage();
    (ctx, memory)
}

fn ctx_with_storage() -> (OpContext, Memory, Arc<InMemoryStorage>) {
    let (cache, _rx) = LocalCache::new(64 * 1024 * 1024);
    let storage = Arc::new(InMemoryStorage::new(ontology()));
    let m = mem("parity-target");
    let mut snap = GraphSnapshot::empty();
    snap.push_test_memory(m.clone());
    cache.publish("org", Arc::new(snap));
    (
        OpContext {
            visibility_ctx: ops_vc("org", "alice", Visibility::Org),
            audit_admin: true,
            storage: storage.clone(),
            cache: Arc::new(cache),
            deadline: chrono::Utc::now() + chrono::Duration::seconds(5),

            ontology: None,
            ingest_preflight: None,
        },
        m,
        storage,
    )
}

#[tokio::test]
async fn promote_visibility_writes_audit_record() {
    let (ctx, m) = ctx_sync();
    ctx.storage.upsert_memory(&m).await.unwrap();
    let out = exocortex_ops::operations::PromoteVisibilityOp
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

    let audit = exocortex_ops::operations::ListAuditRecordsOp
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
    let onto = ontology();
    let mut a = mem("a");
    a.memory_type = 0; // Task (from-side is unconstrained for Causes)
    let mut b = mem("b");
    b.memory_type = 2; // Problem (to-side: Error | Problem)
    ctx.storage.upsert_memory(&a).await.unwrap();
    ctx.storage.upsert_memory(&b).await.unwrap();
    let causes = onto.kind_id("Causes").expect("Causes registered");
    let proposal = DiscoveryProposal {
        discovery_id: "11111111-1111-1111-1111-111111111111".into(),
        region: RegionKey {
            org: "org".into(),
            project: "*".into(),
            memory_type: a.memory_type,
        },
        from: a.id,
        to: b.id,
        kind: causes,
        proposed_visibility: Visibility::Project,
        caller_scope: ctx.visibility_ctx.clone(),
        issued_at: chrono::Utc::now(),
    };
    ctx.storage
        .store_discovery(&DiscoveryRecord {
            discovery_id: proposal.discovery_id.clone(),
            region: proposal.region.clone(),
            from: proposal.from,
            to: proposal.to,
            discovery_type: "transitive".into(),
            quality: 0.6,
            via_types: [1, 2],
            discovery_cycle_id: "acceptance-cycle".into(),
            discovered_at: proposal.issued_at,
        })
        .await
        .unwrap();
    ctx.storage
        .create_discovery_proposal(&proposal)
        .await
        .unwrap();
    let mut widened = proposal.clone();
    widened.discovery_id = "widened-proposal".into();
    widened.proposed_visibility = Visibility::Public;
    assert!(matches!(
        ctx.storage.create_discovery_proposal(&widened).await,
        Err(exocortex_storage::StorageError::ProposalMismatch)
    ));
    let mut endpoint_mismatch = proposal.clone();
    endpoint_mismatch.discovery_id = "endpoint-mismatch".into();
    ctx.storage
        .store_discovery(&DiscoveryRecord {
            discovery_id: endpoint_mismatch.discovery_id.clone(),
            region: endpoint_mismatch.region.clone(),
            from: endpoint_mismatch.from,
            to: endpoint_mismatch.to,
            discovery_type: "transitive".into(),
            quality: 0.6,
            via_types: [1, 2],
            discovery_cycle_id: "mismatch-cycle".into(),
            discovered_at: endpoint_mismatch.issued_at,
        })
        .await
        .unwrap();
    ctx.storage
        .create_discovery_proposal(&endpoint_mismatch)
        .await
        .unwrap();
    let mismatch = exocortex_ops::operations::AcceptDiscoveryOp
        .handle(
            &ctx,
            exocortex_ops::operations::AcceptDiscoveryInput {
                discovery_id: "endpoint-mismatch".into(),
                from: hex(&b.id),
                to: hex(&a.id),
                kind: "Causes".into(),
            },
        )
        .await;
    assert!(matches!(mismatch, Err(exocortex_ops::OpError::BadInput(_))));
    assert!(ctx
        .storage
        .get_discovery_proposal("endpoint-mismatch")
        .await
        .unwrap()
        .is_some());
    // IN3: the caller-supplied kind is resolved and validated (R-T17):
    // (Problem, Causes, Problem) is a legal triple; the committed edge
    // carries the RESOLVED kind, never RelKindId(0).
    let out = exocortex_ops::operations::AcceptDiscoveryOp
        .handle(
            &ctx,
            exocortex_ops::operations::AcceptDiscoveryInput {
                discovery_id: "11111111-1111-1111-1111-111111111111".into(),
                from: hex(&a.id),
                to: hex(&b.id),
                kind: "Causes".into(),
            },
        )
        .await
        .expect("accept");
    assert!(!out.edge_id.is_empty());
    assert!(out.audit_lsn > 0, "R-A2: acceptance is audited");
    use futures::StreamExt as _;
    let mut rs = ctx.storage.stream_all_relationships().await;
    let mut mine = None;
    while let Some(Ok(r)) = rs.next().await {
        if r.kind == causes {
            mine = Some(r);
        }
    }
    let committed = mine.expect("edge committed with the resolved kind");
    assert_eq!(
        committed.from, a.id,
        "IN3: the asserted edge, not its R-T4 inverse companion"
    );
    assert_eq!(
        committed.visibility,
        Visibility::Project,
        "acceptance may not widen beyond the proposal"
    );

    // An illegal triple is rejected (R-T17 now enforced here).
    let illegal = DiscoveryProposal {
        discovery_id: "22222222-2222-2222-2222-222222222222".into(),
        region: RegionKey {
            org: "org".into(),
            project: "*".into(),
            memory_type: a.memory_type,
        },
        from: a.id,
        to: b.id,
        kind: onto.kind_id("Solves").unwrap(),
        proposed_visibility: Visibility::Org,
        caller_scope: ctx.visibility_ctx.clone(),
        issued_at: chrono::Utc::now(),
    };
    ctx.storage
        .store_discovery(&DiscoveryRecord {
            discovery_id: illegal.discovery_id.clone(),
            region: illegal.region.clone(),
            from: illegal.from,
            to: illegal.to,
            discovery_type: "transitive".into(),
            quality: 0.6,
            via_types: [1, 2],
            discovery_cycle_id: "illegal-cycle".into(),
            discovered_at: illegal.issued_at,
        })
        .await
        .unwrap();
    ctx.storage
        .create_discovery_proposal(&illegal)
        .await
        .unwrap();
    let err = exocortex_ops::operations::AcceptDiscoveryOp
        .handle(
            &ctx,
            exocortex_ops::operations::AcceptDiscoveryInput {
                discovery_id: "22222222-2222-2222-2222-222222222222".into(),
                from: hex(&a.id),
                to: hex(&b.id),
                kind: "Solves".into(),
            },
        )
        .await
        .map(|o| o.audit_lsn)
        .expect_err("illegal triple rejected");
    assert!(matches!(err, exocortex_ops::OpError::BadInput(_)));

    let fabricated = exocortex_ops::operations::AcceptDiscoveryOp
        .handle(
            &ctx,
            exocortex_ops::operations::AcceptDiscoveryInput {
                discovery_id: "33333333-3333-3333-3333-333333333333".into(),
                from: hex(&a.id),
                to: hex(&b.id),
                kind: "Causes".into(),
            },
        )
        .await;
    assert!(matches!(fabricated, Err(exocortex_ops::OpError::NotFound)));
}

#[tokio::test]
async fn durable_discovery_survives_engine_restart_and_reaches_acceptance() {
    let (ctx, _m, storage) = ctx_with_storage();
    let mut from = mem("durable-from");
    from.memory_type = 0;
    let mut to = mem("durable-to");
    to.memory_type = 2;
    ctx.storage.upsert_memory(&from).await.unwrap();
    ctx.storage.upsert_memory(&to).await.unwrap();
    let id = "durable-discovery-after-restart";
    ctx.storage
        .store_discovery(&DiscoveryRecord {
            discovery_id: id.into(),
            region: RegionKey {
                org: "org".into(),
                project: "*".into(),
                memory_type: from.memory_type,
            },
            from: from.id,
            to: to.id,
            discovery_type: "transitive".into(),
            quality: 0.6,
            via_types: [1, 2],
            discovery_cycle_id: "finished-engine-cycle".into(),
            discovered_at: chrono::Utc::now(),
        })
        .await
        .unwrap();

    // No DreamsEngine handle participates below: storage is the restart-safe
    // handoff between the completed production cycle and the registry.
    storage.take_read_counts();
    let listed = exocortex_ops::operations::ListDiscoveriesOp
        .handle(
            &ctx,
            exocortex_ops::operations::ListDiscoveriesInput { limit: 20 },
        )
        .await
        .unwrap();
    assert_eq!(listed.discoveries.len(), 1);
    assert_eq!(listed.discoveries[0].quality, 0.6);
    assert_eq!(
        storage.take_read_counts(),
        (0, 1),
        "discovery endpoint visibility uses one batch read, never per-row N+1 reads"
    );

    let issued = exocortex_ops::operations::IssueDiscoveryOp
        .handle(
            &ctx,
            exocortex_ops::operations::IssueDiscoveryInput {
                discovery_id: id.into(),
                kind: "Causes".into(),
            },
        )
        .await
        .unwrap();
    assert_eq!(issued.discovery_id, id);
    assert_eq!(issued.visibility, "org");
    assert!(ctx.storage.get_discovery(id).await.unwrap().is_none());
    assert!(exocortex_ops::operations::ListDiscoveriesOp
        .handle(
            &ctx,
            exocortex_ops::operations::ListDiscoveriesInput { limit: 20 }
        )
        .await
        .unwrap()
        .discoveries
        .is_empty());

    let accepted = exocortex_ops::operations::AcceptDiscoveryOp
        .handle(
            &ctx,
            exocortex_ops::operations::AcceptDiscoveryInput {
                discovery_id: id.into(),
                from: hex(&from.id),
                to: hex(&to.id),
                kind: "Causes".into(),
            },
        )
        .await
        .unwrap();
    assert!(accepted.audit_lsn > 0);
    assert!(ctx
        .storage
        .get_discovery_proposal(id)
        .await
        .unwrap()
        .is_none());
    assert!(matches!(
        exocortex_ops::operations::IssueDiscoveryOp
            .handle(
                &ctx,
                exocortex_ops::operations::IssueDiscoveryInput {
                    discovery_id: id.into(),
                    kind: "Causes".into(),
                },
            )
            .await,
        Err(exocortex_ops::OpError::NotFound)
    ));
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

/// §17.2 tenant isolation on the audit ledger (R-A1/R-A3): two orgs write
/// audit records; one org's read must never surface the other's records —
/// on the volatile in-process path just as on storage.
#[tokio::test]
async fn audit_ledger_is_org_scoped() {
    fn ctx_for(org: &str) -> OpContext {
        let (cache, _rx) = LocalCache::new(16 * 1024 * 1024);
        OpContext {
            ontology: None,
            ingest_preflight: None,
            visibility_ctx: ops_vc(org, "alice", Visibility::Org),
            audit_admin: true,
            storage: Arc::new(InMemoryStorage::new(ontology())),
            cache: Arc::new(cache),
            deadline: chrono::Utc::now() + chrono::Duration::seconds(5),
        }
    }
    let ctx_a = ctx_for("org-a");
    let ctx_b = ctx_for("org-b");

    for (ctx, org) in [(&ctx_a, "org-a"), (&ctx_b, "org-b")] {
        let mut m = mem("isolation-probe");
        m.context.tenant_id = Some(org.into());
        ctx.storage.upsert_memory(&m).await.unwrap();
        let out = exocortex_ops::operations::PromoteVisibilityOp
            .handle(
                ctx,
                PromoteVisibilityInput {
                    memory_id: hex(&m.id),
                    to: "org".into(),
                },
            )
            .await
            .expect("promotion");
        assert!(out.audit_lsn > 0, "{org}: audited");
    }

    for (ctx, org, other) in [(&ctx_a, "org-a", "org-b"), (&ctx_b, "org-b", "org-a")] {
        let rows = exocortex_ops::operations::ListAuditRecordsOp
            .handle(
                ctx,
                exocortex_ops::operations::ListAuditInput { since_lsn: 0 },
            )
            .await
            .unwrap();
        assert!(
            rows.records
                .iter()
                .all(|r| r["org_id"] == *org || r.get("org_id").is_none()),
            "{org} ledger rows must not leak {other}"
        );
        assert!(!rows.records.is_empty(), "{org} sees its own record");
    }
}

/// CR-22 / R-MT4 through the op surface: a caller without visibility for an
/// existing memory gets `Unauthorized` (PermissionDenied), never a silent
/// empty result; a caller with visibility gets the row even on a cold cache.
#[tokio::test]
async fn get_memory_surfaces_permission_denied_not_silent_none() {
    use exocortex_storage::Storage;
    let onto = ontology();
    let (cache, _rx) = LocalCache::new(16 * 1024 * 1024);
    let storage = InMemoryStorage::new(onto.clone());

    // A Private memory authored by someone else.
    let mut m = mem("private-target");
    m.visibility = Visibility::Private;
    let author = "someone-else";
    m.provenance = Provenance::Asserted {
        author: author.into(),
        producer_kind: None,
    };
    storage.upsert_memory(&m).await.unwrap();

    let ctx = OpContext {
        visibility_ctx: ops_vc("org", "alice", Visibility::Project),
        audit_admin: false,
        storage: Arc::new(storage),
        cache: Arc::new(cache),
        deadline: chrono::Utc::now() + chrono::Duration::seconds(5),

        ontology: None,
        ingest_preflight: None,
    };
    let err = exocortex_ops::operations::GetMemory
        .handle(
            &ctx,
            exocortex_ops::operations::GetMemoryInput { id: hex(&m.id) },
        )
        .await
        .expect_err("invisible row must error");
    assert!(
        matches!(err, exocortex_ops::OpError::Unauthorized(_)),
        "PermissionDenied surfaces as Unauthorized, got {err}"
    );

    // A caller with visibility reads through the same op (cold cache →
    // the storage fallthrough fills the miss, R-C8). The memory stays
    // Private but is authored BY alice, so it resolves for her.
    let (cache2, _rx2) = LocalCache::new(16 * 1024 * 1024);
    let storage2 = InMemoryStorage::new(ontology());
    let mut own = mem("own-private");
    own.visibility = Visibility::Private;
    own.context.user_id = Some("alice".into());
    storage2.upsert_memory(&own).await.unwrap();
    let ctx2 = OpContext {
        visibility_ctx: ops_vc("org", "alice", Visibility::Org),
        audit_admin: false,
        storage: Arc::new(storage2),
        cache: Arc::new(cache2),
        deadline: chrono::Utc::now() + chrono::Duration::seconds(5),

        ontology: None,
        ingest_preflight: None,
    };
    let out = exocortex_ops::operations::GetMemory
        .handle(
            &ctx2,
            exocortex_ops::operations::GetMemoryInput { id: hex(&own.id) },
        )
        .await
        .expect("author reads own private memory");
    assert!(out.memory.is_some(), "cold-cache miss fills from storage");
    assert!(
        ctx2.cache
            .get_memory("org", &own.id, &ctx2.visibility_ctx)
            .is_some(),
        "authorized storage fallback hydrates the local cache"
    );
    ctx2.storage.delete_memory(&own.id).await.unwrap();
    let cached = exocortex_ops::operations::GetMemory
        .handle(
            &ctx2,
            exocortex_ops::operations::GetMemoryInput { id: hex(&own.id) },
        )
        .await
        .expect("second read is served from the hydrated cache");
    assert!(
        cached.memory.is_some(),
        "storage deletion after hydration proves the second read did not fall through"
    );
}

/// IN2 (audit): promote_visibility loads through the caller-scoped read —
/// a caller who cannot see another author's Private memory gets
/// Unauthorized (and the row is unchanged), never a silent widening.
#[tokio::test]
async fn promote_visibility_denies_invisible_target() {
    use exocortex_storage::Storage;
    let onto = ontology();
    let (cache, _rx) = LocalCache::new(16 * 1024 * 1024);
    let storage = InMemoryStorage::new(onto.clone());

    let mut m = mem("someone-elses-private");
    m.visibility = Visibility::Private;
    m.provenance = Provenance::Asserted {
        author: "someone-else".into(),
        producer_kind: None,
    };
    storage.upsert_memory(&m).await.unwrap();

    let ctx = OpContext {
        visibility_ctx: ops_vc("org", "alice", Visibility::Project),
        audit_admin: false,
        storage: Arc::new(storage.clone()),
        cache: Arc::new(cache),
        deadline: chrono::Utc::now() + chrono::Duration::seconds(5),

        ontology: None,
        ingest_preflight: None,
    };
    let err = exocortex_ops::operations::PromoteVisibilityOp
        .handle(
            &ctx,
            PromoteVisibilityInput {
                memory_id: hex(&m.id),
                to: "org".into(),
            },
        )
        .await
        .map(|_| ())
        .expect_err("IN2: invisible row must not be promotable");
    assert!(
        matches!(err, exocortex_ops::OpError::Unauthorized(_)),
        "PermissionDenied surfaces as Unauthorized, got {err}"
    );

    // The stored row is unchanged.
    let still = storage.get_memory(&m.id).await.unwrap().expect("row");
    assert_eq!(still.visibility, Visibility::Private);
    assert!(still.valid_until.is_none(), "no tombstone written");
}

/// R6-R18: visibility authorization is a ceiling on Actions as well as reads.
#[tokio::test]
async fn promote_visibility_denies_target_above_caller_ceiling() {
    use exocortex_storage::Storage;
    let onto = ontology();
    let (cache, _rx) = LocalCache::new(16 * 1024 * 1024);
    let storage = InMemoryStorage::new(onto);
    let mut memory = mem("project-memory");
    memory.visibility = Visibility::Project;
    memory.context.project_id = Some("project-a".into());
    storage.upsert_memory(&memory).await.unwrap();

    let mut visibility_ctx = ops_vc("org", "alice", Visibility::Project);
    visibility_ctx.project_ids.push("project-a".into());
    let ctx = OpContext {
        visibility_ctx,
        audit_admin: false,
        storage: Arc::new(storage.clone()),
        cache: Arc::new(cache),
        deadline: chrono::Utc::now() + chrono::Duration::seconds(5),
        ontology: None,
        ingest_preflight: None,
    };
    let error = exocortex_ops::operations::PromoteVisibilityOp
        .handle(
            &ctx,
            PromoteVisibilityInput {
                memory_id: hex(&memory.id),
                to: "org".into(),
            },
        )
        .await
        .map(|_| ())
        .expect_err("Project principal must not promote to Org");
    assert!(matches!(error, exocortex_ops::OpError::Unauthorized(_)));
    assert_eq!(
        storage
            .get_memory(&memory.id)
            .await
            .unwrap()
            .unwrap()
            .visibility,
        Visibility::Project
    );
    assert!(storage.audit_range("org", 0, 10).await.unwrap().is_empty());
}

/// R6-R19: the org-wide audit ledger requires an explicit administrator bit.
#[tokio::test]
async fn list_audit_records_denies_non_admin_context() {
    let (mut ctx, _) = ctx_sync();
    ctx.audit_admin = false;
    let error = exocortex_ops::operations::ListAuditRecordsOp
        .handle(
            &ctx,
            exocortex_ops::operations::ListAuditInput { since_lsn: 0 },
        )
        .await
        .map(|_| ())
        .expect_err("ordinary org principal must not read the audit ledger");
    assert!(matches!(error, exocortex_ops::OpError::Unauthorized(_)));
}

/// IN11 (audit): a past deadline fails the operation with
/// `DeadlineExceeded` — the variant was declared and HTTP-mapped but never
/// constructed, so the REQUEST_TIMEOUT arm was unreachable.
#[tokio::test]
async fn expired_deadline_returns_deadline_exceeded() {
    let onto = ontology();
    let (cache, _rx) = LocalCache::new(16 * 1024 * 1024);
    let ctx = OpContext {
        visibility_ctx: ops_vc("org", "alice", Visibility::Org),
        audit_admin: false,
        storage: Arc::new(InMemoryStorage::new(onto.clone())),
        cache: Arc::new(cache),
        deadline: chrono::Utc::now() - chrono::Duration::seconds(1), // already spent

        ontology: None,
        ingest_preflight: None,
    };
    let err = exocortex_ops::operations::GetMemory
        .handle(
            &ctx,
            exocortex_ops::operations::GetMemoryInput {
                id: hex(&MemoryId::new_v7()),
            },
        )
        .await
        .map(|_| ())
        .expect_err("expired budget");
    assert!(
        matches!(err, exocortex_ops::OpError::DeadlineExceeded),
        "got {err:?}"
    );

    // And a fresh per-request context passes the same check.
    let fresh = OpContext::per_request(
        ops_vc("org", "alice", Visibility::Org),
        Arc::new(InMemoryStorage::new(onto)),
        // rebuild cache handle
        {
            let (c2, _rx2) = LocalCache::new(16 * 1024 * 1024);
            Arc::new(c2)
        },
        chrono::Duration::seconds(5),
    );
    fresh.check_deadline().expect("fresh budget is live");
}

/// D10b (§4.10a): the read path surfaces supersession. `get_memory`
/// marks the stale memory with its successor; `search_memories` ranks
/// the successor above the superseded hit even when the stale row
/// scores higher on raw match.
#[tokio::test]
async fn superseded_state_is_visible_on_reads() {
    use exocortex_kernel::{Relationship, RelationshipId, RelationshipProperties, LSN};

    let onto = ontology();
    let (cache, _rx) = LocalCache::new(16 * 1024 * 1024);
    let mut stale = mem("Connection pool needs a server");
    stale.id = MemoryId([1; 16]);
    let mut fresh = mem("Connection pool works embedded");
    fresh.id = MemoryId([2; 16]);
    let mut snap = GraphSnapshot::empty();
    snap.push_test_memory(stale.clone());
    snap.push_test_memory(fresh.clone());
    let kind = onto.kind_id("Replaces").unwrap();
    let now = chrono::Utc::now();
    snap.push_test_relationship(Relationship {
        id: RelationshipId::derive(fresh.id, kind, stale.id, None),
        kind,
        from: fresh.id,
        to: stale.id,
        visibility: exocortex_kernel::Visibility::Org,
        provenance: Provenance::Asserted {
            author: "t".into(),
            producer_kind: None,
        },
        properties: RelationshipProperties {
            strength: 0.9,
            confidence: 0.8,
            context: None,
            evidence_count: 1,
            success_rate: None,
            validation_count: 0,
            counter_evidence_count: 0,
            last_validated: now,
        },
        description: None,
        bidirectional: false,
        valid_from: now,
        valid_until: None,
        recorded_at: now,
        invalidated_by: None,
        lsn: LSN::new_backend(1),
    });
    cache.publish("org", Arc::new(snap));
    let ctx = OpContext {
        visibility_ctx: ops_vc("org", "alice", Visibility::Org),
        audit_admin: false,
        storage: Arc::new(InMemoryStorage::new(onto.clone())),
        cache: Arc::new(cache),
        deadline: chrono::Utc::now() + chrono::Duration::seconds(5),
        ontology: Some(onto),
        ingest_preflight: None,
    };

    // get_memory: the stale row names its successor.
    let entry = exocortex_ops::entries()
        .into_iter()
        .find(|e| e.mcp_tool_name == "exocortex.get_memory")
        .unwrap();
    let out = (entry.handler)(
        entry,
        &ctx,
        serde_json::to_value(exocortex_ops::operations::GetMemoryInput {
            id: "01010101010101010101010101010101".into(),
        })
        .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(
        out["memory"]["superseded_by"].as_str(),
        Some("02020202020202020202020202020202"),
        "the stale memory names its successor: {out}"
    );

    // search: both hit "Connection pool"; the successor ranks first.
    let entry = exocortex_ops::entries()
        .into_iter()
        .find(|e| e.mcp_tool_name == "exocortex.search_memories")
        .unwrap();
    let out = (entry.handler)(
        entry,
        &ctx,
        serde_json::to_value(exocortex_ops::operations::SearchInput {
            query: "Connection pool".into(),
            limit: 10,
        })
        .unwrap(),
    )
    .await
    .unwrap();
    let hits = out["memories"].as_array().unwrap();
    assert_eq!(hits.len(), 2);
    assert_eq!(
        hits[0]["id"].as_str(),
        Some("02020202020202020202020202020202"),
        "successor outranks the superseded hit: {out}"
    );
    assert_eq!(
        hits[1]["superseded_by"].as_str(),
        Some("02020202020202020202020202020202")
    );
}

// ---- PX6: kernel catalogue <-> operation registry bijection, and the
// three newly registered operations.

/// PX6: every kernel Action/Function has exactly one operation-side
/// implementation. `commit_wrapup` is the ingestion submit path (§5.2,
/// surfaced as `preflight_wrapup` + the signed batch submit, not an
/// interactive op); `traverse_relationships` is registered under the
/// PRD §21.1 example name `find_related`. Both mappings are
/// PRD-grounded and named here so a new kernel entry cannot ship
/// silently unimplemented.
#[test]
fn kernel_catalogue_is_registered() {
    use exocortex_kernel::actions::Action as _;
    use exocortex_kernel::functions::Function as _;

    let registered: std::collections::HashSet<&'static str> =
        entries().iter().map(|e| e.name).collect();
    let kernel_actions = [
        exocortex_kernel::actions::CommitWrapup::NAME,
        exocortex_kernel::actions::AcceptDiscovery::NAME,
        exocortex_kernel::actions::PromoteVisibility::NAME,
        exocortex_kernel::actions::RetractEdge::NAME,
    ];
    let kernel_functions = [
        exocortex_kernel::functions::SearchMemories::NAME,
        exocortex_kernel::functions::TraverseRelationships::NAME,
        exocortex_kernel::functions::GetChain::NAME,
        exocortex_kernel::functions::ExplainEdge::NAME,
    ];
    // Documented mappings: kernel name -> implementing surface.
    let aliases: &[(&'static str, &'static str)] = &[
        // §5.2: the wrapup Action IS the signed batch submit.
        ("commit_wrapup", "preflight_wrapup"),
        // §21.1 names the k-hop traversal op `find_related`.
        ("traverse_relationships", "find_related"),
    ];
    for name in kernel_actions.into_iter().chain(kernel_functions) {
        let implemented = registered.contains(name)
            || aliases
                .iter()
                .any(|(kernel, op)| *kernel == name && registered.contains(op));
        assert!(
            implemented,
            "kernel catalogue entry `{name}` has no registered operation              (and no documented mapping)"
        );
    }
    // The three PX6 registrations exist under their kernel names.
    for name in ["retract_edge", "get_chain", "explain_edge"] {
        assert!(registered.contains(name), "`{name}` must be registered");
    }
}

fn rel_between(from: MemoryId, to: MemoryId, derived: bool) -> exocortex_kernel::Relationship {
    use exocortex_kernel::relationship::RelationshipProperties;
    let kind = exocortex_kernel::kinds::SOLVES;
    let provenance = if derived {
        Provenance::Derived {
            rule_id: "R1".into(),
            evidence: vec![],
        }
    } else {
        Provenance::Asserted {
            author: "t".into(),
            producer_kind: None,
        }
    };
    exocortex_kernel::Relationship {
        id: exocortex_kernel::RelationshipId::derive(from, kind, to, None),
        kind,
        from,
        to,
        visibility: Visibility::Org,
        provenance,
        properties: RelationshipProperties {
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
        lsn: LSN::new_local(0),
    }
}

#[tokio::test]
async fn retract_edge_closes_audits_and_refuses_invisible_endpoints() {
    use exocortex_ops::operations::{RetractEdgeInput, RetractEdgeOp};
    use exocortex_storage::Storage as _;

    let onto = ontology();
    let (cache, _rx) = LocalCache::new(16 * 1024 * 1024);
    let storage = Arc::new(InMemoryStorage::new(onto.clone()));
    let a = mem("retract-a");
    let b = mem("retract-b");
    storage.upsert_memory(&a).await.unwrap();
    storage.upsert_memory(&b).await.unwrap();
    let edge = rel_between(a.id, b.id, false);
    storage.upsert_relationship(&edge).await.unwrap();

    let ctx = OpContext {
        ontology: None,
        visibility_ctx: ops_vc("org", "alice", Visibility::Org),
        audit_admin: true,
        storage: storage.clone(),
        cache: Arc::new(cache),
        deadline: chrono::Utc::now() + chrono::Duration::seconds(5),
        ingest_preflight: None,
    };
    let out = RetractEdgeOp
        .handle(
            &ctx,
            RetractEdgeInput {
                edge_id: hex(&MemoryId(edge.id.0)),
                reason: "superseded by direct observation".into(),
            },
        )
        .await
        .expect("retraction");
    assert!(out.audit_lsn > 0, "audited atomically");
    let closed = storage
        .get_relationship(&edge.id)
        .await
        .unwrap()
        .expect("row present");
    assert!(closed.valid_until.is_some(), "edge closed");

    // Audit ledger carries the action with the reason in its digest input.
    let rows = exocortex_ops::operations::ListAuditRecordsOp
        .handle(
            &ctx,
            exocortex_ops::operations::ListAuditInput { since_lsn: 0 },
        )
        .await
        .unwrap();
    assert!(rows
        .records
        .iter()
        .any(|r| r["action"] == serde_json::json!("retract_edge")));

    // A caller who cannot see one endpoint may not close the edge.
    let (cache2, _rx2) = LocalCache::new(16 * 1024 * 1024);
    let c = mem("retract-c");
    let mut hidden = mem("retract-hidden");
    hidden.visibility = Visibility::Private;
    hidden.provenance = Provenance::Asserted {
        author: "someone-else".into(),
        producer_kind: None,
    };
    storage.upsert_memory(&c).await.unwrap();
    storage.upsert_memory(&hidden).await.unwrap();
    let edge2 = rel_between(c.id, hidden.id, false);
    storage.upsert_relationship(&edge2).await.unwrap();
    let ctx2 = OpContext {
        ontology: None,
        visibility_ctx: ops_vc("org", "alice", Visibility::Org),
        audit_admin: false,
        storage: storage.clone(),
        cache: Arc::new(cache2),
        deadline: chrono::Utc::now() + chrono::Duration::seconds(5),
        ingest_preflight: None,
    };
    let err = RetractEdgeOp
        .handle(
            &ctx2,
            RetractEdgeInput {
                edge_id: hex(&MemoryId(edge2.id.0)),
                reason: "should not work".into(),
            },
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err, exocortex_ops::OpError::Unauthorized(_)),
        "{err:?}"
    );
    let live = storage
        .get_relationship(&edge2.id)
        .await
        .unwrap()
        .expect("still present");
    assert!(live.valid_until.is_none(), "invisible endpoints preserved");
}

#[tokio::test]
async fn get_chain_walks_derived_evidence_and_explain_edge_renders() {
    use exocortex_ops::operations::{ExplainEdgeInput, ExplainEdgeOp, GetChainInput, GetChainOp};
    use exocortex_storage::Storage as _;

    let onto = ontology();
    let (cache, _rx) = LocalCache::new(16 * 1024 * 1024);
    let storage = Arc::new(InMemoryStorage::new(onto.clone()));
    let origin = mem("chain-origin");
    let middle = mem("chain-middle");
    let target = mem("chain-target");
    for m in [&origin, &middle, &target] {
        storage.upsert_memory(m).await.unwrap();
    }
    // origin -[SOLVES]-> middle, derived from nothing; target derived
    // FROM the first edge.
    let support = rel_between(origin.id, middle.id, true);
    storage.upsert_relationship(&support).await.unwrap();
    let mut derived_target = rel_between(middle.id, target.id, true);
    derived_target.provenance = Provenance::Derived {
        rule_id: "R4".into(),
        evidence: vec![support.id],
    };
    derived_target.id =
        exocortex_kernel::RelationshipId::derive(middle.id, derived_target.kind, target.id, None);
    storage.upsert_relationship(&derived_target).await.unwrap();
    let mut derived_memory = target.clone();
    derived_memory.provenance = Provenance::Derived {
        rule_id: "R4".into(),
        evidence: vec![derived_target.id],
    };
    storage.upsert_memory(&derived_memory).await.unwrap();
    // The middle belief is itself derived from the supporting edge, so
    // the walk reaches the origin through two hops.
    let mut derived_middle = middle.clone();
    derived_middle.provenance = Provenance::Derived {
        rule_id: "R1".into(),
        evidence: vec![support.id],
    };
    storage.upsert_memory(&derived_middle).await.unwrap();

    let ctx = OpContext {
        ontology: Some(onto.clone()),
        visibility_ctx: ops_vc("org", "alice", Visibility::Org),
        audit_admin: false,
        storage: storage.clone(),
        cache: Arc::new(cache),
        deadline: chrono::Utc::now() + chrono::Duration::seconds(5),
        ingest_preflight: None,
    };

    let chain = GetChainOp
        .handle(
            &ctx,
            GetChainInput {
                memory: hex(&target.id),
                max_depth: 4,
            },
        )
        .await
        .expect("chain");
    assert!(
        chain.chain.len() >= 3,
        "walked the evidence: {:?}",
        chain.chain
    );
    assert_eq!(chain.chain.last(), Some(&hex(&target.id)));

    let explained = ExplainEdgeOp
        .handle(
            &ctx,
            ExplainEdgeInput {
                edge: hex(&MemoryId(derived_target.id.0)),
            },
        )
        .await
        .expect("derived edges explain");
    assert!(!explained.tree.is_empty(), "Steel rendered a tree");
    assert!(
        explained.tree.contains("(derived"),
        "tree is a derivation over named input facts: {}",
        explained.tree
    );
    assert!(
        explained.tree.contains("Solves"),
        "tree names the edge kinds: {}",
        explained.tree
    );

    // An asserted edge has nothing to explain.
    let asserted = rel_between(origin.id, target.id, false);
    storage.upsert_relationship(&asserted).await.unwrap();
    let err = ExplainEdgeOp
        .handle(
            &ctx,
            ExplainEdgeInput {
                edge: hex(&MemoryId(asserted.id.0)),
            },
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err, exocortex_ops::OpError::BadInput(_)),
        "asserted edge refused: {err:?}"
    );
}

// ---------------------------------------------------------------------------
// PX2: pack-registered verbs on the one registry. The mortgage pack links
// into this test binary, so its verbs appear in `entries()` and dispatch
// through the SAME shared handlers MCP and HTTP use.
// ---------------------------------------------------------------------------

fn composed_ontology() -> Arc<exocortex_kernel::Ontology> {
    Arc::new(
        exocortex_kernel::Ontology::from_packs(vec![
            exocortex_pack_dev_v1::pack_def(),
            exocortex_pack_mortgage_v1::pack_def(),
        ])
        .unwrap(),
    )
}

fn verb_entry(name: &str) -> &'static exocortex_ops::OperationEntry {
    entries()
        .into_iter()
        .find(|e| e.name == name)
        .unwrap_or_else(|| panic!("{name} in the registry"))
}

async fn verb_ctx(onto: &Arc<exocortex_kernel::Ontology>) -> OpContext {
    // AttachRuleFinding declares min_visibility: Project — the caller must
    // be WITHIN that ceiling (KP5), so the default verb caller is
    // Project-scoped.
    let (cache, _rx) = LocalCache::new(16 * 1024 * 1024);
    OpContext {
        ontology: Some(onto.clone()),
        visibility_ctx: ops_vc("org", "alice", Visibility::Project),
        audit_admin: false,
        storage: Arc::new(InMemoryStorage::new(onto.clone())),
        cache: Arc::new(cache),
        deadline: chrono::Utc::now() + chrono::Duration::seconds(5),
        ingest_preflight: None,
    }
}

#[tokio::test]
async fn pack_verbs_join_the_registry_with_pack_identity() {
    let action = verb_entry("exocortex-pack-mortgage-v1.AttachRuleFinding");
    assert_eq!(action.pack, Some("exocortex-pack-mortgage-v1"));
    assert_eq!(action.mcp_tool_name, "exocortex.pack.AttachRuleFinding");
    assert_eq!(
        action.http_path,
        "/v1/packs/exocortex-pack-mortgage-v1/AttachRuleFinding"
    );
    let function = verb_entry("exocortex-pack-mortgage-v1.IsCategoricallyEligible");
    assert_eq!(function.pack, Some("exocortex-pack-mortgage-v1"));
    // Typed schemas exist for --dump-tools and the OpenAPI goldens.
    assert!((action.input_schema)().schema.object.is_some());
    assert!((function.input_schema)().schema.object.is_some());
}

#[tokio::test]
async fn pack_action_commits_audits_and_stamps_provenance() {
    let onto = composed_ontology();
    let ctx = verb_ctx(&onto).await;
    // A loan application and a rule definition to attach the finding to.
    let mut loan = mem("loan file 42");
    loan.memory_type = onto.memory_type_id("LoanApplication").unwrap();
    loan.visibility = Visibility::Private;
    loan.context.tenant_id = Some("org".into());
    loan.context.user_id = Some("alice".into());
    let mut rule = mem("DTI ceiling rule");
    rule.memory_type = onto.memory_type_id("RuleDefinition").unwrap();
    rule.visibility = Visibility::Private;
    rule.context.tenant_id = Some("org".into());
    rule.context.user_id = Some("alice".into());
    ctx.storage.upsert_memory(&loan).await.unwrap();
    ctx.storage.upsert_memory(&rule).await.unwrap();

    let entry = verb_entry("exocortex-pack-mortgage-v1.AttachRuleFinding");
    let out = (entry.handler)(
        entry,
        &ctx,
        serde_json::json!({
            "loan": hex(&loan.id),
            "rule": hex(&rule.id),
            "finding_title": "DTI over ceiling",
            "finding_content": "41% against a 43% ceiling policy",
        }),
    )
    .await
    .expect("pack action dispatches through the registry");
    assert_eq!(out["verb"], "exocortex-pack-mortgage-v1.AttachRuleFinding");
    assert_eq!(out["memories"].as_array().unwrap().len(), 1);
    // finding -> loan (ConcerningApplication) + finding -> rule (UnderRule)
    assert_eq!(out["edges"].as_array().unwrap().len(), 2);
    let audit_lsn = out["audit_lsn"].as_u64().unwrap();

    // The audit row is keyed pack.verb and carries the caller visibility.
    let rows = ctx.storage.audit_range("org", 0, 10).await.unwrap();
    let row = rows
        .iter()
        .find(|r| r["action"] == "exocortex-pack-mortgage-v1.AttachRuleFinding")
        .expect("audit row for the pack action");
    assert_eq!(row["lsn"].as_u64(), Some(audit_lsn));
    assert_eq!(row["actor"], "alice");

    // Provenance: the committed memory names the verb as its author.
    let finding_hex = out["memories"][0].as_str().unwrap();
    let finding = ctx
        .storage
        .get_memory(&MemoryId::parse_hex(finding_hex).unwrap())
        .await
        .unwrap()
        .unwrap();
    match &finding.provenance {
        Provenance::Asserted { author, .. } => {
            assert_eq!(author, "exocortex-pack-mortgage-v1.AttachRuleFinding")
        }
        other => panic!("expected asserted provenance, got {other:?}"),
    }
    assert_eq!(
        finding.memory_type,
        onto.memory_type_id("RuleFinding").unwrap()
    );
}

#[tokio::test]
async fn pack_action_ceiling_is_framework_enforced() {
    let onto = composed_ontology();
    let ctx = verb_ctx(&onto).await;
    let entry = verb_entry("exocortex-pack-mortgage-v1.AttachRuleFinding");
    // AttachRuleFinding declares min_visibility: Project — a caller whose
    // scope is WIDER than the ceiling is refused (KP5: the registration's
    // typed constant decides, never the caller or the body).
    let mut wide = verb_ctx(&onto).await;
    wide.visibility_ctx.max_visibility = Visibility::Org;
    let err = (entry.handler)(
        entry,
        &wide,
        serde_json::json!({
            "loan": "0".repeat(32),
            "rule": "0".repeat(32),
            "finding_title": "x",
            "finding_content": "y",
        }),
    )
    .await
    .unwrap_err();
    assert!(matches!(err, exocortex_ops::OpError::Unauthorized(_)));
    let _ = ctx;
}

#[tokio::test]
async fn pack_function_dispatches_through_scheme() {
    let onto = composed_ontology();
    let ctx = verb_ctx(&onto).await;
    let entry = verb_entry("exocortex-pack-mortgage-v1.IsCategoricallyEligible");
    let eligible = (entry.handler)(
        entry,
        &ctx,
        serde_json::json!({ "income_verified": true, "categorical_kind": "categorical" }),
    )
    .await
    .unwrap();
    assert_eq!(eligible, serde_json::json!(true));
    let ineligible = (entry.handler)(
        entry,
        &ctx,
        serde_json::json!({ "income_verified": false, "categorical_kind": "categorical" }),
    )
    .await
    .unwrap();
    assert_eq!(ineligible, serde_json::json!(false));
}

#[tokio::test]
async fn preflight_action_shares_the_commit_rulebook() {
    let onto = composed_ontology();
    let ctx = verb_ctx(&onto).await;
    // The body's own input contract fails first (bad hex) — the SAME
    // BadInput path the commit dispatch would take.
    let entry = verb_entry("preflight_action");
    let out = (entry.handler)(
        entry,
        &ctx,
        serde_json::json!({
            "pack": "exocortex-pack-mortgage-v1",
            "verb": "AttachRuleFinding",
            "input": {
                "loan": "not-hex",
                "rule": "0".repeat(32),
                "finding_title": "t",
                "finding_content": "c",
            },
        }),
    )
    .await
    .unwrap();
    assert_eq!(out["would_accept"], 0);
    assert_eq!(out["would_reject"], 1);
    assert_eq!(out["rejections"][0]["code"], "Unknown");
    assert!(
        out["rejections"][0]["detail"]
            .as_str()
            .unwrap()
            .contains("32-hex"),
        "{}",
        out["rejections"][0]["detail"]
    );
}

/// PX2: the scheme evaluator's determinism contract — both paths agree,
/// the cached VM cannot leak the previous invocation's input, scalars
/// round-trip, and a broken program is an error, never a panic.
#[test]
fn pack_function_scheme_eval_is_deterministic_across_both_paths() {
    let body = r#"(if (input "income_verified")
        (equal? (input "categorical_kind") "categorical")
        #f)"#;
    let input = serde_json::json!({
        "income_verified": true,
        "categorical_kind": "categorical",
    });
    assert_eq!(
        exocortex_ops::eval_pack_function(body, &input).unwrap(),
        serde_json::json!(true)
    );
    assert_eq!(
        exocortex_ops::eval_pack_function_cached(body, &input).unwrap(),
        serde_json::json!(true)
    );
    let flipped = serde_json::json!({
        "income_verified": false,
        "categorical_kind": "categorical",
    });
    assert_eq!(
        exocortex_ops::eval_pack_function_cached(body, &flipped).unwrap(),
        serde_json::json!(false),
        "the cached VM re-binds `input` per call"
    );
    assert_eq!(
        exocortex_ops::eval_pack_function_cached(
            r#"(+ (input "a") 1)"#,
            &serde_json::json!({ "a": 41 })
        )
        .unwrap(),
        serde_json::json!(42)
    );
    assert!(exocortex_ops::eval_pack_function("(undefined-fn)", &input).is_err());
}

/// PX2 acceptance: the ceiling is enforced by the FRAMEWORK. A body that
/// stamps `Org` under a `Project` ceiling — ignoring the `ActionContext`
/// handle entirely — is refused at prepare; nothing commits.
#[tokio::test]
async fn pack_author_cannot_bypass_the_visibility_ceiling() {
    let hostile = exocortex_kernel::verbs::registered_pack_actions()
        .into_iter()
        .find(|r| r.pack_name == "hostile-verbs-pack")
        .expect("hostile fixture registered");
    assert_eq!(hostile.ceiling, Visibility::Project);

    let mut packs = vec![
        exocortex_pack_dev_v1::pack_def(),
        exocortex_pack_mortgage_v1::pack_def(),
    ];
    packs.push(pack_def()); // the hostile fixture's own builder
    let onto = Arc::new(exocortex_kernel::Ontology::from_packs(packs).unwrap());
    let caller = ops_vc("org", "alice", Visibility::Project);
    let err = match exocortex_ops::pack_verbs::prepare_pack_action(
        &onto,
        hostile,
        &caller,
        serde_json::json!({ "note": "hostile" }),
        &|_| None,
    ) {
        Ok(_) => panic!("the hostile stamp must be refused at prepare"),
        Err(err) => err,
    };
    assert!(
        matches!(err, exocortex_ops::OpError::Unauthorized(ref d) if d.contains("ceiling")),
        "{err:?}"
    );
}

/// D21-b: `preflight_batch` fails LOUDLY on surfaces with no ingest path
/// (standalone MCP) rather than approximating Submit's verdicts with a
/// second validator.
#[tokio::test]
async fn preflight_batch_requires_the_backend_ingest_surface() {
    let (ctx, _m) = ctx_sync();
    let entry = entries()
        .into_iter()
        .find(|e| e.name == "preflight_batch")
        .expect("preflight_batch registered");
    let err = (entry.handler)(
        entry,
        &ctx,
        serde_json::json!({
            "org_id": "org",
            "source_uri": "custom://x",
            "producer_id": "p",
            "memories": []
        }),
    )
    .await
    .expect_err("no ingest handle on this surface");
    assert!(err.to_string().contains("backend ingest surface"), "{err}");
}

/// D7 (§23 #13): `resolve_contradiction` — a human decision over a
/// `Contradicts` edge closes the edge, supersedes the loser to the
/// stale-belief floor (never deletes), writes the decision to the audit
/// ledger in the same commit, and refuses everything else (wrong kind,
/// already resolved, invisible endpoints, empty note).
#[tokio::test]
async fn resolve_contradiction_supersedes_closes_and_audits() {
    use exocortex_ops::operations::{ResolveContradictionInput, ResolveContradictionOp};
    use exocortex_storage::Storage as _;

    let onto = ontology();
    let (cache, _rx) = LocalCache::new(16 * 1024 * 1024);
    let storage = Arc::new(InMemoryStorage::new(onto.clone()));
    let mut a = mem("contradiction-a");
    let mut b = mem("contradiction-b");
    a.context.tenant_id = Some("org".into());
    b.context.tenant_id = Some("org".into());
    storage.upsert_memory(&a).await.unwrap();
    storage.upsert_memory(&b).await.unwrap();
    let kind = onto.kind_id("Contradicts").expect("Contradicts registered");
    let mut edge = rel_between(a.id, b.id, false);
    edge.kind = kind;
    edge.id = exocortex_kernel::RelationshipId::derive(a.id, kind, b.id, None);
    storage.upsert_relationship(&edge).await.unwrap();

    let ctx = OpContext {
        visibility_ctx: ops_vc("org", "alice", Visibility::Org),
        audit_admin: false,
        storage: storage.clone(),
        cache: Arc::new(cache),
        deadline: chrono::Utc::now() + chrono::Duration::seconds(5),
        ontology: Some(onto.clone()),
        ingest_preflight: None,
    };

    let mut hex = String::with_capacity(32);
    for byte in edge.id.0 {
        use std::fmt::Write as _;
        let _ = write!(hex, "{byte:02x}");
    }
    let out = ResolveContradictionOp
        .handle(
            &ctx,
            ResolveContradictionInput {
                edge_id: hex.clone(),
                resolution: "from".into(),
                note: "A reproduces; B was a stale cache observation".into(),
            },
        )
        .await
        .expect("resolution commits");
    assert_eq!(out.resolution, "from");
    assert_eq!(out.superseded.as_deref(), Some(b.id.to_hex()).as_deref());
    assert!(out.audit_lsn > 0);

    // The edge is closed; the loser is at the stale-belief floor, alive.
    let closed = storage.get_relationship(&edge.id).await.unwrap().unwrap();
    assert!(
        closed.valid_until.is_some(),
        "resolved contradictions close"
    );
    let loser = storage.get_memory(&b.id).await.unwrap().unwrap();
    let floor = exocortex_kernel::memory::derived_confidence(true, 0, 0);
    assert!(
        loser.confidence.partial_cmp_score(&floor) != std::cmp::Ordering::Greater,
        "superseded, not deleted: {loser:?}"
    );
    let winner = storage.get_memory(&a.id).await.unwrap().unwrap();
    assert_eq!(winner.confidence, a.confidence, "the winner stands");
    // The decision is in the ledger.
    let rows = storage.audit_range("org", 0, 1000).await.unwrap();
    assert!(
        rows.iter().any(|r| r["action"] == "resolve_contradiction"),
        "audit row lands with the commit: {rows:?}"
    );

    // Already resolved: refused by name.
    let err = ResolveContradictionOp
        .handle(
            &ctx,
            ResolveContradictionInput {
                edge_id: hex,
                resolution: "to".into(),
                note: "again".into(),
            },
        )
        .await
        .unwrap_err();
    assert!(err.to_string().contains("already resolved"), "{err}");

    // Wrong kind: refused. "neither" on a fresh edge: closes, supersedes
    // nothing.
    let mut other = rel_between(a.id, b.id, false);
    other.id = exocortex_kernel::RelationshipId::derive(
        a.id,
        exocortex_kernel::kinds::SOLVES,
        b.id,
        Some("d7"),
    );
    storage.upsert_relationship(&other).await.unwrap();
    let mut other_hex = String::with_capacity(32);
    for byte in other.id.0 {
        use std::fmt::Write as _;
        let _ = write!(other_hex, "{byte:02x}");
    }
    let err = ResolveContradictionOp
        .handle(
            &ctx,
            ResolveContradictionInput {
                edge_id: other_hex.clone(),
                resolution: "from".into(),
                note: "n".into(),
            },
        )
        .await
        .unwrap_err();
    assert!(err.to_string().contains("not Contradicts"), "{err}");

    let mut neither_edge = rel_between(a.id, b.id, false);
    neither_edge.kind = kind;
    neither_edge.id = exocortex_kernel::RelationshipId::derive(a.id, kind, b.id, Some("neither"));
    storage.upsert_relationship(&neither_edge).await.unwrap();
    let mut neither_hex = String::with_capacity(32);
    for byte in neither_edge.id.0 {
        use std::fmt::Write as _;
        let _ = write!(neither_hex, "{byte:02x}");
    }
    let out = ResolveContradictionOp
        .handle(
            &ctx,
            ResolveContradictionInput {
                edge_id: neither_hex,
                resolution: "neither".into(),
                note: "both hold under different scopes".into(),
            },
        )
        .await
        .expect("neither commits");
    assert_eq!(out.superseded, None, "nothing superseded");
    let closed = storage
        .get_relationship(&neither_edge.id)
        .await
        .unwrap()
        .unwrap();
    assert!(closed.valid_until.is_some());
}
