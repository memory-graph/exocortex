//! M7 acceptance (§21.3): registry parity, schema goldens, identical outputs
//! across surfaces, and audit records for promote_visibility /
//! accept_discovery (§21.4 R-A2).

use std::sync::Arc;

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
    let via_registry = (entry.handler)(&ctx, input.clone())
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
    let (cache, _rx) = LocalCache::new(64 * 1024 * 1024);
    let storage = InMemoryStorage::new(ontology());
    let m = mem("parity-target");
    let mut snap = GraphSnapshot::empty();
    snap.push_test_memory(m.clone());
    cache.publish("org", Arc::new(snap));
    (
        OpContext {
            visibility_ctx: ops_vc("org", "alice", Visibility::Org),
            audit_admin: true,
            storage: Arc::new(storage),
            cache: Arc::new(cache),
            deadline: chrono::Utc::now() + chrono::Duration::seconds(5),

            ontology: None,
        },
        m,
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
    ctx.storage
        .create_discovery_proposal(&DiscoveryProposal {
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
        })
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
    let (ctx, _m) = ctx_sync();
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
    let listed = exocortex_ops::operations::ListDiscoveriesOp
        .handle(
            &ctx,
            exocortex_ops::operations::ListDiscoveriesInput { limit: 20 },
        )
        .await
        .unwrap();
    assert_eq!(listed.discoveries.len(), 1);
    assert_eq!(listed.discoveries[0].quality, 0.6);

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
    };
    let out = exocortex_ops::operations::GetMemory
        .handle(
            &ctx2,
            exocortex_ops::operations::GetMemoryInput { id: hex(&own.id) },
        )
        .await
        .expect("author reads own private memory");
    assert!(out.memory.is_some(), "cold-cache miss fills from storage");
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
    };

    // get_memory: the stale row names its successor.
    let out = (exocortex_ops::entries()
        .into_iter()
        .find(|e| e.mcp_tool_name == "exocortex.get_memory")
        .unwrap()
        .handler)(
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
    let out = (exocortex_ops::entries()
        .into_iter()
        .find(|e| e.mcp_tool_name == "exocortex.search_memories")
        .unwrap()
        .handler)(
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
