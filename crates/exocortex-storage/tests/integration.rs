//! Live-FalkorDB integration tests (§6.7 steps 6-11). Behind
//! `--features integration`; skipped (not failed) when `FALKOR_URL` is unset
//! so CI without the docker harness stays green.
//!
//! Bring the harness up with:
//!   docker compose -f tests/docker-compose.yml up -d
//!   FALKOR_URL=falkor://127.0.0.1:6379 cargo test -p exocortex-storage --features integration --test integration

#![cfg(feature = "integration")]

use chrono::{Duration, Utc};
use exocortex_kernel::{
    EntityId, Memory, MemoryContext, MemoryId, Provenance, Relationship, RelationshipId,
    Visibility, LSN,
};
use exocortex_pack_dev_v1::pack_def;
use exocortex_storage::{
    AuditEvent, CypherQuery, DiscoveryAcceptance, DiscoveryProposal, DiscoveryRecord, FalkorConfig,
    FalkorStorage, IngestBatchKey, IngestCommitOutcome, IngestRegionDelta, Invalidation, LeaseKey,
    MemoryFilter, OwnerLease, PostIngestEffect, RegionKey, Storage, StorageError, TraversalSpec,
    VisibilityContext,
};
use futures::StreamExt;
use std::sync::Arc;
use std::time::Duration as StdDuration;

fn ontology() -> Arc<exocortex_kernel::Ontology> {
    Arc::new(exocortex_kernel::Ontology::from_packs(vec![pack_def()]).unwrap())
}

fn falkor_url() -> Option<String> {
    std::env::var("FALKOR_URL").ok().filter(|u| !u.is_empty())
}

/// A unique graph name per run so tests are order-independent.
fn graph_suffix() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static N: AtomicU64 = AtomicU64::new(0);
    format!("it{}", std::process::id() as u64 % 100_000).to_string()
        + &N.fetch_add(1, Ordering::SeqCst).to_string()
}

fn id_hex(bytes: &[u8; 16]) -> String {
    use std::fmt::Write as _;
    bytes
        .iter()
        .fold(String::with_capacity(32), |mut output, byte| {
            write!(output, "{byte:02x}").expect("writing to String cannot fail");
            output
        })
}

async fn connect(node: &str) -> FalkorStorage {
    connect_graph(node, format!("exocortex_test_{}", graph_suffix())).await
}

async fn connect_graph(node: &str, graph_name: String) -> FalkorStorage {
    let url = falkor_url().expect("FALKOR_URL set (checked by runner)");
    let cfg = FalkorConfig {
        falkor_url: url.clone(),
        redis_url: std::env::var("REDIS_URL")
            .unwrap_or_else(|_| url.replacen("falkor://", "redis://", 1)),
        graph_name,
        org_id: "test-org".into(),
        node_id: node.into(),
    };
    FalkorStorage::connect(cfg, ontology())
        .await
        .expect("connect + fingerprint pin")
}

fn mem(title: &str, mt: u8, vis: Visibility) -> Memory {
    Memory {
        id: MemoryId::new_v7(),
        memory_type: mt,
        title: title.into(),
        content: format!("content of {title}"),
        summary: None,
        tags: Default::default(),
        visibility: vis,
        provenance: Provenance::Asserted {
            author: "it".into(),
            producer_kind: None,
        },
        context: MemoryContext {
            timestamp: Utc::now(),
            project_id: Some("proj".into()),
            project_path: None,
            team_id: None,
            tenant_id: Some("test-org".into()),
            session_id: Some("sess".into()),
            user_id: Some("user-1".into()),
            created_by: None,
            files_involved: Default::default(),
            languages: Default::default(),
            frameworks: Default::default(),
            technologies: Default::default(),
            git_commit: None,
            git_branch: None,
            working_directory: None,
            entities: Default::default(),
            additional_metadata: serde_json::json!({"k": "v"}),
        },
        importance: exocortex_kernel::memory::F01::new(0.5).unwrap(),
        confidence: exocortex_kernel::memory::F01::new(0.8).unwrap(),
        effectiveness: None,
        usage_count: 0,
        valid_from: Utc::now(),
        valid_until: None,
        recorded_at: Utc::now(),
        invalidated_by: None,
        embedding: None,
        lsn: LSN::new_local(0),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn stale_visibility_promotion_cannot_narrow_live_current_row() {
    if falkor_url().is_none() {
        eprintln!("SKIP: FALKOR_URL not set");
        return;
    }
    let graph = format!("exocortex_test_{}", graph_suffix());
    let first = connect_graph("promotion-a", graph.clone()).await;
    let second = connect_graph("promotion-b", graph).await;
    let mut memory = mem("private", 3, Visibility::Private);
    memory.context.tenant_id = Some("test-org".into());
    first.upsert_memory(&memory).await.unwrap();
    let audit = || AuditEvent {
        action: "promote_visibility".into(),
        actor: "user".into(),
        org_id: "test-org".into(),
        input_digest: [7; 32],
        output_ids: Default::default(),
        fingerprint: first.ontology_fingerprint(),
        lease_epoch: None,
        recorded_at: Utc::now(),
    };
    let mut to_org = memory.clone();
    to_org.visibility = Visibility::Org;
    let mut stale_to_team = memory.clone();
    stale_to_team.visibility = Visibility::Team;
    first
        .promote_memory_visibility_audited(&to_org, &audit())
        .await
        .unwrap();
    let error = second
        .promote_memory_visibility_audited(&stale_to_team, &audit())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("would narrow"), "{error}");
    assert_eq!(
        second
            .get_memory(&memory.id)
            .await
            .unwrap()
            .unwrap()
            .visibility,
        Visibility::Org
    );
    assert_eq!(
        second.audit_range("test-org", 0, 10).await.unwrap().len(),
        1
    );
}

fn rel(from: MemoryId, to: MemoryId, kind: u32) -> Relationship {
    Relationship {
        id: RelationshipId::derive(from, exocortex_kernel::RelKindId(kind), to, None),
        kind: exocortex_kernel::RelKindId(kind),
        from,
        to,
        visibility: Visibility::Org,
        provenance: Provenance::Asserted {
            author: "it".into(),
            producer_kind: None,
        },
        properties: exocortex_kernel::RelationshipProperties {
            strength: 0.8,
            confidence: 0.8,
            context: None,
            evidence_count: 1,
            success_rate: None,
            validation_count: 0,
            counter_evidence_count: 0,
            last_validated: Utc::now(),
        },
        description: None,
        bidirectional: false,
        valid_from: Utc::now(),
        valid_until: None,
        recorded_at: Utc::now(),
        invalidated_by: None,
        lsn: LSN::new_local(0),
    }
}

fn spec(depth: u8, nodes: u32) -> TraversalSpec {
    TraversalSpec {
        direction: exocortex_storage::Direction::Out,
        kinds: Default::default(),
        max_depth: depth,
        max_nodes: nodes,
        visibility_ctx: VisibilityContext {
            user_id: "user-1".into(),
            org_id: "test-org".into(),
            project_ids: Default::default(),
            team_ids: Default::default(),
            max_visibility: Visibility::Org,
        },
        as_of: None,
    }
}

macro_rules! itest {
    ($name:ident, $body:block) => {
        #[tokio::test(flavor = "multi_thread")]
        async fn $name() {
            if falkor_url().is_none() {
                eprintln!("skipping {}: FALKOR_URL not set", stringify!($name));
                return;
            }
            $body
        }
    };
}

itest!(roundtrip_memory, {
    let s = connect("node-1").await;
    let m = mem("roundtrip", 3, Visibility::Org);
    s.upsert_memory(&m).await.expect("upsert");
    let got = s.get_memory(&m.id).await.expect("get").expect("present");
    // Storage re-serializes from props_json; compare the JSON projection.
    assert_eq!(got.title, m.title);
    assert_eq!(got.content, m.content);
    assert_eq!(got.visibility, m.visibility);
    assert_eq!(got.context.project_id, m.context.project_id);
    assert_eq!(got.provenance, m.provenance);
});

itest!(
    batch_relationship_lookup_returns_existing_ids_in_one_call,
    {
        let s = connect("batch-relationship-lookup").await;
        let a = mem("batch-rel-a", 3, Visibility::Org);
        let b = mem("batch-rel-b", 3, Visibility::Org);
        let c = mem("batch-rel-c", 3, Visibility::Org);
        let first = rel(a.id, b.id, exocortex_kernel::kinds::SOLVES.0);
        let second = rel(b.id, c.id, exocortex_kernel::kinds::SOLVES.0);
        s.upsert_batch(&[a, b, c], &[first.clone(), second.clone()])
            .await
            .unwrap();
        let missing = RelationshipId([0xff; 16]);
        let rows = s
            .get_relationships(&[second.id, missing, first.id])
            .await
            .expect("one production batch relationship query");
        let ids = rows
            .into_iter()
            .map(|relationship| relationship.id)
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(ids, [first.id, second.id].into_iter().collect());
    }
);

itest!(governed_import_is_durable_and_does_not_duplicate_history, {
    let first = connect("governed-import-a").await;
    let graph = first.graph_name_clone();
    let memory = mem("governed-import", 3, Visibility::Org);
    let import_key = format!("backup:{}", graph_suffix());
    assert!(first
        .import_batch_once(&import_key, &[memory.clone()], &[])
        .await
        .unwrap());
    drop(first);

    let url = falkor_url().unwrap();
    let restarted = FalkorStorage::connect(
        FalkorConfig {
            falkor_url: url.clone(),
            redis_url: std::env::var("REDIS_URL")
                .unwrap_or_else(|_| url.replacen("falkor://", "redis://", 1)),
            graph_name: graph,
            org_id: "test-org".into(),
            node_id: "governed-import-b".into(),
        },
        ontology(),
    )
    .await
    .unwrap();
    assert!(!restarted
        .import_batch_once(&import_key, &[memory.clone()], &[])
        .await
        .unwrap());
    let assertions = restarted
        .query_cypher(&CypherQuery {
            template_id: "integration_memory_assertion_count",
            params: serde_json::json!({ "id": id_hex(&memory.id.0) }),
            read_only: true,
            deadline: Utc::now() + Duration::seconds(5),
        })
        .await
        .unwrap();
    assert_eq!(assertions.rows, vec![serde_json::json!([1])]);
    assert_eq!(
        restarted.get_memory(&memory.id).await.unwrap().unwrap().id,
        memory.id
    );
});

itest!(
    migration_compare_and_set_preserves_a_concurrent_current_write,
    {
        let storage = connect("migration-cas").await;
        let memory = mem("migration-old", 3, Visibility::Org);
        storage.upsert_memory(&memory).await.unwrap();
        let captured = storage.get_memory(&memory.id).await.unwrap().unwrap();
        let captured_lsn = captured.lsn.value;

        let mut concurrent = captured.clone();
        concurrent.title = "migration-concurrent".into();
        storage.upsert_memory(&concurrent).await.unwrap();
        let migration = storage
        .query_cypher(&CypherQuery {
            template_id: "migrate_memory_schema_v1",
            params: serde_json::json!({
                "id": id_hex(&captured.id.0),
                "memory_type_label": ontology().memory_type_names[captured.memory_type as usize],
                "memory_type_id": captured.memory_type,
                "props_json": serde_json::to_string(&captured).unwrap(),
                "tags": Vec::<String>::new(),
                "entity_ids": Vec::<String>::new(),
                "tenant_id": captured.context.tenant_id,
                "user_id": captured.context.user_id,
                "project_id": captured.context.project_id,
                "team_id": captured.context.team_id,
                "visibility": captured.visibility as u8,
                "valid_from": captured.valid_from.to_rfc3339(),
                "valid_until": captured.valid_until.map(|time| time.to_rfc3339()),
                "invalidated_by": captured.invalidated_by.map(|id| id_hex(&id.0)),
                "recorded_at": captured.recorded_at.to_rfc3339(),
                "lsn": captured_lsn,
                "expected_schema_version": 1,
            }),
            read_only: false,
            deadline: Utc::now() + Duration::seconds(5),
        })
        .await
        .unwrap();
        assert!(
            migration.rows.is_empty(),
            "stale captured LSN must lose the CAS"
        );
        let current = storage.get_memory(&memory.id).await.unwrap().unwrap();
        assert_eq!(current.title.as_str(), "migration-concurrent");
        assert!(current.lsn.value > captured_lsn);
    }
);

itest!(
    future_schema_transition_blocks_every_stale_v0_migration_write,
    {
        let storage = connect("migration-future-race").await;
        let memory = mem("future-owned", 3, Visibility::Org);
        storage.upsert_memory(&memory).await.unwrap();
        storage.make_legacy_schema_for_testing().await.unwrap();
        let captured = storage.get_memory(&memory.id).await.unwrap().unwrap();
        storage
            .query_cypher(&CypherQuery {
                template_id: "claim_schema_v0",
                params: serde_json::json!({}),
                read_only: false,
                deadline: Utc::now() + Duration::seconds(5),
            })
            .await
            .unwrap();
        storage
            .query_cypher(&CypherQuery {
                template_id: "integration_make_future_schema_without_fingerprint",
                params: serde_json::json!({ "version": 2 }),
                read_only: false,
                deadline: Utc::now() + Duration::seconds(5),
            })
            .await
            .unwrap();
        let attempted = storage
        .query_cypher(&CypherQuery {
            template_id: "migrate_memory_schema_v1",
            params: serde_json::json!({
                "id": id_hex(&captured.id.0),
                "memory_type_label": ontology().memory_type_names[captured.memory_type as usize],
                "memory_type_id": captured.memory_type,
                "props_json": serde_json::to_string(&captured).unwrap(),
                "tags": Vec::<String>::new(),
                "entity_ids": Vec::<String>::new(),
                "tenant_id": "wrong-future-overwrite",
                "user_id": captured.context.user_id,
                "project_id": captured.context.project_id,
                "team_id": captured.context.team_id,
                "visibility": captured.visibility as u8,
                "valid_from": captured.valid_from.to_rfc3339(),
                "valid_until": captured.valid_until.map(|time| time.to_rfc3339()),
                "invalidated_by": captured.invalidated_by.map(|id| id_hex(&id.0)),
                "recorded_at": captured.recorded_at.to_rfc3339(),
                "lsn": captured.lsn.value,
                "expected_schema_version": 0,
            }),
            read_only: false,
            deadline: Utc::now() + Duration::seconds(5),
        })
        .await
        .unwrap();
        assert!(attempted.rows.is_empty());
        let finish = storage
            .query_cypher(&CypherQuery {
                template_id: "finish_schema_migration_v1",
                params: serde_json::json!({ "from_version": 0, "to_version": 1 }),
                read_only: false,
                deadline: Utc::now() + Duration::seconds(5),
            })
            .await
            .unwrap();
        assert!(finish.rows.is_empty());
        assert_ne!(
            storage
                .get_memory(&memory.id)
                .await
                .unwrap()
                .unwrap()
                .context
                .tenant_id
                .as_deref(),
            Some("wrong-future-overwrite")
        );
        let schema = storage
            .query_cypher(&CypherQuery {
                template_id: "read_schema_version",
                params: serde_json::json!({}),
                read_only: true,
                deadline: Utc::now() + Duration::seconds(5),
            })
            .await
            .unwrap();
        assert_eq!(schema.rows, vec![serde_json::json!([2])]);
    }
);

itest!(
    regional_relationship_order_uses_persisted_relationship_id_tiebreaker,
    {
        let storage = connect("regional-order").await;
        let mut from = mem("regional-from", 3, Visibility::Org);
        from.id = MemoryId([1; 16]);
        let mut to = mem("regional-to", 3, Visibility::Org);
        to.id = MemoryId([2; 16]);
        storage
            .upsert_batch(&[from.clone(), to.clone()], &[])
            .await
            .unwrap();
        let kind = exocortex_kernel::kinds::FIXES;
        let mut left = rel(from.id, to.id, kind.0);
        left.id = RelationshipId([0x10; 16]);
        let mut right = left.clone();
        right.id = RelationshipId([0x20; 16]);
        storage
            .upsert_batch(&[], &[right.clone(), left.clone()])
            .await
            .unwrap();

        let rows = storage
            .relationships_in_region(
                &RegionKey {
                    org: "test-org".into(),
                    project: "proj".into(),
                    memory_type: 3,
                },
                10,
            )
            .await
            .unwrap();
        let tuples: Vec<_> = rows
            .iter()
            .map(|row| (row.from, row.to, row.kind, row.id))
            .collect();
        assert!(tuples.windows(2).all(|pair| pair[0] < pair[1]));
        let forward: Vec<_> = rows
            .iter()
            .filter(|row| row.from == from.id && row.to == to.id && row.kind == kind)
            .map(|row| row.id)
            .collect();
        assert_eq!(forward, vec![left.id, right.id]);
    }
);

itest!(
    soft_delete_serializes_the_new_recorded_time_for_both_row_kinds,
    {
        let s = connect("delete-recorded-at").await;
        let from = mem("delete-from", 3, Visibility::Org);
        let to = mem("delete-to", 3, Visibility::Org);
        let relationship = rel(from.id, to.id, exocortex_kernel::kinds::FIXES.0);
        s.upsert_batch(&[from.clone(), to], std::slice::from_ref(&relationship))
            .await
            .unwrap();
        s.delete_memory(&from.id).await.unwrap();
        s.delete_relationship(&relationship.id).await.unwrap();

        let closed_memory = s.get_memory(&from.id).await.unwrap().unwrap();
        let closed_relationship = s.get_relationship(&relationship.id).await.unwrap().unwrap();
        assert!(closed_memory.valid_until.is_some());
        assert!(closed_memory.recorded_at > from.recorded_at);
        assert!(closed_relationship.valid_until.is_some());
        assert!(closed_relationship.recorded_at > relationship.recorded_at);
    }
);

itest!(find_by_entity_reads_persisted_memory_entity_ids, {
    let s = connect("entity-query").await;
    let entity = EntityId([9; 16]);
    let mut matching = mem("matching-entity", 3, Visibility::Org);
    matching.context.entities.push(entity);
    let other = mem("other-entity", 3, Visibility::Org);
    s.upsert_batch(&[matching.clone(), other], &[])
        .await
        .expect("persist canonical memory contexts");

    let rows = s
        .find_by_entity(
            &entity,
            &MemoryFilter {
                limit: 10,
                visibility_ctx: VisibilityContext {
                    user_id: "user-1".into(),
                    org_id: "test-org".into(),
                    project_ids: ["proj".into()].into_iter().collect(),
                    max_visibility: Visibility::Org,
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await
        .expect("entity lookup");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, matching.id);
});

itest!(
    attribute_cohort_uses_scalar_index_and_tracks_replacements,
    {
        let graph = format!("exocortex_test_{}", graph_suffix());
        let s = connect_graph("attribute-index", graph.clone()).await;
        let mut matching = mem("attribute-indexed", 3, Visibility::Org);
        matching.tags.push("needle".into());
        s.upsert_memory(&matching)
            .await
            .expect("atomically persist row and attribute posting");

        let rows = s
            .memories_sharing_attributes(&["needle".into()], &[], 10)
            .await
            .expect("indexed tag lookup");
        assert_eq!(
            rows.iter().map(|row| row.id).collect::<Vec<_>>(),
            [matching.id]
        );

        matching.tags.clear();
        matching.tags.push("replacement".into());
        s.upsert_memory(&matching)
            .await
            .expect("atomically replace row and attribute posting");
        assert!(s
            .memories_sharing_attributes(&["needle".into()], &[], 10)
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            s.memories_sharing_attributes(&["replacement".into()], &[], 10)
                .await
                .unwrap()
                .len(),
            1
        );

        let redis_url = std::env::var("REDIS_URL")
            .unwrap_or_else(|_| falkor_url().unwrap().replacen("falkor://", "redis://", 1));
        let client = redis::Client::open(redis_url).expect("profile client");
        let mut connection = client
            .get_multiplexed_async_connection()
            .await
            .expect("profile connection");
        let profile: redis::Value = redis::cmd("GRAPH.PROFILE")
        .arg(&graph)
        .arg("UNWIND ['t:replacement'] AS key MATCH (attribute:_MemoryAttribute {key: key})-[:_INDEXES_MEMORY]->(m:Memory) RETURN DISTINCT m ORDER BY m.lsn ASC LIMIT 10")
        .query_async(&mut connection)
        .await
        .expect("profile production query shape");
        let plan = format!("{profile:?}");
        assert!(plan.contains("Node By Index Scan"), "{plan}");
        assert!(!plan.contains("Node By Label Scan"), "{plan}");
    }
);

itest!(find_by_entity_filters_tenant_before_limit, {
    let s = connect("entity-tenant-limit").await;
    let entity = EntityId([10; 16]);
    let mut matching = mem("matching-tenant", 3, Visibility::Org);
    matching.context.entities.push(entity);
    let mut tenantless = mem("tenantless", 3, Visibility::Org);
    tenantless.context.entities.push(entity);
    tenantless.context.tenant_id = None;
    tenantless.recorded_at = matching.recorded_at + Duration::seconds(1);
    s.upsert_batch(&[matching.clone(), tenantless], &[])
        .await
        .unwrap();

    let rows = s
        .find_by_entity(
            &entity,
            &MemoryFilter {
                limit: 1,
                visibility_ctx: VisibilityContext {
                    user_id: "user-1".into(),
                    org_id: "test-org".into(),
                    project_ids: ["proj".into()].into_iter().collect(),
                    max_visibility: Visibility::Org,
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        rows.iter().map(|memory| memory.id).collect::<Vec<_>>(),
        [matching.id]
    );
});

itest!(
    startup_migrates_legacy_entity_index_and_temporal_assertions,
    {
        let first = connect("legacy-schema-a").await;
        let graph = first.graph_name_clone();
        let entity = EntityId([11; 16]);
        let mut from = mem("legacy-from", 3, Visibility::Org);
        from.context.tenant_id = None;
        from.context.entities.push(entity);
        let mut to = mem("legacy-to", 3, Visibility::Org);
        to.context.tenant_id = None;
        let relationship = rel(from.id, to.id, exocortex_kernel::kinds::FIXES.0);
        first
            .upsert_batch(&[from.clone(), to.clone()], &[relationship])
            .await
            .expect("seed current rows and v1 assertions");
        first
            .make_legacy_schema_for_testing()
            .await
            .expect("downgrade graph to the pre-v1 shape");
        drop(first);

        let url = falkor_url().unwrap();
        let restarted = FalkorStorage::connect(
            FalkorConfig {
                falkor_url: url.clone(),
                redis_url: std::env::var("REDIS_URL")
                    .unwrap_or_else(|_| url.replacen("falkor://", "redis://", 1)),
                graph_name: graph,
                org_id: "test-org".into(),
                node_id: "legacy-schema-b".into(),
            },
            ontology(),
        )
        .await
        .expect("startup migration succeeds");
        assert_eq!(
            restarted.migration_peak_rows_for_testing(),
            1,
            "migration retains only the row currently being transformed"
        );

        let rows = restarted
            .find_by_entity(
                &entity,
                &MemoryFilter {
                    limit: 10,
                    visibility_ctx: VisibilityContext {
                        user_id: "user-1".into(),
                        org_id: "test-org".into(),
                        project_ids: ["proj".into()].into_iter().collect(),
                        max_visibility: Visibility::Org,
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .await
            .expect("legacy entity ids are denormalized at startup");
        assert_eq!(rows.iter().map(|row| row.id).collect::<Vec<_>>(), [from.id]);
        assert_eq!(
            rows[0].context.tenant_id.as_deref(),
            Some("test-org"),
            "genuine pre-tenant canonical JSON is adopted by the configured graph org"
        );
        let state = restarted
            .get_state_at(Utc::now() + Duration::seconds(1))
            .await
            .expect("bootstrapped temporal state");
        assert_eq!(state.memory_count, 2);
        assert_eq!(state.relationship_count, 2, "edge plus required inverse");
    }
);

itest!(ingest_settlement_survives_backend_reconnect, {
    let first = connect("ingest-restart-a").await;
    let graph = first.graph_name_clone();
    let key = IngestBatchKey {
        org_id: "test-org".into(),
        producer_id: "producer".into(),
        batch_id: format!("restart-{}", graph_suffix()).into(),
    };
    let committed = mem("first-ingest", 3, Visibility::Org);
    let outcome = first
        .commit_ingest_batch(&key, &[committed.clone()], &[], 1)
        .await
        .expect("first atomic ingest");
    let original = match outcome {
        IngestCommitOutcome::Committed { settled, .. } => settled,
        other => panic!("expected first commit, got {other:?}"),
    };
    drop(first);

    let url = falkor_url().unwrap();
    let restarted = FalkorStorage::connect(
        FalkorConfig {
            falkor_url: url.clone(),
            redis_url: std::env::var("REDIS_URL")
                .unwrap_or_else(|_| url.replacen("falkor://", "redis://", 1)),
            graph_name: graph,
            org_id: "test-org".into(),
            node_id: "ingest-restart-b".into(),
        },
        ontology(),
    )
    .await
    .expect("reconnect same durable graph");
    let must_not_commit = mem("second-ingest", 3, Visibility::Org);
    let replay = restarted
        .commit_ingest_batch(&key, &[must_not_commit.clone()], &[], 1)
        .await
        .expect("settled replay");
    let replay = match replay {
        IngestCommitOutcome::Duplicate(settled) => settled,
        other => panic!("expected durable duplicate, got {other:?}"),
    };
    assert_eq!(
        replay, original,
        "reconnect returns the exact settled result"
    );
    assert!(restarted.get_memory(&committed.id).await.unwrap().is_some());
    assert!(restarted
        .get_memory(&must_not_commit.id)
        .await
        .unwrap()
        .is_none());
});

itest!(
    ingest_effect_outbox_survives_reconnect_and_acknowledges_once,
    {
        let first = connect("ingest-outbox-a").await;
        let graph = first.graph_name_clone();
        let key = IngestBatchKey {
            org_id: "test-org".into(),
            producer_id: "producer".into(),
            batch_id: format!("outbox-{}", graph_suffix()).into(),
        };
        let memory = mem("outbox-memory", 3, Visibility::Org);
        let effect = PostIngestEffect {
            effect_id: format!("{}/{}/{}", key.org_id, key.producer_id, key.batch_id).into(),
            session_memory_ids: vec![memory.id],
            region_deltas: vec![IngestRegionDelta {
                region: RegionKey {
                    org: "test-org".into(),
                    project: "proj".into(),
                    memory_type: memory.memory_type,
                },
                memories: 1,
                relationships: 0,
            }],
        };
        assert!(matches!(
            first
                .commit_ingest_batch_with_effect(&key, &[memory.clone()], &[], 1, &effect)
                .await
                .unwrap(),
            IngestCommitOutcome::Committed { .. }
        ));
        drop(first);

        let url = falkor_url().unwrap();
        let restarted = Arc::new(
            FalkorStorage::connect(
                FalkorConfig {
                    falkor_url: url.clone(),
                    redis_url: std::env::var("REDIS_URL")
                        .unwrap_or_else(|_| url.replacen("falkor://", "redis://", 1)),
                    graph_name: graph,
                    org_id: "test-org".into(),
                    node_id: "ingest-outbox-b".into(),
                },
                ontology(),
            )
            .await
            .unwrap(),
        );
        let contender = FalkorStorage::connect(
            FalkorConfig {
                falkor_url: url.clone(),
                redis_url: std::env::var("REDIS_URL")
                    .unwrap_or_else(|_| url.replacen("falkor://", "redis://", 1)),
                graph_name: restarted.graph_name_clone(),
                org_id: "test-org".into(),
                node_id: "ingest-outbox-c".into(),
            },
            ontology(),
        )
        .await
        .unwrap();
        assert_eq!(
            restarted.pending_ingest_effects(10).await.unwrap(),
            [effect.clone()]
        );
        assert!(matches!(
            restarted
                .commit_ingest_batch_with_effect(&key, &[memory.clone()], &[], 1, &effect)
                .await
                .unwrap(),
            IngestCommitOutcome::Duplicate(_)
        ));
        assert_eq!(
            restarted.pending_ingest_effects(10).await.unwrap(),
            [effect.clone()]
        );
        let peer = mem("outbox-peer", 3, Visibility::Org);
        let derived = rel(memory.id, peer.id, 1);
        let first_memories = [peer.clone()];
        let second_memories = [peer.clone()];
        let first_relationships = [derived.clone()];
        let second_relationships = [derived.clone()];
        let (first_once, second_once) = tokio::join!(
            restarted.upsert_batch_once(
                "reasoning:live-effect",
                &first_memories,
                &first_relationships,
            ),
            contender.upsert_batch_once(
                "reasoning:live-effect",
                &second_memories,
                &second_relationships,
            ),
        );
        assert_eq!(
            usize::from(matches!(first_once, Ok(true)))
                + usize::from(matches!(second_once, Ok(true))),
            1,
            "exactly one independent live client commits; a publication owner may make the loser retry"
        );
        let assertion_count = restarted
            .query_cypher(&CypherQuery {
                template_id: "integration_relationship_assertion_count",
                params: serde_json::json!({ "id": id_hex(&derived.id.0) }),
                read_only: true,
                deadline: Utc::now() + Duration::seconds(5),
            })
            .await
            .unwrap();
        assert_eq!(assertion_count.rows, vec![serde_json::json!([1])]);
        assert!(restarted
            .upsert_batch_once("reasoning:live-no-output", &[], &[])
            .await
            .unwrap());
        assert!(!contender
            .upsert_batch_once("reasoning:live-no-output", &[], &[])
            .await
            .unwrap());
        let repair_peer = mem("outbox-repair-peer", 3, Visibility::Org);
        let repair_edge = rel(memory.id, repair_peer.id, 1);
        let region = RegionKey {
            org: "test-org".into(),
            project: "proj".into(),
            memory_type: 3,
        };
        let mut feed = contender.subscribe_invalidations(&region).await.unwrap();
        restarted.fail_next_publish_for_testing();
        assert!(restarted
            .upsert_batch_once(
                "reasoning:publish-repair",
                &[repair_peer.clone()],
                &[repair_edge.clone()],
            )
            .await
            .is_err());
        assert!(!contender
            .upsert_batch_once("reasoning:publish-repair", &[], &[])
            .await
            .unwrap());
        tokio::time::timeout(StdDuration::from_secs(2), async {
            loop {
                if matches!(feed.next().await, Some(Ok(Invalidation::RelationshipUpserted { id, .. })) if id == repair_edge.id) {
                    break;
                }
            }
        })
        .await
        .expect("marker replay republishes the committed relationship invalidation");
        let retained_payloads = restarted
            .query_cypher(&CypherQuery {
                template_id: "integration_idempotent_publication_payload_count",
                params: serde_json::json!({ "operation_key": "reasoning:publish-repair" }),
                read_only: true,
                deadline: Utc::now() + Duration::seconds(5),
            })
            .await
            .unwrap();
        assert_eq!(
            retained_payloads.rows,
            vec![serde_json::json!([0])],
            "completed marker releases its no-longer-needed publication payload"
        );
        let lease_peer = mem("outbox-lease-peer", 3, Visibility::Org);
        let lease_edge = rel(memory.id, lease_peer.id, 1);
        restarted.pause_next_publish_for_testing();
        let owner_storage = restarted.clone();
        let owner_edge = lease_edge.clone();
        let owner = tokio::spawn(async move {
            owner_storage
                .upsert_batch_once("reasoning:lease-loss", &[lease_peer], &[owner_edge])
                .await
        });
        restarted.wait_for_paused_publish_for_testing().await;
        contender
            .query_cypher(&CypherQuery {
                template_id: "integration_expire_idempotent_publication_claim",
                params: serde_json::json!({ "operation_key": "reasoning:lease-loss" }),
                read_only: false,
                deadline: Utc::now() + Duration::seconds(5),
            })
            .await
            .unwrap();
        restarted.release_paused_publish_for_testing();
        assert!(
            owner.await.unwrap().is_err(),
            "expired owner cannot report publication success before takeover"
        );
        assert!(!contender
            .upsert_batch_once("reasoning:lease-loss", &[], &[])
            .await
            .unwrap());
        let (claim_a, claim_b) = tokio::join!(
            restarted.claim_ingest_effect("worker-a", 2_000),
            contender.claim_ingest_effect("worker-b", 2_000),
        );
        let (winner, loser) = match (claim_a.unwrap(), claim_b.unwrap()) {
            (Some(claimed), None) => {
                assert_eq!(claimed, effect);
                ("worker-a", "worker-b")
            }
            (None, Some(claimed)) => {
                assert_eq!(claimed, effect);
                ("worker-b", "worker-a")
            }
            claims => panic!("exactly one simultaneous live claimant must win: {claims:?}"),
        };
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        assert!(restarted
            .renew_ingest_effect_claim(effect.effect_id.as_str(), winner, 3_000)
            .await
            .unwrap());
        assert!(!restarted
            .renew_ingest_effect_claim(effect.effect_id.as_str(), loser, 3_000)
            .await
            .unwrap());
        tokio::time::sleep(std::time::Duration::from_millis(2_000)).await;
        assert!(
            contender
                .claim_ingest_effect(loser, 30_000)
                .await
                .unwrap()
                .is_none(),
            "renewal must exclude live contenders beyond the original lease"
        );
        tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;
        assert!(
            !restarted
                .acknowledge_ingest_effect(effect.effect_id.as_str(), winner)
                .await
                .unwrap(),
            "an expired live owner cannot acknowledge before reclaim"
        );
        assert_eq!(
            contender.claim_ingest_effect(loser, 30_000).await.unwrap(),
            Some(effect.clone()),
            "an abandoned claim becomes retryable after its deadline"
        );
        assert!(!restarted
            .acknowledge_ingest_effect(effect.effect_id.as_str(), winner)
            .await
            .unwrap());
        assert!(restarted
            .acknowledge_ingest_effect(effect.effect_id.as_str(), loser)
            .await
            .unwrap());
        assert!(restarted
            .pending_ingest_effects(10)
            .await
            .unwrap()
            .is_empty());
    }
);

itest!(corrupt_ingest_settlement_fails_closed_on_retry, {
    for (case, accepted, rejected, assigned_lsn) in [
        (
            "wrong-type",
            serde_json::json!("1"),
            serde_json::json!(0),
            serde_json::json!(1),
        ),
        (
            "negative",
            serde_json::json!(1),
            serde_json::json!(-1),
            serde_json::json!(1),
        ),
        (
            "overflow",
            serde_json::json!(u64::from(u32::MAX) + 1),
            serde_json::json!(0),
            serde_json::json!(1),
        ),
    ] {
        let storage = connect(&format!("ingest-corrupt-{case}")).await;
        let key = IngestBatchKey {
            org_id: "test-org".into(),
            producer_id: "producer".into(),
            batch_id: format!("corrupt-{case}-{}", graph_suffix()).into(),
        };
        let committed = mem(&format!("committed-{case}"), 3, Visibility::Org);
        storage
            .commit_ingest_batch(&key, &[committed.clone()], &[], 1)
            .await
            .unwrap();
        storage
            .query_cypher(&CypherQuery {
                template_id: "integration_corrupt_ingest_settlement",
                params: serde_json::json!({
                    "org_id": key.org_id,
                    "producer_id": key.producer_id,
                    "batch_id": key.batch_id,
                    "accepted": accepted,
                    "rejected": rejected,
                    "assigned_lsn": assigned_lsn,
                }),
                read_only: false,
                deadline: Utc::now() + Duration::seconds(5),
            })
            .await
            .unwrap();

        let must_not_commit = mem(&format!("must-not-commit-{case}"), 3, Visibility::Org);
        assert!(matches!(
            storage
                .commit_ingest_batch(&key, &[must_not_commit.clone()], &[], 1)
                .await,
            Err(StorageError::CorruptMetadata {
                key: "ingest_settlement",
                ..
            })
        ));
        assert!(storage.get_memory(&committed.id).await.unwrap().is_some());
        assert!(storage
            .get_memory(&must_not_commit.id)
            .await
            .unwrap()
            .is_none());
    }
});

itest!(credential_bearing_backend_url_errors_are_redacted, {
    const SENTINEL: &str = "R6_Q3_URL_CREDENTIAL_SENTINEL_8d92e4";
    let live_falkor = falkor_url().unwrap();
    let cases = [
        (
            format!("falkor://sentinel:{SENTINEL}@["),
            "redis://127.0.0.1:1".to_string(),
        ),
        (
            live_falkor.clone(),
            format!("redis://sentinel:{SENTINEL}@["),
        ),
        (
            live_falkor,
            format!("redis://sentinel:{SENTINEL}@127.0.0.1:1"),
        ),
    ];
    for (index, (falkor_url, redis_url)) in cases.into_iter().enumerate() {
        let error = match FalkorStorage::connect(
            FalkorConfig {
                falkor_url,
                redis_url,
                graph_name: format!("credential_redaction_{}_{}", graph_suffix(), index),
                org_id: "test-org".into(),
                node_id: "credential-redaction".into(),
            },
            ontology(),
        )
        .await
        {
            Ok(_) => panic!("malformed or unreachable credential-bearing URL must fail"),
            Err(error) => error,
        };
        let diagnostic = format!("{error}\n{error:?}");
        assert!(
            !diagnostic.contains(SENTINEL),
            "backend diagnostic reproduced URL credential: {diagnostic}"
        );
    }
});

itest!(fingerprint_mismatch_aborts_startup, {
    // First process pins the fingerprint.
    let s = connect("node-1").await;
    let graph = s.graph_name_clone();
    drop(s);
    // Second process with a DIFFERENT ontology (extra pack kind) must refuse.
    let mut altered = pack_def();
    altered.kinds.push(exocortex_kernel::RelMeta {
        id: exocortex_kernel::RelKindId(0x8000_0000 | 0x0200),
        display_name: "DriftKind".into(),
        bucket: exocortex_kernel::RelBucket::Extension(9),
        inverse: None,
        bidirectional: false,
        default_strength: 0.5,
        computed_only: false,
    });
    let onto2 = Arc::new(exocortex_kernel::Ontology::from_packs(vec![altered]).unwrap());
    let url = falkor_url().unwrap();
    let cfg = FalkorConfig {
        falkor_url: url.clone(),
        redis_url: std::env::var("REDIS_URL")
            .unwrap_or_else(|_| url.replacen("falkor://", "redis://", 1)),
        graph_name: graph,
        org_id: "test-org".into(),
        node_id: "node-2".into(),
    };
    let err = match FalkorStorage::connect(cfg, onto2).await {
        Ok(_) => panic!("expected fingerprint mismatch"),
        Err(e) => e,
    };
    assert!(
        matches!(
            err,
            exocortex_storage::StorageError::FingerprintMismatch { .. }
        ),
        "expected FingerprintMismatch, got {err:?}"
    );
});

itest!(
    concurrent_initializers_cannot_overwrite_the_winning_fingerprint,
    {
        let url = falkor_url().unwrap();
        let graph_name = format!("fingerprint-cas-{}", graph_suffix());
        let config = |node: &str| FalkorConfig {
            falkor_url: url.clone(),
            redis_url: std::env::var("REDIS_URL")
                .unwrap_or_else(|_| url.replacen("falkor://", "redis://", 1)),
            graph_name: graph_name.clone(),
            org_id: "test-org".into(),
            node_id: node.into(),
        };
        let first_ontology = ontology();
        let mut incompatible = exocortex_kernel::Ontology::from_packs(vec![pack_def()]).unwrap();
        incompatible.fingerprint.0[0] ^= 0xff;
        let incompatible = Arc::new(incompatible);
        let (first, second) = tokio::join!(
            FalkorStorage::connect(config("fingerprint-cas-a"), first_ontology),
            FalkorStorage::connect(config("fingerprint-cas-b"), incompatible),
        );
        assert_eq!(
            usize::from(first.is_ok()) + usize::from(second.is_ok()),
            1,
            "exactly one incompatible initializer may pin the empty graph"
        );
        let loser = match (first, second) {
            (Err(error), Ok(_)) | (Ok(_), Err(error)) => error,
            _ => unreachable!("success count checked above"),
        };
        assert!(matches!(loser, StorageError::FingerprintMismatch { .. }));
    }
);

itest!(
    reconnect_repairs_post_migration_legacy_write_without_steady_read_queries,
    {
        let s = connect("legacy-after-migration").await;
        let graph = s.graph_name_clone();
        let mut legacy = mem("legacy-after-v1", 3, Visibility::Org);
        legacy.context.tenant_id = None;
        s.upsert_memory(&legacy).await.unwrap();
        let vc = VisibilityContext {
            user_id: "user-1".into(),
            org_id: "test-org".into(),
            project_ids: ["proj".into()].into_iter().collect(),
            max_visibility: Visibility::Org,
            ..Default::default()
        };
        assert!(matches!(
            s.get_memory_for(&legacy.id, &vc).await,
            Err(exocortex_storage::StorageError::PermissionDenied)
        ));

        s.query_cypher(&CypherQuery {
            template_id: "integration_remove_current_memory_assertion",
            params: serde_json::json!({ "id": id_hex(&legacy.id.0) }),
            read_only: false,
            deadline: Utc::now() + Duration::seconds(5),
        })
        .await
        .unwrap();
        drop(s);
        let url = falkor_url().unwrap();
        let restarted = FalkorStorage::connect(
            FalkorConfig {
                falkor_url: url.clone(),
                redis_url: std::env::var("REDIS_URL")
                    .unwrap_or_else(|_| url.replacen("falkor://", "redis://", 1)),
                graph_name: graph,
                org_id: "test-org".into(),
                node_id: "legacy-after-migration-restart".into(),
            },
            ontology(),
        )
        .await
        .expect("connect-time compatibility repair");
        assert!(
            restarted.take_legacy_repair_query_count() > 0,
            "compatibility probes execute during connection"
        );
        let repaired = restarted
            .get_memory_for(&legacy.id, &vc)
            .await
            .unwrap()
            .expect("legacy-shaped row is repaired during reconnect");
        assert_eq!(repaired.context.tenant_id.as_deref(), Some("test-org"));
        restarted.get_memories(&[legacy.id]).await.unwrap();
        restarted
            .find_by_entity(
                &EntityId([99; 16]),
                &MemoryFilter {
                    limit: 1,
                    visibility_ctx: vc.clone(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let mut memory_stream = restarted.stream_all_memories().await;
        while memory_stream.next().await.is_some() {}
        drop(memory_stream);
        let mut relationship_stream = restarted.stream_all_relationships().await;
        while relationship_stream.next().await.is_some() {}
        drop(relationship_stream);
        assert_eq!(
            restarted.take_legacy_repair_query_count(),
            0,
            "canonical point reads must not run compatibility repair templates"
        );
    }
);

itest!(malformed_fingerprint_fails_closed_without_rewrite, {
    let s = connect("corrupt-fingerprint").await;
    let graph = s.graph_name_clone();
    let malformed = "0".repeat(63);
    s.query_cypher(&CypherQuery {
        template_id: "write_fingerprint",
        params: serde_json::json!({ "fp": malformed }),
        read_only: false,
        deadline: Utc::now() + Duration::seconds(5),
    })
    .await
    .unwrap();

    let url = falkor_url().unwrap();
    let cfg = FalkorConfig {
        falkor_url: url.clone(),
        redis_url: std::env::var("REDIS_URL")
            .unwrap_or_else(|_| url.replacen("falkor://", "redis://", 1)),
        graph_name: graph,
        org_id: "test-org".into(),
        node_id: "corrupt-reader".into(),
    };
    let err = match FalkorStorage::connect(cfg, ontology()).await {
        Ok(_) => panic!("malformed fingerprint must fail startup"),
        Err(err) => err,
    };
    assert!(matches!(
        err,
        exocortex_storage::StorageError::CorruptMetadata {
            key: "ontology_fingerprint",
            ..
        }
    ));

    let persisted = s
        .query_cypher(&CypherQuery {
            template_id: "read_fingerprint",
            params: serde_json::json!({}),
            read_only: true,
            deadline: Utc::now() + Duration::seconds(5),
        })
        .await
        .unwrap();
    assert_eq!(persisted.rows, vec![serde_json::json!([malformed])]);
});

itest!(
    future_schema_is_rejected_before_absent_fingerprint_is_pinned,
    {
        let first = connect("future-schema-a").await;
        let graph = first.graph_name_clone();
        first
            .make_future_schema_without_fingerprint_for_testing()
            .await
            .unwrap();
        let url = falkor_url().unwrap();
        let result = FalkorStorage::connect(
            FalkorConfig {
                falkor_url: url.clone(),
                redis_url: std::env::var("REDIS_URL")
                    .unwrap_or_else(|_| url.replacen("falkor://", "redis://", 1)),
                graph_name: graph.clone(),
                org_id: "test-org".into(),
                node_id: "future-schema-b".into(),
            },
            ontology(),
        )
        .await;
        assert!(matches!(
            result,
            Err(StorageError::CorruptMetadata {
                key: "schema_version",
                ..
            })
        ));

        let fingerprint = first
            .query_cypher(&CypherQuery {
                template_id: "read_fingerprint",
                params: serde_json::json!({}),
                read_only: true,
                deadline: Utc::now() + Duration::seconds(5),
            })
            .await
            .unwrap();
        assert!(
            fingerprint.rows.is_empty(),
            "rejected startup did not pin a fingerprint"
        );
        let schema = first
            .query_cypher(&CypherQuery {
                template_id: "read_schema_version",
                params: serde_json::json!({}),
                read_only: true,
                deadline: Utc::now() + Duration::seconds(5),
            })
            .await
            .unwrap();
        assert_eq!(schema.rows, vec![serde_json::json!([2])]);
    }
);

itest!(atomic_schema_guard_refuses_fingerprint_mutation_directly, {
    let storage = connect("direct-schema-guard").await;
    storage
        .make_future_schema_without_fingerprint_for_testing()
        .await
        .unwrap();
    storage
        .query_cypher(&CypherQuery {
            template_id: "write_fingerprint_if_schema_compatible",
            params: serde_json::json!({
                "fp": "direct-guard-must-not-persist",
                "max_schema": 1,
            }),
            read_only: false,
            deadline: Utc::now() + Duration::seconds(5),
        })
        .await
        .unwrap();
    let fingerprint = storage
        .query_cypher(&CypherQuery {
            template_id: "read_fingerprint",
            params: serde_json::json!({}),
            read_only: true,
            deadline: Utc::now() + Duration::seconds(5),
        })
        .await
        .unwrap();
    assert!(
        fingerprint.rows.is_empty(),
        "atomic guard left fingerprint state unchanged"
    );
    let schema = storage
        .query_cypher(&CypherQuery {
            template_id: "read_schema_version",
            params: serde_json::json!({}),
            read_only: true,
            deadline: Utc::now() + Duration::seconds(5),
        })
        .await
        .unwrap();
    assert_eq!(schema.rows, vec![serde_json::json!([2])]);
});

itest!(
    discovery_acceptance_is_scoped_consumed_and_audited_atomically,
    {
        let s = connect("discovery-atomic").await;
        let from = mem("proposal-from", 0, Visibility::Org);
        let to = mem("proposal-to", 2, Visibility::Org);
        s.upsert_memory(&from).await.unwrap();
        s.upsert_memory(&to).await.unwrap();
        let kind = exocortex_kernel::kinds::CAUSES;
        let scope = VisibilityContext {
            user_id: "alice".into(),
            org_id: "test-org".into(),
            project_ids: Default::default(),
            team_ids: Default::default(),
            max_visibility: Visibility::Org,
        };
        let region = RegionKey {
            org: "test-org".into(),
            project: "*".into(),
            memory_type: from.memory_type,
        };
        let proposal = DiscoveryProposal {
            discovery_id: "live-proposal".into(),
            region: region.clone(),
            from: from.id,
            to: to.id,
            kind,
            proposed_visibility: Visibility::Project,
            caller_scope: scope.clone(),
            issued_at: Utc::now(),
        };
        s.store_discovery(&DiscoveryRecord {
            discovery_id: proposal.discovery_id.clone(),
            region: region.clone(),
            from: from.id,
            to: to.id,
            discovery_type: "transitive".into(),
            quality: 0.6,
            via_types: [1, 2],
            discovery_cycle_id: "live-cycle".into(),
            discovered_at: proposal.issued_at,
        })
        .await
        .unwrap();
        s.create_discovery_proposal(&proposal).await.unwrap();
        s.create_discovery_proposal(&proposal).await.unwrap();
        let proposed_reissue = s
            .store_discovery(&DiscoveryRecord {
                discovery_id: proposal.discovery_id.clone(),
                region: proposal.region.clone(),
                from: proposal.from,
                to: proposal.to,
                discovery_type: "transitive".into(),
                quality: 0.6,
                via_types: [1, 2],
                discovery_cycle_id: "live-cycle".into(),
                discovered_at: proposal.issued_at,
            })
            .await;
        assert!(
            matches!(proposed_reissue, Err(StorageError::ProposalMismatch)),
            "proposed discovery reissue result: {proposed_reissue:?}"
        );
        assert!(s.get_discovery("live-proposal").await.unwrap().is_none());
        assert!(s.list_discoveries("test-org", 10).await.unwrap().is_empty());
        let mut conflicting = proposal.clone();
        conflicting.to = MemoryId::new_v7();
        assert!(matches!(
            s.create_discovery_proposal(&conflicting).await,
            Err(StorageError::ProposalMismatch)
        ));
        let mut relationship = rel(from.id, to.id, kind.0);
        relationship.visibility = Visibility::Project;
        relationship.id = RelationshipId::derive(from.id, kind, to.id, Some("live-proposal"));
        let acceptance = DiscoveryAcceptance {
            discovery_id: proposal.discovery_id.clone(),
            region,
            caller_scope: scope,
            relationship: relationship.clone(),
            audit: AuditEvent {
                action: "accept_discovery".into(),
                actor: "alice".into(),
                org_id: "test-org".into(),
                input_digest: [9; 32],
                output_ids: ["edge".into()].into_iter().collect(),
                fingerprint: s.ontology_fingerprint(),
                lease_epoch: None,
                recorded_at: Utc::now(),
            },
        };
        let committed = s.accept_discovery(&acceptance).await.unwrap();
        assert!(committed.lsn > 0);
        assert!(s
            .get_discovery_proposal("live-proposal")
            .await
            .unwrap()
            .is_none());
        assert!(matches!(
            s.accept_discovery(&acceptance).await,
            Err(StorageError::ProposalMismatch)
        ));
        assert!(matches!(
            s.create_discovery_proposal(&proposal).await,
            Err(StorageError::ProposalNotFound)
        ));
        assert!(matches!(
            s.store_discovery(&DiscoveryRecord {
                discovery_id: proposal.discovery_id.clone(),
                region: proposal.region.clone(),
                from: proposal.from,
                to: proposal.to,
                discovery_type: "transitive".into(),
                quality: 0.6,
                via_types: [1, 2],
                discovery_cycle_id: "live-cycle".into(),
                discovered_at: proposal.issued_at,
            })
            .await,
            Err(StorageError::ProposalMismatch)
        ));
        let audits = s.audit_range("test-org", 0, 10).await.unwrap();
        assert_eq!(audits.len(), 1);
        assert_eq!(audits[0]["action"], "accept_discovery");
    }
);

itest!(
    malformed_discovery_proposal_fails_closed_without_consumption,
    {
        let s = connect("malformed-proposal").await;
        let from = mem("malformed-from", 3, Visibility::Org);
        let to = mem("malformed-to", 3, Visibility::Org);
        s.upsert_batch(&[from.clone(), to.clone()], &[])
            .await
            .unwrap();
        let region = RegionKey {
            org: "test-org".into(),
            project: "*".into(),
            memory_type: from.memory_type,
        };
        let record = DiscoveryRecord {
            discovery_id: "malformed-proposal".into(),
            region: region.clone(),
            from: from.id,
            to: to.id,
            discovery_type: "transitive".into(),
            quality: 0.6,
            via_types: [1, 2],
            discovery_cycle_id: "malformed-cycle".into(),
            discovered_at: Utc::now(),
        };
        s.store_discovery(&record).await.unwrap();
        let scope = VisibilityContext {
            user_id: "user-1".into(),
            org_id: "test-org".into(),
            max_visibility: Visibility::Org,
            ..Default::default()
        };
        let kind = exocortex_kernel::kinds::FIXES;
        let proposal = DiscoveryProposal {
            discovery_id: record.discovery_id.clone(),
            region: region.clone(),
            from: from.id,
            to: to.id,
            kind,
            proposed_visibility: Visibility::Org,
            caller_scope: scope.clone(),
            issued_at: Utc::now(),
        };
        s.create_discovery_proposal(&proposal).await.unwrap();
        s.query_cypher(&CypherQuery {
            template_id: "integration_corrupt_discovery_proposal",
            params: serde_json::json!({
                "discovery_id": proposal.discovery_id,
                "props_json": "not-json",
            }),
            read_only: false,
            deadline: Utc::now() + Duration::seconds(5),
        })
        .await
        .unwrap();
        assert!(matches!(
            s.get_discovery_proposal(&proposal.discovery_id).await,
            Err(StorageError::CorruptMetadata {
                key: "discovery_proposal",
                ..
            })
        ));
        let relationship = rel(from.id, to.id, kind.0);
        assert!(matches!(
            s.accept_discovery(&DiscoveryAcceptance {
                discovery_id: proposal.discovery_id.clone(),
                region,
                caller_scope: scope,
                relationship: relationship.clone(),
                audit: AuditEvent {
                    action: "accept_discovery".into(),
                    actor: "user-1".into(),
                    org_id: "test-org".into(),
                    input_digest: [5; 32],
                    output_ids: Default::default(),
                    fingerprint: s.ontology_fingerprint(),
                    lease_epoch: None,
                    recorded_at: Utc::now(),
                },
            })
            .await,
            Err(StorageError::CorruptMetadata {
                key: "discovery_proposal",
                ..
            })
        ));
        let persisted = s
            .query_cypher(&CypherQuery {
                template_id: "discovery_proposal_get",
                params: serde_json::json!({ "discovery_id": proposal.discovery_id }),
                read_only: true,
                deadline: Utc::now() + Duration::seconds(5),
            })
            .await
            .unwrap();
        assert_eq!(
            persisted.rows,
            vec![serde_json::json!(["not-json"])],
            "failed acceptance preserves the unconsumed corrupt proposal verbatim"
        );
        assert!(s
            .get_relationship(&relationship.id)
            .await
            .unwrap()
            .is_none());
        assert!(s.audit_range("test-org", 0, 10).await.unwrap().is_empty());
    }
);

itest!(
    wrong_type_discovery_proposal_fails_closed_without_mutation,
    {
        let s = connect("wrong-type-proposal").await;
        let from = mem("wrong-type-from", 3, Visibility::Org);
        let to = mem("wrong-type-to", 3, Visibility::Org);
        s.upsert_batch(&[from.clone(), to.clone()], &[])
            .await
            .unwrap();
        let region = RegionKey {
            org: "test-org".into(),
            project: "*".into(),
            memory_type: from.memory_type,
        };
        let record = DiscoveryRecord {
            discovery_id: "wrong-type-proposal".into(),
            region: region.clone(),
            from: from.id,
            to: to.id,
            discovery_type: "transitive".into(),
            quality: 0.6,
            via_types: [1, 2],
            discovery_cycle_id: "wrong-type-cycle".into(),
            discovered_at: Utc::now(),
        };
        s.store_discovery(&record).await.unwrap();
        let scope = VisibilityContext {
            user_id: "user-1".into(),
            org_id: "test-org".into(),
            max_visibility: Visibility::Org,
            ..Default::default()
        };
        let kind = exocortex_kernel::kinds::FIXES;
        let proposal = DiscoveryProposal {
            discovery_id: record.discovery_id.clone(),
            region: region.clone(),
            from: from.id,
            to: to.id,
            kind,
            proposed_visibility: Visibility::Org,
            caller_scope: scope.clone(),
            issued_at: Utc::now(),
        };
        s.create_discovery_proposal(&proposal).await.unwrap();
        s.query_cypher(&CypherQuery {
            template_id: "integration_corrupt_discovery_proposal",
            params: serde_json::json!({
                "discovery_id": proposal.discovery_id,
                "props_json": 17,
            }),
            read_only: false,
            deadline: Utc::now() + Duration::seconds(5),
        })
        .await
        .unwrap();
        assert!(matches!(
            s.get_discovery_proposal(&proposal.discovery_id).await,
            Err(StorageError::CorruptMetadata {
                key: "discovery_proposal",
                ..
            })
        ));
        let relationship = rel(from.id, to.id, kind.0);
        assert!(matches!(
            s.accept_discovery(&DiscoveryAcceptance {
                discovery_id: proposal.discovery_id.clone(),
                region,
                caller_scope: scope,
                relationship: relationship.clone(),
                audit: AuditEvent {
                    action: "accept_discovery".into(),
                    actor: "user-1".into(),
                    org_id: "test-org".into(),
                    input_digest: [6; 32],
                    output_ids: Default::default(),
                    fingerprint: s.ontology_fingerprint(),
                    lease_epoch: None,
                    recorded_at: Utc::now(),
                },
            })
            .await,
            Err(StorageError::CorruptMetadata {
                key: "discovery_proposal",
                ..
            })
        ));
        let persisted = s
            .query_cypher(&CypherQuery {
                template_id: "discovery_proposal_get",
                params: serde_json::json!({ "discovery_id": proposal.discovery_id }),
                read_only: true,
                deadline: Utc::now() + Duration::seconds(5),
            })
            .await
            .unwrap();
        assert_eq!(
            persisted.rows,
            vec![serde_json::json!([17])],
            "failed acceptance preserves the unconsumed wrong-type proposal verbatim"
        );
        assert!(s
            .get_relationship(&relationship.id)
            .await
            .unwrap()
            .is_none());
        assert!(s.audit_range("test-org", 0, 10).await.unwrap().is_empty());
    }
);

itest!(three_hop_traverse, {
    let s = connect("node-1").await;
    let a = mem("A", 4, Visibility::Org);
    let b = mem("B", 5, Visibility::Org);
    let c = mem("C", 6, Visibility::Org);
    let d = mem("D", 7, Visibility::Org);
    for m in [&a, &b, &c, &d] {
        s.upsert_memory(m).await.unwrap();
    }
    let kind = exocortex_kernel::kinds::SOLVES;
    s.upsert_relationship(&rel(a.id, b.id, kind.0))
        .await
        .unwrap();
    s.upsert_relationship(&rel(b.id, c.id, kind.0))
        .await
        .unwrap();
    s.upsert_relationship(&rel(c.id, d.id, kind.0))
        .await
        .unwrap();

    let out = s.traverse(&a.id, &spec(3, 100)).await.expect("traverse");
    let mut titles: Vec<_> = out.iter().map(|m| m.title.to_string()).collect();
    titles.sort();
    assert_eq!(titles, vec!["B", "C", "D"], "3-hop chain reachable");

    let one = s.traverse(&a.id, &spec(1, 100)).await.unwrap();
    assert_eq!(one.len(), 1, "depth bound respected");
});

itest!(traverse_applies_exact_caller_visibility, {
    let s = connect("traverse-visibility").await;
    let seed = mem("seed", 4, Visibility::Org);
    let mut private_ok = mem("private-ok", 4, Visibility::Private);
    private_ok.context.user_id = Some("user-1".into());
    let mut private_foreign = mem("private-foreign", 4, Visibility::Private);
    private_foreign.context.user_id = Some("user-2".into());
    let mut project_ok = mem("project-ok", 4, Visibility::Project);
    project_ok.context.project_id = Some("allowed-project".into());
    let mut project_foreign = mem("project-foreign", 4, Visibility::Project);
    project_foreign.context.project_id = Some("other-project".into());
    let mut team_ok = mem("team-ok", 4, Visibility::Team);
    team_ok.context.team_id = Some("allowed-team".into());
    let mut team_foreign = mem("team-foreign", 4, Visibility::Team);
    team_foreign.context.team_id = Some("other-team".into());
    let mut tenant_foreign = mem("tenant-foreign", 4, Visibility::Org);
    tenant_foreign.context.tenant_id = Some("other-org".into());
    let rows = [
        seed.clone(),
        private_ok.clone(),
        private_foreign.clone(),
        project_ok.clone(),
        project_foreign.clone(),
        team_ok.clone(),
        team_foreign.clone(),
        tenant_foreign.clone(),
    ];
    s.upsert_batch(&rows, &[]).await.unwrap();
    let kind = exocortex_kernel::kinds::SOLVES;
    let edges: Vec<_> = rows
        .iter()
        .skip(1)
        .map(|target| rel(seed.id, target.id, kind.0))
        .collect();
    s.upsert_batch(&[], &edges).await.unwrap();
    let mut traversal = spec(1, 100);
    traversal.visibility_ctx.project_ids = ["allowed-project".into()].into_iter().collect();
    traversal.visibility_ctx.team_ids = ["allowed-team".into()].into_iter().collect();
    let visible = s.traverse(&seed.id, &traversal).await.unwrap();
    let titles: std::collections::HashSet<_> =
        visible.iter().map(|memory| memory.title.as_str()).collect();
    assert_eq!(
        titles,
        ["private-ok", "project-ok", "team-ok"]
            .into_iter()
            .collect()
    );
});

itest!(
    traverse_never_crosses_hidden_intermediates_and_honors_direction,
    {
        let s = connect("traverse-hidden-intermediates").await;
        let seed = mem("seed", 4, Visibility::Org);
        let visible_out = mem("visible-out", 4, Visibility::Org);
        let visible_in = mem("visible-in", 4, Visibility::Org);
        let scoped_edge_target = mem("scoped-edge-target", 4, Visibility::Org);
        let mut hidden = vec![
            mem("private-hidden", 4, Visibility::Private),
            mem("project-hidden", 4, Visibility::Project),
            mem("team-hidden", 4, Visibility::Team),
            mem("tenant-hidden", 4, Visibility::Org),
        ];
        hidden[0].context.user_id = Some("other-user".into());
        hidden[1].context.project_id = Some("other-project".into());
        hidden[2].context.team_id = Some("other-team".into());
        hidden[3].context.tenant_id = Some("other-org".into());
        let terminals: Vec<_> = (0..hidden.len())
            .map(|index| mem(&format!("terminal-{index}"), 4, Visibility::Org))
            .collect();
        let mut memories = vec![
            seed.clone(),
            visible_out.clone(),
            visible_in.clone(),
            scoped_edge_target.clone(),
        ];
        memories.extend(hidden.iter().cloned());
        memories.extend(terminals.iter().cloned());
        s.upsert_batch(&memories, &[]).await.unwrap();
        let kind = exocortex_kernel::kinds::SOLVES;
        let mut edges = vec![
            rel(seed.id, visible_out.id, kind.0),
            rel(visible_in.id, seed.id, kind.0),
        ];
        let mut subjectless_project_edge = rel(seed.id, scoped_edge_target.id, kind.0);
        subjectless_project_edge.visibility = Visibility::Project;
        edges.push(subjectless_project_edge);
        for (intermediate, terminal) in hidden.iter().zip(&terminals) {
            edges.push(rel(seed.id, intermediate.id, kind.0));
            edges.push(rel(intermediate.id, terminal.id, kind.0));
        }
        s.upsert_batch(&[], &edges).await.unwrap();
        let mut traversal = spec(2, 100);
        traversal.kinds.push(kind);
        traversal.visibility_ctx.project_ids = ["proj".into()].into_iter().collect();

        traversal.direction = exocortex_storage::Direction::Out;
        let out = s.traverse(&seed.id, &traversal).await.unwrap();
        assert_eq!(
            out.iter().map(|memory| memory.id).collect::<Vec<_>>(),
            [visible_out.id]
        );

        traversal.direction = exocortex_storage::Direction::In;
        let incoming = s.traverse(&seed.id, &traversal).await.unwrap();
        assert_eq!(
            incoming.iter().map(|memory| memory.id).collect::<Vec<_>>(),
            [visible_in.id]
        );

        traversal.direction = exocortex_storage::Direction::Both;
        let both: std::collections::HashSet<_> = s
            .traverse(&seed.id, &traversal)
            .await
            .unwrap()
            .into_iter()
            .map(|memory| memory.id)
            .collect();
        assert_eq!(both, [visible_out.id, visible_in.id].into_iter().collect());
    }
);

itest!(
    discovery_records_survive_reconnect_and_fail_closed_on_corruption,
    {
        let graph = format!("exocortex_test_{}", graph_suffix());
        let s = connect_graph("discovery-record-writer", graph.clone()).await;
        let now = Utc::now();
        let make_record = |id: &str, org: &str, seconds: i64| DiscoveryRecord {
            discovery_id: id.into(),
            region: RegionKey {
                org: org.into(),
                project: "proj".into(),
                memory_type: 3,
            },
            from: MemoryId::new_v7(),
            to: MemoryId::new_v7(),
            discovery_type: "transitive".into(),
            quality: 0.6,
            via_types: [1, 2],
            discovery_cycle_id: "cycle".into(),
            discovered_at: now + Duration::seconds(seconds),
        };
        let old = make_record("record-old", "test-org", 0);
        let new = make_record("record-new", "test-org", 1);
        let foreign = make_record("record-foreign", "foreign-org", 2);
        for record in [&old, &new, &foreign] {
            s.store_discovery(record).await.unwrap();
        }
        s.store_discovery(&old).await.unwrap();
        let mut conflict = old.clone();
        conflict.quality = 0.9;
        assert!(matches!(
            s.store_discovery(&conflict).await,
            Err(StorageError::ProposalMismatch)
        ));
        drop(s);

        let reconnected = connect_graph("discovery-record-reader", graph).await;
        assert_eq!(
            reconnected.get_discovery("record-old").await.unwrap(),
            Some(old)
        );
        let listed = reconnected.list_discoveries("test-org", 1).await.unwrap();
        assert_eq!(listed, vec![new]);
        assert!(reconnected
            .list_discoveries("missing-org", 10)
            .await
            .unwrap()
            .is_empty());
        reconnected
            .query_cypher(&CypherQuery {
                template_id: "integration_corrupt_discovery_record",
                params: serde_json::json!({
                    "discovery_id": "record-new",
                    "props_json": "not-json",
                }),
                read_only: false,
                deadline: Utc::now() + Duration::seconds(5),
            })
            .await
            .unwrap();
        assert!(matches!(
            reconnected.get_discovery("record-new").await,
            Err(StorageError::CorruptMetadata {
                key: "discovery_record",
                ..
            })
        ));
        assert!(matches!(
            reconnected.list_discoveries("test-org", 10).await,
            Err(StorageError::CorruptMetadata {
                key: "discovery_record",
                ..
            })
        ));
    }
);

itest!(discovery_publish_is_durable_ordered_and_idempotent_live, {
    let storage = connect("discovery-publication").await;
    let region = RegionKey {
        org: "test-org".into(),
        project: "proj".into(),
        memory_type: 3,
    };
    let mut feed = storage.subscribe_invalidations(&region).await.unwrap();
    tokio::time::sleep(StdDuration::from_millis(150)).await;
    let record = DiscoveryRecord {
        discovery_id: format!("discovery-event-{}", graph_suffix()).into(),
        region: region.clone(),
        from: MemoryId::new_v7(),
        to: MemoryId::new_v7(),
        discovery_type: "transitive".into(),
        quality: 0.7,
        via_types: [1, 2],
        discovery_cycle_id: "publication-cycle".into(),
        discovered_at: Utc::now(),
    };

    storage.store_discovery(&record).await.unwrap();
    let event_lsn = tokio::time::timeout(StdDuration::from_secs(2), async {
        loop {
            match feed
                .next()
                .await
                .expect("publication stream remains open")
                .expect("publication decodes")
            {
                Invalidation::DiscoveryAvailable {
                    record: published,
                    lsn,
                } if published.discovery_id == record.discovery_id => {
                    assert_eq!(published, record);
                    break lsn;
                }
                _ => {}
            }
        }
    })
    .await
    .expect("new discovery publishes");
    assert_eq!(
        storage.get_discovery(&record.discovery_id).await.unwrap(),
        Some(record.clone()),
        "the durable record is readable when publication is observed"
    );
    let persisted_lsn = storage
        .query_cypher(&CypherQuery {
            template_id: "integration_get_discovery_lsn",
            params: serde_json::json!({ "discovery_id": record.discovery_id }),
            read_only: true,
            deadline: Utc::now() + Duration::seconds(5),
        })
        .await
        .unwrap();
    assert_eq!(persisted_lsn.rows, vec![serde_json::json!([event_lsn])]);

    storage.store_discovery(&record).await.unwrap();
    assert!(
        tokio::time::timeout(StdDuration::from_millis(100), async {
            loop {
                if matches!(
                    feed.next().await,
                    Some(Ok(Invalidation::DiscoveryAvailable { record: published, .. }))
                        if published.discovery_id == record.discovery_id
                ) {
                    break;
                }
            }
        })
        .await
        .is_err(),
        "an immutable exact retry must not publish a duplicate event"
    );
});

itest!(discovery_outbox_retries_after_live_publication_failure, {
    let storage = connect("discovery-outbox-retry").await;
    let region = RegionKey {
        org: "test-org".into(),
        project: "proj".into(),
        memory_type: 3,
    };
    let graph = storage.graph_name_clone();
    let record = DiscoveryRecord {
        discovery_id: format!("discovery-retry-{}", graph_suffix()).into(),
        region: region.clone(),
        from: MemoryId::new_v7(),
        to: MemoryId::new_v7(),
        discovery_type: "transitive".into(),
        quality: 0.7,
        via_types: [1, 2],
        discovery_cycle_id: "retry-cycle".into(),
        discovered_at: Utc::now(),
    };
    storage.fail_next_publish_for_testing();
    assert!(storage.store_discovery(&record).await.is_err());
    assert_eq!(
        storage.get_discovery(&record.discovery_id).await.unwrap(),
        Some(record.clone()),
        "publication failure does not roll back the durable outbox record"
    );

    let url = falkor_url().unwrap();
    let recovery = FalkorStorage::connect(
        FalkorConfig {
            falkor_url: url.clone(),
            redis_url: std::env::var("REDIS_URL")
                .unwrap_or_else(|_| url.replacen("falkor://", "redis://", 1)),
            graph_name: graph,
            org_id: "test-org".into(),
            node_id: "discovery-recovery".into(),
        },
        ontology(),
    )
    .await
    .unwrap();
    let mut feed = recovery.subscribe_invalidations(&region).await.unwrap();
    let published = tokio::time::timeout(StdDuration::from_secs(2), async {
        loop {
            if let Some(Ok(Invalidation::DiscoveryAvailable {
                record: observed,
                lsn,
            })) = feed.next().await
            {
                if observed.discovery_id == record.discovery_id {
                    break lsn;
                }
            }
        }
    })
    .await
    .expect("durable pending event is retried");
    let persisted = storage
        .query_cypher(&CypherQuery {
            template_id: "integration_get_discovery_lsn",
            params: serde_json::json!({ "discovery_id": record.discovery_id }),
            read_only: true,
            deadline: Utc::now() + Duration::seconds(5),
        })
        .await
        .unwrap();
    assert_eq!(persisted.rows, vec![serde_json::json!([published])]);
});

itest!(
    relationships_in_region_is_exact_ordered_and_budgeted_live,
    {
        let s = connect("region-relationships").await;
        let mut a = mem("region-a", 3, Visibility::Org);
        let mut b = mem("region-b", 3, Visibility::Org);
        let mut foreign_project = mem("region-foreign-project", 3, Visibility::Org);
        foreign_project.context.project_id = Some("other".into());
        let foreign_type = mem("region-foreign-type", 4, Visibility::Org);
        for memory in [&mut a, &mut b] {
            memory.context.project_id = Some("proj".into());
        }
        s.upsert_batch(
            &[
                a.clone(),
                b.clone(),
                foreign_project.clone(),
                foreign_type.clone(),
            ],
            &[],
        )
        .await
        .unwrap();
        s.upsert_batch(
            &[],
            &[
                rel(a.id, foreign_project.id, 1),
                rel(a.id, foreign_type.id, 1),
                rel(a.id, b.id, 1),
            ],
        )
        .await
        .unwrap();
        let region = RegionKey {
            org: "test-org".into(),
            project: "proj".into(),
            memory_type: 3,
        };
        let rows = s.relationships_in_region(&region, 2).await.unwrap();
        assert_eq!(rows.len(), 2, "forward plus inverse in-region edge");
        assert!(rows.windows(2).all(|pair| {
            (pair[0].from, pair[0].to, pair[0].kind, pair[0].id)
                <= (pair[1].from, pair[1].to, pair[1].kind, pair[1].id)
        }));
        assert!(s.relationships_in_region(&region, 1).await.is_err());
    }
);

itest!(bi_temporal_valid_at, {
    let s = connect("node-1").await;
    let t0 = Utc::now() - Duration::hours(2);
    let t1 = Utc::now() - Duration::hours(1);
    let mut windowed = mem("vt", 2, Visibility::Org);
    windowed.valid_from = t0;
    windowed.valid_until = Some(t1);
    windowed.recorded_at = t0;
    s.upsert_memory(&windowed).await.unwrap();

    let at0 = s
        .valid_at(&windowed.id, t0)
        .await
        .unwrap()
        .expect("valid inside window");
    assert_eq!(at0.id, windowed.id);
    assert!(
        s.valid_at(&windowed.id, t1).await.unwrap().is_none(),
        "window closed at t1"
    );
    assert!(
        s.valid_at(&windowed.id, t0 - Duration::hours(1))
            .await
            .unwrap()
            .is_none(),
        "not valid before t0"
    );

    // A later external snapshot of the same identity appends an assertion;
    // it must not destroy the earlier validity window.
    let mut successor = windowed.clone();
    successor.content = "later snapshot".into();
    successor.valid_from = t1;
    successor.valid_until = None;
    successor.recorded_at = t1;
    s.upsert_memory(&successor).await.unwrap();
    let old = s
        .valid_at(&windowed.id, t0)
        .await
        .unwrap()
        .expect("earlier assertion remains addressable");
    assert_ne!(old.content, successor.content);
    let at1 = s
        .valid_at(&successor.id, t1)
        .await
        .unwrap()
        .expect("successor valid at t1");
    assert_eq!(at1.id, successor.id);
    assert_eq!(at1.content, successor.content);
    let t2 = t1 + Duration::seconds(1);
    let mut correction = successor.clone();
    correction.content = "later correction".into();
    correction.valid_from = t0;
    correction.recorded_at = t2;
    s.upsert_memory(&correction).await.unwrap();
    assert_eq!(
        s.valid_at(&successor.id, t1)
            .await
            .unwrap()
            .unwrap()
            .content,
        successor.content,
        "future-recorded correction must not leak into an earlier knowledge cut"
    );
    assert_eq!(
        s.valid_at(&successor.id, t2)
            .await
            .unwrap()
            .unwrap()
            .content,
        correction.content
    );
    assert_eq!(
        s.get_memory(&successor.id).await.unwrap().unwrap().content,
        correction.content,
        "ordinary reads expose only the current row"
    );
});

itest!(lease_race_single_winner, {
    let a = connect("node-A").await;
    // Cluster peers for one org share the data graph. The lease lives in that
    // graph so its epoch guard and the owner mutation can be one atomic query.
    let url = falkor_url().unwrap();
    let b = FalkorStorage::connect(
        FalkorConfig {
            falkor_url: url.clone(),
            redis_url: std::env::var("REDIS_URL")
                .unwrap_or_else(|_| url.replacen("falkor://", "redis://", 1)),
            graph_name: a.graph_name_clone(),
            org_id: "test-org".into(),
            node_id: "node-B".into(),
        },
        ontology(),
    )
    .await
    .expect("connect peer to shared graph");
    let key = LeaseKey::Dreams {
        org: format!("race-{}", graph_suffix()).into(),
        region: "p:1".into(),
    };
    let first: OwnerLease = a
        .acquire_lease(&key, StdDuration::from_secs(30))
        .await
        .expect("A wins");
    assert!(
        b.acquire_lease(&key, StdDuration::from_secs(30))
            .await
            .is_err(),
        "B must lose the race"
    );
    a.release_lease(first).await.unwrap();
    let second = b
        .acquire_lease(&key, StdDuration::from_secs(30))
        .await
        .expect("B wins after release");
    assert!(second.epoch > 0, "epoch fencing increments");
});

itest!(
    cross_node_durable_mutations_cannot_commit_below_graph_frontier,
    {
        let a = Arc::new(connect("ordered-node-a").await);
        let url = falkor_url().unwrap();
        let b = FalkorStorage::connect(
            FalkorConfig {
                falkor_url: url.clone(),
                redis_url: std::env::var("REDIS_URL")
                    .unwrap_or_else(|_| url.replacen("falkor://", "redis://", 1)),
                graph_name: a.graph_name_clone(),
                org_id: "test-org".into(),
                node_id: "ordered-node-b".into(),
            },
            ontology(),
        )
        .await
        .unwrap();
        let lower = mem("lower-lsn", 3, Visibility::Org);
        let mut higher = lower.clone();
        higher.title = "higher-lsn".into();

        a.pause_next_lsn_for_testing();
        let delayed = {
            let a = a.clone();
            tokio::spawn(async move { a.upsert_memory(&lower).await })
        };
        a.wait_for_paused_lsn_for_testing().await;
        let high_commit = b.upsert_memory(&higher).await.unwrap();
        a.release_paused_lsn_for_testing();
        let low_result = delayed.await.unwrap();
        assert!(
            low_result.is_err(),
            "a lower allocated LSN cannot commit late"
        );
        let current = b.get_memory(&higher.id).await.unwrap().unwrap();
        assert_eq!(current.title, higher.title);
        assert_eq!(current.lsn.value, high_commit.lsn);
    }
);

itest!(invalidation_end_to_end, {
    let sub = connect("sub-node").await;
    let publisher_graph = sub.graph_name_clone();
    let org = format!("inv-{}", graph_suffix());
    // Rebind the publisher to the SAME graph/org so the channel matches.
    let url = falkor_url().unwrap();
    let publisher = FalkorStorage::connect(
        FalkorConfig {
            falkor_url: url.clone(),
            redis_url: std::env::var("REDIS_URL")
                .unwrap_or_else(|_| url.replacen("falkor://", "redis://", 1)),
            graph_name: publisher_graph,
            org_id: org.clone().into(),
            node_id: "pub-node".into(),
        },
        ontology(),
    )
    .await
    .unwrap();
    let _ = org;

    let region = RegionKey {
        org: "test-org".into(),
        project: "*".into(),
        memory_type: 0,
    };
    let mut stream = publisher
        .subscribe_invalidations(&region)
        .await
        .expect("subscribe");
    // Give the subscription a moment to establish.
    tokio::time::sleep(StdDuration::from_millis(150)).await;
    let m = mem("invalidated", 1, Visibility::Org);
    publisher.upsert_memory(&m).await.expect("upsert");

    let deadline = std::time::Instant::now() + StdDuration::from_secs(5);
    let mut got = None;
    while std::time::Instant::now() < deadline {
        match tokio::time::timeout(StdDuration::from_millis(500), stream.next()).await {
            Ok(Some(Ok(inv))) => {
                got = Some(inv);
                break;
            }
            Ok(Some(Err(e))) => panic!("stream error: {e:?}"),
            Ok(None) => break,
            Err(_) => continue,
        }
    }
    match got.expect("invalidation observed") {
        Invalidation::MemoryUpserted { id, lsn } => {
            assert_eq!(id, m.id);
            assert!(lsn > 0);
        }
        other => panic!("expected MemoryUpserted, got {other:?}"),
    }
});

itest!(batch_invalidation_publish_uses_one_redis_round_trip, {
    let url = falkor_url().unwrap();
    let org = format!("batch-publish-{}", graph_suffix());
    let publisher = FalkorStorage::connect(
        FalkorConfig {
            falkor_url: url.clone(),
            redis_url: std::env::var("REDIS_URL")
                .unwrap_or_else(|_| url.replacen("falkor://", "redis://", 1)),
            graph_name: format!("exocortex_test_{}", graph_suffix()),
            org_id: org.clone().into(),
            node_id: "batch-publisher".into(),
        },
        ontology(),
    )
    .await
    .unwrap();
    let region = RegionKey {
        org: org.into(),
        project: "*".into(),
        memory_type: 0,
    };
    let mut stream = publisher
        .subscribe_invalidations(&region)
        .await
        .expect("subscribe before batch commit");
    tokio::time::sleep(StdDuration::from_millis(100)).await;
    publisher.take_publish_round_trips_for_testing();
    let memories = (0..64)
        .map(|index| mem(&format!("publish-batch-{index}"), 3, Visibility::Org))
        .collect::<Vec<_>>();
    publisher.upsert_batch(&memories, &[]).await.unwrap();
    assert_eq!(
        publisher.take_publish_round_trips_for_testing(),
        1,
        "all compatible feed frames must share one Redis pipeline request"
    );

    let expected = memories
        .iter()
        .map(|memory| memory.id)
        .collect::<std::collections::HashSet<_>>();
    let mut observed = std::collections::HashSet::new();
    let receive = async {
        while observed.len() < expected.len() {
            match stream.next().await {
                Some(Ok(Invalidation::MemoryUpserted { id, .. })) => {
                    observed.insert(id);
                }
                Some(Ok(other)) => panic!("unexpected batch invalidation: {other:?}"),
                Some(Err(error)) => panic!("batch stream error: {error:?}"),
                None => panic!("batch stream ended early"),
            }
        }
    };
    tokio::time::timeout(StdDuration::from_secs(5), receive)
        .await
        .expect("every pipelined compatibility frame is delivered");
    assert_eq!(observed, expected);
});

itest!(stream_memories_roundtrip, {
    let s = connect("node-1").await;
    for i in 0..10 {
        s.upsert_memory(&mem(&format!("stream-{i}"), i % 13, Visibility::Org))
            .await
            .unwrap();
    }
    let mut n = 0;
    let mut stream = s.stream_all_memories().await;
    while let Some(row) = stream.next().await {
        row.expect("row");
        n += 1;
    }
    assert_eq!(n, 10, "stream returns every upserted memory");
});

itest!(bulk_streams_fetch_pages_only_when_consumed, {
    let s = connect("lazy-streams").await;
    let a = mem("lazy-a", 3, Visibility::Org);
    let b = mem("lazy-b", 3, Visibility::Org);
    s.upsert_batch(&[a.clone(), b.clone()], &[rel(a.id, b.id, 1)])
        .await
        .unwrap();
    s.take_stream_page_counts();

    let mut memories = s.stream_all_memories().await;
    memories.next().await.unwrap().unwrap();
    drop(memories);
    assert_eq!(
        s.take_stream_page_counts(),
        (1, 0),
        "dropping an early memory consumer must not fetch the terminal page"
    );

    let mut relationships = s.stream_all_relationships().await;
    relationships.next().await.unwrap().unwrap();
    drop(relationships);
    assert_eq!(
        s.take_stream_page_counts(),
        (0, 1),
        "dropping an early relationship consumer must not fetch the terminal page"
    );
});

itest!(
    bulk_streams_validate_entire_cursor_page_before_yielding_rows,
    {
        let s = connect("corrupt-stream-cursor").await;
        let from = mem("cursor-from", 3, Visibility::Org);
        let to = mem("cursor-to", 3, Visibility::Org);
        let relationship = rel(from.id, to.id, exocortex_kernel::kinds::FIXES.0);
        s.upsert_batch(
            &[from.clone(), to.clone()],
            std::slice::from_ref(&relationship),
        )
        .await
        .unwrap();
        for (template_id, params) in [
            (
                "integration_corrupt_memory_stream_lsn",
                serde_json::json!({ "id": id_hex(&from.id.0), "lsn": 1 }),
            ),
            (
                "integration_corrupt_memory_stream_lsn",
                serde_json::json!({ "id": id_hex(&to.id.0), "lsn": 1 }),
            ),
            (
                "integration_corrupt_relationship_stream_lsn",
                serde_json::json!({ "rel_id": id_hex(&relationship.id.0), "lsn": -1 }),
            ),
        ] {
            s.query_cypher(&CypherQuery {
                template_id,
                params,
                read_only: false,
                deadline: Utc::now() + Duration::seconds(5),
            })
            .await
            .unwrap();
        }
        let mut memories = s.stream_all_memories().await;
        assert!(matches!(
            memories.next().await,
            Some(Err(StorageError::CorruptMetadata {
                key: "stream_cursor",
                ..
            }))
        ));
        drop(memories);
        let mut relationships = s.stream_all_relationships().await;
        assert!(matches!(
            relationships.next().await,
            Some(Err(StorageError::CorruptMetadata {
                key: "stream_cursor",
                ..
            }))
        ));
    }
);
