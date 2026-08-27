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
    AuditEvent, CypherQuery, DiscoveryAcceptance, DiscoveryProposal, FalkorConfig, FalkorStorage,
    IngestBatchKey, IngestCommitOutcome, Invalidation, LeaseKey, MemoryFilter, OwnerLease,
    RegionKey, Storage, StorageError, TraversalSpec, VisibilityContext,
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

async fn connect(node: &str) -> FalkorStorage {
    let url = falkor_url().expect("FALKOR_URL set (checked by runner)");
    let cfg = FalkorConfig {
        falkor_url: url.clone(),
        redis_url: std::env::var("REDIS_URL")
            .unwrap_or_else(|_| url.replacen("falkor://", "redis://", 1)),
        graph_name: format!("exocortex_test_{}", graph_suffix()),
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
            tenant_id: None,
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
    assert!(matches!(outcome, IngestCommitOutcome::Committed { .. }));
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
    assert!(matches!(replay, IngestCommitOutcome::Duplicate(_)));
    assert!(restarted.get_memory(&committed.id).await.unwrap().is_some());
    assert!(restarted
        .get_memory(&must_not_commit.id)
        .await
        .unwrap()
        .is_none());
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
        s.create_discovery_proposal(&proposal).await.unwrap();
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
        let audits = s.audit_range("test-org", 0, 10).await.unwrap();
        assert_eq!(audits.len(), 1);
        assert_eq!(audits[0]["action"], "accept_discovery");
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

itest!(bi_temporal_valid_at, {
    // The §6.4 upsert template MERGEs memories by id (history-preserving
    // supersession uses distinct rows + `invalidated_by`, not in-place
    // versions), so bi-temporality is asserted over validity windows here.
    let s = connect("node-1").await;
    let t0 = Utc::now() - Duration::hours(2);
    let t1 = Utc::now() - Duration::hours(1);
    let mut windowed = mem("vt", 2, Visibility::Org);
    windowed.valid_from = t0;
    windowed.valid_until = Some(t1);
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

    // Supersession with a fresh row: the old row closes, the new one opens.
    let mut successor = mem("vt2", 2, Visibility::Org);
    successor.valid_from = t1;
    s.upsert_memory(&successor).await.unwrap();
    let at1 = s
        .valid_at(&successor.id, t1)
        .await
        .unwrap()
        .expect("successor valid at t1");
    assert_eq!(at1.id, successor.id);
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
