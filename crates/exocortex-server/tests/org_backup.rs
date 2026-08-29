//! BR2 acceptance (BR-PRD's deferred backend leg): the durable org
//! store as a portable file — round trip across storage instances with
//! byte-faithful rows, fingerprint and org gates, idempotent re-import.

use exocortex_kernel::{
    Memory, MemoryContext, MemoryId, Provenance, Relationship, RelationshipId,
    RelationshipProperties, Visibility, LSN,
};
use exocortex_server::org_backup;
use exocortex_storage::{InMemoryStorage, RegionKey, Storage};
use futures::StreamExt;
use std::sync::Arc;

fn ontology() -> Arc<exocortex_kernel::Ontology> {
    let _ = std::hint::black_box(exocortex_pack_dev_v1::pack_def().name.clone());
    Arc::new(exocortex_kernel::pack::load_registered_packs().unwrap())
}

fn fingerprint_hex(o: &exocortex_kernel::Ontology) -> String {
    use std::fmt::Write as _;
    let mut s = String::with_capacity(64);
    for b in o.fingerprint.0 {
        let _ = write!(s, "{b:02x}");
    }
    s
}

fn test_mem(title: &str, n: u8) -> Memory {
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
            additional_metadata: serde_json::Value::Null,
            ..serde_memory_context_defaults()
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
        lsn: LSN::new_backend(7),
    }
}

fn serde_memory_context_defaults() -> MemoryContext {
    MemoryContext {
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
    }
}

fn backup_document(
    ontology: &exocortex_kernel::Ontology,
    memories: Vec<Memory>,
    relationships: Vec<Relationship>,
) -> org_backup::OrgBackup {
    org_backup::OrgBackup {
        format: org_backup::FORMAT.into(),
        version: org_backup::VERSION,
        created_at: chrono::Utc::now().to_rfc3339(),
        org_id: "org".into(),
        ontology_fingerprint: fingerprint_hex(ontology),
        ontology_summary: Some(ontology.summary.clone()),
        memories,
        relationships,
    }
}

fn test_rel(from: MemoryId, to: MemoryId, kind: exocortex_kernel::RelKindId) -> Relationship {
    Relationship {
        id: RelationshipId::derive(from, kind, to, None),
        kind,
        from,
        to,
        visibility: Visibility::Org,
        provenance: Provenance::Asserted {
            author: "t".into(),
            producer_kind: None,
        },
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
        lsn: LSN::new_backend(7),
    }
}

fn valid_noncomputed_triple(
    ontology: &exocortex_kernel::Ontology,
) -> (u8, u8, exocortex_kernel::RelKindId) {
    ontology
        .triples_by_kind
        .keys()
        .filter(|kind| {
            ontology.kinds_by_id.get(kind).is_some_and(|metadata| {
                !metadata.computed_only
                    && !metadata.bidirectional
                    && metadata.inverse.is_some_and(|inverse| inverse != **kind)
            })
        })
        .find_map(|kind| {
            (0..ontology.memory_type_names.len() as u8).find_map(|from| {
                (0..ontology.memory_type_names.len() as u8)
                    .find(|to| {
                        exocortex_kernel::validator::validate_triple(ontology, from, *kind, *to)
                            .is_ok()
                    })
                    .map(|to| (from, to, *kind))
            })
        })
        .expect("dev ontology has a non-computed valid triple")
}

async fn rows<S: Storage>(s: &S) -> (Vec<Memory>, Vec<Relationship>) {
    let mut ms = s.stream_all_memories().await;
    let mut memories = Vec::new();
    while let Some(memory) = ms.next().await {
        memories.push(memory.expect("memory stream must remain readable"));
    }
    let mut rs = s.stream_all_relationships().await;
    let mut rels = Vec::new();
    while let Some(relationship) = rs.next().await {
        rels.push(relationship.expect("relationship stream must remain readable"));
    }
    (memories, rels)
}

#[tokio::test]
async fn org_round_trip_is_byte_faithful_across_storage_instances() {
    let onto = ontology();
    let fp = fingerprint_hex(&onto);
    let a = InMemoryStorage::new(onto.clone());
    let (from_type, to_type, kind) = valid_noncomputed_triple(&onto);
    let mut m1 = test_mem("auth-bridge", 1);
    m1.memory_type = from_type;
    let mut m2 = test_mem("policy-engine", 2);
    m2.memory_type = to_type;
    let rel = test_rel(m1.id, m2.id, kind);
    a.upsert_memory(&m1).await.unwrap();
    a.upsert_memory(&m2).await.unwrap();
    a.upsert_relationship(&rel).await.unwrap();
    let (_, mut a_rels) = rows(&a).await;

    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("org.json");
    let (nm, nr) = org_backup::export_org(&a, "org", &fp, &onto.summary, &file)
        .await
        .unwrap();
    // 2 memories + the edge and its R-T4 inverse companion (the write
    // path materialized it at upsert time; the backup carries the store
    // as it is).
    assert_eq!((nm, nr), (2, 2));

    // A REBUILT backend: fresh storage, restore, identical rows.
    let b = InMemoryStorage::new(onto.clone());
    let report = org_backup::import_org(&b, &onto, "org", &file)
        .await
        .unwrap();
    assert_eq!(report.memories, 2);
    assert_eq!(report.relationships, 2);
    let (b_mems, mut b_rels) = rows(&b).await;
    assert_eq!(b_mems.len(), 2);
    let strip_lsn = |mut value: serde_json::Value| {
        if let Some(object) = value.as_object_mut() {
            object.remove("lsn");
        }
        value
    };
    // Storage does not promise stream order. Compare every field on both the
    // requested edge and its materialized inverse after canonical ID sorting;
    // LSN is the sole storage-authority difference allowed by the contract.
    a_rels.sort_by_key(|relationship| relationship.id);
    b_rels.sort_by_key(|relationship| relationship.id);
    assert_eq!(a_rels.len(), 2);
    assert_eq!(b_rels.len(), a_rels.len());
    for (original, restored) in a_rels.iter().zip(&b_rels) {
        assert_eq!(
            strip_lsn(serde_json::to_value(restored).unwrap()),
            strip_lsn(serde_json::to_value(original).unwrap()),
            "relationship restore must preserve full original and inverse rows"
        );
        assert_eq!(
            restored.lsn.space,
            exocortex_kernel::ids::LsnSpace::Backend,
            "restored relationships are re-stamped in Backend space"
        );
    }
    for m in [&m1, &m2] {
        let got = b_mems.iter().find(|x| x.id == m.id).expect("row present");
        let (g, w) = (
            serde_json::to_value(got).unwrap(),
            serde_json::to_value(m).unwrap(),
        );
        // Identity, provenance, content, and temporal fields preserved
        // exactly. LSN is the one sanctioned difference: storage is the
        // sequence authority (§6.2) and re-stamps rows at upsert — the
        // restore's ordering is what matters, not the old counter.
        assert_eq!(
            strip_lsn(g.clone()),
            strip_lsn(w.clone()),
            "byte-faithful restore modulo the storage-assigned LSN"
        );
        assert!(
            got.lsn.space == exocortex_kernel::ids::LsnSpace::Backend,
            "re-stamped in Backend space"
        );
    }
}

#[tokio::test]
async fn legacy_unstamped_embedding_backup_imports_as_known_v1_model() {
    let onto = ontology();
    let mut document = serde_json::to_value(backup_document(
        &onto,
        vec![test_mem("legacy-embedding", 41)],
        vec![],
    ))
    .unwrap();
    document["memories"][0]["embedding"] = serde_json::json!([0.25, -0.5, 0.75]);

    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("legacy-org.json");
    std::fs::write(&file, serde_json::to_vec(&document).unwrap()).unwrap();
    let restored = InMemoryStorage::new(onto.clone());
    org_backup::import_org(&restored, &onto, "org", &file)
        .await
        .expect("the documented pre-stamp backup shape remains readable");

    let memory = restored
        .get_memory(&MemoryId([41; 16]))
        .await
        .unwrap()
        .expect("restored legacy memory");
    let embedding = memory.embedding.expect("legacy vector is retained");
    assert_eq!(embedding.model.name.as_str(), "bge-small");
    assert_eq!(embedding.model.version.as_str(), "v1");
    assert_eq!(embedding.vector, [0.25, -0.5, 0.75]);
}

#[tokio::test]
async fn fingerprint_mismatch_aborts_before_any_write() {
    let onto = ontology();
    let fp = fingerprint_hex(&onto);
    let a = InMemoryStorage::new(onto.clone());
    a.upsert_memory(&test_mem("x", 9)).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("org.json");
    org_backup::export_org(&a, "org", &fp, &onto.summary, &file)
        .await
        .unwrap();

    // Tamper the ontology identity (OC-PRD D2): the summary is the
    // load-bearing field on post-OC documents; the hex is a report.
    let mut doc: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&file).unwrap()).unwrap();
    doc["ontology_summary"]["memory_types"][0] = serde_json::json!("NotARealType");
    doc["ontology_fingerprint"] = serde_json::json!("0".repeat(64));
    std::fs::write(&file, serde_json::to_string(&doc).unwrap()).unwrap();

    let b = InMemoryStorage::new(onto.clone());
    let err = org_backup::import_org(&b, &onto, "org", &file)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("ontology"));
    let (b_mems, _) = rows(&b).await;
    assert!(b_mems.is_empty(), "gate runs before the first upsert");
}

#[tokio::test]
async fn org_mismatch_aborts() {
    let onto = ontology();
    let fp = fingerprint_hex(&onto);
    let a = InMemoryStorage::new(onto.clone());
    a.upsert_memory(&test_mem("x", 3)).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("org.json");
    org_backup::export_org(&a, "org", &fp, &onto.summary, &file)
        .await
        .unwrap();

    let b = InMemoryStorage::new(onto.clone());
    let err = org_backup::import_org(&b, &onto, "other-org", &file)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("org mismatch"));
}

#[tokio::test]
async fn re_import_converges_without_duplicates() {
    let onto = ontology();
    let fp = fingerprint_hex(&onto);
    let a = InMemoryStorage::new(onto.clone());
    a.upsert_memory(&test_mem("x", 4)).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("org.json");
    org_backup::export_org(&a, "org", &fp, &onto.summary, &file)
        .await
        .unwrap();

    let b = InMemoryStorage::new(onto.clone());
    let region = RegionKey {
        org: "*".into(),
        project: "*".into(),
        memory_type: 0,
    };
    let mut invalidations = b.subscribe_invalidations(&region).await.unwrap();
    org_backup::import_org(&b, &onto, "org", &file)
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), invalidations.next())
        .await
        .expect("first import publishes its committed row")
        .expect("stream remains open")
        .expect("invalidation is valid");
    let imported_id = test_mem("x", 4).id;
    assert_eq!(b.memory_history(&imported_id).len(), 1);
    org_backup::import_org(&b, &onto, "org", &file)
        .await
        .unwrap();
    assert_eq!(
        b.memory_history(&imported_id).len(),
        1,
        "a repeated governed import must not append assertion history"
    );
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(50), invalidations.next())
            .await
            .is_err(),
        "a repeated governed import must not republish invalidations"
    );
    let (mems, rels) = rows(&b).await;
    assert_eq!(mems.len(), 1, "upsert semantics: no duplicates");
    assert!(rels.is_empty());
}

#[tokio::test]
async fn oversized_org_backup_is_rejected_without_touching_storage() {
    let onto = ontology();
    let target = InMemoryStorage::new(onto.clone());
    let existing = test_mem("existing", 41);
    target.upsert_memory(&existing).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("oversized.json");
    let oversized = std::fs::File::create(&file).unwrap();
    oversized
        .set_len(org_backup::MAX_ORG_BACKUP_BYTES + 1)
        .unwrap();
    drop(oversized);

    let error = org_backup::import_org(&target, &onto, "org", &file)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("maximum supported size"));
    let (memories, relationships) = rows(&target).await;
    assert_eq!(memories.len(), 1);
    assert_eq!(memories[0].id, existing.id);
    assert!(relationships.is_empty());
}

#[tokio::test]
async fn invalid_late_relationship_rolls_back_the_entire_org_import() {
    let onto = ontology();
    let target = InMemoryStorage::new(onto.clone());
    let memory = test_mem("would-have-been-partial", 51);
    let relationship = test_rel(
        memory.id,
        MemoryId([99; 16]),
        onto.kind_id("Fixes").unwrap(),
    );
    let document = org_backup::OrgBackup {
        format: org_backup::FORMAT.into(),
        version: org_backup::VERSION,
        created_at: chrono::Utc::now().to_rfc3339(),
        org_id: "org".into(),
        ontology_fingerprint: fingerprint_hex(&onto),
        ontology_summary: Some(onto.summary.clone()),
        memories: vec![memory.clone()],
        relationships: vec![relationship],
    };
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("invalid-late-row.json");
    std::fs::write(&file, serde_json::to_vec(&document).unwrap()).unwrap();

    org_backup::import_org(&target, &onto, "org", &file)
        .await
        .unwrap_err();
    let (memories, relationships) = rows(&target).await;
    assert!(
        memories.is_empty(),
        "a late relationship failure must roll back earlier memory rows"
    );
    assert!(relationships.is_empty());
}

#[tokio::test]
async fn governance_violations_fail_before_atomic_restore() {
    let onto = ontology();
    let (from_type, to_type, kind) = valid_noncomputed_triple(&onto);
    let mut from = test_mem("from", 61);
    from.memory_type = from_type;
    let mut to = test_mem("to", 62);
    to.memory_type = to_type;
    let valid_relationship = test_rel(from.id, to.id, kind);
    let valid = backup_document(
        &onto,
        vec![from.clone(), to.clone()],
        vec![valid_relationship.clone()],
    );

    let mut wrong_tenant = serde_json::to_value(&valid).unwrap();
    wrong_tenant["memories"][0]["context"]["tenant_id"] = serde_json::json!("foreign-org");

    let mut unknown_type = serde_json::to_value(&valid).unwrap();
    unknown_type["memories"][0]["memory_type"] = serde_json::json!(255);

    let mut widened = valid_relationship.clone();
    let mut private_from = from.clone();
    private_from.visibility = Visibility::Private;
    private_from.context.user_id = Some("owner".into());
    widened.visibility = Visibility::Org;
    let widened = serde_json::to_value(backup_document(
        &onto,
        vec![private_from, to.clone()],
        vec![widened],
    ))
    .unwrap();

    let mut proposed = serde_json::to_value(backup_document(
        &onto,
        vec![from.clone(), to.clone()],
        vec![valid_relationship.clone()],
    ))
    .unwrap();
    proposed["relationships"][0]["provenance"] = serde_json::json!({
        "Proposed": {
            "discovery_id": "00000000-0000-0000-0000-000000000001",
            "score": 0.8
        }
    });

    let similar_to = onto.kind_id("SimilarTo").unwrap();
    let computed_only = serde_json::to_value(backup_document(
        &onto,
        vec![from.clone(), to.clone()],
        vec![test_rel(from.id, to.id, similar_to)],
    ))
    .unwrap();

    let (bad_from, bad_to, bad_kind) = onto
        .triples_by_kind
        .keys()
        .find_map(|kind| {
            (0..onto.memory_type_names.len() as u8).find_map(|from_type| {
                (0..onto.memory_type_names.len() as u8)
                    .find(|to_type| {
                        exocortex_kernel::validator::validate_triple(
                            &onto, from_type, *kind, *to_type,
                        )
                        .is_err()
                    })
                    .map(|to_type| (from_type, to_type, *kind))
            })
        })
        .expect("dev ontology has a constrained triple");
    let mut invalid_from = from.clone();
    invalid_from.memory_type = bad_from;
    let mut invalid_to = to.clone();
    invalid_to.memory_type = bad_to;
    let invalid_triple = serde_json::to_value(backup_document(
        &onto,
        vec![invalid_from, invalid_to],
        vec![test_rel(from.id, to.id, bad_kind)],
    ))
    .unwrap();

    for (name, document) in [
        ("wrong tenant", wrong_tenant),
        ("unknown type", unknown_type),
        ("widened visibility", widened),
        ("proposed provenance", proposed),
        ("computed-only provenance", computed_only),
        ("invalid triple", invalid_triple),
    ] {
        let target = InMemoryStorage::new(onto.clone());
        let sentinel = test_mem("preexisting", 70);
        target.upsert_memory(&sentinel).await.unwrap();
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("invalid.json");
        std::fs::write(&file, serde_json::to_vec(&document).unwrap()).unwrap();

        let error = org_backup::import_org(&target, &onto, "org", &file)
            .await
            .expect_err(name);
        assert!(!error.to_string().is_empty(), "{name}");
        let (memories, relationships) = rows(&target).await;
        assert_eq!(memories.len(), 1, "{name}: partial memory restore");
        assert_eq!(memories[0].id, sentinel.id, "{name}: sentinel changed");
        assert!(relationships.is_empty(), "{name}: partial edge restore");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn live_falkor_governance_failure_leaves_existing_graph_unchanged() {
    let Ok(url) = std::env::var("FALKOR_URL") else {
        eprintln!(
            "UNEXECUTED live_falkor_governance_failure_leaves_existing_graph_unchanged: FALKOR_URL not set"
        );
        return;
    };
    if url.is_empty() {
        eprintln!(
            "UNEXECUTED live_falkor_governance_failure_leaves_existing_graph_unchanged: FALKOR_URL empty"
        );
        return;
    }
    let onto = ontology();
    let storage = exocortex_storage::FalkorStorage::connect(
        exocortex_storage::FalkorConfig {
            redis_url: url.replacen("falkor://", "redis://", 1),
            falkor_url: url,
            graph_name: format!(
                "org_restore_governance_{}_{}",
                std::process::id(),
                chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
            ),
            org_id: "org".into(),
            node_id: "restore-test".into(),
        },
        onto.clone(),
    )
    .await
    .unwrap();
    let sentinel = test_mem("live-preexisting", 81);
    storage.upsert_memory(&sentinel).await.unwrap();
    let (before_memories, before_relationships) = rows(&storage).await;
    assert_eq!(before_memories.len(), 1);
    assert!(before_relationships.is_empty());
    let mut foreign = test_mem("foreign", 82);
    foreign.context.tenant_id = Some("foreign-org".into());
    let (from_type, to_type, kind) = valid_noncomputed_triple(&onto);
    foreign.memory_type = from_type;
    let mut valid_peer = test_mem("valid-peer", 83);
    valid_peer.memory_type = to_type;
    let relationship = test_rel(foreign.id, valid_peer.id, kind);
    let document = backup_document(&onto, vec![foreign, valid_peer], vec![relationship]);
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("invalid-live.json");
    std::fs::write(&file, serde_json::to_vec(&document).unwrap()).unwrap();

    org_backup::import_org(&storage, &onto, "org", &file)
        .await
        .unwrap_err();
    let (memories, relationships) = rows(&storage).await;
    assert_eq!(memories.len(), 1);
    assert_eq!(memories[0].id, sentinel.id);
    assert!(relationships.is_empty());
}
