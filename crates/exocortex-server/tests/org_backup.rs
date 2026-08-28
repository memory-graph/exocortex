//! BR2 acceptance (BR-PRD's deferred backend leg): the durable org
//! store as a portable file — round trip across storage instances with
//! byte-faithful rows, fingerprint and org gates, idempotent re-import.

use exocortex_kernel::{
    Memory, MemoryContext, MemoryId, Provenance, Relationship, RelationshipId,
    RelationshipProperties, Visibility, LSN,
};
use exocortex_server::org_backup;
use exocortex_storage::{InMemoryStorage, Storage};
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

async fn rows<S: Storage>(s: &S) -> (Vec<Memory>, Vec<Relationship>) {
    let mut ms = s.stream_all_memories().await;
    let mut memories = Vec::new();
    while let Some(Ok(m)) = ms.next().await {
        memories.push(m);
    }
    let mut rs = s.stream_all_relationships().await;
    let mut rels = Vec::new();
    while let Some(Ok(r)) = rs.next().await {
        rels.push(r);
    }
    (memories, rels)
}

#[tokio::test]
async fn org_round_trip_is_byte_faithful_across_storage_instances() {
    let onto = ontology();
    let fp = fingerprint_hex(&onto);
    let a = InMemoryStorage::new(onto.clone());
    let m1 = test_mem("auth-bridge", 1);
    let m2 = test_mem("policy-engine", 2);
    let rel = test_rel(m1.id, m2.id, onto.kind_id("Fixes").unwrap());
    a.upsert_memory(&m1).await.unwrap();
    a.upsert_memory(&m2).await.unwrap();
    a.upsert_relationship(&rel).await.unwrap();

    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("org.json");
    let (nm, nr) = org_backup::export_org(&a, "org", &fp, &file).await.unwrap();
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
    // The restored edge plus its R-T4 inverse companion (materialized
    // by the write path) — both are legitimate; the ORIGINAL row must
    // be present under its original id.
    assert!(!b_rels.is_empty());
    // Storage does not promise relationship order. Reverse the observed
    // sequence so this regression cannot accidentally rely on the adapter's
    // current iteration order.
    b_rels.reverse();
    assert!(
        b_rels.iter().any(|r| r.id == rel.id),
        "the backed-up edge restored under its own id"
    );
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
        let strip_lsn = |mut v: serde_json::Value| {
            if let Some(o) = v.as_object_mut() {
                o.remove("lsn");
            }
            v
        };
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
async fn fingerprint_mismatch_aborts_before_any_write() {
    let onto = ontology();
    let fp = fingerprint_hex(&onto);
    let a = InMemoryStorage::new(onto.clone());
    a.upsert_memory(&test_mem("x", 9)).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("org.json");
    org_backup::export_org(&a, "org", &fp, &file).await.unwrap();

    // Tamper.
    let mut doc: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&file).unwrap()).unwrap();
    doc["ontology_fingerprint"] = serde_json::json!("0".repeat(64));
    std::fs::write(&file, serde_json::to_string(&doc).unwrap()).unwrap();

    let b = InMemoryStorage::new(onto.clone());
    let err = org_backup::import_org(&b, &onto, "org", &file)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("fingerprint"));
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
    org_backup::export_org(&a, "org", &fp, &file).await.unwrap();

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
    org_backup::export_org(&a, "org", &fp, &file).await.unwrap();

    let b = InMemoryStorage::new(onto.clone());
    org_backup::import_org(&b, &onto, "org", &file)
        .await
        .unwrap();
    org_backup::import_org(&b, &onto, "org", &file)
        .await
        .unwrap();
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
