//! D22 (training-corpus direction): the corpus cut. What is pinned here:
//! - bi-temporal correctness: `as_of` yields exactly what the graph
//!   believed then — rows recorded later are absent (no future leakage),
//!   rows superseded before the cut are absent, rows still believed are
//!   present;
//! - edges only when both endpoints made the cut (no dangling rows in a
//!   training corpus);
//! - per-record lineage: provenance, raw external coordinates (R-T18a),
//!   entities, and LSN per exported row;
//! - the manifest names the fingerprint, the cut, the computed-only
//!   kinds present, and the D24 egress boundary.

use std::sync::Arc;

use exocortex_kernel::{Memory, MemoryContext, MemoryId, Provenance, Visibility, LSN};
use exocortex_server::corpus_export::export_corpus;
use exocortex_storage::{InMemoryStorage, Storage};

fn ontology() -> Arc<exocortex_kernel::Ontology> {
    Arc::new(
        exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()]).unwrap(),
    )
}

fn memory(
    key: &str,
    _type_name: &str,
    recorded: chrono::DateTime<chrono::Utc>,
    valid_from: chrono::DateTime<chrono::Utc>,
    valid_until: Option<chrono::DateTime<chrono::Utc>>,
) -> Memory {
    Memory {
        rights: None,
        id: MemoryId::new_v7(),
        memory_type: 0,
        title: key.into(),
        content: format!("content {key}"),
        summary: None,
        tags: Default::default(),
        visibility: Visibility::Org,
        provenance: Provenance::Asserted {
            author: "corpus-test".into(),
            producer_kind: None,
        },
        context: MemoryContext {
            timestamp: recorded,
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
        embedding: None,
        importance: exocortex_kernel::memory::F01::new(0.5).unwrap(),
        confidence: exocortex_kernel::memory::F01::new(0.8).unwrap(),
        effectiveness: None,
        usage_count: 0,
        valid_from,
        valid_until,
        recorded_at: recorded,
        invalidated_by: None,
        lsn: LSN::new_local(1),
    }
}

fn ts(year: i32) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::parse_from_rfc3339(&format!("{year}-06-01T00:00:00Z"))
        .unwrap()
        .with_timezone(&chrono::Utc)
}

#[tokio::test]
async fn corpus_cut_is_temporally_clean_and_carries_lineage() {
    let onto = ontology();
    let storage = InMemoryStorage::new(onto.clone());
    let cut = ts(2026);

    // Believed at the cut: recorded before, valid before, still open.
    let live = memory("live", "Fix", ts(2024), ts(2024), None);
    // Recorded AFTER the cut: must be absent — no future leakage.
    let future = memory("future", "Fix", ts(2027), ts(2027), None);
    // Valid only in the past: closed before the cut.
    let stale = memory("stale", "Fix", ts(2023), ts(2023), Some(ts(2025)));
    // Valid from after the cut: not yet believed.
    let pending = memory("pending", "Fix", ts(2025), ts(2027), None);

    let fix_id = onto.memory_type_id("Fix").unwrap();
    let mut live = live;
    live.memory_type = fix_id;
    let mut future = future;
    future.memory_type = fix_id;
    let mut stale = stale;
    stale.memory_type = fix_id;
    let mut pending = pending;
    pending.memory_type = fix_id;

    storage.upsert_memory(&live).await.unwrap();
    storage.upsert_memory(&future).await.unwrap();
    storage.upsert_memory(&stale).await.unwrap();
    storage.upsert_memory(&pending).await.unwrap();

    // One external-snapshot row with raw coordinates, believed at the cut.
    let mut external = memory("external-row", "Fix", ts(2024), ts(2024), None);
    external.memory_type = fix_id;
    external.provenance = Provenance::ExternalSnapshot(exocortex_kernel::ExternalSnapshot {
        source_uri: "iceberg://lake/events".into(),
        snapshot_id: "snap-1".into(),
        schema_hash: [1u8; 32],
        observed_at: ts(2024),
        external_key: exocortex_kernel::ExternalKey {
            table_uuid: "0102030405060708090a0b0c0d0e0f10".into(),
            logical_pk: b"pk-7".to_vec(),
            mapping_version: 1,
        },
        producer_id: "table-adapter".into(),
    });
    storage.upsert_memory(&external).await.unwrap();

    // An edge between two rows in the cut, and one dangling at the future
    // row — the dangling one must NOT be exported.
    let kind = onto.kind_id("RelatedTo").unwrap();
    let both = exocortex_kernel::Relationship {
        id: exocortex_kernel::RelationshipId::derive(live.id, kind, external.id, None),
        kind,
        from: live.id,
        to: external.id,
        visibility: Visibility::Org,
        provenance: Provenance::Asserted {
            author: "corpus-test".into(),
            producer_kind: None,
        },
        properties: exocortex_kernel::relationship::RelationshipProperties {
            strength: 0.5,
            confidence: 0.5,
            context: None,
            evidence_count: 0,
            success_rate: None,
            validation_count: 0,
            counter_evidence_count: 0,
            last_validated: ts(2024),
        },
        description: None,
        bidirectional: false,
        valid_from: ts(2024),
        valid_until: None,
        recorded_at: ts(2024),
        invalidated_by: None,
        lsn: LSN::new_local(2),
    };
    let dangling = exocortex_kernel::Relationship {
        id: exocortex_kernel::RelationshipId::derive(live.id, kind, future.id, None),
        kind,
        from: live.id,
        to: future.id,
        ..both.clone()
    };
    storage.upsert_relationship(&both).await.unwrap();
    storage.upsert_relationship(&dangling).await.unwrap();

    let dir = tempfile::tempdir().unwrap();
    let manifest = export_corpus(&storage, &onto, Some(cut), dir.path())
        .await
        .unwrap();

    assert_eq!(manifest.memories, 2, "live + external-row made the cut");
    assert_eq!(manifest.edges, 1, "only the fully-anchored edge exports");
    assert_eq!(manifest.as_of.as_deref(), Some(cut.to_rfc3339()).as_deref());

    let memories: Vec<Memory> = std::fs::read_to_string(dir.path().join("memories.jsonl"))
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let titles: Vec<&str> = memories.iter().map(|m| m.title.as_ref()).collect();
    assert!(titles.contains(&"live"), "{titles:?}");
    assert!(titles.contains(&"external-row"), "{titles:?}");
    assert!(!titles.contains(&"future"), "no future leakage: {titles:?}");
    assert!(
        !titles.contains(&"stale"),
        "superseded before the cut: {titles:?}"
    );
    assert!(!titles.contains(&"pending"), "not yet believed: {titles:?}");

    let edges: Vec<serde_json::Value> = std::fs::read_to_string(dir.path().join("edges.jsonl"))
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(edges.len(), 1);

    let lineage: Vec<serde_json::Value> = std::fs::read_to_string(dir.path().join("lineage.jsonl"))
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(lineage.len(), 2);
    let external_lineage = lineage
        .iter()
        .find(|row| row["provenance"] == "external-snapshot")
        .expect("external row lineage");
    assert!(
        external_lineage["external_key"]
            .as_str()
            .unwrap()
            .starts_with("0102030405060708090a0b0c0d0e0f10:pk-7"),
        "raw external coordinates, not a digest"
    );
    assert_eq!(external_lineage["source"], "iceberg://lake/events");
    let live_lineage = lineage
        .iter()
        .find(|row| row["provenance"] == "asserted")
        .expect("asserted row lineage");
    assert_eq!(live_lineage["source"], "corpus-test");

    let manifest_doc: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(dir.path().join("manifest.json")).unwrap())
            .unwrap();
    assert_eq!(manifest_doc["format"], "exocortex-corpus");
    assert_eq!(
        manifest_doc["compatibility_fingerprint"]
            .as_str()
            .unwrap()
            .len(),
        64
    );
    assert!(
        manifest_doc["egress"].as_str().unwrap().contains("D24"),
        "the egress boundary is stated on every export"
    );
}

/// The now-cut: an absent `as_of` exports the current state — everything
/// open and recorded, the same predicate with T = now.
#[tokio::test]
async fn corpus_cut_defaults_to_now() {
    let onto = ontology();
    let storage = InMemoryStorage::new(onto.clone());
    let fix_id = onto.memory_type_id("Fix").unwrap();
    let mut live = memory(
        "live-now",
        "Fix",
        chrono::Utc::now() - chrono::Duration::days(1),
        chrono::Utc::now() - chrono::Duration::days(1),
        None,
    );
    live.memory_type = fix_id;
    storage.upsert_memory(&live).await.unwrap();

    let dir = tempfile::tempdir().unwrap();
    let manifest = export_corpus(&storage, &onto, None, dir.path())
        .await
        .unwrap();
    assert_eq!(manifest.memories, 1);
    assert_eq!(manifest.as_of, None);
}

#[tokio::test(flavor = "multi_thread")]
async fn d24_egress_verdict_is_computed_from_row_rights() {
    let onto = ontology();
    let fix_id = onto.memory_type_id("Fix").unwrap();

    // Fully-covered corpus: every row claims licence + consent.
    let covered = InMemoryStorage::new(onto.clone());
    let mut row = memory("covered", "Fix", ts(2024), ts(2024), None);
    row.memory_type = fix_id;
    row.rights = Some(exocortex_kernel::memory::Rights {
        licence: Some("Apache-2.0".into()),
        consent_basis: Some("contractual".into()),
        retention_until: None,
        redacted: false,
    });
    covered.upsert_memory(&row).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let manifest = export_corpus(&covered, &onto, None, dir.path())
        .await
        .unwrap();
    assert!(
        manifest.egress.starts_with("permitted:"),
        "the verdict flips when every row is covered: {}",
        manifest.egress
    );

    // One uncovered row poisons the whole corpus (fail closed).
    let mixed = InMemoryStorage::new(onto.clone());
    mixed.upsert_memory(&row).await.unwrap();
    let mut bare = memory("bare", "Fix", ts(2024), ts(2024), None);
    bare.memory_type = fix_id;
    mixed.upsert_memory(&bare).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let manifest = export_corpus(&mixed, &onto, None, dir.path())
        .await
        .unwrap();
    assert!(
        manifest.egress.starts_with("NOT permitted:"),
        "an uncovered row blocks egress: {}",
        manifest.egress
    );
    assert!(manifest.egress.contains("1/2"), "{}", manifest.egress);
    assert!(
        manifest.egress.contains("claim none"),
        "{}",
        manifest.egress
    );
}
