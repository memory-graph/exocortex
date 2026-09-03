//! Live-Falkor release budget for the indexed relationship point-read used by
//! cache invalidation and SSE hydration. Executed by `cargo xtask bench` when
//! `FALKOR_URL` is available; the gate reports it as unexecuted otherwise.

#![cfg(feature = "integration")]

use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use exocortex_kernel::{
    Memory, MemoryContext, MemoryId, Provenance, Relationship, RelationshipId,
    RelationshipProperties, Visibility, LSN,
};
use exocortex_storage::{FalkorConfig, FalkorStorage, Storage};

const RELATIONSHIP_COUNT: usize = 10_000;
const SAMPLE_COUNT: usize = 1_000;
// This is the remote backend hop, not the sub-millisecond ArcSwap cache read.
// It retains a 25x margin inside the 500 ms invalidation-delivery contract.
const POINT_READ_P99_BUDGET: Duration = Duration::from_millis(20);

fn ontology() -> Arc<exocortex_kernel::Ontology> {
    Arc::new(
        exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()]).unwrap(),
    )
}

fn memory(id: MemoryId, title: &str) -> Memory {
    Memory {
        rights: None,
        id,
        memory_type: 3,
        title: title.into(),
        content: title.into(),
        summary: None,
        tags: Default::default(),
        visibility: Visibility::Org,
        provenance: Provenance::Asserted {
            author: "live-bench".into(),
            producer_kind: None,
        },
        context: MemoryContext {
            timestamp: Utc::now(),
            project_id: Some("bench".into()),
            project_path: None,
            team_id: None,
            tenant_id: Some("bench-org".into()),
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
        valid_from: Utc::now(),
        valid_until: None,
        recorded_at: Utc::now(),
        invalidated_by: None,
        embedding: None,
        lsn: LSN::new_local(0),
    }
}

fn relationship(id: RelationshipId, from: MemoryId, to: MemoryId) -> Relationship {
    Relationship {
        id,
        kind: exocortex_kernel::kinds::SOLVES,
        from,
        to,
        visibility: Visibility::Org,
        provenance: Provenance::Asserted {
            author: "live-bench".into(),
            producer_kind: None,
        },
        properties: RelationshipProperties {
            strength: 0.5,
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

fn relationship_id(index: usize) -> RelationshipId {
    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(&(index as u64).to_be_bytes());
    bytes[8..].copy_from_slice(b"livebnch");
    RelationshipId(bytes)
}

#[tokio::test(flavor = "multi_thread")]
async fn indexed_relationship_point_read_meets_live_falkor_budget() {
    let url = std::env::var("FALKOR_URL").expect("xtask requires FALKOR_URL for this target");
    let storage = FalkorStorage::connect(
        FalkorConfig {
            redis_url: std::env::var("REDIS_URL")
                .unwrap_or_else(|_| url.replacen("falkor://", "redis://", 1)),
            falkor_url: url,
            graph_name: format!("exocortex_live_bench_{}", std::process::id()),
            org_id: "bench-org".into(),
            node_id: "live-bench".into(),
        },
        ontology(),
    )
    .await
    .expect("connect live Falkor");
    let from = MemoryId([1; 16]);
    let to = MemoryId([2; 16]);
    storage
        .upsert_batch(&[memory(from, "from"), memory(to, "to")], &[])
        .await
        .unwrap();
    for start in (0..RELATIONSHIP_COUNT).step_by(100) {
        let rows: Vec<_> = (start..start + 100)
            .map(|index| relationship(relationship_id(index), from, to))
            .collect();
        storage.upsert_batch(&[], &rows).await.unwrap();
    }

    storage
        .get_relationship(&relationship_id(0))
        .await
        .unwrap()
        .expect("warm indexed read");
    let mut samples = Vec::with_capacity(SAMPLE_COUNT);
    for index in 0..SAMPLE_COUNT {
        let started = Instant::now();
        storage
            .get_relationship(&relationship_id(index * 10))
            .await
            .unwrap()
            .expect("indexed relationship exists");
        samples.push(started.elapsed());
    }
    samples.sort_unstable();
    let p99 = samples[(SAMPLE_COUNT * 99 / 100).saturating_sub(1)];
    println!(
        "live Falkor indexed relationship read: rows={RELATIONSHIP_COUNT} samples={SAMPLE_COUNT} p99={p99:?}"
    );
    assert!(
        p99 <= POINT_READ_P99_BUDGET,
        "live Falkor point-read p99 {p99:?} exceeds {POINT_READ_P99_BUDGET:?}"
    );
}
