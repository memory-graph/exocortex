//! Round-1 W3/W4/W8 acceptance: ingest computes embeddings (fake embedder),
//! a Dreams cycle over ingested data produces anchors + MCR² stamps +
//! SimilarTo edges with Computed provenance (excluded from hairball
//! accounting), and session-wrapup submits enqueue reasoning that lands
//! derived edges off the interactive path.

use std::sync::Arc;
use std::time::Duration;

use exocortex_ingest::{FakeEmbedder, IngestServer};
use exocortex_kernel::Ontology;
use exocortex_pack_dev_v1::pack_def;
use exocortex_storage::{InMemoryStorage, RegionKey, Storage};
use exocortex_wire::ingest::v1::{
    ingest_service_server::IngestService, IngestBatch, MemoryDraft, ProducerIdentity,
    RegisterSourceRequest,
};

const HMAC_KEY: [u8; 32] = [7u8; 32];

fn server(storage: Arc<InMemoryStorage>, onto: Arc<Ontology>) -> IngestServer<InMemoryStorage> {
    IngestServer::new(storage, onto, HMAC_KEY).with_embedder(Arc::new(FakeEmbedder::default()))
}

async fn register(srv: &IngestServer<InMemoryStorage>, uri: &str) {
    use tonic::Request;
    srv.register_source(Request::new(RegisterSourceRequest {
        org_id: "org".into(),
        source_uri: uri.into(),
        producer_id: "e2e".into(),
        ceiling: 3,
        source_flavor: "custom".into(),
    }))
    .await
    .unwrap();
}

fn draft(key: &str, title: &str, content: &str) -> MemoryDraft {
    MemoryDraft {
        draft_key: key.into(),
        id: String::new(),
        memory_type: "Solution".into(),
        title: title.into(),
        content: content.into(),
        tags: vec![],
        visibility: 3,
        valid_from: None,
        valid_until: None,
        external_key: None,
    }
}

async fn submit(
    srv: &IngestServer<InMemoryStorage>,
    uri: &str,
    batch_id: &str,
    drafts: Vec<MemoryDraft>,
) -> exocortex_wire::ingest::v1::IngestAck {
    use tonic::Request;
    let mut b = IngestBatch {
        org_id: "org".into(),
        source_uri: uri.into(),
        producer_id: "e2e".into(),
        batch_id: batch_id.into(),
        mapping_version: "1".into(),
        ontology_fingerprint: srv.ontology.fingerprint.0.to_vec(),
        ceiling: 3,
        checksum: String::new(),
        observed_at: None,
        recorded_at: None,
        snapshot: None,
        memories: drafts,
        relationships: vec![],
        producer: Some(ProducerIdentity {
            node_id: "n".into(),
            agent_id: String::new(),
            adapter_id: String::new(),
            hmac_signature: vec![],
        }),
    };
    exocortex_wire::signing::prepare_batch(&HMAC_KEY, &mut b);
    srv.submit(Request::new(b)).await.unwrap().into_inner()
}

async fn all_relationships(storage: &InMemoryStorage) -> Vec<exocortex_kernel::Relationship> {
    use futures::StreamExt;
    let mut out = vec![];
    let mut rs = storage.stream_all_relationships().await;
    while let Some(Ok(r)) = rs.next().await {
        out.push(r);
    }
    out
}

fn display_name(onto: &Ontology, r: &exocortex_kernel::Relationship) -> String {
    onto.kinds_by_id
        .get(&r.kind)
        .map(|m| m.display_name.to_string())
        .unwrap_or_else(|| format!("kind:{}", r.kind.0))
}

/// W3: a Dreams cycle over IngestService-ingested data (fake embedder)
/// produces anchors, MCR² stamps, merged ids, and SimilarTo edges with
/// Computed provenance that never count toward hairball_fraction.
#[tokio::test]
async fn dreams_cycle_over_ingested_data() {
    let onto = Arc::new(Ontology::from_packs(vec![pack_def()]).unwrap());
    let storage = Arc::new(InMemoryStorage::new(onto.clone()));
    let srv = server(storage.clone(), onto.clone());
    register(&srv, "session://dreams").await;

    // Same-class (Solution) texts for the fake embedder:
    // - exact duplicates ("zeta eta theta" x2) -> merge at cosine 1.0
    // - 3-of-4 word overlap -> cosine ≈ 0.866: SimilarTo (>0.85) but below
    //   the 0.92 merge threshold.
    let ack = submit(
        &srv,
        "session://dreams",
        "b1",
        vec![
            draft("m1", "alpha beta gamma", "alpha beta gamma"),
            draft("m2", "alpha beta gamma delta", "alpha beta gamma delta"),
            draft("m3", "alpha beta gamma epsilon", "alpha beta gamma epsilon"),
            draft("m4", "zeta eta theta", "zeta eta theta"),
            draft("m5", "zeta eta theta two", "zeta eta theta"),
        ],
    )
    .await;
    assert_eq!(ack.accepted, 5, "{:?}", ack.rejections);

    // Embeddings were stored (§7.5 backend-assigned).
    {
        use futures::StreamExt;
        let mut ms = storage.stream_all_memories().await;
        let mut n = 0;
        while let Some(Ok(m)) = ms.next().await {
            assert!(m.embedding.is_some(), "ingest stored an embedding");
            n += 1;
        }
        assert_eq!(n, 5);
    }

    let engine = exocortex_dreams::DreamsEngine::new(
        storage.clone(),
        exocortex_dreams::trigger::DreamsTrigger::default(),
        0.01,
        0.05,
        false,
        "dreams-e2e".into(),
    );
    let region = RegionKey {
        org: "o".into(),
        project: "p".into(),
        memory_type: onto.memory_type_id("Solution").unwrap(),
    };
    let res = engine.try_consolidate(&region).await.expect("cycle");

    // Anchors: the cycle saw ingested, embedded memories.
    assert!(res.memories_input >= 5, "anchors from ingested data");
    // MCR² stamps on both sides of the cycle.
    assert_eq!(res.mcr2_before.n_memories, res.memories_input as usize);
    assert!(res.mcr2_after.n_memories >= 1);
    assert!(res.mcr2_before.delta_r.is_finite());
    // Merge happened for the exact duplicates.
    assert!(
        !res.merged.is_empty(),
        "duplicate pair merged (input={} output={})",
        res.memories_input,
        res.memories_output
    );

    // SimilarTo edges exist, all with Computed{SimilarityHnsw, 0.85}.
    assert!(
        !res.similar_edges.is_empty(),
        "SimilarTo edges created this cycle"
    );
    let rels = all_relationships(&storage).await;
    let similar: Vec<_> = rels
        .iter()
        .filter(|r| display_name(&onto, r) == "SimilarTo")
        .collect();
    assert!(!similar.is_empty(), "SimilarTo rows persisted");
    for r in &similar {
        assert!(
            matches!(
                &r.provenance,
                exocortex_kernel::Provenance::Computed {
                    producer: exocortex_kernel::provenance::ComputedProducer::SimilarityHnsw,
                    threshold
                } if *threshold == 0.85
            ),
            "R-T14: SimilarTo carries Computed provenance, got {:?}",
            r.provenance
        );
    }

    // §11.6.1: the similarity edges never counted toward hairball out-degrees.
    assert_eq!(
        res.sparsity_after.hairball_fraction, 0.0,
        "SimilarTo excluded from hairball accounting"
    );
}

/// W8: a session-wrapup submit enqueues `SessionWrapup` reasoning after the
/// storage commit; the spawned engine loop derives a Solves edge from the
/// D1 rule (Fix Fixes X => Fix Solves X) within a bounded wait.
#[tokio::test]
async fn session_wrapup_enqueues_reasoning_derived_edge() {
    let onto = Arc::new(Ontology::from_packs(vec![pack_def()]).unwrap());
    let storage = Arc::new(InMemoryStorage::new(onto.clone()));
    let engine = Arc::new(exocortex_reasoning::ReasoningEngine::new(
        storage.clone(),
        64,
        3,
    ));
    let srv = server(storage.clone(), onto.clone()).with_reasoning(engine.clone());
    register(&srv, "session://reason").await;

    // Fix --Fixes--> Problem triggers D1 (ImpliedSolves).
    let mut fix = draft("f", "the fix", "apply the patch");
    fix.memory_type = "Fix".into();
    let mut problem = draft("p", "the bug", "crash on start");
    problem.memory_type = "Problem".into();
    let mut b = IngestBatch {
        org_id: "org".into(),
        source_uri: "session://reason".into(),
        producer_id: "e2e".into(),
        batch_id: "wrapup-1".into(),
        mapping_version: "1".into(),
        ontology_fingerprint: srv.ontology.fingerprint.0.to_vec(),
        ceiling: 3,
        checksum: String::new(),
        observed_at: None,
        recorded_at: None,
        snapshot: None,
        memories: vec![fix, problem],
        relationships: vec![exocortex_wire::ingest::v1::RelationshipDraft {
            from_draft_key: "f".into(),
            to_draft_key: "p".into(),
            kind: "Fixes".into(),
            strength: 0.9,
            confidence: 0.9,
            context: String::new(),
            visibility: 3,
        }],
        producer: Some(ProducerIdentity {
            node_id: "n".into(),
            agent_id: String::new(),
            adapter_id: String::new(),
            hmac_signature: vec![],
        }),
    };
    exocortex_wire::signing::prepare_batch(&HMAC_KEY, &mut b);
    let ack = srv
        .submit(tonic::Request::new(b))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(ack.rejected, 0, "{:?}", ack.rejections);

    // Spawn the engine consumer; the derived edge must appear quickly.
    let runner = tokio::spawn({
        let engine = engine.clone();
        async move { engine.run().await }
    });

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut derived_solves = false;
    while tokio::time::Instant::now() < deadline {
        let rels = all_relationships(&storage).await;
        derived_solves = rels.iter().any(|r| {
            display_name(&onto, r) == "Solves"
                && matches!(
                    &r.provenance,
                    exocortex_kernel::Provenance::Derived { rule_id, .. } if rule_id == "D1"
                )
        });
        if derived_solves {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    runner.abort();
    assert!(
        derived_solves,
        "SessionWrapup enqueue landed a D1-derived Solves edge"
    );
}
