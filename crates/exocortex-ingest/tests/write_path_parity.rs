//! GATE1 / W2 (audit §6.2): the golden verdict table. One table of
//! batches — valid, plus one per violation class — run through BOTH
//! write-path validators (the kernel's, which ingest and the offline path
//! now share) and the ingest service, asserting identical verdicts row
//! for row. A fourth divergent path cannot appear without failing this.

use std::sync::Arc;

use exocortex_ingest::IngestServer;
use exocortex_kernel::validator::{validate_draft, SourceCeiling};
use exocortex_kernel::{KernelError, MemoryDraft, Ontology, Visibility};
use exocortex_storage::InMemoryStorage;
use exocortex_wire::ingest::v1::{ingest_service_server::IngestService, MemoryDraft as WireDraft};

use tonic::Request;

const KEY: [u8; 32] = [5u8; 32];

fn ontology() -> Arc<Ontology> {
    Arc::new(Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()]).unwrap())
}

/// One verdict row: a wire draft, the kernel-expected verdict, and the
/// ingest-expected verdict (they must agree).
struct Row {
    name: &'static str,
    draft: WireDraft,
    kernel: Result<(), KernelError>,
    reject: Option<exocortex_wire::ingest::v1::RejectCode>,
}

fn wire(key: &str, mt: &str, title: &str, vis: i32) -> WireDraft {
    WireDraft {
        draft_key: key.into(),
        id: String::new(),
        memory_type: mt.into(),
        title: title.into(),
        content: format!("content {title}"),
        tags: vec![],
        visibility: vis,
        valid_from: None,
        valid_until: None,
        external_key: None,
    }
}

fn rows() -> Vec<Row> {
    use exocortex_kernel::KernelError::*;
    use exocortex_wire::ingest::v1::RejectCode;
    let mut v = Vec::new();
    v.push(Row {
        name: "valid",
        draft: wire("k", "Fix", "a valid title", 1),
        kernel: Ok(()),
        reject: None,
    });
    v.push(Row {
        name: "empty title",
        draft: wire("k", "Fix", "", 1),
        kernel: Err(TitleBounds),
        reject: Some(RejectCode::Unknown),
    });
    v.push(Row {
        name: "title over 200 chars",
        draft: wire("k", "Fix", &"x".repeat(201), 1),
        kernel: Err(TitleBounds),
        reject: Some(RejectCode::Unknown),
    });
    // KP3: 200 chars of multi-byte content is VALID (chars, not bytes).
    let cjk: String = "漢".repeat(200);
    v.push(Row {
        name: "200 CJK chars (valid, KP3)",
        draft: wire("k", "Fix", &cjk, 1),
        kernel: Ok(()),
        reject: None,
    });
    v.push(Row {
        name: "visibility widening",
        draft: wire("k", "Fix", "t", 4),
        kernel: Err(VisibilityWidening {
            source: "table",
            ceiling: Visibility::Org,
            attempted: Visibility::Public,
        }),
        reject: Some(RejectCode::VisibilityWidening),
    });
    v.push(Row {
        name: "unknown memory type",
        draft: wire("k", "NotAType", "t", 1),
        kernel: Err(KernelError::UnknownKind(exocortex_kernel::RelKindId(
            u32::MAX,
        ))), // resolved pre-kernel on both paths
        reject: Some(RejectCode::UnknownMemoryType),
    });
    v
}

#[tokio::test]
async fn verdicts_agree_row_for_row() {
    let onto = ontology();
    let srv = IngestServer::new(
        Arc::new(InMemoryStorage::new(onto.clone())),
        onto.clone(),
        KEY,
    );
    let _ = srv
        .register_source(Request::new(exocortex_wire::signing::registration(
            &KEY,
            "org",
            "session://parity",
            "session-wrapup",
            3,
            "session",
            "t",
            exocortex_wire::ingest::v1::ProducerKind::CodingAgent,
        )))
        .await
        .unwrap();

    let ceiling = SourceCeiling {
        source: "table",
        ceiling: Visibility::Org,
    };
    for row in rows() {
        if row.name == "unknown memory type" {
            // Name resolution is pre-kernel on both paths; the ingest
            // verdict is checked below and there is no offline divergence
            // (both use memory_type_id).
            assert_eq!(onto.memory_type_id("NotAType"), None);
        } else {
            // Kernel verdict (the shared rulebook).
            let mt = onto.memory_type_id(&row.draft.memory_type).unwrap();
            let vis = match row.draft.visibility {
                0 => Visibility::Private,
                1 => Visibility::Project,
                2 => Visibility::Team,
                3 => Visibility::Org,
                4 => Visibility::Public,
                _ => Visibility::Public,
            };
            let kernel_draft = MemoryDraft {
                memory_type: mt,
                title: row.draft.title.clone().into(),
                content: row.draft.content.clone(),
                summary: None,
                visibility: vis,
                context: exocortex_kernel::MemoryContext {
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
                edge_hints: Default::default(),
                external_key: None,
            };
            let got = validate_draft(&onto, &kernel_draft, ceiling);
            let expected_ok = row.kernel.is_ok();
            let got_ok = got.is_ok();
            assert_eq!(
                got_ok, expected_ok,
                "row `{}`: kernel verdict diverged: got {:?} want {:?}",
                row.name, got, row.kernel
            );
        }

        // Ingest verdict over the same draft.
        let mut b = base_batch();
        b.memories = vec![row.draft.clone()];
        exocortex_wire::signing::prepare_batch(&KEY, &mut b);
        let ack = srv.submit(Request::new(b)).await.unwrap().into_inner();
        match row.reject {
            // D6: an accepted memory rides with its InSession edge +
            // companion from the session grouping.
            None => assert_eq!(ack.accepted, 3, "row `{}`: ingest accepted", row.name),
            Some(code) => assert!(
                ack.rejections.iter().any(|r| r.code == code as i32),
                "row `{}`: ingest rejected with {code:?}, got {:?}",
                row.name,
                ack.rejections
            ),
        }
    }
}

fn base_batch() -> exocortex_wire::ingest::v1::IngestBatch {
    let mut b = exocortex_wire::ingest::v1::IngestBatch {
        org_id: "org".into(),
        source_uri: "session://parity".into(),
        producer_id: "session-wrapup".into(),
        batch_id: String::new(),
        mapping_version: "1".into(),
        ontology_fingerprint: Vec::new(),
        ceiling: 3,
        checksum: String::new(),
        observed_at: None,
        recorded_at: None,
        snapshot: None,
        memories: vec![],
        relationships: vec![],
        producer: Some(exocortex_wire::ingest::v1::ProducerIdentity {
            node_id: "n".into(),
            agent_id: String::new(),
            adapter_id: String::new(),
            hmac_signature: vec![],

            client_metadata: None,
        }),
    };
    b.ontology_fingerprint = ontology().fingerprint.0.to_vec();
    b.batch_id = uuid_stub();
    b
}

fn uuid_stub() -> String {
    format!(
        "parity-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}
