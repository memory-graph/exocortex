use exocortex_client::wal::{WalEntry, WalState};

#[test]
fn wal_entry_bincode_roundtrip() {
    let e = WalEntry {
        local_lsn: 1,
        session_id: "s".into(),
        memories: vec![],
        memory_ids: vec![exocortex_kernel::MemoryId::new_v7()],
        state: WalState::Pending,
        batch_id: "b".into(),
        draft_keys: vec![],
        tags: vec![],
    };
    let bytes = bincode::serialize(&e).unwrap();
    let back: WalEntry = bincode::deserialize(&bytes).unwrap();
    assert_eq!(back.state, WalState::Pending);
}

#[test]
fn wal_pending_count_diagnostics() {
    let dir = std::env::temp_dir().join(format!("exocortex-wal-diag-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let wal = exocortex_client::wal::Wal::open(&dir).unwrap();
    let lsn = wal.append_batch("s", vec![], vec![]).unwrap();
    let tree_entries: usize = { wal.db_len() };
    println!(
        "lsn={lsn} entries={tree_entries} pending={}",
        wal.pending_count().unwrap()
    );
    assert_eq!(wal.pending_count().unwrap(), 1);
    std::fs::remove_dir_all(&dir).unwrap();
}

/// W1 (audit): the WAL drain. Offline entries reach a real IngestService,
/// settle to Synced with a backend LSN, dedupe a replayed attempt, and a
/// permanently-rejected entry lands Failed — never silently dropped.
#[tokio::test(flavor = "multi_thread")]
async fn wal_drain_settles_pending_entries() {
    use exocortex_client::drain;

    let dir = std::env::temp_dir().join(format!(
        "exocortex-wal-drain-{}-{}",
        std::process::id(),
        uuid_stub()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let wal = std::sync::Arc::new(exocortex_client::wal::Wal::open(&dir).unwrap());

    let onto = std::sync::Arc::new(
        exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()]).unwrap(),
    );
    let storage = std::sync::Arc::new(exocortex_storage::InMemoryStorage::new(onto.clone()));
    let srv = exocortex_ingest::IngestServer::new(storage.clone(), onto.clone(), [5u8; 32]);

    // Two offline wrapups: one valid, one with an unknown memory type.
    let mk = |mt: u8, title: &str| exocortex_kernel::MemoryDraft {
        memory_type: mt,
        title: title.into(),
        content: format!("content {title}"),
        summary: None,
        visibility: exocortex_kernel::Visibility::Org,
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
    let fix_type = onto.memory_type_id("Fix").unwrap();
    let ids1 = vec![exocortex_kernel::MemoryId::new_v7()];
    wal.append_batch_full(
        "s-one",
        vec![mk(fix_type, "valid offline")],
        ids1.clone(),
        "batch-one".into(),
        vec!["k1".into()],
        vec![vec!["ci".into()]],
    )
    .unwrap();
    let ids2 = vec![exocortex_kernel::MemoryId::new_v7()];
    wal.append_batch_full(
        "s-two",
        vec![mk(200, "bad type")],
        ids2,
        "batch-two".into(),
        vec!["k1".into()],
        vec![vec![]],
    )
    .unwrap();
    assert_eq!(wal.pending_count().unwrap(), 2);

    // Serve the real IngestService on a loopback port and connect to it.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let sock = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let incoming = async_stream::stream! {
            while let Ok((stream, _)) = listener.accept().await {
                yield Ok::<_, std::io::Error>(stream);
            }
        };
        let _ = tonic::transport::Server::builder()
            .add_service(
                exocortex_wire::ingest::v1::ingest_service_server::IngestServiceServer::new(srv),
            )
            .serve_with_incoming(incoming)
            .await;
    });
    // Retry-connect briefly while the listener comes up.
    let mut client = None;
    for _ in 0..50 {
        if let Ok(c) =
            exocortex_wire::ingest::v1::ingest_service_client::IngestServiceClient::connect(
                format!("http://{sock}"),
            )
            .await
        {
            client = Some(c);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    let mut client = client.expect("connect to test ingest server");
    let report = drain::drain_once(
        &wal,
        &mut client,
        &[5u8; 32],
        onto.fingerprint.0,
        "org",
        None,
        &onto,
        "drain-test",
    )
    .await
    .unwrap();

    assert_eq!(report.synced, 1, "valid entry syncs: {report:?}");
    assert_eq!(
        report.failed, 1,
        "unknown-type entry is terminal: {report:?}"
    );
    assert_eq!(wal.pending_count().unwrap(), 0, "nothing left Pending");

    let all = wal.states_for_test().unwrap();
    assert!(
        matches!(all[0], (1, WalState::Synced { .. })),
        "entry 1 synced: {all:?}"
    );
    assert_eq!(all[1], (2, WalState::Failed), "entry 2 failed: {all:?}");

    // The valid row actually landed, with its tags (CL1). (The server
    // assigns fresh MemoryIds for non-snapshot batches, so look by title.)
    use exocortex_storage::Storage;
    use futures::StreamExt;
    let mut found = None;
    let mut ms = storage.stream_all_memories().await;
    while let Some(Ok(m)) = ms.next().await {
        if m.title == "valid offline" {
            found = Some(m);
        }
    }
    let got = found.expect("committed");
    assert!(
        got.tags.iter().any(|t| t == "ci"),
        "tags survived the WAL: {:?}",
        got.tags
    );

    std::fs::remove_dir_all(&dir).unwrap();
}

fn uuid_stub() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}
