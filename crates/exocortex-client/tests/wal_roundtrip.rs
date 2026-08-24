use exocortex_client::wal::{WalEntry, WalState};

#[test]
fn wal_entry_bincode_roundtrip() {
    let e = WalEntry {
        local_lsn: 1,
        session_id: "s".into(),
        memories: vec![],
        memory_ids: vec![exocortex_kernel::MemoryId::new_v7()],
        state: WalState::Pending,
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
        wal.pending_count()
    );
    assert_eq!(wal.pending_count(), 1);
    std::fs::remove_dir_all(&dir).unwrap();
}
