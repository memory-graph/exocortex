//! Local WAL over `sled` — the offline write buffer (§8.2, §13.3).
//!
//! M3 scope: append + state transitions only; reconciliation lands with sync
//! (M5+). Entries carry `{ Pending, Synced, Failed }` states (R-M11) and the
//! log is bounded by `wal_max_bytes` (default 100 MB, R-Sc8).
//!
//! Codec note (recorded): §9.6 pins bincode for the WAL, but `MemoryDraft`
//! carries `serde_json::Value` (`additional_metadata`, §7.14), which bincode
//! cannot deserialize (`deserialize_any` unsupported). Entries are therefore
//! length-framed JSON with a codec-version byte; `rkyv` remains deferred as
//! §24 q3 suggests.

use exocortex_kernel::{MemoryDraft, MemoryId};
use serde::{Deserialize, Serialize};

/// WAL entry states (R-M11).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum WalState {
    /// Written locally, not yet acknowledged by the backend.
    Pending,
    /// Backend assigned LSNs; entry reconciled.
    Synced {
        /// First backend LSN assigned to this batch.
        backend_lsn: u64,
    },
    /// Terminal failure surfaced to the operator.
    Failed,
}

/// One WAL record: a wrapup batch with its assigned local LSNs.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WalEntry {
    /// Monotonic local sequence number for the batch.
    pub local_lsn: u64,
    /// The producer identity that wrote the batch.
    pub session_id: String,
    /// The batch contents.
    pub memories: Vec<MemoryDraft>,
    /// Ids assigned to the drafts at apply time (parallel to `memories`).
    pub memory_ids: Vec<MemoryId>,
    /// Current state.
    pub state: WalState,
    /// Stable batch id (W1/IN7): derived from the CONTENT at append time,
    /// so a drain retry hits the server's (producer_id, batch_id)
    /// idempotency entry instead of double-committing. Older entries
    /// without one derive it at drain time.
    #[serde(default)]
    pub batch_id: String,
    /// Draft keys parallel to `memories` (W1: needed to rebuild edge
    /// references on drain).
    #[serde(default)]
    pub draft_keys: Vec<String>,
    /// Tags parallel to `memories` (CL1: the offline path must not
    /// silently drop the harness-supplied tags before they are durable).
    #[serde(default)]
    pub tags: Vec<Vec<String>>,
}

/// Errors surfaced by the WAL.
#[derive(Debug, thiserror::Error)]
pub enum WalError {
    /// sled failure.
    #[error("wal io: {0}")]
    Io(String),
    /// A record exists but cannot be decoded by this build. The original
    /// sled value is deliberately left untouched for operator recovery.
    #[error("wal record {local_lsn} is corrupt or incompatible: {detail}")]
    Corrupt {
        /// Record key in the local WAL.
        local_lsn: u64,
        /// Codec or storage detail.
        detail: String,
    },
    /// The configured byte budget is exhausted (R-Sc8 `WAL Full`).
    #[error("wal full: {0} bytes used of {1} budget")]
    Full(#[allow(dead_code)] u64, #[allow(dead_code)] u64),
}

/// The `wal_max_bytes` default (§16: 100 MB).
pub const WAL_MAX_BYTES: u64 = 100 * 1024 * 1024;

/// One `--tail-audit` row (D5): the developer-facing projection of a WAL
/// entry.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TailRow {
    /// The entry's local LSN.
    pub local_lsn: u64,
    /// When it was written (first draft's context timestamp).
    pub recorded_at: String,
    /// Still Pending (not yet drained to a backend).
    pub pending: bool,
    /// Stable batch id (IN7).
    pub batch_id: String,
    /// Drafts in the batch.
    pub memory_count: usize,
    /// Their producer-local keys.
    pub draft_keys: Vec<String>,
}

/// Sled-backed write-ahead log.
pub struct Wal {
    tree: sled::Tree,
    max_bytes: u64,
    append_gate: std::sync::Mutex<()>,
}

impl Wal {
    /// Open (or create) the WAL under `dir`.
    pub fn open(dir: &std::path::Path) -> Result<Self, WalError> {
        Self::open_with_budget(dir, WAL_MAX_BYTES)
    }

    /// Open with an explicit byte budget.
    pub fn open_with_budget(dir: &std::path::Path, max_bytes: u64) -> Result<Self, WalError> {
        let db = sled::open(dir).map_err(|e| WalError::Io(e.to_string()))?;
        let tree = db
            .open_tree("wal")
            .map_err(|e| WalError::Io(e.to_string()))?;
        Ok(Self {
            tree,
            max_bytes,
            append_gate: std::sync::Mutex::new(()),
        })
    }

    fn used_bytes(&self) -> Result<u64, WalError> {
        self.tree.iter().try_fold(0u64, |used, item| {
            let (_, value) = item.map_err(|error| WalError::Io(error.to_string()))?;
            Ok(used.saturating_add(value.len() as u64 + 16))
        })
    }

    /// Append one batch; assigns and returns the batch's local LSN.
    /// Fails with `WalError::Full` at 100% of the budget (R-Sc8); at 90% the
    /// caller should surface the `WAL Near Full` warning (`Self::near_full`).
    pub fn append_batch(
        &self,
        session_id: &str,
        memories: Vec<MemoryDraft>,
        memory_ids: Vec<MemoryId>,
    ) -> Result<u64, WalError> {
        self.append_batch_full(
            session_id,
            memories,
            memory_ids,
            String::new(),
            Vec::new(),
            Vec::new(),
        )
    }

    /// Append with the stable batch id and the rebuild inputs the drain
    /// needs (draft keys + tags; W1/IN7/CL1).
    pub fn append_batch_full(
        &self,
        session_id: &str,
        memories: Vec<MemoryDraft>,
        memory_ids: Vec<MemoryId>,
        batch_id: String,
        draft_keys: Vec<String>,
        tags: Vec<Vec<String>>,
    ) -> Result<u64, WalError> {
        let entry = WalEntry {
            local_lsn: 0,
            session_id: session_id.to_string(),
            memories,
            memory_ids,
            state: WalState::Pending,
            batch_id,
            draft_keys,
            tags,
        };
        self.insert_entry(entry)
    }

    /// Atomically return the existing entry for an exact content-derived
    /// batch id, or append it once. This is the offline equivalent of the
    /// backend's durable ingest claim: response loss and concurrent retries
    /// cannot mint a second WAL row.
    pub fn append_batch_full_idempotent(
        &self,
        session_id: &str,
        memories: Vec<MemoryDraft>,
        memory_ids: Vec<MemoryId>,
        batch_id: String,
        draft_keys: Vec<String>,
        tags: Vec<Vec<String>>,
    ) -> Result<u64, WalError> {
        let entry = WalEntry {
            local_lsn: 0,
            session_id: session_id.to_string(),
            memories,
            memory_ids,
            state: WalState::Pending,
            batch_id,
            draft_keys,
            tags,
        };
        let _append = self
            .append_gate
            .lock()
            .map_err(|_| WalError::Io("WAL append serialization mutex is poisoned".to_string()))?;
        if !entry.batch_id.is_empty() {
            for item in self.tree.iter() {
                let (key, raw) = item.map_err(|error| WalError::Io(error.to_string()))?;
                let local_lsn = key_to_lsn(&key)?;
                if decode_at(local_lsn, &raw)?.batch_id == entry.batch_id {
                    return Ok(local_lsn);
                }
            }
        }
        self.insert_entry_locked(entry)
    }

    /// BR-PRD: append one IMPORTED entry, preserving its ids, batch id,
    /// rebuild inputs, and state verbatim; only the local LSN is re-keyed
    /// to this WAL's next slot. State preservation is load-bearing — a
    /// `Synced` entry must not re-drain, a `Failed` one keeps its history.
    pub fn append_imported(&self, mut entry: WalEntry) -> Result<u64, WalError> {
        entry.local_lsn = 0;
        self.insert_entry(entry)
    }

    /// Atomically append a complete imported backup. Every row is encoded and
    /// the aggregate budget is checked before a single sled key is mutated.
    pub fn append_imported_batch(&self, mut entries: Vec<WalEntry>) -> Result<u64, WalError> {
        self.append_imported_batch_with(&mut entries, || Ok(()))
    }

    fn append_imported_batch_with(
        &self,
        entries: &mut [WalEntry],
        before_commit: impl FnOnce() -> Result<(), WalError>,
    ) -> Result<u64, WalError> {
        if entries.is_empty() {
            return Ok(0);
        }
        let _append = self
            .append_gate
            .lock()
            .map_err(|_| WalError::Io("WAL append serialization mutex is poisoned".to_string()))?;
        let first_lsn = self.next_lsn()?;
        let mut batch = sled::Batch::default();
        let mut encoded_bytes = 0u64;
        for (offset, entry) in entries.iter_mut().enumerate() {
            entry.local_lsn = first_lsn
                .checked_add(offset as u64)
                .ok_or_else(|| WalError::Io("WAL local LSN overflow during import".to_string()))?;
            let bytes = encode_entry(entry).map_err(WalError::Io)?;
            encoded_bytes = encoded_bytes.saturating_add(bytes.len() as u64 + 16);
            batch.insert(entry.local_lsn.to_be_bytes().to_vec(), bytes);
        }
        let used = self.used_bytes()?;
        if used.saturating_add(encoded_bytes) > self.max_bytes {
            return Err(WalError::Full(used, self.max_bytes));
        }
        before_commit()?;
        self.tree
            .apply_batch(batch)
            .map_err(|e| WalError::Io(e.to_string()))?;
        self.tree.flush().map_err(|e| WalError::Io(e.to_string()))?;
        Ok(first_lsn)
    }

    fn next_lsn(&self) -> Result<u64, WalError> {
        self.tree
            .last()
            .map_err(|e| WalError::Io(e.to_string()))?
            .map(|(key, _)| {
                key_to_lsn(&key)?
                    .checked_add(1)
                    .ok_or_else(|| WalError::Io("WAL local LSN space is exhausted".to_string()))
            })
            .transpose()
            .map(|next| next.unwrap_or(1))
    }

    /// Assign the next local LSN, run the byte budget (R-Sc8), persist.
    fn insert_entry(&self, entry: WalEntry) -> Result<u64, WalError> {
        let _append = self
            .append_gate
            .lock()
            .map_err(|_| WalError::Io("WAL append serialization mutex is poisoned".to_string()))?;
        self.insert_entry_locked(entry)
    }

    fn insert_entry_locked(&self, mut entry: WalEntry) -> Result<u64, WalError> {
        entry.local_lsn = self.next_lsn()?;
        let local_lsn = entry.local_lsn;
        let bytes = encode_entry(&entry).map_err(WalError::Io)?;
        let used = self.used_bytes()?;
        if used.saturating_add(bytes.len() as u64) > self.max_bytes {
            return Err(WalError::Full(used, self.max_bytes));
        }
        self.tree
            .insert(local_lsn.to_be_bytes(), bytes)
            .map_err(|e| WalError::Io(e.to_string()))?;
        self.tree.flush().map_err(|e| WalError::Io(e.to_string()))?;
        Ok(local_lsn)
    }

    /// Diagnostics: number of raw entries.
    #[doc(hidden)]
    pub fn db_len(&self) -> usize {
        self.tree.len()
    }

    /// True at >= 90% of the budget (R-Sc8 `WAL Near Full`).
    pub fn near_full(&self) -> Result<bool, WalError> {
        Ok(self.used_bytes()?.saturating_mul(10) >= self.max_bytes.saturating_mul(9))
    }

    /// Count of entries still `Pending`.
    pub fn pending_count(&self) -> Result<usize, WalError> {
        Ok(self
            .decoded_entries()?
            .into_iter()
            .filter(|entry| entry.state == WalState::Pending)
            .count())
    }

    /// Every `Pending` entry in local-LSN order (W1: the drain input).
    pub fn pending_entries(&self) -> Result<Vec<WalEntry>, WalError> {
        Ok(self
            .decoded_entries()?
            .into_iter()
            .filter(|entry| entry.state == WalState::Pending)
            .collect())
    }

    /// SR-PRD F2: fetch one entry by its local LSN — the live write-back
    /// reads back exactly what was appended, through the same materialize
    /// path boot seeding uses (one implementation, no drift).
    pub fn entry(&self, local_lsn: u64) -> Result<Option<WalEntry>, WalError> {
        let raw = self
            .tree
            .get(local_lsn.to_be_bytes())
            .map_err(|e| WalError::Io(e.to_string()))?;
        raw.map(|raw| decode_at(local_lsn, &raw)).transpose()
    }

    /// SR-PRD F3: every entry in local-LSN order, ALL states — standalone
    /// boot seeds from the WAL because nothing else will ever deliver
    /// these rows server-side (`Pending`, `Synced`, and `Failed` alike).
    pub fn entries(&self) -> Result<Vec<WalEntry>, WalError> {
        self.decoded_entries()
    }

    fn decoded_entries(&self) -> Result<Vec<WalEntry>, WalError> {
        self.tree
            .iter()
            .map(|item| {
                let (key, value) = item.map_err(|e| WalError::Io(e.to_string()))?;
                let local_lsn = key_to_lsn(&key)?;
                decode_at(local_lsn, &value)
            })
            .collect()
    }

    /// D5 `--tail-audit`: the N most recent entries (newest first),
    /// pending + settled, with the fields a developer scanning "did my
    /// wrapup fire?" needs. Undecodable entries are listed by LSN rather
    /// than skipped (W-audit-§2.5: errors that erase themselves).
    pub fn tail(&self, n: usize) -> Vec<TailRow> {
        let mut rows: Vec<TailRow> = Vec::new();
        for item in self.tree.iter().rev() {
            if rows.len() >= n {
                break;
            }
            let Ok((k, v)) = item else { continue };
            let lsn = u64::from_be_bytes(k.as_ref().try_into().unwrap_or([0u8; 8]));
            match decode_entry(&v) {
                Ok(e) => rows.push(TailRow {
                    local_lsn: lsn,
                    recorded_at: e
                        .memories
                        .first()
                        .map(|m| m.context.timestamp.to_rfc3339())
                        .unwrap_or_else(|| "unknown".into()),
                    pending: e.state == WalState::Pending,
                    batch_id: e.batch_id,
                    memory_count: e.memories.len(),
                    draft_keys: e.draft_keys,
                }),
                Err(err) => rows.push(TailRow {
                    local_lsn: lsn,
                    recorded_at: "undecodable".into(),
                    pending: true,
                    batch_id: format!("decode error: {err}"),
                    memory_count: 0,
                    draft_keys: vec![],
                }),
            }
        }
        rows
    }

    /// Transition an entry Pending -> Synced (R-M11).
    pub fn mark_synced(&self, local_lsn: u64, backend_lsn: u64) -> Result<(), WalError> {
        self.update_state(local_lsn, |e| e.state = WalState::Synced { backend_lsn })
    }

    /// Transition an entry Pending -> Failed (terminal, surfaced).
    pub fn mark_failed(&self, local_lsn: u64) -> Result<(), WalError> {
        self.update_state(local_lsn, |e| e.state = WalState::Failed)
    }

    fn update_state(&self, local_lsn: u64, f: impl FnOnce(&mut WalEntry)) -> Result<(), WalError> {
        let key = local_lsn.to_be_bytes();
        let Some(raw) = self
            .tree
            .get(key)
            .map_err(|e| WalError::Io(e.to_string()))?
        else {
            return Err(WalError::Io(format!("no wal entry {local_lsn}")));
        };
        let mut entry: WalEntry = decode_entry(&raw).map_err(WalError::Io)?;
        f(&mut entry);
        let bytes = encode_entry(&entry).map_err(WalError::Io)?;
        self.tree
            .insert(key, bytes)
            .map_err(|e| WalError::Io(e.to_string()))?;
        Ok(())
    }
}

fn key_to_lsn(key: &[u8]) -> Result<u64, WalError> {
    let bytes: [u8; 8] = key.try_into().map_err(|_| WalError::Corrupt {
        local_lsn: 0,
        detail: format!("invalid {}-byte sled key", key.len()),
    })?;
    Ok(u64::from_be_bytes(bytes))
}

fn decode_at(local_lsn: u64, raw: &[u8]) -> Result<WalEntry, WalError> {
    decode_entry(raw).map_err(|detail| WalError::Corrupt { local_lsn, detail })
}

/// Entry codec: `[version u8][len u32 BE][json]`. Version 1.
const WAL_CODEC_VERSION: u8 = 1;

fn encode_entry(e: &WalEntry) -> Result<Vec<u8>, String> {
    let json = serde_json::to_vec(e).map_err(|x| x.to_string())?;
    let mut out = Vec::with_capacity(json.len() + 5);
    out.push(WAL_CODEC_VERSION);
    out.extend_from_slice(&(json.len() as u32).to_be_bytes());
    out.extend_from_slice(&json);
    Ok(out)
}

fn decode_entry(bytes: &[u8]) -> Result<WalEntry, String> {
    if bytes.first() != Some(&WAL_CODEC_VERSION) {
        return Err(format!("unknown wal codec version {bytes:?}"));
    }
    let declared = bytes
        .get(1..5)
        .ok_or("short wal entry")?
        .try_into()
        .map(u32::from_be_bytes)
        .map_err(|_| "short wal entry")? as usize;
    let json = bytes.get(5..).ok_or("short wal entry")?;
    if json.len() != declared {
        return Err(format!(
            "wal entry length mismatch: declared {declared}, actual {}",
            json.len()
        ));
    }
    serde_json::from_slice(json).map_err(|x| x.to_string())
}

impl Wal {
    /// Test-only: every entry's (local_lsn, state) in LSN order.
    #[doc(hidden)]
    pub fn states_for_test(&self) -> Result<Vec<(u64, WalState)>, WalError> {
        Ok(self
            .entries()?
            .into_iter()
            .map(|entry| (entry.local_lsn, entry.state))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn draft(title: &str) -> exocortex_kernel::MemoryDraft {
        use exocortex_kernel::{MemoryContext, Visibility};
        exocortex_kernel::MemoryDraft {
            memory_type: 3,
            title: title.into(),
            content: "c".into(),
            summary: None,
            visibility: Visibility::Org,
            context: MemoryContext {
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
        }
    }

    #[test]
    fn append_assigns_monotonic_lsns_and_transitions_state() {
        let dir = std::env::temp_dir().join(format!("exocortex-wal-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let wal = Wal::open(&dir).unwrap();
        let a = wal
            .append_batch(
                "s1",
                vec![draft("a")],
                vec![exocortex_kernel::MemoryId::new_v7()],
            )
            .unwrap();
        let b = wal
            .append_batch(
                "s2",
                vec![draft("b")],
                vec![exocortex_kernel::MemoryId::new_v7()],
            )
            .unwrap();
        assert!(b > a);
        assert_eq!(wal.pending_count().unwrap(), 2);
        wal.mark_synced(a, 100).unwrap();
        assert_eq!(wal.pending_count().unwrap(), 1);
        wal.mark_failed(b).unwrap();
        assert_eq!(wal.pending_count().unwrap(), 0);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn tree_owns_its_database_context_and_flushes_without_a_db_field() {
        let dir =
            std::env::temp_dir().join(format!("exocortex-tree-owner-{}", uuid::Uuid::new_v4()));
        {
            // `open` drops its local sled::Db handle before returning. The
            // retained Tree owns an Arc<TreeInner> with the page-cache context.
            let wal = Wal::open(&dir).unwrap();
            wal.append_batch("s", vec![draft("durable")], vec![MemoryId::new_v7()])
                .unwrap();
        }
        let reopened = Wal::open(&dir).unwrap();
        assert_eq!(reopened.entries().unwrap().len(), 1);
        drop(reopened);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn bounded_wal_rejects_at_budget() {
        let dir = std::env::temp_dir().join(format!("exocortex-wal-full-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let wal = Wal::open_with_budget(&dir, 4096).unwrap();
        assert!(wal
            .append_batch(
                "s",
                vec![draft("big")],
                vec![exocortex_kernel::MemoryId::new_v7()]
            )
            .is_ok());
        let mut last = Ok(0);
        for i in 0..64 {
            last = wal.append_batch(
                "s",
                vec![draft(&format!("entry-{i}-with-padding-padding-padding"))],
                vec![exocortex_kernel::MemoryId::new_v7()],
            );
            if last.is_err() {
                break;
            }
        }
        assert!(
            matches!(last, Err(WalError::Full(_, _))),
            "budget enforced (R-Sc8)"
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn corrupt_record_is_reported_with_lsn_and_left_untouched() {
        let dir = std::env::temp_dir().join(format!("exocortex-corrupt-{}", uuid::Uuid::new_v4()));
        let wal = Wal::open(&dir).unwrap();
        wal.tree
            .insert(7u64.to_be_bytes(), b"not-a-wal-record")
            .unwrap();
        wal.tree.flush().unwrap();

        let err = wal.entries().unwrap_err();
        assert!(matches!(err, WalError::Corrupt { local_lsn: 7, .. }));
        assert_eq!(
            wal.tree.get(7u64.to_be_bytes()).unwrap().unwrap().as_ref(),
            b"not-a-wal-record",
            "diagnosis must preserve original recovery bytes"
        );
        assert!(matches!(
            wal.pending_entries().unwrap_err(),
            WalError::Corrupt { local_lsn: 7, .. }
        ));
        drop(wal);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn framed_length_mismatch_is_rejected_in_both_directions() {
        let entry = WalEntry {
            local_lsn: 1,
            session_id: "framing".into(),
            memories: vec![draft("framing")],
            memory_ids: vec![MemoryId::new_v7()],
            state: WalState::Pending,
            batch_id: "framing".into(),
            draft_keys: vec!["k".into()],
            tags: vec![vec![]],
        };
        let encoded = encode_entry(&entry).unwrap();
        let actual = u32::try_from(encoded.len() - 5).unwrap();

        for declared in [actual - 1, actual + 1] {
            let mut corrupt = encoded.clone();
            corrupt[1..5].copy_from_slice(&declared.to_be_bytes());
            let error = decode_entry(&corrupt).unwrap_err();
            assert!(
                error.contains("length mismatch"),
                "declared {declared}, actual {actual}: {error}"
            );
        }
    }

    #[test]
    fn imported_batch_budget_failure_leaves_destination_unchanged() {
        let dir = std::env::temp_dir().join(format!("exocortex-import-{}", uuid::Uuid::new_v4()));
        let wal = Wal::open_with_budget(&dir, 1).unwrap();
        let entry = WalEntry {
            local_lsn: 99,
            session_id: "import".into(),
            memories: vec![draft("staged")],
            memory_ids: vec![MemoryId::new_v7()],
            state: WalState::Pending,
            batch_id: "batch".into(),
            draft_keys: vec!["k".into()],
            tags: vec![vec![]],
        };
        assert!(matches!(
            wal.append_imported_batch(vec![entry.clone(), entry]),
            Err(WalError::Full(_, _))
        ));
        assert_eq!(wal.db_len(), 0, "no imported prefix may survive failure");
        drop(wal);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn imported_batch_injected_precommit_failure_preserves_existing_wal() {
        let dir = std::env::temp_dir().join(format!(
            "exocortex-import-injected-{}",
            uuid::Uuid::new_v4()
        ));
        let wal = Wal::open(&dir).unwrap();
        wal.append_batch(
            "existing",
            vec![draft("existing")],
            vec![MemoryId::new_v7()],
        )
        .unwrap();
        let before = wal.entries().unwrap();
        let mut imported = vec![WalEntry {
            local_lsn: 99,
            session_id: "import".into(),
            memories: vec![draft("staged")],
            memory_ids: vec![MemoryId::new_v7()],
            state: WalState::Pending,
            batch_id: "batch".into(),
            draft_keys: vec!["k".into()],
            tags: vec![vec![]],
        }];
        let error = wal
            .append_imported_batch_with(&mut imported, || {
                Err(WalError::Io("injected before atomic commit".into()))
            })
            .unwrap_err();
        assert!(matches!(error, WalError::Io(_)));
        let after = wal.entries().unwrap();
        assert_eq!(after.len(), before.len());
        assert_eq!(after[0].local_lsn, before[0].local_lsn);
        assert_eq!(after[0].session_id, before[0].session_id);
        drop(wal);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
