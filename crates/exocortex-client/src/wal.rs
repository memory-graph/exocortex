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
    #[allow(dead_code)] // flushed through in append_batch; kept for db lifetime
    db: sled::Db,
    tree: sled::Tree,
    max_bytes: u64,
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
            db,
            tree,
            max_bytes,
        })
    }

    fn used_bytes(&self) -> u64 {
        self.tree
            .iter()
            .values()
            .flatten()
            .map(|v| v.len() as u64 + 16)
            .sum()
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

    /// BR-PRD: append one IMPORTED entry, preserving its ids, batch id,
    /// rebuild inputs, and state verbatim; only the local LSN is re-keyed
    /// to this WAL's next slot. State preservation is load-bearing — a
    /// `Synced` entry must not re-drain, a `Failed` one keeps its history.
    pub fn append_imported(&self, mut entry: WalEntry) -> Result<u64, WalError> {
        entry.local_lsn = 0;
        self.insert_entry(entry)
    }

    /// Assign the next local LSN, run the byte budget (R-Sc8), persist.
    fn insert_entry(&self, mut entry: WalEntry) -> Result<u64, WalError> {
        entry.local_lsn = self
            .tree
            .last()
            .map_err(|e| WalError::Io(e.to_string()))?
            .map(|(k, _)| {
                let mut b = [0u8; 8];
                b.copy_from_slice(&k);
                u64::from_be_bytes(b) + 1
            })
            .unwrap_or(1);
        let local_lsn = entry.local_lsn;
        let bytes = encode_entry(&entry).map_err(WalError::Io)?;
        if self.used_bytes() + bytes.len() as u64 > self.max_bytes {
            return Err(WalError::Full(self.used_bytes(), self.max_bytes));
        }
        self.tree
            .insert(local_lsn.to_be_bytes(), bytes)
            .map_err(|e| WalError::Io(e.to_string()))?;
        self.db.flush().map_err(|e| WalError::Io(e.to_string()))?;
        Ok(local_lsn)
    }

    /// Diagnostics: number of raw entries.
    #[doc(hidden)]
    pub fn db_len(&self) -> usize {
        self.tree.len()
    }

    /// True at >= 90% of the budget (R-Sc8 `WAL Near Full`).
    pub fn near_full(&self) -> bool {
        self.used_bytes() * 10 >= self.max_bytes * 9
    }

    /// Count of entries still `Pending`.
    pub fn pending_count(&self) -> usize {
        self.tree
            .iter()
            .values()
            .flatten()
            .filter_map(|v| decode_entry(&v).ok())
            .filter(|e| e.state == WalState::Pending)
            .count()
    }

    /// Every `Pending` entry in local-LSN order (W1: the drain input).
    pub fn pending_entries(&self) -> Vec<WalEntry> {
        self.tree
            .iter()
            .values()
            .flatten()
            .filter_map(|v| decode_entry(&v).ok())
            .filter(|e| e.state == WalState::Pending)
            .collect()
    }

    /// SR-PRD F2: fetch one entry by its local LSN — the live write-back
    /// reads back exactly what was appended, through the same materialize
    /// path boot seeding uses (one implementation, no drift).
    pub fn entry(&self, local_lsn: u64) -> Option<WalEntry> {
        let raw = self.tree.get(local_lsn.to_be_bytes()).ok().flatten()?;
        decode_entry(&raw).ok()
    }

    /// SR-PRD F3: every entry in local-LSN order, ALL states — standalone
    /// boot seeds from the WAL because nothing else will ever deliver
    /// these rows server-side (`Pending`, `Synced`, and `Failed` alike).
    pub fn entries(&self) -> Vec<WalEntry> {
        self.tree
            .iter()
            .values()
            .flatten()
            .filter_map(|v| decode_entry(&v).ok())
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
    let json = bytes.get(5..).ok_or("short wal entry")?;
    serde_json::from_slice(json).map_err(|x| x.to_string())
}

impl Wal {
    /// Test-only: every entry's (local_lsn, state) in LSN order.
    #[doc(hidden)]
    pub fn states_for_test(&self) -> Vec<(u64, WalState)> {
        self.tree
            .iter()
            .values()
            .flatten()
            .filter_map(|v| decode_entry(&v).ok())
            .map(|e| (e.local_lsn, e.state))
            .collect()
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
        assert_eq!(wal.pending_count(), 2);
        wal.mark_synced(a, 100).unwrap();
        assert_eq!(wal.pending_count(), 1);
        wal.mark_failed(b).unwrap();
        assert_eq!(wal.pending_count(), 0);
        std::fs::remove_dir_all(&dir).unwrap();
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
}
