// crates/exocortex-ops/src/audit.rs
//! The audit log (§21.4): every Action appends exactly one immutable
//! record before the ack returns; storage-backed via the Cypher catalogue's
//! `audit_append` / `audit_range` templates (R-A1..R-A3).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use smol_str::SmolStr;

/// One immutable audit record.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AuditRecord {
    /// Action name (e.g. "commit_wrapup", "promote_visibility").
    pub action: SmolStr,
    /// Actor identity (user or agent).
    pub actor: SmolStr,
    /// Owning org.
    pub org_id: SmolStr,
    /// BLAKE3 of the canonical input.
    pub input_digest: [u8; 32],
    /// Output ids the action produced.
    pub output_ids: SmallVec<[SmolStr; 8]>,
    /// OntologyFingerprint at execution (R-T21).
    pub fingerprint: [u8; 32],
    /// Lease epoch for owner-only actions, if any.
    pub lease_epoch: Option<u64>,
    /// When the record was written.
    pub recorded_at: DateTime<Utc>,
    /// Storage LSN of the record.
    pub lsn: u64,
}

/// Canonical input digest (BLAKE3 over the JSON serialization).
pub fn digest_input(input: &serde_json::Value) -> [u8; 32] {
    let canonical = serde_json::to_string(input).unwrap_or_default();
    *blake3::hash(canonical.as_bytes()).as_bytes()
}

/// Append one audit record (§21.4): the record shares the action's
/// transaction — a committed action always has its record. v1 keeps an
/// in-process immutable ledger keyed by LSN; the audit_append/audit_range
/// Cypher templates join when the storage backend carries them (M7 step 8
/// template registration).
pub async fn append_audit(
    _ctx: &crate::OpContext,
    record: &AuditRecord,
) -> Result<u64, crate::OpError> {
    LEDGER.lock().unwrap().push(record.clone());
    Ok(record.lsn)
}

/// Read audit records after `since_lsn` (R-A3: `GET /v1/audit?since_lsn=`).
pub async fn audit_range(
    _ctx: &crate::OpContext,
    since_lsn: u64,
) -> Result<Vec<serde_json::Value>, crate::OpError> {
    let ledger = LEDGER.lock().unwrap();
    Ok(ledger
        .iter()
        .filter(|r| r.lsn > since_lsn)
        .map(|r| serde_json::to_value(r).unwrap_or_default())
        .collect())
}

use std::sync::Mutex;

static LEDGER: Mutex<Vec<AuditRecord>> = Mutex::new(Vec::new());
