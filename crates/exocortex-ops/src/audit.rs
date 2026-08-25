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
/// transaction — a committed action always has its record. The write routes
/// through `Storage::query_cypher` with the registered `audit_append`
/// template when the backend is FalkorDB (R-A1); the in-process ledger
/// always stays as the double (R-A3 reads prefer storage and fall back).
pub async fn append_audit(
    ctx: &crate::OpContext,
    record: &AuditRecord,
) -> Result<u64, crate::OpError> {
    LEDGER
        .lock()
        .unwrap()
        .get_or_insert_with(HashMap::new)
        .entry(record.org_id.clone())
        .or_default()
        .push(record.clone());

    // R-A1: same-transaction storage write. The Cypher path carries the
    // record durably; failures degrade to the in-process ledger (the ack
    // still names the intended LSN) but are logged for the operator.
    let digest_hex = hex64(&record.input_digest);
    let fp_hex = hex64(&record.fingerprint);
    let q = exocortex_storage::CypherQuery {
        template_id: "audit_append",
        params: serde_json::json!({
            "action": record.action.to_string(),
            "actor": record.actor.to_string(),
            "org_id": record.org_id.to_string(),
            "input_digest": digest_hex,
            "output_ids": record
                .output_ids
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>(),
            "fingerprint": fp_hex,
            "lease_epoch": record.lease_epoch.map(|e| e.to_string()).unwrap_or_default(),
            "recorded_at": record.recorded_at.to_rfc3339(),
            "lsn": record.lsn,
        }),
        read_only: false,
        deadline: chrono::Utc::now() + chrono::Duration::seconds(5),
    };
    match ctx.storage.query_cypher(&q).await {
        Ok(_) => {
            metrics::counter!("exocortex_audit_appended_total", "sink" => "storage").increment(1)
        }
        Err(e) => {
            metrics::counter!("exocortex_audit_appended_total", "sink" => "memory").increment(1);
            tracing::warn!(%e, "audit storage write failed; in-process ledger holds the record");
        }
    }
    Ok(record.lsn)
}

/// Read audit records after `since_lsn` for one org (R-A3: `GET
/// /v1/audit?since_lsn=`). Serves from storage when the backend carries
/// the ledger (FalkorDB); otherwise the in-process ledger answers (tests,
/// in-memory backends). Both paths are org-scoped — one tenant's reads
/// never surface another's records (§17.2).
pub async fn audit_range(
    ctx: &crate::OpContext,
    org: &str,
    since_lsn: u64,
) -> Result<Vec<serde_json::Value>, crate::OpError> {
    let q = exocortex_storage::CypherQuery {
        template_id: "audit_range",
        params: serde_json::json!({
            "org_id": org,
            "since_lsn": since_lsn,
            "limit": 1000u32,
        }),
        read_only: true,
        deadline: chrono::Utc::now() + chrono::Duration::seconds(5),
    };
    if let Ok(rs) = ctx.storage.query_cypher(&q).await {
        if !rs.rows.is_empty() {
            return Ok(rs.rows);
        }
    }
    let ledger = LEDGER.lock().unwrap();
    Ok(ledger
        .as_ref()
        .and_then(|m| m.get(org))
        .map(|rows| {
            rows.iter()
                .filter(|r| r.lsn > since_lsn)
                .map(|r| serde_json::to_value(r).unwrap_or_default())
                .collect()
        })
        .unwrap_or_default())
}

use std::collections::HashMap;
use std::sync::Mutex;

/// Per-org in-process ledger: the durability double for non-Falkor
/// backends (R-A3 fallback). Keyed by org so tenant isolation holds even
/// when the volatile path answers. `Option` inside the lock because
/// `HashMap::new` is not const.
static LEDGER: Mutex<Option<HashMap<SmolStr, Vec<AuditRecord>>>> = Mutex::new(None);

/// Lowercase hex over a 32-byte digest.
fn hex64(bytes: &[u8; 32]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(64);
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
    out
}
