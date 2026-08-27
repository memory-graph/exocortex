//! The audit log (§21.4): every Action appends exactly one immutable
//! record before the ack returns; storage-backed via the Cypher catalogue's
//! `audit_append` / `audit_range` templates (R-A1..R-A3).

/// One immutable audit event. Storage assigns its LSN in the same atomic
/// transaction as the protected mutation.
pub use exocortex_storage::AuditEvent as AuditRecord;

/// Canonical input digest (BLAKE3 over the JSON serialization).
pub fn digest_input(input: &serde_json::Value) -> [u8; 32] {
    let canonical = serde_json::to_string(input).unwrap_or_default();
    *blake3::hash(canonical.as_bytes()).as_bytes()
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
    ctx.storage
        .audit_range(org, since_lsn, 1000)
        .await
        .map_err(|e| crate::OpError::Storage(e.to_string()))
}
