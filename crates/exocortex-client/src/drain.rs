//! The WAL drain (W1 / audit §6.1): buffered offline wrapups finally reach
//! the backend. Each `Pending` entry is rebuilt into an `IngestBatch` and
//! signed FRESH through `exocortex_wire::signing::prepare_batch` — stored
//! signatures are never replayed (R2's requirement). Dispositions come from
//! the adapter SDK's exhaustive R13 `classify` table; this crate does not
//! grow a second one.

use std::sync::Arc;

use exocortex_adapter_sdk::classify::{classify, Disposition};
use exocortex_kernel::{Ontology, Visibility};
use exocortex_wire::ingest::v1::{
    ingest_service_client::IngestServiceClient, IngestAck, IngestBatch, MemoryDraft as WireDraft,
    ProducerIdentity, RegisterSourceRequest, RelationshipDraft as WireRel,
};
use tonic::transport::Channel;

use crate::wal::{Wal, WalEntry};

/// The producer id every session-wrapup batch carries.
pub const PRODUCER_ID: &str = "session-wrapup";

/// Outcome of one `drain_once` pass.
#[derive(Debug, Default, Clone, Copy)]
pub struct DrainReport {
    /// Entries examined.
    pub attempted: usize,
    /// Entries that reached `Synced` this pass.
    pub synced: usize,
    /// Entries that reached `Failed` (terminal) this pass.
    pub failed: usize,
    /// Entries left Pending (transport errors; retried next pass).
    pub still_pending: usize,
}

/// One pass over the WAL in local-LSN order. Stops at the first transport
/// failure (the backend is unreachable; later entries retry next pass).
#[allow(clippy::too_many_arguments)] // W1: one struct-shaped pipeline, not API surface
pub async fn drain_once(
    wal: &Wal,
    client: &mut IngestServiceClient<Channel>,
    hmac_key: &[u8; 32],
    fingerprint: [u8; 32],
    org_id: &str,
    auth_token: Option<&str>,
    ontology: &Ontology,
    node_id: &str,
) -> Result<DrainReport, tonic::Status> {
    let mut report = DrainReport::default();
    for entry in wal
        .pending_entries()
        .map_err(|error| tonic::Status::data_loss(error.to_string()))?
    {
        report.attempted += 1;
        let mut batch = match rebuild_batch(&entry, fingerprint, org_id, ontology) {
            Ok(b) => b,
            Err(e) => {
                // The stored entry can no longer be shaped for the wire
                // (e.g. its ontology ids no longer resolve): terminal.
                tracing::error!(local_lsn = entry.local_lsn, %e, "wal entry unshapable; marking failed");
                let _ = wal.mark_failed(entry.local_lsn);
                report.failed += 1;
                continue;
            }
        };
        // Register (signed, idempotent) before the first submit (R-I3);
        // the server rejects unsigned registrations (WS1).
        let mut registration = RegisterSourceRequest {
            default_rights: None,
            org_id: org_id.to_string(),
            source_uri: batch.source_uri.clone(),
            producer_id: batch.producer_id.clone(),
            ceiling: batch.ceiling,
            source_flavor: "session".into(),
            projection: None,
            producer_kind: exocortex_wire::ingest::v1::ProducerKind::CodingAgent.into(),
            producer: Some(ProducerIdentity {
                node_id: node_id.to_string(),
                agent_id: String::new(),
                adapter_id: String::new(),
                hmac_signature: vec![],
                client_metadata: None,
            }),
        };
        exocortex_wire::signing::sign_registration(hmac_key, &mut registration);
        let mut reg_req = tonic::Request::new(registration);
        if let Some(token) = auth_token {
            if let Ok(v) = format!("Bearer {token}").parse() {
                reg_req.metadata_mut().insert("authorization", v);
            }
        }
        if let Err(e) = client.register_source(reg_req).await {
            tracing::warn!(%e, "drain: register_source failed; retrying next pass");
            report.still_pending += 1;
            continue;
        }
        // Fresh checksum + signature every attempt (never replay stored
        // signed bytes — R2).
        exocortex_wire::signing::prepare_batch(hmac_key, &mut batch);
        let mut req = tonic::Request::new(batch);
        if let Some(token) = auth_token {
            if let Ok(v) = format!("Bearer {token}").parse() {
                req.metadata_mut().insert("authorization", v);
            }
        }
        match client.submit(req).await {
            Ok(ack) => settle(wal, &entry, ack.into_inner(), &mut report),
            Err(e) => {
                // Transport-level failure: the backend is unreachable or
                // refused; leave Pending and stop the pass.
                tracing::warn!(%e, "drain: submit transport error; stopping pass");
                report.still_pending += 1;
                return Ok(report);
            }
        }
    }
    Ok(report)
}

/// Apply the R13 triage table to an ack and settle the WAL entry.
fn settle(wal: &Wal, entry: &WalEntry, ack: IngestAck, report: &mut DrainReport) {
    if ack.accepted > 0 {
        let backend_lsn = if ack.assigned_lsn > 0 {
            ack.assigned_lsn
        } else {
            // Duplicates replay the ORIGINAL ack's lsn; 0 only if the
            // original committed nothing.
            0
        };
        let _ = wal.mark_synced(entry.local_lsn, backend_lsn);
        metrics::counter!("exocortex_wal_drain_synced_total").increment(1);
        report.synced += 1;
        return;
    }
    // Everything rejected: classify each row; the worst disposition wins.
    let mut worst = Disposition::Success;
    for row in &ack.rejections {
        let code = exocortex_wire::ingest::v1::RejectCode::try_from(row.code)
            .unwrap_or(exocortex_wire::ingest::v1::RejectCode::Unknown);
        let d = classify(code);
        if rank(d) > rank(worst) {
            worst = d;
        }
    }
    match worst {
        Disposition::Success => {
            // DuplicateBatch replay: already committed; reconcile.
            let _ = wal.mark_synced(entry.local_lsn, ack.assigned_lsn);
            metrics::counter!("exocortex_wal_drain_synced_total").increment(1);
            report.synced += 1;
        }
        Disposition::Retry => {
            report.still_pending += 1;
        }
        Disposition::Permanent | Disposition::Fatal => {
            tracing::error!(
                local_lsn = entry.local_lsn,
                ?ack.rejections,
                "wal entry terminally rejected; marked Failed"
            );
            let _ = wal.mark_failed(entry.local_lsn);
            metrics::counter!("exocortex_wal_drain_failed_total").increment(1);
            report.failed += 1;
        }
    }
}

fn rank(d: Disposition) -> u8 {
    match d {
        Disposition::Success => 0,
        Disposition::Retry => 1,
        Disposition::Permanent => 2,
        Disposition::Fatal => 3,
    }
}

/// Rebuild a wire batch from one stored entry. Draft keys, tags, and edge
/// hints ride the entry; ids are the ones assigned at append time, so the
/// server-side rows land under the ids the client already acked.
fn rebuild_batch(
    entry: &WalEntry,
    fingerprint: [u8; 32],
    org_id: &str,
    ontology: &Ontology,
) -> Result<IngestBatch, String> {
    let n = entry.memories.len();
    let keys = entry.draft_keys.clone();
    if keys.len() != n {
        return Err(format!(
            "entry {} carries {} draft keys for {} memories",
            entry.local_lsn,
            keys.len(),
            n
        ));
    }
    let mut drafts = Vec::with_capacity(n);
    for (i, d) in entry.memories.iter().enumerate() {
        let mt_label = ontology
            .memory_type_names
            .get(d.memory_type as usize)
            .ok_or_else(|| format!("memory type id {} no longer resolves", d.memory_type))?
            .clone();
        let tags = entry.tags.get(i).cloned().unwrap_or_default();
        drafts.push(WireDraft {
            rights: None,
            draft_key: keys[i].clone(),
            id: hex16(&entry.memory_ids[i].0),
            memory_type: mt_label.to_string(),
            title: d.title.to_string(),
            content: d.content.clone(),
            tags,
            visibility: d.visibility as i32,
            valid_from: None,
            valid_until: None,
            external_key: None,
        });
    }
    // Edge hints -> wire relationship drafts. W5: an edge is never more
    // visible than the narrower of its endpoints.
    let vis_of = |i: usize| -> Visibility { entry.memories[i].visibility };
    let mut rels = Vec::new();
    for (i, d) in entry.memories.iter().enumerate() {
        for hint in &d.edge_hints {
            let Some(to_pos) = entry.memory_ids.iter().position(|id| *id == hint.to) else {
                continue;
            };
            let kind_label = ontology
                .kinds_by_id
                .get(&hint.kind)
                .map(|k| k.display_name.to_string())
                .ok_or_else(|| format!("kind id {:?} no longer resolves", hint.kind))?;
            let narrower = exocortex_kernel::relationship_visibility(vis_of(i), vis_of(to_pos));
            rels.push(WireRel {
                from_draft_key: keys[i].clone(),
                to_draft_key: keys[to_pos].clone(),
                kind: kind_label,
                strength: hint.strength.unwrap_or(0.0),
                confidence: hint.confidence.unwrap_or(0.8),
                context: String::new(),
                visibility: narrower as i32,
                to_memory_id: String::new(),
            });
        }
    }
    // Stable batch id: stored at append time (IN7); derived from content
    // for legacy entries that predate the field.
    let batch_id = if entry.batch_id.is_empty() {
        content_batch_id(&entry.session_id, &drafts, &rels)
    } else {
        entry.batch_id.clone()
    };
    Ok(IngestBatch {
        org_id: org_id.to_string(),
        source_uri: format!("session://{}", entry.session_id),
        producer_id: PRODUCER_ID.into(),
        batch_id,
        mapping_version: "session-wrapup:1.0.0".into(),
        ontology_fingerprint: fingerprint.to_vec(),
        ceiling: 3,
        checksum: String::new(),
        observed_at: None,
        recorded_at: None,
        snapshot: None,
        memories: drafts,
        relationships: rels,
        producer: Some(ProducerIdentity {
            node_id: String::new(),
            agent_id: String::new(),
            adapter_id: String::new(),
            hmac_signature: vec![],
            client_metadata: None,
        }),
    })
}

/// Fields of a draft that participate in the content-bound batch id. One
/// trait so the MCP preflight shape and the wire shape derive the SAME id
/// for the same session content (IN7): the offline WAL stamp and the online
/// submission must hit one idempotency registry entry.
pub trait BatchIdDraft {
    fn draft_key(&self) -> &str;
    fn memory_type(&self) -> &str;
    fn title(&self) -> &str;
    fn content(&self) -> &str;
    fn visibility(&self) -> i32;
    fn tags(&self) -> &[String];
}

/// Fields of an edge hint that participate in the content-bound batch id.
pub trait BatchIdEdge {
    fn source_key(&self) -> &str;
    fn to_draft_key(&self) -> &str;
    fn kind(&self) -> &str;
    fn strength(&self) -> f32;
}

impl BatchIdDraft for WireDraft {
    fn draft_key(&self) -> &str {
        &self.draft_key
    }
    fn memory_type(&self) -> &str {
        &self.memory_type
    }
    fn title(&self) -> &str {
        &self.title
    }
    fn content(&self) -> &str {
        &self.content
    }
    fn visibility(&self) -> i32 {
        self.visibility
    }
    fn tags(&self) -> &[String] {
        &self.tags
    }
}

impl BatchIdEdge for WireRel {
    fn source_key(&self) -> &str {
        &self.from_draft_key
    }
    fn to_draft_key(&self) -> &str {
        &self.to_draft_key
    }
    fn kind(&self) -> &str {
        &self.kind
    }
    fn strength(&self) -> f32 {
        self.strength
    }
}

fn visibility_discriminant(label: &str) -> i32 {
    match label.to_lowercase().as_str() {
        "private" => 0,
        "project" => 1,
        "team" => 2,
        "org" => 3,
        // Unknown labels never persist: every capture path rejects the
        // draft before the WAL entry is written. -1 keeps the preimage
        // distinct from any accepted visibility.
        _ => -1,
    }
}

impl BatchIdDraft for exocortex_ops::preflight::PreflightMemoryDraft {
    fn draft_key(&self) -> &str {
        &self.draft_key
    }
    fn memory_type(&self) -> &str {
        &self.memory_type
    }
    fn title(&self) -> &str {
        &self.title
    }
    fn content(&self) -> &str {
        &self.content
    }
    fn visibility(&self) -> i32 {
        visibility_discriminant(&self.visibility)
    }
    fn tags(&self) -> &[String] {
        &self.tags
    }
}

impl BatchIdEdge for exocortex_ops::preflight::PreflightEdgeHint {
    fn source_key(&self) -> &str {
        &self.from_draft_key
    }
    fn to_draft_key(&self) -> &str {
        &self.to_draft_key
    }
    fn kind(&self) -> &str {
        &self.kind
    }
    fn strength(&self) -> f32 {
        self.strength
    }
}

/// Deterministic content-bound batch id (IN7): the same wrapup retried
/// after a lost response hits the server's idempotency registry.
pub fn content_batch_id<D: BatchIdDraft, E: BatchIdEdge>(
    session_id: &str,
    drafts: &[D],
    rels: &[E],
) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(session_id.as_bytes());
    for d in drafts {
        hasher.update(d.draft_key().as_bytes());
        hasher.update(&[0x1e]);
        hasher.update(d.memory_type().as_bytes());
        hasher.update(&[0x1e]);
        hasher.update(d.title().as_bytes());
        hasher.update(&[0x1e]);
        hasher.update(d.content().as_bytes());
        hasher.update(&[0x1e]);
        hasher.update(&d.visibility().to_le_bytes());
        for t in d.tags() {
            hasher.update(t.as_bytes());
            hasher.update(&[0x1f]);
        }
        hasher.update(&[0x1e]);
    }
    for r in rels {
        hasher.update(r.source_key().as_bytes());
        hasher.update(&[0x1e]);
        hasher.update(r.to_draft_key().as_bytes());
        hasher.update(&[0x1e]);
        hasher.update(r.kind().as_bytes());
        hasher.update(&[0x1e]);
        hasher.update(&r.strength().to_le_bytes());
    }
    hasher.finalize().to_hex().to_string()
}

/// Hex-render a MemoryId (16 bytes -> 32 chars).
fn hex16(b: &[u8; 16]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(32);
    for byte in b {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// Convenience: run passes until quiescent (startup flush).
#[allow(clippy::too_many_arguments)]
pub async fn drain_all(
    wal: Arc<Wal>,
    client: &mut IngestServiceClient<Channel>,
    hmac_key: [u8; 32],
    fingerprint: [u8; 32],
    org_id: String,
    auth_token: Option<String>,
    ontology: Arc<Ontology>,
    node_id: String,
) {
    loop {
        match drain_once(
            &wal,
            client,
            &hmac_key,
            fingerprint,
            &org_id,
            auth_token.as_deref(),
            &ontology,
            &node_id,
        )
        .await
        {
            Ok(report) => {
                if report.attempted == 0 || report.still_pending == 0 {
                    break;
                }
                // Retry the transport-failed remainder after a pause.
                tokio::time::sleep(std::time::Duration::from_secs(30)).await;
            }
            Err(e) => {
                tracing::warn!(%e, "wal drain pass failed; will retry");
                tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                match wal.pending_count() {
                    Ok(0) => break,
                    Ok(_) => {}
                    Err(error) => {
                        tracing::error!(%error, "WAL corruption stops drain retries");
                        break;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preflight_and_wire_drafts_derive_one_batch_id() {
        let preflight = exocortex_ops::preflight::PreflightMemoryDraft {
            draft_key: "k1".into(),
            memory_type: "Fix".into(),
            title: "title".into(),
            content: "content".into(),
            visibility: "org".into(),
            tags: vec!["rust".into()],
        };
        let preflight_edge = exocortex_ops::preflight::PreflightEdgeHint {
            from_draft_key: "k1".into(),
            to_draft_key: "k2".into(),
            to_memory_id: String::new(),
            kind: "Solves".into(),
            strength: 0.5,
        };
        let wire = WireDraft {
            rights: None,
            draft_key: "k1".into(),
            id: String::new(),
            memory_type: "Fix".into(),
            title: "title".into(),
            content: "content".into(),
            tags: vec!["rust".into()],
            visibility: 3,
            valid_from: None,
            valid_until: None,
            external_key: None,
        };
        let wire_rel = WireRel {
            from_draft_key: "k1".into(),
            to_draft_key: "k2".into(),
            kind: "Solves".into(),
            strength: 0.5,
            confidence: 0.8,
            context: String::new(),
            visibility: 1,
            to_memory_id: String::new(),
        };
        assert_eq!(
            content_batch_id("session", &[preflight], &[preflight_edge]),
            content_batch_id("session", &[wire], &[wire_rel]),
            "the offline WAL stamp and the online submission of one wrapup must share one id"
        );
    }
}
