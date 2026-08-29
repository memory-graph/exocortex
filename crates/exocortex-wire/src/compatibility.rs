//! Compatibility-fingerprint comparison for components that cannot
//! link the kernel (R-I1/R-I4: the adapter SDK depends on wire only).
//!
//! This is the wire-side projection of the kernel's AdapterSdk row in
//! the OC-PRD D2 policy table (see
//! `exocortex_kernel::compatibility`): drift between the fingerprint
//! negotiated at connect and the one the server now reports is fatal,
//! surfaced as a typed error naming both values. Subset acceptance
//! for producers is enforced server-side at the ingest boundary,
//! which can evaluate structure; the SDK holds hashes only and keeps
//! exact equality.
//!
//! Every fingerprint comparison outside the kernel must go through
//! this module or the kernel's policy functions — the
//! `compatibility-policy` gate rejects raw comparisons elsewhere.

/// AdapterSdk boundary rule (OC-PRD D2): the server's current
/// compatibility fingerprint must equal the one negotiated at connect.
pub fn negotiated_fingerprint_still_current(
    negotiated: &[u8; 32],
    current: &[u8],
) -> Result<(), FingerprintDrift> {
    if negotiated.as_slice() == current {
        return Ok(());
    }
    let offered: [u8; 32] = current.try_into().map_err(|_| FingerprintDrift {
        detail: format!("server fingerprint is {} bytes, expected 32", current.len()),
    })?;
    Err(FingerprintDrift {
        detail: format!(
            "negotiated {} but the server now reports {} — the backend's \
             ontology drifted mid-run; reconnect and re-negotiate before \
             resubmitting",
            hex(negotiated),
            hex(&offered)
        ),
    })
}

/// Typed drift failure (the wire-side shape of the kernel's
/// `CompatibilityError::PeerMismatch` for the AdapterSdk boundary).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FingerprintDrift {
    /// Human-legible cause naming both fingerprints.
    pub detail: String,
}

impl std::fmt::Display for FingerprintDrift {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ontology fingerprint drift: {}", self.detail)
    }
}

impl std::error::Error for FingerprintDrift {}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn drift_is_named_and_fatal() {
        assert!(negotiated_fingerprint_still_current(&[1; 32], &[1; 32]).is_ok());
        let err = negotiated_fingerprint_still_current(&[1; 32], &[2; 32]).unwrap_err();
        assert!(err.to_string().contains("0101"), "{err}");
        assert!(err.to_string().contains("0202"), "{err}");
        assert!(err.to_string().contains("re-negotiate"), "{err}");
        let err = negotiated_fingerprint_still_current(&[1; 32], &[2; 8]).unwrap_err();
        assert!(err.detail.contains("expected 32"), "{}", err.detail);
    }
}
