//! D21-d (adapter-contract PRD §3.4): the canonical projection schema
//! hash — the digest over a declared column set that a registered
//! projection carries and every batch's `ExternalSnapshot.schema_hash`
//! must match.
//!
//! This lives in `exocortex-wire` because BOTH sides of the boundary
//! must compute the identical value from the identical inputs: the
//! server derives it from the registration (stored against the source)
//! and every table-flavored adapter derives it from the columns it
//! observes, before any wire traffic. The digest itself is the ONE
//! wire digest (`signing::content_digest_hex`); this module owns only
//! the canonical column-set preimage, so adapters never re-derive (or
//! drift from) the server's formula.

/// The canonical digest over a declared column set as RAW 32 bytes —
/// the exact width §18.6 pins for `ExternalSnapshotInfo.schema_hash`
/// on the wire (the ingest boundary width-checks it before comparing,
/// hex-encoded, against the registered projection's stored value).
pub fn schema_hash(columns: &[(String, String)]) -> [u8; 32] {
    let mut sorted = columns.to_vec();
    sorted.sort();
    let mut preimage = String::new();
    for (name, data_type) in &sorted {
        preimage.push_str(name);
        preimage.push('\u{0}');
        preimage.push_str(data_type);
        preimage.push('\u{0}');
    }
    crate::signing::content_digest(preimage.as_bytes())
}

/// Lowercase-hex form of [`schema_hash`] — 64 chars, the storage and
/// comparison representation the server derives from a registration.
pub fn schema_hash_hex(columns: &[(String, String)]) -> String {
    let digest = schema_hash(columns);
    let mut encoded = String::with_capacity(digest.len() * 2);
    use std::fmt::Write as _;
    for byte in digest {
        write!(&mut encoded, "{byte:02x}").expect("writing to String cannot fail");
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cols(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(n, t)| (n.to_string(), t.to_string()))
            .collect()
    }

    #[test]
    fn schema_hash_is_order_independent_and_sixty_four_hex() {
        let a = schema_hash_hex(&cols(&[("id", "int64"), ("title", "string")]));
        let b = schema_hash_hex(&cols(&[("title", "string"), ("id", "int64")]));
        assert_eq!(a, b, "column ORDER must not matter — parquet files in one directory may declare leaf order differently");
        assert_eq!(a.len(), 64, "32-byte digest in hex");
        assert!(a.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn name_type_and_boundary_all_participate() {
        let base = schema_hash_hex(&cols(&[("id", "int64"), ("t", "string")]));
        // A renamed column differs.
        assert_ne!(
            base,
            schema_hash_hex(&cols(&[("id2", "int64"), ("t", "string")]))
        );
        // A retyped column differs.
        assert_ne!(
            base,
            schema_hash_hex(&cols(&[("id", "int32"), ("t", "string")]))
        );
        // A boundary shift that concatenates to the same bytes differs:
        // ("id", "6int") vs ("id6", "int") would collide without the
        // NUL separators.
        assert_ne!(
            schema_hash_hex(&cols(&[("id", "6int")])),
            schema_hash_hex(&cols(&[("id6", "int")]))
        );
    }

    #[test]
    fn empty_column_set_is_a_defined_value() {
        // An empty declared schema still hashes (to the digest of the
        // empty preimage) rather than erroring — the drift comparison
        // stays total.
        let v = schema_hash_hex(&[]);
        assert_eq!(v.len(), 64);
    }
}
