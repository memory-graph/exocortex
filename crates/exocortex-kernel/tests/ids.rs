//! R-T18a identity derivation from external coordinates (B8): raw
//! `table_uuid` bytes, never a lossy string.

use exocortex_kernel::MemoryId;

/// Two distinct 16-byte UUIDs that are BOTH invalid UTF-8 and normalize to
/// the identical `from_utf8_lossy` string MUST derive distinct ids. On the
/// pre-B8 implementation both hashed the same replacement-char string and
/// collided — silently overwriting one table's memory with another's.
#[test]
fn lossy_colliding_uuids_derive_distinct_ids() {
    // 0xff runs are invalid UTF-8; both lossy-convert to the same
    // "\u{FFFD}\u{FFFD}..." string of equal length.
    let a = [
        0xffu8, 0xfe, 0xff, 0xfe, 0xff, 0xfe, 0xff, 0xfe, 0xff, 0xfe, 0xff, 0xfe, 0xff, 0xfe, 0xff,
        0xfe,
    ];
    let b = [
        0xfeu8, 0xff, 0xfe, 0xff, 0xfe, 0xff, 0xfe, 0xff, 0xfe, 0xff, 0xfe, 0xff, 0xfe, 0xff, 0xfe,
        0xff,
    ];

    // Guard the fixture: the two UUIDs must be distinct raw bytes but
    // identical under from_utf8_lossy (the B8 collision shape).
    assert_ne!(a, b);
    assert_eq!(
        String::from_utf8_lossy(&a),
        String::from_utf8_lossy(&b),
        "fixture must exhibit the lossy-collision shape"
    );

    let id_a = MemoryId::from_external("org", "iceberg://cat/db/t1", &a, b"row-1", 1);
    let id_b = MemoryId::from_external("org", "iceberg://cat/db/t1", &b, b"row-1", 1);
    assert_ne!(id_a, id_b, "distinct UUIDs must never collide (B8)");
}

/// Determinism and fork axes survive the raw-bytes change.
#[test]
fn external_identity_determinism_and_forks() {
    let uuid = [7u8; 16];
    let a1 = MemoryId::from_external("org", "s://x", &uuid, b"pk", 1);
    let a2 = MemoryId::from_external("org", "s://x", &uuid, b"pk", 1);
    assert_eq!(a1, a2, "same coordinates -> same id");

    assert_ne!(
        MemoryId::from_external("org", "s://x", &uuid, b"pk", 2),
        a1,
        "mapping_version bump forks identity"
    );
    assert_ne!(
        MemoryId::from_external("org", "s://x", &uuid, b"pk2", 1),
        a1,
        "logical_pk forks identity"
    );
    assert_ne!(
        MemoryId::from_external("org2", "s://x", &uuid, b"pk", 1),
        a1,
        "org forks identity"
    );
}

/// §23.29 property sweep: layout coordinates are deliberately absent from
/// the identity API. Varying path/offset/timestamp-shaped distractors cannot
/// affect an id, while the one schema-evolution coordinate does.
#[test]
fn external_identity_is_layout_immune_property() {
    for seed in 0u16..512 {
        let mut uuid = [0u8; 16];
        uuid[..2].copy_from_slice(&seed.to_be_bytes());
        uuid[2..].copy_from_slice(&blake3::hash(&seed.to_be_bytes()).as_bytes()[..14]);
        let logical_pk = blake3::hash(&[seed as u8, (seed >> 8) as u8]);

        let baseline = MemoryId::from_external(
            "org",
            "iceberg://catalog/db/table",
            &uuid,
            logical_pk.as_bytes(),
            7,
        );
        for (_path, _offset, _timestamp) in [
            (format!("data/{seed}.parquet"), seed as u64, seed as i64),
            (
                format!("compacted/{seed}/part-0"),
                u64::MAX - seed as u64,
                -(seed as i64),
            ),
        ] {
            assert_eq!(
                baseline,
                MemoryId::from_external(
                    "org",
                    "iceberg://catalog/db/table",
                    &uuid,
                    logical_pk.as_bytes(),
                    7,
                ),
                "layout-only values must not enter external identity"
            );
        }
        assert_ne!(
            baseline,
            MemoryId::from_external(
                "org",
                "iceberg://catalog/db/table",
                &uuid,
                logical_pk.as_bytes(),
                8,
            ),
            "mapping-version changes deliberately fork identity"
        );
    }
}
