//! D21 (adapter-contract PRD §3.1/§3.4, steps a+d): the projection
//! contract and the source schema-evolution policy.
//!
//! What is pinned here, row by row against the PRD's D4 table:
//! - registration without a projection is refused for table-shaped
//!   flavors and names the missing declaration;
//! - a mapped field absent from the declared schema is refused
//!   (indistinguishable from removal/renaming — fail closed);
//! - a projection change without a `mapping_version` bump is refused;
//! - unmapped additions are ACCEPTED at a deliberate re-registration and
//!   write exactly one audit row;
//! - a mapped column removed, retyped, or renamed fails closed;
//! - a batch whose snapshot `schema_hash` drifted rejects every row with
//!   `SCHEMA_DRIFT` until the projection is re-registered;
//! - exceeding the declared window bound rejects the batch naming the
//!   bound (`PROJECTION_BOUND_EXCEEDED`);
//! - a snapshot naming an already-superseded id is `SOURCE_REWOUND`.

use std::sync::Arc;

use exocortex_ingest::IngestServer;
use exocortex_pack_dev_v1::pack_def;
use exocortex_storage::{InMemoryStorage, Storage};
use exocortex_wire::ingest::v1::{
    ingest_service_server::IngestService, ExternalKey, ExternalSnapshotInfo, IngestBatch,
    MemoryDraft, ProducerIdentity, ProjectionBounds, ProjectionDescriptor, ProjectionField,
    RegisterSourceRequest, RejectCode, SourceColumn,
};

fn server() -> IngestServer<InMemoryStorage> {
    let onto = Arc::new(exocortex_kernel::Ontology::from_packs(vec![pack_def()]).unwrap());
    IngestServer::new(
        Arc::new(InMemoryStorage::new(onto.clone())),
        onto,
        [5u8; 32],
    )
}

fn projection(mapping_version: u32, columns: &[(&str, &str)], bound: u64) -> ProjectionDescriptor {
    ProjectionDescriptor {
        selector: "table:events where kind='fix'".into(),
        fields: vec![ProjectionField {
            source_field: "fix_title".into(),
            memory_type: "Fix".into(),
            kind: String::new(),
        }],
        source_schema: columns
            .iter()
            .map(|(name, ty)| SourceColumn {
                name: (*name).into(),
                data_type: (*ty).into(),
            })
            .collect(),
        mapping_version,
        bounds: Some(ProjectionBounds {
            max_rows_per_window: bound,
            max_rows_per_run: 1000,
            max_graph_share_percent: 25,
        }),
        last_snapshot_id: "snap-1".into(),
    }
}

fn schema_hash_of(columns: &[(&str, &str)]) -> Vec<u8> {
    let stored: Vec<(String, String)> = columns
        .iter()
        .map(|(n, t)| ((*n).to_string(), (*t).to_string()))
        .collect();
    let hex = exocortex_ingest::service::projection_schema_hash(&stored);
    (0..32)
        .map(|i| u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).unwrap())
        .collect()
}

async fn register(
    srv: &IngestServer<InMemoryStorage>,
    descriptor: Option<ProjectionDescriptor>,
) -> Result<(), tonic::Status> {
    register_as(srv, "table-adapter", "iceberg://lake/events", descriptor).await
}

async fn register_as(
    srv: &IngestServer<InMemoryStorage>,
    producer: &str,
    source_uri: &str,
    descriptor: Option<ProjectionDescriptor>,
) -> Result<(), tonic::Status> {
    let mut r = RegisterSourceRequest {
        org_id: "org".into(),
        source_uri: source_uri.into(),
        producer_id: producer.into(),
        ceiling: 3,
        source_flavor: "iceberg".into(),
        producer_kind: 4,
        producer: Some(ProducerIdentity {
            node_id: "node".into(),
            agent_id: String::new(),
            adapter_id: "adapter".into(),
            hmac_signature: vec![],
            client_metadata: None,
        }),
        projection: descriptor,
    };
    exocortex_wire::signing::sign_registration(&[5u8; 32], &mut r);
    srv.register_source(tonic::Request::new(r))
        .await
        .map(|_| ())
}

fn table_batch(
    snapshot_id: &str,
    rows: usize,
    schema_hash: Vec<u8>,
    fingerprint: Vec<u8>,
) -> IngestBatch {
    let memories: Vec<MemoryDraft> = (0..rows)
        .map(|i| MemoryDraft {
            draft_key: format!("r{i}"),
            id: String::new(),
            memory_type: "Fix".into(),
            title: format!("row {i}"),
            content: "table row content".into(),
            tags: vec![],
            visibility: 3,
            valid_from: None,
            valid_until: None,
            external_key: Some(ExternalKey {
                table_uuid: vec![1u8; 16],
                logical_pk: format!("pk-{i}"),
                mapping_version: 1,
            }),
        })
        .collect();
    let mut b = IngestBatch {
        org_id: "org".into(),
        source_uri: "iceberg://lake/events".into(),
        producer_id: "table-adapter".into(),
        batch_id: format!("b-{snapshot_id}-{rows}"),
        mapping_version: "iceberg:1".into(),
        ontology_fingerprint: fingerprint,
        ceiling: 3,
        checksum: String::new(),
        observed_at: None,
        recorded_at: None,
        snapshot: Some(ExternalSnapshotInfo {
            snapshot_id: snapshot_id.into(),
            schema_hash,
            source_flavor: "iceberg".into(),
        }),
        memories,
        relationships: vec![],
        producer: Some(ProducerIdentity {
            node_id: "node".into(),
            agent_id: String::new(),
            adapter_id: "adapter".into(),
            hmac_signature: vec![],
            client_metadata: None,
        }),
    };
    exocortex_wire::signing::prepare_batch(&[5u8; 32], &mut b);
    b
}

const V1_SCHEMA: &[(&str, &str)] = &[("fix_title", "string"), ("created_at", "timestamp")];

#[tokio::test]
async fn table_registration_requires_a_projection() {
    let srv = server();
    let err = register(&srv, None).await.unwrap_err();
    assert!(
        err.message().contains("declared projection"),
        "names the missing declaration: {}",
        err.message()
    );
}

#[tokio::test]
async fn mapped_field_outside_the_declared_schema_fails_closed() {
    let srv = server();
    let mut bad = projection(1, V1_SCHEMA, 10);
    bad.fields[0].source_field = "renamed_column".into();
    let err = register(&srv, Some(bad)).await.unwrap_err();
    assert!(
        err.message()
            .contains("absent from the declared source schema"),
        "{}",
        err.message()
    );
}

#[tokio::test]
async fn projection_change_without_a_mapping_bump_is_refused() {
    let srv = server();
    register(&srv, Some(projection(1, V1_SCHEMA, 10)))
        .await
        .unwrap();
    let mut changed = projection(1, V1_SCHEMA, 10);
    changed.selector = "table:events where kind='fix' and env='prod'".into();
    let err = register(&srv, Some(changed)).await.unwrap_err();
    assert!(
        err.message().contains("mapping_version bump"),
        "{}",
        err.message()
    );
}

#[tokio::test]
async fn unmapped_addition_is_accepted_and_writes_exactly_one_audit_row() {
    let srv = server();
    register(&srv, Some(projection(1, V1_SCHEMA, 10)))
        .await
        .unwrap();
    let before = srv.storage.audit_range("org", 0, 1000).await.unwrap().len();
    // A new unmapped column arrives with the deliberate bump.
    let extended = projection(
        2,
        &[
            ("fix_title", "string"),
            ("created_at", "timestamp"),
            ("operator_note", "string"),
        ],
        10,
    );
    register(&srv, Some(extended)).await.unwrap();
    let rows = srv.storage.audit_range("org", 0, 1000).await.unwrap();
    assert_eq!(rows.len(), before + 1, "exactly one audit row");
    assert_eq!(rows.last().unwrap()["action"], "schema_extended");
}

#[tokio::test]
async fn mapped_column_removal_retype_and_rename_fail_closed() {
    let srv = server();
    register(&srv, Some(projection(1, V1_SCHEMA, 10)))
        .await
        .unwrap();

    // Removed: fix_title vanishes.
    let removed = projection(2, &[("created_at", "timestamp")], 10);
    let err = register(&srv, Some(removed)).await.unwrap_err();
    assert!(
        err.message().contains("removed or renamed"),
        "{}",
        err.message()
    );

    // Retyped: fix_title becomes an int under the same name.
    let retyped = projection(2, &[("fix_title", "long"), ("created_at", "timestamp")], 10);
    let err = register(&srv, Some(retyped)).await.unwrap_err();
    assert!(err.message().contains("retyped"), "{}", err.message());

    // Renamed: removed + added — indistinguishable from removal.
    let renamed = projection(
        2,
        &[("fix_heading", "string"), ("created_at", "timestamp")],
        10,
    );
    let err = register(&srv, Some(renamed)).await.unwrap_err();
    assert!(
        err.message().contains("removed or renamed"),
        "{}",
        err.message()
    );
}

#[tokio::test]
async fn drifted_schema_hash_rejects_every_row_until_re_registration() {
    let srv = server();
    register(&srv, Some(projection(1, V1_SCHEMA, 10)))
        .await
        .unwrap();
    let b = table_batch(
        "snap-1",
        2,
        schema_hash_of(&[("fix_title", "long")]),
        srv.ontology.fingerprint.0.to_vec(),
    );
    let ack = srv
        .submit(tonic::Request::new(b))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(ack.accepted, 0);
    assert!(ack
        .rejections
        .iter()
        .all(|r| r.code == RejectCode::SchemaDrift as i32));

    // The deliberate re-registration with the drifted schema (retyped
    // columns would be refused; here the source legitimately added a
    // column and the mapping re-registered against it).
    let extended = projection(
        2,
        &[
            ("fix_title", "string"),
            ("created_at", "timestamp"),
            ("operator_note", "string"),
        ],
        10,
    );
    register(&srv, Some(extended)).await.unwrap();
    let b = table_batch(
        "snap-2",
        2,
        schema_hash_of(&[
            ("fix_title", "string"),
            ("created_at", "timestamp"),
            ("operator_note", "string"),
        ]),
        srv.ontology.fingerprint.0.to_vec(),
    );
    let mut b = b;
    b.batch_id = "after-reregistration".into();
    exocortex_wire::signing::prepare_batch(&[5u8; 32], &mut b);
    let ack = srv
        .submit(tonic::Request::new(b))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(ack.accepted, 2, "re-registered schema admits rows");
}

#[tokio::test]
async fn bound_exceeded_rejects_the_batch_naming_the_bound() {
    let srv = server();
    register(&srv, Some(projection(1, V1_SCHEMA, 1)))
        .await
        .unwrap();
    let b = table_batch(
        "snap-1",
        3,
        schema_hash_of(V1_SCHEMA),
        srv.ontology.fingerprint.0.to_vec(),
    );
    let ack = srv
        .submit(tonic::Request::new(b))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(ack.accepted, 0);
    let rejection = &ack.rejections[0];
    assert_eq!(rejection.code, RejectCode::ProjectionBoundExceeded as i32);
    assert!(
        rejection.detail.contains("max_rows_per_window"),
        "names the bound: {}",
        rejection.detail
    );
}

#[tokio::test]
async fn rewound_snapshot_is_rejected_with_its_own_code() {
    let srv = server();
    register(&srv, Some(projection(1, V1_SCHEMA, 10)))
        .await
        .unwrap();
    let hash = schema_hash_of(V1_SCHEMA);
    for (id, bid) in [("snap-1", "w1"), ("snap-2", "w2")] {
        let mut b = table_batch(id, 1, hash.clone(), srv.ontology.fingerprint.0.to_vec());
        b.batch_id = bid.into();
        exocortex_wire::signing::prepare_batch(&[5u8; 32], &mut b);
        let ack = srv
            .submit(tonic::Request::new(b))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(ack.accepted, 1);
    }
    // The source rolls back to snap-1: distinguishable by code alone.
    let mut b = table_batch("snap-1", 1, hash, srv.ontology.fingerprint.0.to_vec());
    b.batch_id = "w3".into();
    exocortex_wire::signing::prepare_batch(&[5u8; 32], &mut b);
    let ack = srv
        .submit(tonic::Request::new(b))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(ack.accepted, 0);
    assert!(
        ack.rejections
            .iter()
            .all(|r| r.code == RejectCode::SourceRewound as i32),
        "{:#?}",
        ack.rejections
    );
}

// ---------------------------------------------------------------------------
// D13: deterministic entity resolution. The same external row ingested
// through two different producers converges on ONE external-identity
// entity — no fuzzy matching, no name dependence, no source dependence.
// ---------------------------------------------------------------------------

/// Two snapshot memories over the SAME (table_uuid, logical_pk) from
/// different sources share the external join point; retrieval by that
/// entity returns both (the graph is a model of a world, not a pile of
/// documents).
#[tokio::test]
async fn external_identity_joins_across_producers() {
    use exocortex_wire::ingest::v1::{ExternalKey, ExternalSnapshotInfo};

    let srv = server();
    register(&srv, Some(projection(1, V1_SCHEMA, 10)))
        .await
        .unwrap();
    // The second producer registers its own mirror source (same declared
    // schema shape — the projection contract is per source).
    register_as(
        &srv,
        "mirror-adapter",
        "iceberg://mirror/events",
        Some(projection(1, V1_SCHEMA, 10)),
    )
    .await
    .unwrap();
    let hash = schema_hash_of(V1_SCHEMA);

    let submit_from = |producer: &str, source_uri: &str, batch_id: &str, title: &str| {
        let mut memories = Vec::new();
        for (i, name) in [title, title].iter().enumerate() {
            memories.push(exocortex_wire::ingest::v1::MemoryDraft {
                draft_key: format!("k{i}"),
                id: String::new(),
                memory_type: "Fix".into(),
                title: (*name).into(),
                content: format!("content for {name}"),
                tags: vec![],
                visibility: 3,
                valid_from: None,
                valid_until: None,
                external_key: Some(ExternalKey {
                    table_uuid: vec![9u8; 16],
                    logical_pk: format!("row-{}", name.len()),
                    mapping_version: 1,
                }),
            });
        }
        let _ = memories.pop(); // one memory per batch
        let mut b = table_batch(
            "snap-1",
            1,
            hash.clone(),
            srv.ontology.fingerprint.0.to_vec(),
        );
        b.producer_id = producer.into();
        b.source_uri = source_uri.into();
        b.batch_id = batch_id.into();
        b.memories = memories;
        b.snapshot = Some(ExternalSnapshotInfo {
            snapshot_id: format!("snap-{batch_id}"),
            schema_hash: hash.clone(),
            source_flavor: "iceberg".into(),
        });
        exocortex_wire::signing::prepare_batch(&[5u8; 32], &mut b);
        b
    };

    // Same row, two producers, two sources.
    for (producer, source, batch) in [
        ("table-adapter", "iceberg://lake/events", "d13-a"),
        ("mirror-adapter", "iceberg://mirror/events", "d13-b"),
    ] {
        let mut request =
            tonic::Request::new(submit_from(producer, source, batch, "same external row"));
        request
            .extensions_mut()
            .insert(exocortex_storage::VisibilityContext {
                user_id: "alice".into(),
                org_id: "org".into(),
                project_ids: Default::default(),
                team_ids: Default::default(),
                max_visibility: exocortex_kernel::Visibility::Org,
            });
        let ack = srv.submit(request).await.unwrap().into_inner();
        assert_eq!(ack.accepted, 1, "{:#?}", ack.rejections);
    }

    // The external join point: derive it the way ingest did (over the
    // canonical hex rendering of the table uuid, B8) and query.
    let table_hex: String = hex_of(&[9u8; 16]);
    let entity = exocortex_kernel::EntityId::from_external("org", table_hex.as_bytes(), b"row-17");
    use exocortex_storage::{MemoryFilter, Storage as _};
    let filter = MemoryFilter {
        limit: 20,
        visibility_ctx: exocortex_storage::VisibilityContext {
            user_id: "alice".into(),
            org_id: "org".into(),
            project_ids: Default::default(),
            team_ids: Default::default(),
            max_visibility: exocortex_kernel::Visibility::Org,
        },
        ..MemoryFilter::default()
    };
    let rows = srv.storage.find_by_entity(&entity, &filter).await.unwrap();
    eprintln!(
        "rows: {:?}",
        rows.iter().map(|m| m.title.to_string()).collect::<Vec<_>>()
    );
    let joined: Vec<&str> = rows.iter().map(|m| m.title.as_str()).collect();
    assert_eq!(
        joined.len(),
        2,
        "both producers' memories hang off the one external entity: {joined:?}"
    );

    // Divergence: a different logical_pk is a different row.
    let other_row =
        exocortex_kernel::EntityId::from_external("org", table_hex.as_bytes(), b"row-99");
    let none = srv
        .storage
        .find_by_entity(&other_row, &filter)
        .await
        .unwrap();
    assert!(none.is_empty());
    let other_table_hex: String = hex_of(&[8u8; 16]);
    let other_table =
        exocortex_kernel::EntityId::from_external("org", other_table_hex.as_bytes(), b"row-17");
    assert_ne!(entity, other_table);
    // Domain separation from the name-based space.
    assert_ne!(
        entity,
        exocortex_kernel::EntityId::from_parts("org", 0, "row-17")
    );
}

fn hex_of(bytes: &[u8; 16]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(32);
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
    out
}
