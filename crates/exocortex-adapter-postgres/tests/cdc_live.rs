//! D20 live leg: the replication session against a REAL Postgres with
//! `wal_level=logical` and the wal2json plugin. Gated exactly like
//! the Falkor/Redis live legs — feature `integration` + `POSTGRES_URL`
//! — and it SKIPS LOUDLY when either is absent, so a green default
//! run never claims live coverage it did not execute.
//!
//! The leg proves the whole first-party session: SCRAM
//! authentication, slot creation, START_REPLICATION with wal2json
//! options, the CopyBoth framing (including the 'W' response this
//! crate parses itself), and the standby status updates that keep
//! the connection alive — driven by real INSERTs through the
//! hermetic parse/mapping layers.
//!
//! Requires the test role to have REPLICATION and the server to have
//! wal2json in shared_preload_libraries, e.g.:
//!
//! ```sh
//! docker run -e POSTGRES_PASSWORD=cdc -p 5432:5432 \
//!   -e POSTGRES_INITDB_ARGS="-A scram-sha-256" \
//!   ghcr.io/…/postgres-wal2json:latest   # any wal2json image
//! POSTGRES_URL=postgres://postgres:cdc@127.0.0.1:5432 \
//! FALKOR_URL=… cargo test -p exocortex-adapter-postgres \
//!   --features integration --test cdc_live -- --nocapture
//! ```

#[cfg(feature = "integration")]
#[tokio::test(flavor = "multi_thread")]
async fn live_postgres_logical_replication_delivers_mapped_rows() {
    let Some(dsn) = std::env::var("POSTGRES_URL")
        .ok()
        .filter(|v| !v.trim().is_empty())
    else {
        eprintln!("live Postgres CDC suite UNEXECUTED (POSTGRES_URL unset)");
        return;
    };

    use exocortex_adapter_postgres::replication::{ReplicationSession, StreamEvent};
    use exocortex_adapter_postgres::{map_change, parse_change, CdcMapping, MappedChange};

    let slot = format!("exocortex_test_{}", std::process::id());
    let mut session = ReplicationSession::connect(&dsn).await.expect("connect");
    session
        .create_slot_if_not_exists(&slot)
        .await
        .expect("slot");

    // Driver connection for the fixture writes: a plain SQL session
    // over the same crate's minimal client is not a query client, so
    // drive writes through psql when present; otherwise surface the
    // manual setup honestly.
    let table = "public.exocortex_cdc_live";
    let psql = std::process::Command::new("psql")
        .arg(&dsn)
        .arg("-c")
        .arg(format!(
            "DROP TABLE IF EXISTS {table}; CREATE TABLE {table} (id bigint primary key, \
             title text, detail text, tags text, parent_id bigint); \
             INSERT INTO {table} VALUES (1, 'live finding', 'arrived over the wal', 'live, cdc', NULL);"
        ))
        .output();
    match psql {
        Ok(output) if output.status.success() => {}
        Ok(output) => panic!(
            "fixture writes failed (psql required for the live leg): {}",
            String::from_utf8_lossy(&output.stderr)
        ),
        Err(error) => panic!(
            "psql is required to drive the live leg's writes (install it or point \
             POSTGRES_URL at a server you can write to): {error}"
        ),
    }

    let mapping: CdcMapping = serde_json::from_str(
        r#"{
            "table": "public.exocortex_cdc_live",
            "memory_type": "Problem",
            "title_column": "title",
            "content_columns": ["detail"],
            "pk_columns": ["id"],
            "tags_column": "tags",
            "parent_column": "parent_id",
            "parent_kind": "Causes",
            "mapping_version": 1,
            "column_types": {"id":"int8","title":"text","detail":"text","tags":"text","parent_id":"int8"}
        }"#,
    )
    .unwrap();

    let (tx, mut rx) = tokio::sync::mpsc::channel(16);
    let stream_slot = slot.clone();
    let stream_table = mapping.table.clone();
    let stream = tokio::spawn(async move {
        let session = ReplicationSession::connect(&dsn).await.expect("reconnect");
        session
            .stream_changes(&stream_slot, 0, &[stream_table], |event| {
                if let StreamEvent::Change { payload, .. } = event {
                    tx.blocking_send(payload).ok();
                }
                Ok(())
            })
            .await
    });

    let mut mapped = Vec::new();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    while mapped.len() < 1 && std::time::Instant::now() < deadline {
        match tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv()).await {
            Ok(Some(payload)) => {
                let change = parse_change(&payload).expect("live payload parses");
                match map_change(&mapping, &change).expect("live change maps") {
                    MappedChange::Row(row) => {
                        assert_eq!(row.pk, "1");
                        assert_eq!(row.title, "live finding");
                        mapped.push(row);
                    }
                    MappedChange::OtherTable => {}
                    MappedChange::SkippedNoPk => {}
                    MappedChange::Delete { .. } => {}
                }
            }
            Ok(None) => break,
            Err(_) => continue,
        }
    }
    stream.abort();
    let _ = stream.await;

    // Slot hygiene: drop the test slot so reruns start clean.
    let mut cleanup = ReplicationSession::connect(&dsn)
        .await
        .expect("cleanup connect");
    let _ = cleanup.drop_slot(&slot).await;

    assert!(
        !mapped.is_empty(),
        "the live stream delivered no mapped rows within the deadline"
    );
}

#[cfg(not(feature = "integration"))]
#[test]
fn live_cdc_suite_requires_the_integration_feature() {
    // Loud, not silent: the default suite reports exactly what is not
    // running (the storage-conformance umbrella prints the same line
    // for its live legs).
    eprintln!(
        "live Postgres CDC suite UNEXECUTED (exocortex-adapter-postgres built without the \
         `integration` feature)"
    );
}
