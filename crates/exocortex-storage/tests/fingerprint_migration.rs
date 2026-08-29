//! OC-PRD S5 (docs/prd/ontology-compatibility-prd.md §6): existing
//! graphs survive the two-level fingerprint. Behind
//! `--features integration`; skipped (loudly) when `FALKOR_URL` is
//! unset so CI without the docker harness stays green.
//!
//!   docker compose -f tests/docker-compose.yml up -d
//!   FALKOR_URL=falkor://127.0.0.1:6379 \
//!     cargo test -p exocortex-storage --features integration --test fingerprint_migration

#![cfg(feature = "integration")]

use chrono::{Duration, Utc};
use exocortex_kernel::Ontology;
use exocortex_pack_dev_v1::pack_def;
use exocortex_storage::{CypherQuery, FalkorConfig, FalkorStorage, Storage, StorageError};

fn falkor_url() -> Option<String> {
    std::env::var("FALKOR_URL").ok().filter(|u| !u.is_empty())
}

fn graph_suffix() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static N: AtomicU64 = AtomicU64::new(0);
    format!("fpm{}", std::process::id() as u64 % 100_000)
        + &N.fetch_add(1, Ordering::SeqCst).to_string()
}

fn ontology() -> std::sync::Arc<Ontology> {
    std::sync::Arc::new(Ontology::from_packs(vec![pack_def()]).unwrap())
}

fn grown_ontology() -> std::sync::Arc<Ontology> {
    let mut grown = pack_def();
    grown.memory_type_names.push("FutureThing".into());
    std::sync::Arc::new(Ontology::from_packs(vec![grown]).unwrap())
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut o, b| {
            write!(o, "{b:02x}").unwrap();
            o
        })
}

async fn connect(
    graph_name: String,
    onto: std::sync::Arc<Ontology>,
) -> Result<FalkorStorage, StorageError> {
    let url = falkor_url().expect("FALKOR_URL set (checked by runner)");
    FalkorStorage::connect(
        FalkorConfig {
            falkor_url: url.clone(),
            redis_url: std::env::var("REDIS_URL")
                .unwrap_or_else(|_| url.replacen("falkor://", "redis://", 1)),
            graph_name,
            org_id: "test-org".into(),
            node_id: "fp-migration".into(),
        },
        onto,
    )
    .await
}

async fn read_pin(s: &FalkorStorage) -> String {
    let rs = s
        .query_cypher(&CypherQuery {
            template_id: "read_fingerprint",
            params: serde_json::json!({}),
            read_only: true,
            deadline: Utc::now() + Duration::seconds(5),
        })
        .await
        .unwrap();
    rs.rows
        .first()
        .and_then(|row| row.as_array())
        .and_then(|cells| cells.first())
        .and_then(|cell| cell.as_str())
        .expect("pin row")
        .to_string()
}

async fn write_pin(s: &FalkorStorage, fp: &str) {
    s.query_cypher(&CypherQuery {
        template_id: "write_fingerprint",
        params: serde_json::json!({ "fp": fp }),
        read_only: false,
        deadline: Utc::now() + Duration::seconds(5),
    })
    .await
    .unwrap();
}

macro_rules! itest {
    ($name:ident, $body:block) => {
        #[tokio::test(flavor = "multi_thread")]
        async fn $name() {
            if falkor_url().is_none() {
                eprintln!("skipping {}: FALKOR_URL not set", stringify!($name));
                return;
            }
            $body
        }
    };
}

/// A fixture graph pinned with the scheme-v1 value boots against the
/// post-change binary, is rewritten to a scheme-2 record (keeping the
/// legacy value recognized for pre-OC producers), and boots again
/// unchanged.
#[tokio::test(flavor = "multi_thread")]
async fn legacy_v1_pin_boots_migrates_and_reboots() {
    if falkor_url().is_none() {
        eprintln!("skipping legacy_v1_pin_boots_migrates_and_reboots: FALKOR_URL not set");
        return;
    }
    {
        let graph = format!("exocortex_fpm_{}", graph_suffix());
        let onto = ontology();
        let legacy = hex(&onto.build_fingerprint.0);
        // Seed a graph, then overwrite the pin with the bare 64-hex shape
        // a pre-OC binary would have written.
        let seeder = connect(graph.clone(), onto.clone()).await.unwrap();
        write_pin(&seeder, &legacy).await;
        drop(seeder);

        // Boot against the "legacy" graph: migration, not refusal.
        let s = connect(graph.clone(), onto.clone()).await.unwrap();
        let pin = read_pin(&s).await;
        let record: exocortex_kernel::PinnedOntology =
            serde_json::from_str(&pin).expect("migrated to a scheme-2 record");
        assert_eq!(record.scheme, 2);
        assert_eq!(record.compatibility, hex(&onto.fingerprint.0));
        assert_eq!(record.build, legacy);
        assert_eq!(record.accepted, vec![legacy]);
        // The recognized window keeps the legacy value alive for pre-OC
        // producers rolling through the upgrade.
        let recognized = s.recognized_ontology_fingerprints();
        assert!(recognized.contains(&onto.build_fingerprint.0));
        assert!(recognized.contains(&onto.fingerprint.0));
        drop(s);

        // Boot again: unchanged (satisfied, no further mutation).
        let s2 = connect(graph.clone(), onto.clone()).await.unwrap();
        let pin2 = read_pin(&s2).await;
        let record2: exocortex_kernel::PinnedOntology = serde_json::from_str(&pin2).unwrap();
        assert_eq!(record2, record);
    }
}

/// A legacy pin that does not match this build's v1-scheme
/// recomputation refuses startup exactly as before, repeatedly —
/// nothing rewrites the stored value to manufacture a boot.
itest!(mismatched_legacy_pin_refuses_without_rewrite, {
    let graph = format!("exocortex_fpm_{}", graph_suffix());
    let onto = ontology();
    let s = connect(graph.clone(), onto.clone()).await.unwrap();
    let wrong = hex(&[7u8; 32]);
    write_pin(&s, &wrong).await;
    drop(s);

    for _ in 0..2 {
        let err = match connect(graph.clone(), onto.clone()).await {
            Ok(_) => panic!("mismatched legacy pin must refuse startup"),
            Err(e) => e,
        };
        assert!(
            matches!(err, StorageError::FingerprintMismatch { .. }),
            "{err:?}"
        );
    }
});

/// A superset runtime advances a pin written by the subset ontology
/// and retains the prior fingerprint in the recognized window; the
/// subset runtime can then no longer boot.
#[tokio::test(flavor = "multi_thread")]
async fn superset_runtime_advances_the_pin() {
    if falkor_url().is_none() {
        eprintln!("skipping superset_runtime_advances_the_pin: FALKOR_URL not set");
        return;
    }
    {
        let graph = format!("exocortex_fpm_{}", graph_suffix());
        let small = ontology();
        let grown = grown_ontology();
        assert!(small.summary.is_subset_of(&grown.summary));

        let s = connect(graph.clone(), small.clone()).await.unwrap();
        assert_eq!(
            s.recognized_ontology_fingerprints(),
            vec![small.fingerprint.0]
        );
        drop(s);

        let s2 = connect(graph.clone(), grown.clone()).await.unwrap();
        let pin_grown = read_pin(&s2).await;
        let record: exocortex_kernel::PinnedOntology = serde_json::from_str(&pin_grown).unwrap();
        assert_eq!(record.compatibility, hex(&grown.fingerprint.0));
        assert_eq!(record.accepted, vec![hex(&small.fingerprint.0)]);
        assert_eq!(
            s2.recognized_ontology_fingerprints(),
            vec![grown.fingerprint.0, small.fingerprint.0]
        );
        drop(s2);

        assert!(matches!(
            connect(graph.clone(), small.clone()).await,
            Err(StorageError::FingerprintMismatch { .. })
        ));
    }
}
