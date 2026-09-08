//! D10 (§23 #23): the cluster wire protocol proven in-process —
//! protobuf-encoded invalidation deltas fan out across three nodes at
//! a floor throughput, and an additive-only wire-schema change (an
//! unknown proto field from a newer peer) decodes and admits on
//! current nodes without breaking them. The version-mismatch refusal
//! leg is `cluster.rs::peer_admission_rejects_wire_version_mismatch`.

use std::sync::Arc;
use std::time::Instant;

use exocortex_cluster::ClusterNode;
use exocortex_kernel::MemoryId;
use exocortex_pack_dev_v1::pack_def;
use exocortex_storage::{InMemoryStorage, Invalidation};

fn ontology_fp() -> exocortex_kernel::OntologyFingerprint {
    exocortex_kernel::Ontology::from_packs(vec![pack_def()])
        .unwrap()
        .fingerprint
}

/// §23 #23 (throughput leg): 10k signed protobuf invalidation deltas
/// admit-and-publish across a three-node cluster faster than the floor
/// (>= 5,000 deltas/sec in-process — HMAC-SHA256 sign+verify plus the
/// broadcast per delta), and every node's change log holds the full
/// fan-out in LSN order.
#[tokio::test(flavor = "multi_thread")]
async fn invalidations_fan_out_across_three_nodes_at_floor_throughput() {
    let storage = InMemoryStorage::new(Arc::new(
        exocortex_kernel::Ontology::from_packs(vec![pack_def()]).unwrap(),
    ));
    let nodes = [
        ClusterNode::new(
            Arc::new(storage.clone_dyn()),
            "node-a".into(),
            ontology_fp(),
            [7u8; 32],
        ),
        ClusterNode::new(
            Arc::new(storage.clone_dyn()),
            "node-b".into(),
            ontology_fp(),
            [7u8; 32],
        ),
        ClusterNode::new(
            Arc::new(storage.clone_dyn()),
            "node-c".into(),
            ontology_fp(),
            [7u8; 32],
        ),
    ];

    const DELTAS: u64 = 10_000;
    let started = Instant::now();
    for lsn in 1..=DELTAS {
        let env = nodes[0].envelope(Invalidation::MemoryUpserted {
            id: MemoryId::new_v7(),
            lsn,
        });
        nodes[0].admit_and_publish(env).expect("admit + publish");
    }
    let elapsed = started.elapsed();
    let per_second = DELTAS as f64 / elapsed.as_secs_f64();
    assert!(
        per_second >= 5_000.0,
        "fan-out floor 5,000 deltas/sec, measured {per_second:.0}/s over {elapsed:?}"
    );

    // Peers genuinely receive: replay the publisher's retained window
    // into node-b and node-c (the in-process stand-in for transport
    // delivery) and assert every node's log holds the fan-out — the
    // test name says three nodes, so the proof must too.
    let floor = nodes[0].change_log().replay_floor();
    let deliveries = match nodes[0].change_log().replay_since(floor - 1) {
        exocortex_cluster::change_log::Replay::Fresh(deltas) => deltas,
        exocortex_cluster::change_log::Replay::TooOld => {
            panic!("replay from the floor must be fresh")
        }
    };
    for node in [&nodes[1], &nodes[2]] {
        for delta in &deliveries {
            node.admit_and_publish(delta.clone())
                .expect("peer admits + publishes the delivered envelope");
        }
        let log = node.change_log();
        let frontier = log.frontier().expect("peer frontier after delivery");
        assert_eq!(frontier, DELTAS, "the fan-out reached this node's log");
        let replay = match log.replay_since(floor - 1) {
            exocortex_cluster::change_log::Replay::Fresh(deltas) => deltas,
            exocortex_cluster::change_log::Replay::TooOld => {
                panic!("peer replay from the floor must be fresh")
            }
        };
        let mut last = 0u64;
        for delta in replay {
            let lsn = delta.inv.as_ref().map(|inv| inv.backend_lsn).unwrap_or(0);
            assert!(lsn > last, "LSN order preserved on the peer ring");
            last = lsn;
        }
        assert_eq!(last, DELTAS, "peer order reaches the frontier");
    }
}

/// §23 #23 (additive-rollout leg): an additive-only wire-schema change
/// is an UNKNOWN FIELD on current nodes. A newer peer's envelope
/// carrying field 99 decodes under the current schema (protobuf
/// forward-compat), the payload verifies, and the node admits it —
/// one upgraded node does not break the others.
#[tokio::test]
async fn additive_wire_schema_decodes_and_admits_on_current_nodes() {
    let storage = InMemoryStorage::new(Arc::new(
        exocortex_kernel::Ontology::from_packs(vec![pack_def()]).unwrap(),
    ));
    let node = ClusterNode::new(
        Arc::new(storage.clone_dyn()),
        "node-a".into(),
        ontology_fp(),
        [7u8; 32],
    );
    let env = node.envelope(Invalidation::MemoryUpserted {
        id: MemoryId::new_v7(),
        lsn: 9,
    });

    // Encode, then append an unknown field (99, varint) — exactly the
    // bytes a newer peer with one added field emits and an older node
    // receives over the wire. Tag varint for field 99, wire type 0:
    // (99 << 3) = 792 -> [0x98, 0x06], then value 1.
    let mut bytes = prost::Message::encode_to_vec(&env);
    bytes.extend_from_slice(&[0x98, 0x06, 1]);

    let decoded: exocortex_wire::cluster::v1::InvalidationEnvelope =
        prost::Message::decode(bytes.as_slice()).expect("unknown fields are skipped");
    assert_eq!(decoded, env, "the additive field is inert");
    node.admit(&decoded).expect("additive envelope admits");
}
