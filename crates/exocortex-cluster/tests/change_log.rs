//! D25 (palantir-expansion PRD §3.3 S7): the change-feed seam's
//! conformance suite. These invariants are the written contract every
//! `ChangeLog` implementation must preserve — the in-process
//! `RingChangeLog` today, an opt-in durable broker (D26) tomorrow. The
//! case for the seam is CS1: an empty ring once answered "you are
//! current" and silently dropped every invalidation in the gap.

use exocortex_cluster::change_log::{ChangeLog, Replay, RingChangeLog};
use exocortex_storage::Invalidation as StorageInv;
use exocortex_wire::cluster::v1::InvalidationEnvelope;

fn envelope(lsn: u64) -> InvalidationEnvelope {
    // Build through the same production converter the node uses, so
    // the suite exercises real wire shapes.
    let inv = StorageInv::MemoryUpserted {
        id: exocortex_kernel::MemoryId([lsn as u8; 16]),
        lsn,
    };
    let inv_pb = exocortex_cluster::sse::invalidation_to_pb(&inv);
    let mut env = InvalidationEnvelope {
        wire_version: 1,
        ontology_fingerprint: vec![0; 32],
        emitter_node_id: "conformance".into(),
        inv: Some(inv_pb),
        hmac: vec![],
    };
    exocortex_wire::signing::sign_invalidation_envelope(&[7; 32], &mut env);
    env
}

/// LSN order: replay returns envelopes strictly after `since_lsn`,
/// oldest first.
#[test]
fn replay_returns_strictly_newer_envelopes_oldest_first() {
    let log = RingChangeLog::new();
    for lsn in 1..=5 {
        log.append(envelope(lsn));
    }
    match log.replay_since(2) {
        Replay::Fresh(envs) => {
            let lsns: Vec<u64> = envs
                .iter()
                .map(|e| e.inv.as_ref().unwrap().backend_lsn)
                .collect();
            assert_eq!(lsns, vec![3, 4, 5]);
        }
        Replay::TooOld => panic!("mid-buffer replay must be fresh"),
    }
    assert!(matches!(log.replay_since(5), Replay::Fresh(envs) if envs.is_empty()));
}

/// Floor truth: the floor is the oldest still-buffered LSN, never the
/// newest observed — a floor above the buffer front would tell clients
/// to skip events the log still holds (R6-R280).
#[test]
fn floor_is_the_oldest_buffered_lsn_not_the_frontier() {
    let log = RingChangeLog::with_capacity(3);
    for lsn in 11..=15 {
        log.append(envelope(lsn));
    }
    assert_eq!(log.replay_floor(), 13, "oldest of the last 3");
    assert_eq!(log.frontier(), Some(15));
    // since=12 asks for 13+ and the floor is 13: exactly continuous.
    assert!(matches!(log.replay_since(12), Replay::Fresh(_)));
    // since=11 needs 12, which the log no longer holds: a real gap.
    assert!(matches!(log.replay_since(11), Replay::TooOld));
}

/// No silent gaps (CS1): an empty log is fresh-empty only before
/// anything was ever appended; after eviction, a `since_lsn` below the
/// high-water mark is TooOld even though the ring is literally empty.
#[test]
fn empty_log_is_only_fresh_before_the_first_append() {
    let log = RingChangeLog::with_capacity(2);
    assert!(matches!(log.replay_since(0), Replay::Fresh(envs) if envs.is_empty()));
    for lsn in 1..=4 {
        log.append(envelope(lsn));
    }
    // Evicted everything (capacity 2 holds 3 and 4); the empty-ring
    // branch must still refuse a resume at 1.
    assert!(matches!(log.replay_since(1), Replay::TooOld));
    assert!(matches!(log.replay_since(4), Replay::Fresh(envs) if envs.is_empty()));
}

/// Saturation (CS8): `since_lsn = u64::MAX` answers instead of
/// overflowing.
#[test]
fn u64_max_since_answers_without_overflow() {
    let log = RingChangeLog::new();
    log.append(envelope(1));
    assert!(matches!(log.replay_since(u64::MAX), Replay::Fresh(envs) if envs.is_empty()));
}

/// Frontier: `None` until the first append, the max LSN afterwards,
/// surviving eviction.
#[test]
fn frontier_tracks_the_high_water_mark() {
    let log = RingChangeLog::with_capacity(1);
    assert_eq!(log.frontier(), None);
    log.append(envelope(9));
    log.append(envelope(10));
    assert_eq!(log.replay_floor(), 10, "capacity 1 evicted 9");
    assert_eq!(log.frontier(), Some(10));
}

/// The seam is dyn-safe: `ClusterNode` exposes it as `&dyn ChangeLog`
/// and the SSE handler consults exactly that. (Compile-level check +
/// the trait's Send/Sync bound.)
#[test]
fn the_seam_is_object_safe() {
    fn takes_dyn(log: &dyn ChangeLog) -> u64 {
        log.append(envelope(1));
        log.replay_floor()
    }
    let log = RingChangeLog::new();
    assert_eq!(takes_dyn(&log), 1);
}
