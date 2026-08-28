#![cfg(feature = "integration")]

use std::{sync::Arc, time::Duration};

use exocortex_dreams::fire::{
    AcknowledgeOutcome, DrainResult, FireMessage, FireOutcome, QuietHours, RecordWriteOutcome,
    RedisFireQueue, DREAMS_QUEUE_MAX,
};
use exocortex_dreams::trigger::DreamsTrigger;
use exocortex_dreams::DreamsEngine;
use exocortex_storage::{InMemoryStorage, RegionKey};

async fn isolated_queue(org: &str) -> Option<(redis::Client, RedisFireQueue, String)> {
    let Ok(redis_url) = std::env::var("REDIS_URL") else {
        eprintln!("SKIP fire_live: REDIS_URL is absent; live Redis suite unexecuted");
        return None;
    };
    let client = redis::Client::open(redis_url).expect("REDIS_URL must be valid");
    let conn = client
        .get_multiplexed_async_connection()
        .await
        .expect("connect to live Redis");
    let key = format!("exocortex:test:dreams:queue:{}", uuid::Uuid::new_v4());
    let queue = RedisFireQueue::new_with_queue_key(conn, QuietHours::none(), org, key.clone());
    Some((client, queue, key))
}

#[tokio::test]
async fn queue_atomically_caps_at_one_thousand_and_drops_newest() {
    let Some((client, mut queue, key)) = isolated_queue("org").await else {
        return;
    };
    let region = RegionKey {
        org: "org".into(),
        project: "project".into(),
        memory_type: 3,
    };
    for index in 0..DREAMS_QUEUE_MAX {
        assert_eq!(
            queue.fire(&region, &format!("node-{index}")).await.unwrap(),
            FireOutcome::Queued
        );
    }
    assert_eq!(
        queue.fire(&region, "newest-dropped").await.unwrap(),
        FireOutcome::Dropped
    );

    let mut inspect = client
        .get_multiplexed_async_connection()
        .await
        .expect("second Redis connection");
    let len: u64 = redis::cmd("LLEN")
        .arg(&key)
        .query_async(&mut inspect)
        .await
        .unwrap();
    assert_eq!(len, DREAMS_QUEUE_MAX);
    let tail: Vec<String> = redis::cmd("LRANGE")
        .arg(&key)
        .arg(-1)
        .arg(-1)
        .query_async(&mut inspect)
        .await
        .unwrap();
    let tail: FireMessage = serde_json::from_str(&tail[0]).unwrap();
    assert_eq!(tail.fired_by, "node-999");
    let _: u64 = redis::cmd("DEL")
        .arg(&key)
        .arg(format!("{key}:deferred"))
        .arg(format!("{key}:processing"))
        .query_async(&mut inspect)
        .await
        .unwrap();
}

#[tokio::test]
async fn organization_queue_rejects_cross_org_regions_before_redis_write() {
    let Some((client, mut queue, key)) = isolated_queue("org-a").await else {
        return;
    };
    let foreign = RegionKey {
        org: "org-b".into(),
        project: "project".into(),
        memory_type: 3,
    };
    let error = queue.fire(&foreign, "foreign-node").await.unwrap_err();
    assert!(error.to_string().contains("rejects region organization"));
    let mut inspect = client
        .get_multiplexed_async_connection()
        .await
        .expect("second Redis connection");
    let len: u64 = redis::cmd("LLEN")
        .arg(&key)
        .query_async(&mut inspect)
        .await
        .unwrap();
    assert_eq!(len, 0);
}

#[tokio::test]
async fn quiet_hours_defers_durably_without_requeueing() {
    let Some((client, mut queue, key)) = isolated_queue("org").await else {
        return;
    };
    let hour: u8 = chrono::Utc::now().format("%H").to_string().parse().unwrap();
    queue.quiet_hours = QuietHours {
        start_hour: hour,
        end_hour: (hour + 1) % 24,
        enabled: true,
        utc_offset_minutes: 0,
    };
    let region = RegionKey {
        org: "org".into(),
        project: "project".into(),
        memory_type: 3,
    };
    assert_eq!(
        queue.fire(&region, "quiet-node").await.unwrap(),
        FireOutcome::Queued
    );
    let second_region = RegionKey {
        memory_type: 4,
        ..region.clone()
    };
    assert_eq!(
        queue.fire(&second_region, "quiet-node").await.unwrap(),
        FireOutcome::Queued
    );
    let result = queue.drain(Duration::from_secs(1)).await.unwrap();
    assert_eq!(result, DrainResult::Deferred);

    let mut inspect = client
        .get_multiplexed_async_connection()
        .await
        .expect("second Redis connection");
    let len: u64 = redis::cmd("LLEN")
        .arg(&key)
        .query_async(&mut inspect)
        .await
        .unwrap();
    assert_eq!(len, 1, "one existing item remains ready");
    let deferred: u64 = redis::cmd("ZCARD")
        .arg(format!("{key}:deferred"))
        .query_async(&mut inspect)
        .await
        .unwrap();
    assert_eq!(deferred, 1, "quiet work remains durable in Redis");
    let processing: u64 = redis::cmd("LLEN")
        .arg(format!("{key}:processing"))
        .query_async(&mut inspect)
        .await
        .unwrap();
    assert_eq!(processing, 0, "deferral atomically leaves in-flight state");

    assert_eq!(
        queue.drain(Duration::from_secs(1)).await.unwrap(),
        DrainResult::Deferred,
        "the second unmarked item is reordered once"
    );
    assert!(matches!(
        queue.drain(Duration::from_secs(1)).await.unwrap(),
        DrainResult::Ready(_)
    ));
    let _: u64 = redis::cmd("DEL")
        .arg(&key)
        .arg(format!("{key}:deferred"))
        .arg(format!("{key}:processing"))
        .query_async(&mut inspect)
        .await
        .unwrap();
}

#[tokio::test]
async fn quiet_hours_never_delays_a_lone_fire() {
    let Some((_client, mut queue, _key)) = isolated_queue("org").await else {
        return;
    };
    let hour: u8 = chrono::Utc::now().format("%H").to_string().parse().unwrap();
    queue.quiet_hours = QuietHours {
        start_hour: hour,
        end_hour: (hour + 1) % 24,
        enabled: true,
        utc_offset_minutes: 0,
    };
    let region = RegionKey {
        org: "org".into(),
        project: "only".into(),
        memory_type: 3,
    };
    queue.fire(&region, "quiet-node").await.unwrap();
    assert!(matches!(
        queue.drain(Duration::from_secs(1)).await.unwrap(),
        DrainResult::Ready(_)
    ));
}

#[tokio::test]
async fn shared_counters_coalesce_and_acknowledge_exact_fired_snapshot() {
    let Some((client, mut queue, key)) = isolated_queue("shared-org").await else {
        return;
    };
    let region = RegionKey {
        org: "shared-org".into(),
        project: "shared-project".into(),
        memory_type: 3,
    };
    let trigger = DreamsTrigger {
        memory_threshold: 2,
        edge_threshold: u32::MAX,
        age_floor_days: u32::MAX,
        min_interval_hours: 0,
    };
    assert!(matches!(
        queue
            .record_write(&region, 1, 0, trigger, "follower-a")
            .await
            .unwrap(),
        RecordWriteOutcome::Accumulated(c) if c.memories_since_last_cycle == 1
    ));
    assert!(matches!(
        queue
            .record_write(&region, 1, 0, trigger, "follower-b")
            .await
            .unwrap(),
        RecordWriteOutcome::Queued(c) if c.memories_since_last_cycle == 2
    ));
    assert!(matches!(
        queue
            .record_write(&region, 1, 0, trigger, "follower-a")
            .await
            .unwrap(),
        RecordWriteOutcome::Accumulated(c) if c.memories_since_last_cycle == 3
    ));
    let notification = match queue.drain(Duration::from_secs(1)).await.unwrap() {
        DrainResult::Ready(notification) => notification,
        other => panic!("expected ready notification, got {other:?}"),
    };
    assert_eq!(
        notification.fired_at.unwrap().memories_since_last_cycle,
        2,
        "the owner receives the atomic fire-time snapshot"
    );
    assert!(matches!(
        queue
            .acknowledge(&notification, true, trigger, "owner")
            .await
            .unwrap(),
        AcknowledgeOutcome::Acknowledged(c) if c.memories_since_last_cycle == 1
    ));

    assert!(matches!(
        queue
            .record_write(&region, 1, 0, trigger, "follower-b")
            .await
            .unwrap(),
        RecordWriteOutcome::Queued(c) if c.memories_since_last_cycle == 2
    ));
    let failed = match queue.drain(Duration::from_secs(1)).await.unwrap() {
        DrainResult::Ready(notification) => notification,
        other => panic!("expected ready notification, got {other:?}"),
    };
    let failed_fire_id = failed.fire_id.clone();
    assert!(matches!(
        queue
            .acknowledge(&failed, false, trigger, "owner")
            .await
            .unwrap(),
        AcknowledgeOutcome::Requeued(c) if c.memories_since_last_cycle == 2
    ));
    let retry = match queue.drain(Duration::from_secs(1)).await.unwrap() {
        DrainResult::Ready(notification) => notification,
        other => panic!("expected retry notification, got {other:?}"),
    };
    assert_eq!(
        retry.fire_id, failed_fire_id,
        "ambiguous graph success must retry under the same tombstone identity"
    );
    assert_eq!(retry.fired_at.unwrap().memories_since_last_cycle, 2);
    assert!(matches!(
        queue
            .acknowledge(&retry, true, trigger, "owner")
            .await
            .unwrap(),
        AcknowledgeOutcome::Acknowledged(c) if c.memories_since_last_cycle == 0
    ));

    let mut inspect = client
        .get_multiplexed_async_connection()
        .await
        .expect("second Redis connection");
    let counter_key = format!(
        "exocortex:dreams:counters:{}",
        serde_json::to_string(&region).unwrap()
    );
    let _: u64 = redis::cmd("DEL")
        .arg(&key)
        .arg(format!("{key}:deferred"))
        .arg(format!("{key}:processing"))
        .arg(counter_key)
        .query_async(&mut inspect)
        .await
        .unwrap();
}

#[tokio::test]
async fn ambiguous_success_retry_preserves_post_fire_writes() {
    let Some((client, mut queue, key)) = isolated_queue("snapshot-org").await else {
        return;
    };
    let region = RegionKey {
        org: "snapshot-org".into(),
        project: "snapshot-project".into(),
        memory_type: 3,
    };
    let trigger = DreamsTrigger {
        memory_threshold: 1,
        edge_threshold: u32::MAX,
        age_floor_days: u32::MAX,
        min_interval_hours: 0,
    };
    assert!(matches!(
        queue
            .record_write(&region, 2, 0, trigger, "writer")
            .await
            .unwrap(),
        RecordWriteOutcome::Queued(c) if c.memories_since_last_cycle == 2
    ));
    let original = match queue.drain(Duration::from_secs(1)).await.unwrap() {
        DrainResult::Ready(notification) => notification,
        other => panic!("expected original notification, got {other:?}"),
    };
    assert!(matches!(
        queue
            .record_write(&region, 1, 0, trigger, "writer")
            .await
            .unwrap(),
        RecordWriteOutcome::Accumulated(c) if c.memories_since_last_cycle == 3
    ));
    assert!(matches!(
        queue
            .acknowledge(&original, false, trigger, "owner")
            .await
            .unwrap(),
        AcknowledgeOutcome::Requeued(c) if c.memories_since_last_cycle == 3
    ));
    let retry = match queue.drain(Duration::from_secs(1)).await.unwrap() {
        DrainResult::Ready(notification) => notification,
        other => panic!("expected ambiguous retry, got {other:?}"),
    };
    assert_eq!(retry.fire_id, original.fire_id);
    assert_eq!(retry.fired_at, original.fired_at);
    assert!(matches!(
        queue
            .acknowledge(&retry, true, trigger, "owner")
            .await
            .unwrap(),
        AcknowledgeOutcome::Requeued(c) if c.memories_since_last_cycle == 1
    ));
    let follow_up = match queue.drain(Duration::from_secs(1)).await.unwrap() {
        DrainResult::Ready(notification) => notification,
        other => panic!("expected post-fire follow-up, got {other:?}"),
    };
    assert_ne!(follow_up.fire_id, original.fire_id);
    assert_eq!(follow_up.fired_at.unwrap().memories_since_last_cycle, 1);

    let mut inspect = client
        .get_multiplexed_async_connection()
        .await
        .expect("second Redis connection");
    let counter_key = format!(
        "exocortex:dreams:counters:{}",
        serde_json::to_string(&region).unwrap()
    );
    let _: u64 = redis::cmd("DEL")
        .arg(&key)
        .arg(format!("{key}:deferred"))
        .arg(format!("{key}:processing"))
        .arg(counter_key)
        .query_async(&mut inspect)
        .await
        .unwrap();
}

#[tokio::test]
async fn engine_on_write_uses_shared_transport_even_as_follower() {
    let Some((client, queue, key)) = isolated_queue("engine-org").await else {
        return;
    };
    let queue = Arc::new(tokio::sync::Mutex::new(queue));
    let ontology = Arc::new(
        exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()]).unwrap(),
    );
    let engine = DreamsEngine::new(
        Arc::new(InMemoryStorage::new(ontology)),
        DreamsTrigger {
            memory_threshold: 1,
            edge_threshold: u32::MAX,
            age_floor_days: u32::MAX,
            min_interval_hours: 0,
        },
        0.01,
        0.05,
        false,
        "follower".into(),
    )
    .with_leader_gate(Arc::new(std::sync::atomic::AtomicBool::new(false)))
    .with_distributed_fire(queue.clone());
    let region = RegionKey {
        org: "engine-org".into(),
        project: "engine-project".into(),
        memory_type: 3,
    };
    engine.on_write(region.clone()).await.unwrap();
    let notification = match queue
        .lock()
        .await
        .drain(Duration::from_secs(1))
        .await
        .unwrap()
    {
        DrainResult::Ready(notification) => notification,
        other => panic!("expected follower-generated notification, got {other:?}"),
    };
    assert_eq!(notification.region, region);
    assert_eq!(notification.fired_at.unwrap().memories_since_last_cycle, 1);
    let fire_id = notification.fire_id.clone();
    assert_eq!(queue.lock().await.recover_inflight().await.unwrap(), 1);
    let recovered = match queue
        .lock()
        .await
        .drain(Duration::from_secs(1))
        .await
        .unwrap()
    {
        DrainResult::Ready(notification) => notification,
        other => panic!("expected recovered in-flight notification, got {other:?}"),
    };
    assert_eq!(recovered.fire_id, fire_id);

    let mut inspect = client
        .get_multiplexed_async_connection()
        .await
        .expect("second Redis connection");
    let counter_key = format!(
        "exocortex:dreams:counters:{}",
        serde_json::to_string(&region).unwrap()
    );
    let _: u64 = redis::cmd("DEL")
        .arg(&key)
        .arg(format!("{key}:deferred"))
        .arg(format!("{key}:processing"))
        .arg(counter_key)
        .query_async(&mut inspect)
        .await
        .unwrap();
}

#[tokio::test]
async fn stable_write_event_is_counted_once_after_an_ambiguous_retry() {
    let Some((client, mut queue, key)) = isolated_queue("event-org").await else {
        return;
    };
    let region = RegionKey {
        org: "event-org".into(),
        project: "project".into(),
        memory_type: 3,
    };
    let trigger = DreamsTrigger {
        memory_threshold: u32::MAX,
        edge_threshold: u32::MAX,
        age_floor_days: u32::MAX,
        min_interval_hours: 0,
    };
    let event_id = format!("batch:{}", uuid::Uuid::new_v4());
    let first = queue
        .record_write_once(&region, 3, 2, trigger, "node", &event_id)
        .await
        .unwrap();
    let retry = queue
        .record_write_once(&region, 3, 2, trigger, "node", &event_id)
        .await
        .unwrap();
    assert!(matches!(
        first,
        RecordWriteOutcome::Accumulated(c)
            if c.memories_since_last_cycle == 3 && c.edges_since_last_cycle == 2
    ));
    assert!(matches!(
        retry,
        RecordWriteOutcome::Accumulated(c)
            if c.memories_since_last_cycle == 3 && c.edges_since_last_cycle == 2
    ));

    let mut inspect = client
        .get_multiplexed_async_connection()
        .await
        .expect("second Redis connection");
    let counter_key = format!(
        "exocortex:dreams:counters:{}",
        serde_json::to_string(&region).unwrap()
    );
    let values: (u32, u32) = redis::cmd("HMGET")
        .arg(&counter_key)
        .arg("memories")
        .arg("edges")
        .query_async(&mut inspect)
        .await
        .unwrap();
    assert_eq!(values, (3, 2));
    let _: u64 = redis::cmd("DEL")
        .arg(&key)
        .arg(format!("{key}:deferred"))
        .arg(format!("{key}:processing"))
        .arg(counter_key)
        .arg(format!(
            "exocortex:dreams:counters:{}:processed-events",
            serde_json::to_string(&region).unwrap()
        ))
        .query_async(&mut inspect)
        .await
        .unwrap();
}

#[tokio::test]
async fn interleaved_effect_retry_is_counted_once() {
    let Some((client, mut queue, key)) = isolated_queue("interleaved-org").await else {
        return;
    };
    let region = RegionKey {
        org: "interleaved-org".into(),
        project: "project".into(),
        memory_type: 3,
    };
    let trigger = DreamsTrigger {
        memory_threshold: u32::MAX,
        edge_threshold: u32::MAX,
        age_floor_days: u32::MAX,
        min_interval_hours: 0,
    };
    let suffix = uuid::Uuid::new_v4();
    let event_a = format!("effect-a:{suffix}");
    let event_b = format!("effect-b:{suffix}");
    for (event, memories, edges) in [(&event_a, 3, 2), (&event_b, 1, 1), (&event_a, 3, 2)] {
        assert!(matches!(
            queue
                .record_write_once(&region, memories, edges, trigger, "node", event)
                .await
                .unwrap(),
            RecordWriteOutcome::Accumulated(_)
        ));
    }

    let mut inspect = client
        .get_multiplexed_async_connection()
        .await
        .expect("second Redis connection");
    let counter_key = format!(
        "exocortex:dreams:counters:{}",
        serde_json::to_string(&region).unwrap()
    );
    let values: (u32, u32) = redis::cmd("HMGET")
        .arg(&counter_key)
        .arg("memories")
        .arg("edges")
        .query_async(&mut inspect)
        .await
        .unwrap();
    assert_eq!(values, (4, 3), "A/B/A must apply A exactly once");
    let processed: u64 = redis::cmd("SCARD")
        .arg(format!("{counter_key}:processed-events"))
        .query_async(&mut inspect)
        .await
        .unwrap();
    assert_eq!(processed, 2);

    queue
        .forget_write_once(&region, &event_a)
        .await
        .expect("authoritatively settled effect is reclaimable");
    assert!(matches!(
        queue
            .record_write_once(&region, 1, 1, trigger, "node", &event_b)
            .await
            .unwrap(),
        RecordWriteOutcome::Accumulated(c)
            if c.memories_since_last_cycle == 4 && c.edges_since_last_cycle == 3
    ));
    let processed: u64 = redis::cmd("SCARD")
        .arg(format!("{counter_key}:processed-events"))
        .query_async(&mut inspect)
        .await
        .unwrap();
    assert_eq!(
        processed, 1,
        "settled A is reclaimed while still-pending B remains idempotent"
    );
    let _: u64 = redis::cmd("DEL")
        .arg(&key)
        .arg(format!("{key}:deferred"))
        .arg(format!("{key}:processing"))
        .arg(&counter_key)
        .arg(format!("{counter_key}:processed-events"))
        .query_async(&mut inspect)
        .await
        .unwrap();
}

#[tokio::test]
async fn high_cardinality_settled_effects_do_not_accumulate_identities() {
    let Some((client, mut queue, key)) = isolated_queue("settled-cardinality-org").await else {
        return;
    };
    let region = RegionKey {
        org: "settled-cardinality-org".into(),
        project: "project".into(),
        memory_type: 3,
    };
    let trigger = DreamsTrigger {
        memory_threshold: u32::MAX,
        edge_threshold: u32::MAX,
        age_floor_days: u32::MAX,
        min_interval_hours: 0,
    };
    for sequence in 0..2_000 {
        let event = format!("settled-effect:{sequence}");
        queue
            .record_write_once(&region, 1, 0, trigger, "node", &event)
            .await
            .unwrap();
        queue.forget_write_once(&region, &event).await.unwrap();
    }

    let counter_key = format!(
        "exocortex:dreams:counters:{}",
        serde_json::to_string(&region).unwrap()
    );
    let mut inspect = client
        .get_multiplexed_async_connection()
        .await
        .expect("second Redis connection");
    let processed: u64 = redis::cmd("SCARD")
        .arg(format!("{counter_key}:processed-events"))
        .query_async(&mut inspect)
        .await
        .unwrap();
    assert_eq!(processed, 0, "acknowledged traffic must not accumulate");
    let memories: u32 = redis::cmd("HGET")
        .arg(&counter_key)
        .arg("memories")
        .query_async(&mut inspect)
        .await
        .unwrap();
    assert_eq!(memories, 2_000);
    let _: u64 = redis::cmd("DEL")
        .arg(&key)
        .arg(format!("{key}:deferred"))
        .arg(format!("{key}:processing"))
        .arg(&counter_key)
        .arg(format!("{counter_key}:processed-events"))
        .query_async(&mut inspect)
        .await
        .unwrap();
}

#[tokio::test]
async fn partial_multi_region_cleanup_resumes_after_restart_without_reapplying() {
    let Some((client, mut queue, key)) = isolated_queue("cleanup-recovery-org").await else {
        return;
    };
    let regions = [
        RegionKey {
            org: "cleanup-recovery-org".into(),
            project: "first".into(),
            memory_type: 3,
        },
        RegionKey {
            org: "cleanup-recovery-org".into(),
            project: "second".into(),
            memory_type: 3,
        },
    ];
    let trigger = DreamsTrigger {
        memory_threshold: u32::MAX,
        edge_threshold: u32::MAX,
        age_floor_days: u32::MAX,
        min_interval_hours: 0,
    };
    let event = format!("recoverable-cleanup:{}", uuid::Uuid::new_v4());
    for region in &regions {
        queue
            .record_write_once(region, 1, 0, trigger, "node", &event)
            .await
            .unwrap();
    }
    queue.forget_write_once(&regions[0], &event).await.unwrap();
    drop(queue); // crash after cleaning only the first region

    let conn = client
        .get_multiplexed_async_connection()
        .await
        .expect("reconnect after partial cleanup");
    let mut restarted = RedisFireQueue::new_with_queue_key(
        conn,
        QuietHours::none(),
        regions[0].org.as_str(),
        key.clone(),
    );
    for region in &regions {
        restarted.forget_write_once(region, &event).await.unwrap();
    }

    let mut inspect = client
        .get_multiplexed_async_connection()
        .await
        .expect("inspection connection");
    for region in &regions {
        let counter_key = format!(
            "exocortex:dreams:counters:{}",
            serde_json::to_string(region).unwrap()
        );
        let processed: u64 = redis::cmd("SCARD")
            .arg(format!("{counter_key}:processed-events"))
            .query_async(&mut inspect)
            .await
            .unwrap();
        assert_eq!(processed, 0, "every resumed region identity is reclaimed");
        let memories: u32 = redis::cmd("HGET")
            .arg(&counter_key)
            .arg("memories")
            .query_async(&mut inspect)
            .await
            .unwrap();
        assert_eq!(memories, 1, "cleanup recovery never reapplies the effect");
        let _: u64 = redis::cmd("DEL")
            .arg(counter_key)
            .query_async(&mut inspect)
            .await
            .unwrap();
    }
    let _: u64 = redis::cmd("DEL")
        .arg(&key)
        .arg(format!("{key}:deferred"))
        .arg(format!("{key}:processing"))
        .query_async(&mut inspect)
        .await
        .unwrap();
}

#[tokio::test]
async fn counters_and_failed_cycle_survive_standalone_queue_restarts() {
    let Some((client, mut first_process, key)) = isolated_queue("restart-org").await else {
        return;
    };
    let region = RegionKey {
        org: "restart-org".into(),
        project: "standalone".into(),
        memory_type: 3,
    };
    let trigger = DreamsTrigger {
        memory_threshold: 2,
        edge_threshold: u32::MAX,
        age_floor_days: u32::MAX,
        min_interval_hours: 0,
    };
    assert!(matches!(
        first_process
            .record_write_once(&region, 1, 0, trigger, "standalone", "effect:one")
            .await
            .unwrap(),
        RecordWriteOutcome::Accumulated(c) if c.memories_since_last_cycle == 1
    ));
    drop(first_process);

    let second_connection = client.get_multiplexed_async_connection().await.unwrap();
    let mut second_process = RedisFireQueue::new_with_queue_key(
        second_connection,
        QuietHours::none(),
        "restart-org",
        key.clone(),
    );
    assert!(matches!(
        second_process
            .record_write_once(&region, 1, 0, trigger, "standalone", "effect:two")
            .await
            .unwrap(),
        RecordWriteOutcome::Queued(c) if c.memories_since_last_cycle == 2
    ));
    let in_flight = match second_process.drain(Duration::from_secs(1)).await.unwrap() {
        DrainResult::Ready(notification) => notification,
        other => panic!("expected durable fire after restart, got {other:?}"),
    };
    let fire_id = in_flight.fire_id.clone();
    drop(second_process);

    let third_connection = client.get_multiplexed_async_connection().await.unwrap();
    let mut third_process = RedisFireQueue::new_with_queue_key(
        third_connection,
        QuietHours::none(),
        "restart-org",
        key.clone(),
    );
    assert_eq!(third_process.recover_inflight().await.unwrap(), 1);
    let recovered = match third_process.drain(Duration::from_secs(1)).await.unwrap() {
        DrainResult::Ready(notification) => notification,
        other => panic!("expected recovered in-flight fire, got {other:?}"),
    };
    assert_eq!(recovered.fire_id, fire_id);
    assert!(matches!(
        third_process
            .acknowledge(&recovered, false, trigger, "standalone")
            .await
            .unwrap(),
        AcknowledgeOutcome::Requeued(c) if c.memories_since_last_cycle == 2
    ));
    drop(third_process);

    let fourth_connection = client.get_multiplexed_async_connection().await.unwrap();
    let mut fourth_process = RedisFireQueue::new_with_queue_key(
        fourth_connection,
        QuietHours::none(),
        "restart-org",
        key.clone(),
    );
    let retry = match fourth_process.drain(Duration::from_secs(1)).await.unwrap() {
        DrainResult::Ready(notification) => notification,
        other => panic!("expected failed cycle retry after restart, got {other:?}"),
    };
    assert_eq!(retry.fired_at.unwrap().memories_since_last_cycle, 2);
    assert!(matches!(
        fourth_process
            .acknowledge(&retry, true, trigger, "standalone")
            .await
            .unwrap(),
        AcknowledgeOutcome::Acknowledged(c) if c.memories_since_last_cycle == 0
    ));

    let mut inspect = client.get_multiplexed_async_connection().await.unwrap();
    let counter_key = format!(
        "exocortex:dreams:counters:{}",
        serde_json::to_string(&region).unwrap()
    );
    let _: u64 = redis::cmd("DEL")
        .arg(&key)
        .arg(format!("{key}:deferred"))
        .arg(format!("{key}:processing"))
        .arg(counter_key)
        .query_async(&mut inspect)
        .await
        .unwrap();
}
