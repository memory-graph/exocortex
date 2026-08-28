//! §12.2 transport + quiet hours: the Dreams fire channel as Redis
//! RPUSH/BLPOP (`exocortex:dreams:queue`), with the R-Dr13 Lua counter
//! reset and the R-Dr14 quiet-hours preference ordering. The in-process
//! mpsc channel in `DreamsEngine` remains the same-shape local transport;
//! this module is the multi-node production one.

use std::time::Duration;

use exocortex_storage::RegionKey;
use smol_str::SmolStr;

use crate::trigger::{DreamsTrigger, RegionWriteCounters};

/// The queue key (§12.2).
pub const DREAMS_QUEUE_KEY: &str = "exocortex:dreams:queue";
/// R-Dr15's hard Redis queue ceiling.
pub const DREAMS_QUEUE_MAX: u64 = 1000;

/// R-Dr14: quiet hours defer only while the backlog is shorter than this;
/// at or above it the preference yields and the backlog still runs.
pub const QUIET_HOURS_BACKLOG_MIN: u64 = 32;

const ENQUEUE_BOUNDED_LUA: &str = r#"
local key = KEYS[1]
local deferred = KEYS[2]
local processing = KEYS[3]
local maximum = tonumber(ARGV[1])
if redis.call('LLEN', key) + redis.call('ZCARD', deferred) + redis.call('LLEN', processing) >= maximum then
    return 0
end
redis.call('RPUSH', key, ARGV[2])
return 1
"#;

const RECORD_WRITE_LUA: &str = r#"
local queue = KEYS[1]
local counters = KEYS[2]
local deferred = KEYS[3]
local processing = KEYS[4]
local processed_events = KEYS[5]
local function require_type(key, expected)
    local actual = redis.call('TYPE', key)['ok']
    if actual ~= 'none' and actual ~= expected then
        error('Dreams key has wrong type: expected ' .. expected)
    end
end
require_type(queue, 'list')
require_type(counters, 'hash')
require_type(deferred, 'zset')
require_type(processing, 'list')
require_type(processed_events, 'set')
local memory_raw = redis.call('HGET', counters, 'memories')
local edge_raw = redis.call('HGET', counters, 'edges')
local success_raw = redis.call('HGET', counters, 'last_success')
local settled_generation_raw = redis.call('HGET', counters, 'settled_generation')
if (memory_raw and not tonumber(memory_raw))
    or (edge_raw and not tonumber(edge_raw))
    or (success_raw and not tonumber(success_raw))
    or (settled_generation_raw and not tonumber(settled_generation_raw)) then
    return redis.error_reply('Dreams counter metadata is not numeric')
end
local delivery_generation = tonumber(ARGV[15])
local settled_generation = tonumber(settled_generation_raw or '0')
if delivery_generation > 0 and (delivery_generation <= settled_generation
    or redis.call('SISMEMBER', processed_events, ARGV[14]) == 1) then
    return {
        tonumber(redis.call('HGET', counters, 'memories') or '0'),
        tonumber(redis.call('HGET', counters, 'edges') or '0'),
        0
    }
end
local memories = math.min(4294967295, tonumber(memory_raw or '0') + tonumber(ARGV[2]))
local edges = math.min(4294967295, tonumber(edge_raw or '0') + tonumber(ARGV[3]))
if delivery_generation > 0 then
    redis.call('SADD', processed_events, ARGV[14])
end
redis.call('HSET', counters, 'memories', memories, 'edges', edges)
redis.call('HSETNX', counters, 'last_success', ARGV[6])
local elapsed = math.max(0, tonumber(ARGV[6]) - tonumber(redis.call('HGET', counters, 'last_success')))
local threshold = memories >= tonumber(ARGV[4]) or edges >= tonumber(ARGV[5]) or elapsed >= tonumber(ARGV[8])
if elapsed < tonumber(ARGV[7]) or not threshold or redis.call('HEXISTS', counters, 'pending') == 1 then
    return {memories, edges, 0}
end
if redis.call('LLEN', queue) + redis.call('ZCARD', deferred) + redis.call('LLEN', processing) >= tonumber(ARGV[1]) then
    return {memories, edges, 2}
end
redis.call('HSET', counters, 'pending', ARGV[13])
local payload = cjson.encode({
    region = {org = ARGV[9], project = ARGV[10], memory_type = tonumber(ARGV[11])},
    fired_by = ARGV[12],
    fired_at = {
        memories_since_last_cycle = memories,
        edges_since_last_cycle = edges,
        seconds_since_last_cycle = elapsed
    },
    fire_id = ARGV[13]
})
redis.call('RPUSH', queue, payload)
return {memories, edges, 1}
"#;

const SETTLE_WRITE_LUA: &str = r#"
local counters = KEYS[1]
local processed_events = KEYS[2]
local function require_type(key, expected)
    local actual = redis.call('TYPE', key)['ok']
    if actual ~= 'none' and actual ~= expected then
        error('Dreams key has wrong type: expected ' .. expected)
    end
end
require_type(counters, 'hash')
require_type(processed_events, 'set')
local current_raw = redis.call('HGET', counters, 'settled_generation')
if current_raw and not tonumber(current_raw) then
    return redis.error_reply('Dreams settled generation is not numeric')
end
local current = tonumber(current_raw or '0')
local settled = tonumber(ARGV[2])
if settled > current then
    redis.call('HSET', counters, 'settled_generation', settled)
end
if ARGV[3] ~= '1' then
    redis.call('SREM', processed_events, ARGV[1])
end
return 1
"#;

const ACKNOWLEDGE_LUA: &str = r#"
local queue = KEYS[1]
local counters = KEYS[2]
local deferred = KEYS[3]
local processing = KEYS[4]
if redis.call('HGET', counters, 'pending') ~= ARGV[1] then
    return {0, 0, 0}
end
redis.call('LREM', processing, 1, ARGV[16])
local memories = tonumber(redis.call('HGET', counters, 'memories') or '0')
local edges = tonumber(redis.call('HGET', counters, 'edges') or '0')
local retry_memories = tonumber(ARGV[3])
local retry_edges = tonumber(ARGV[4])
if ARGV[2] == '1' then
    memories = math.max(0, memories - tonumber(ARGV[3]))
    edges = math.max(0, edges - tonumber(ARGV[4]))
    retry_memories = memories
    retry_edges = edges
    redis.call('HSET', counters, 'memories', memories, 'edges', edges, 'last_success', ARGV[5])
end
redis.call('HDEL', counters, 'pending')
local elapsed = math.max(0, tonumber(ARGV[5]) - tonumber(redis.call('HGET', counters, 'last_success') or ARGV[5]))
local threshold = memories >= tonumber(ARGV[7]) or edges >= tonumber(ARGV[8]) or elapsed >= tonumber(ARGV[10])
local retry = ARGV[2] == '0' or (elapsed >= tonumber(ARGV[9]) and threshold)
if not retry then
    return {memories, edges, 1}
end
if redis.call('LLEN', queue) + redis.call('ZCARD', deferred) + redis.call('LLEN', processing) >= tonumber(ARGV[6]) then
    return {memories, edges, 3}
end
redis.call('HSET', counters, 'pending', ARGV[15])
local payload = cjson.encode({
    region = {org = ARGV[11], project = ARGV[12], memory_type = tonumber(ARGV[13])},
    fired_by = ARGV[14],
    fired_at = {
        memories_since_last_cycle = retry_memories,
        edges_since_last_cycle = retry_edges,
        seconds_since_last_cycle = elapsed
    },
    fire_id = ARGV[15]
})
redis.call('RPUSH', queue, payload)
return {memories, edges, 2}
"#;

const PROMOTE_DEFERRED_LUA: &str = r#"
local queue = KEYS[1]
local deferred = KEYS[2]
local due = redis.call('ZRANGEBYSCORE', deferred, '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
local promoted = 0
for _, payload in ipairs(due) do
    if redis.call('LLEN', queue) >= tonumber(ARGV[2]) then
        break
    end
    if redis.call('ZREM', deferred, payload) == 1 then
        redis.call('RPUSH', queue, payload)
        promoted = promoted + 1
    end
end
return promoted
"#;

const DEFER_LUA: &str = r#"
redis.call('LREM', KEYS[2], 1, ARGV[2])
redis.call('ZADD', KEYS[1], ARGV[1], ARGV[3])
return 1
"#;

const RECOVER_INFLIGHT_LUA: &str = r#"
local payloads = redis.call('LRANGE', KEYS[2], 0, -1)
redis.call('DEL', KEYS[2])
for _, payload in ipairs(payloads) do
    redis.call('RPUSH', KEYS[1], payload)
end
return #payloads
"#;

/// One queued region payload (JSON): region coordinates + who fired.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FireMessage {
    /// The region to consolidate.
    pub region: RegionKey,
    /// Firing node id (diagnostics).
    pub fired_by: SmolStr,
    /// Shared counter snapshot owned by this fire. Absent on legacy explicit
    /// fire messages serialized before distributed counters were introduced.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fired_at: Option<RegionWriteCounters>,
    /// Unique pending token used to reject stale acknowledgements.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fire_id: Option<SmolStr>,
    /// Exact Redis list member retained out-of-band for atomic acknowledgement.
    #[serde(skip)]
    pub queued_payload: Option<String>,
    /// One-pass quiet-hours reorder marker; marked work executes next time.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub quiet_deferred: bool,
}

/// Outcome of an atomic Redis enqueue.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FireOutcome {
    /// The region was appended below the queue ceiling.
    Queued,
    /// The queue was already full; the newest fire was dropped.
    Dropped,
}

/// Result of atomically recording a shared write counter.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecordWriteOutcome {
    /// Counters were retained but a fire was already pending or not due.
    Accumulated(RegionWriteCounters),
    /// This write atomically created the one pending fire.
    Queued(RegionWriteCounters),
    /// The queue was full; counters remain durable and the newest fire dropped.
    Dropped(RegionWriteCounters),
}

/// Stable effect identity paired with its authoritative delivery generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeliveryFence<'a> {
    /// Durable outbox effect identity.
    pub event_id: &'a str,
    /// Monotonic claim generation assigned by storage.
    pub generation: u64,
}

/// Result of acknowledging an owner notification.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AcknowledgeOutcome {
    /// The pending token was stale or already acknowledged.
    Stale,
    /// The notification was acknowledged without another due fire.
    Acknowledged(RegionWriteCounters),
    /// Retained counters were atomically queued for another cycle.
    Requeued(RegionWriteCounters),
    /// Retained counters remain durable, but the follow-up fire was dropped.
    Dropped(RegionWriteCounters),
}

/// One result from the Redis queue drainer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DrainResult {
    /// No queue item arrived before the requested timeout.
    TimedOut,
    /// The region should execute now.
    Ready(FireMessage),
    /// Quiet-hours preference durably moved the item behind existing work once.
    Deferred,
}

/// R-Dr14 quiet hours: user preference ordering for when Dreams cycles may
/// run. Evaluation uses the explicitly configured fixed UTC offset. The PRD
/// default is 02:00-06:00 UTC.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QuietHours {
    /// Start hour (local, 0-23), inclusive.
    pub start_hour: u8,
    /// End hour (local, 0-23), exclusive.
    pub end_hour: u8,
    /// Whether the window is active (wraps midnight when start > end).
    pub enabled: bool,
    /// Fixed UTC offset used for deterministic region-local evaluation.
    pub utc_offset_minutes: i16,
}

impl Default for QuietHours {
    fn default() -> Self {
        Self {
            start_hour: 2,
            end_hour: 6,
            enabled: true,
            utc_offset_minutes: 0,
        }
    }
}

impl std::str::FromStr for QuietHours {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (start, end) = value
            .split_once('-')
            .ok_or_else(|| anyhow::anyhow!("quiet hours must use START-END"))?;
        anyhow::ensure!(
            start.len() == 2 && end.len() == 2,
            "quiet hours must use two-digit UTC hours"
        );
        let start_hour: u8 = start.parse()?;
        let end_hour: u8 = end.parse()?;
        anyhow::ensure!(start_hour < 24 && end_hour < 24, "hours must be 00-23");
        anyhow::ensure!(start_hour != end_hour, "quiet window must not be empty");
        Ok(Self {
            start_hour,
            end_hour,
            enabled: true,
            utc_offset_minutes: 0,
        })
    }
}

impl QuietHours {
    /// No quiet window (cycles may run any time).
    pub fn none() -> Self {
        Self {
            enabled: false,
            ..Self::default()
        }
    }

    /// The PRD default 02:00-06:00 UTC window.
    pub fn nightly() -> Self {
        Self::default()
    }

    /// Apply the explicitly configured fixed UTC offset for this queue.
    pub fn with_utc_offset_minutes(mut self, offset: i16) -> anyhow::Result<Self> {
        anyhow::ensure!((-1439..=1439).contains(&offset), "UTC offset out of range");
        self.utc_offset_minutes = offset;
        Ok(self)
    }

    /// Is `local_hour` inside the quiet window?
    pub fn contains(&self, local_hour: u8) -> bool {
        if !self.enabled {
            return false;
        }
        if self.start_hour <= self.end_hour {
            local_hour >= self.start_hour && local_hour < self.end_hour
        } else {
            // Wraps midnight (e.g. 23..7).
            local_hour >= self.start_hour || local_hour < self.end_hour
        }
    }

    fn current_hour(&self) -> u8 {
        let shifted =
            chrono::Utc::now() + chrono::Duration::minutes(self.utc_offset_minutes.into());
        shifted.format("%H").to_string().parse().unwrap_or(0)
    }

    fn should_reorder(&self, local_hour: u8, other_work: u64) -> bool {
        self.contains(local_hour)
            && other_work > 0
            && other_work.saturating_add(1) < QUIET_HOURS_BACKLOG_MIN
    }
}

/// The Redis-backed fire transport. Requires a `redis` async client
/// (multiplexed); Lua coalesces counters, bounded lists hold ready/in-flight
/// work, and a sorted set durably holds quiet-hour deferrals.
pub struct RedisFireQueue {
    conn: redis::aio::MultiplexedConnection,
    org: SmolStr,
    queue_key: SmolStr,
    deferred_key: SmolStr,
    processing_key: SmolStr,
    /// Per-node quiet hours preference (R-Dr14).
    pub quiet_hours: QuietHours,
}

impl RedisFireQueue {
    /// Connect over an existing client.
    pub fn new(
        conn: redis::aio::MultiplexedConnection,
        quiet_hours: QuietHours,
        org: impl Into<SmolStr>,
    ) -> Self {
        let org = org.into();
        let queue_key = format!(
            "{DREAMS_QUEUE_KEY}:org:{}",
            serde_json::to_string(org.as_str()).expect("organization serialization is infallible")
        );
        Self {
            conn,
            org,
            deferred_key: format!("{queue_key}:deferred").into(),
            processing_key: format!("{queue_key}:processing").into(),
            queue_key: queue_key.into(),
            quiet_hours,
        }
    }

    /// Build an isolated queue for executable transport tests.
    #[doc(hidden)]
    #[cfg(feature = "testing")]
    pub fn new_with_queue_key(
        conn: redis::aio::MultiplexedConnection,
        quiet_hours: QuietHours,
        org: impl Into<SmolStr>,
        queue_key: impl Into<SmolStr>,
    ) -> Self {
        let queue_key = queue_key.into();
        Self {
            conn,
            org: org.into(),
            deferred_key: format!("{queue_key}:deferred").into(),
            processing_key: format!("{queue_key}:processing").into(),
            queue_key,
            quiet_hours,
        }
    }

    /// Atomically append a region below the R-Dr15 queue ceiling. Saturation
    /// drops the newest fire and leaves existing work untouched.
    pub async fn fire(
        &mut self,
        region: &RegionKey,
        fired_by: &str,
    ) -> anyhow::Result<FireOutcome> {
        self.ensure_region(region)?;
        let msg = FireMessage {
            region: region.clone(),
            fired_by: fired_by.into(),
            fired_at: None,
            fire_id: Some(uuid::Uuid::new_v4().to_string().into()),
            queued_payload: None,
            quiet_deferred: false,
        };
        let payload = serde_json::to_string(&msg)?;
        let script = redis::Script::new(ENQUEUE_BOUNDED_LUA);
        let queued: i64 = script
            .key(self.queue_key.as_str())
            .key(self.deferred_key.as_str())
            .key(self.processing_key.as_str())
            .arg(DREAMS_QUEUE_MAX)
            .arg(payload)
            .invoke_async(&mut self.conn)
            .await?;
        if queued == 1 {
            metrics::counter!("exocortex_dreams_fired_total", "transport" => "redis").increment(1);
            Ok(FireOutcome::Queued)
        } else {
            metrics::counter!(
                "exocortex_dreams_queue_dropped_total",
                "reason" => "capacity"
            )
            .increment(1);
            Ok(FireOutcome::Dropped)
        }
    }

    /// Atomically update the shared per-region counters and coalesce at most
    /// one pending fire. All nodes, including followers, call this after a
    /// committed write; only the owner drains and acknowledges notifications.
    pub async fn record_write(
        &mut self,
        region: &RegionKey,
        memories: u32,
        edges: u32,
        trigger: DreamsTrigger,
        fired_by: &str,
    ) -> anyhow::Result<RecordWriteOutcome> {
        let event_id = uuid::Uuid::new_v4().to_string();
        self.record_write_once_inner(
            region,
            memories,
            edges,
            trigger,
            fired_by,
            DeliveryFence {
                event_id: &event_id,
                generation: 0,
            },
        )
        .await
    }

    /// Record one stable durable ingest effect. Identities remain until the
    /// authoritative outbox acknowledges the effect, so interleaved ambiguous
    /// retries remain exact without retaining settled traffic forever.
    pub async fn record_write_once(
        &mut self,
        region: &RegionKey,
        memories: u32,
        edges: u32,
        trigger: DreamsTrigger,
        fired_by: &str,
        delivery: DeliveryFence<'_>,
    ) -> anyhow::Result<RecordWriteOutcome> {
        anyhow::ensure!(
            delivery.generation > 0,
            "Dreams delivery generation must be positive"
        );
        self.record_write_once_inner(region, memories, edges, trigger, fired_by, delivery)
            .await
    }

    async fn record_write_once_inner(
        &mut self,
        region: &RegionKey,
        memories: u32,
        edges: u32,
        trigger: DreamsTrigger,
        fired_by: &str,
        delivery: DeliveryFence<'_>,
    ) -> anyhow::Result<RecordWriteOutcome> {
        self.ensure_region(region)?;
        anyhow::ensure!(
            !delivery.event_id.is_empty(),
            "Dreams write event id must not be empty"
        );
        let now = chrono::Utc::now().timestamp().max(0) as u64;
        let fire_id = uuid::Uuid::new_v4().to_string();
        let values: (u32, u32, u8) = redis::Script::new(RECORD_WRITE_LUA)
            .key(self.queue_key.as_str())
            .key(counter_key(region))
            .key(self.deferred_key.as_str())
            .key(self.processing_key.as_str())
            .key(processed_event_key(region))
            .arg(DREAMS_QUEUE_MAX)
            .arg(memories)
            .arg(edges)
            .arg(trigger.memory_threshold)
            .arg(trigger.edge_threshold)
            .arg(now)
            .arg(u64::from(trigger.min_interval_hours) * 3600)
            .arg(u64::from(trigger.age_floor_days) * 86_400)
            .arg(region.org.as_str())
            .arg(region.project.as_str())
            .arg(region.memory_type)
            .arg(fired_by)
            .arg(fire_id)
            .arg(delivery.event_id)
            .arg(delivery.generation)
            .invoke_async(&mut self.conn)
            .await?;
        let snapshot = RegionWriteCounters {
            memories_since_last_cycle: values.0,
            edges_since_last_cycle: values.1,
            seconds_since_last_cycle: 0,
        };
        Ok(match values.2 {
            1 => RecordWriteOutcome::Queued(snapshot),
            2 => {
                metrics::counter!(
                    "exocortex_dreams_queue_dropped_total",
                    "reason" => "capacity"
                )
                .increment(1);
                RecordWriteOutcome::Dropped(snapshot)
            }
            _ => RecordWriteOutcome::Accumulated(snapshot),
        })
    }

    /// Reclaim one effect identity only after its authoritative outbox row has
    /// been acknowledged. Pending identities must never be removed here.
    pub async fn forget_write_once(
        &mut self,
        region: &RegionKey,
        event_id: &str,
        delivery_generation: u64,
        retain_legacy_identity: bool,
    ) -> anyhow::Result<()> {
        self.ensure_region(region)?;
        anyhow::ensure!(
            !event_id.is_empty(),
            "Dreams write event id must not be empty"
        );
        anyhow::ensure!(
            delivery_generation > 0,
            "Dreams delivery generation must be positive"
        );
        let _: u64 = redis::Script::new(SETTLE_WRITE_LUA)
            .key(counter_key(region))
            .key(processed_event_key(region))
            .arg(event_id)
            .arg(delivery_generation)
            .arg(u8::from(retain_legacy_identity))
            .invoke_async(&mut self.conn)
            .await?;
        Ok(())
    }

    /// Acknowledge a distributed notification by its unique pending token.
    /// Success subtracts exactly the fired snapshot; failure retains it. Both
    /// paths atomically decide whether the retained counters need one retry.
    pub async fn acknowledge(
        &mut self,
        notification: &FireMessage,
        success: bool,
        trigger: DreamsTrigger,
        fired_by: &str,
    ) -> anyhow::Result<AcknowledgeOutcome> {
        self.ensure_region(&notification.region)?;
        if notification.fired_at.is_none() {
            let Some(payload) = &notification.queued_payload else {
                return Ok(AcknowledgeOutcome::Stale);
            };
            let removed: u64 = redis::cmd("LREM")
                .arg(self.processing_key.as_str())
                .arg(1)
                .arg(payload)
                .query_async(&mut self.conn)
                .await?;
            return Ok(if removed == 1 {
                AcknowledgeOutcome::Acknowledged(RegionWriteCounters::default())
            } else {
                AcknowledgeOutcome::Stale
            });
        }
        let (Some(fired_at), Some(fire_id), Some(payload)) = (
            notification.fired_at,
            &notification.fire_id,
            &notification.queued_payload,
        ) else {
            return Ok(AcknowledgeOutcome::Stale);
        };
        let now = chrono::Utc::now().timestamp().max(0) as u64;
        let retry_fire_id = if success {
            uuid::Uuid::new_v4().to_string()
        } else {
            fire_id.to_string()
        };
        let values: (u32, u32, u8) = redis::Script::new(ACKNOWLEDGE_LUA)
            .key(self.queue_key.as_str())
            .key(counter_key(&notification.region))
            .key(self.deferred_key.as_str())
            .key(self.processing_key.as_str())
            .arg(fire_id.as_str())
            .arg(u8::from(success))
            .arg(fired_at.memories_since_last_cycle)
            .arg(fired_at.edges_since_last_cycle)
            .arg(now)
            .arg(DREAMS_QUEUE_MAX)
            .arg(trigger.memory_threshold)
            .arg(trigger.edge_threshold)
            .arg(u64::from(trigger.min_interval_hours) * 3600)
            .arg(u64::from(trigger.age_floor_days) * 86_400)
            .arg(notification.region.org.as_str())
            .arg(notification.region.project.as_str())
            .arg(notification.region.memory_type)
            .arg(fired_by)
            // A failure may be an ambiguous success after graph settlement,
            // so it retains the identity and exact snapshot. A successful
            // cycle's post-fire writes instead receive a fresh identity.
            .arg(retry_fire_id)
            .arg(payload)
            .invoke_async(&mut self.conn)
            .await?;
        let counters = RegionWriteCounters {
            memories_since_last_cycle: values.0,
            edges_since_last_cycle: values.1,
            seconds_since_last_cycle: 0,
        };
        Ok(match values.2 {
            1 => AcknowledgeOutcome::Acknowledged(counters),
            2 => AcknowledgeOutcome::Requeued(counters),
            3 => {
                metrics::counter!(
                    "exocortex_dreams_queue_dropped_total",
                    "reason" => "capacity"
                )
                .increment(1);
                AcknowledgeOutcome::Dropped(counters)
            }
            _ => AcknowledgeOutcome::Stale,
        })
    }

    /// Atomically move one region to the durable in-flight list. `timeout`
    /// bounds the wait.
    /// Returns an explicit timeout, ready item, or defer schedule.
    ///
    /// R-Dr14: quiet hours is a *preference, not a gate* — during the
    /// window the worker durably defers only while the
    /// backlog is short; a backlog at or above
    /// [`QUIET_HOURS_BACKLOG_MIN`] runs anyway so cycles never starve.
    pub async fn drain(&mut self, timeout: Duration) -> anyhow::Result<DrainResult> {
        let now = chrono::Utc::now().timestamp().max(0) as u64;
        let _: u64 = redis::Script::new(PROMOTE_DEFERRED_LUA)
            .key(self.queue_key.as_str())
            .key(self.deferred_key.as_str())
            .arg(now)
            .arg(DREAMS_QUEUE_MAX)
            .invoke_async(&mut self.conn)
            .await?;
        let payload: String = match redis::cmd("BLMOVE")
            .arg(self.queue_key.as_str())
            .arg(self.processing_key.as_str())
            .arg("LEFT")
            .arg("RIGHT")
            .arg(timeout.as_secs_f64())
            .query_async(&mut self.conn)
            .await
        {
            Ok(pair) => pair,
            Err(e) => {
                if e.to_string().contains("timeout") {
                    return Ok(DrainResult::TimedOut);
                }
                return Err(e.into());
            }
        };
        let mut msg: FireMessage = serde_json::from_str(&payload)?;
        self.ensure_region(&msg.region)?;
        msg.queued_payload = Some(payload.clone());
        let local_hour = self.quiet_hours.current_hour();
        let other_work = self.reorderable_backlog_len().await?;
        if !msg.quiet_deferred && self.quiet_hours.should_reorder(local_hour, other_work) {
            msg.quiet_deferred = true;
            msg.queued_payload = None;
            let deferred_payload = serde_json::to_string(&msg)?;
            let _: u8 = redis::Script::new(DEFER_LUA)
                .key(self.deferred_key.as_str())
                .key(self.processing_key.as_str())
                .arg(now)
                .arg(&payload)
                .arg(deferred_payload)
                .invoke_async(&mut self.conn)
                .await?;
            metrics::counter!("exocortex_dreams_deferred_quiet_total").increment(1);
            return Ok(DrainResult::Deferred);
        }
        Ok(DrainResult::Ready(msg))
    }

    /// Work that can be reordered behind the current in-flight item.
    async fn reorderable_backlog_len(&mut self) -> anyhow::Result<u64> {
        let (ready, deferred): (u64, u64) = redis::pipe()
            .cmd("LLEN")
            .arg(self.queue_key.as_str())
            .cmd("ZCARD")
            .arg(self.deferred_key.as_str())
            .query_async(&mut self.conn)
            .await?;
        Ok(ready.saturating_add(deferred))
    }

    /// Requeue notifications left in the durable in-flight list by a dead
    /// owner. Call exactly once after acquiring Dreams ownership.
    pub async fn recover_inflight(&mut self) -> anyhow::Result<u64> {
        Ok(redis::Script::new(RECOVER_INFLIGHT_LUA)
            .key(self.queue_key.as_str())
            .key(self.processing_key.as_str())
            .invoke_async(&mut self.conn)
            .await?)
    }

    fn ensure_region(&self, region: &RegionKey) -> anyhow::Result<()> {
        anyhow::ensure!(
            region.org == self.org,
            "Dreams queue for organization {:?} rejects region organization {:?}",
            self.org,
            region.org
        );
        Ok(())
    }
}

fn counter_key(region: &RegionKey) -> String {
    format!(
        "exocortex:dreams:counters:{}",
        serde_json::to_string(region).expect("RegionKey serialization is infallible")
    )
}

fn processed_event_key(region: &RegionKey) -> String {
    format!("{}:processed-events", counter_key(region))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quiet_hours_windows() {
        let none = QuietHours::none();
        assert!(!none.contains(3));
        let nightly = QuietHours::nightly();
        assert!(nightly.contains(2));
        assert!(nightly.contains(5));
        assert!(!nightly.contains(6), "end hour is exclusive");
        assert!(!nightly.contains(23));
        let day = QuietHours {
            start_hour: 9,
            end_hour: 17,
            enabled: true,
            utc_offset_minutes: -360,
        };
        assert!(day.contains(9));
        assert!(!day.contains(17), "end hour is exclusive");
    }

    #[test]
    fn fire_message_round_trips_region() {
        let region = RegionKey {
            org: "o".into(),
            project: "p".into(),
            memory_type: 3,
        };
        let message = FireMessage {
            region: region.clone(),
            fired_by: "node-a".into(),
            fired_at: None,
            fire_id: None,
            queued_payload: None,
            quiet_deferred: false,
        };
        let encoded = serde_json::to_string(&message).unwrap();
        let back: FireMessage = serde_json::from_str(&encoded).unwrap();
        assert_eq!(back.region, region);
        assert_eq!(
            encoded, r#"{"region":{"org":"o","project":"p","memory_type":3},"fired_by":"node-a"}"#,
            "RegionKey keeps the legacy queued JSON shape"
        );
    }

    #[test]
    fn quiet_hours_parse_is_validated_and_defaults_to_prd_utc_window() {
        assert_eq!(QuietHours::default(), "02-06".parse().unwrap());
        assert_eq!(QuietHours::default().utc_offset_minutes, 0);
        assert!("2-06".parse::<QuietHours>().is_err());
        assert!("24-06".parse::<QuietHours>().is_err());
        assert!("02-02".parse::<QuietHours>().is_err());
        assert!(QuietHours::default().with_utc_offset_minutes(1440).is_err());
    }

    #[test]
    fn quiet_hours_reorders_short_backlogs_but_never_a_lone_or_large_queue() {
        let quiet = QuietHours::default();
        assert!(!quiet.should_reorder(3, 0), "a lone fire runs immediately");
        assert!(quiet.should_reorder(3, 1));
        assert!(quiet.should_reorder(3, 30));
        assert!(
            !quiet.should_reorder(3, 31),
            "32 total items meet the progress threshold"
        );
        assert!(!quiet.should_reorder(7, 1), "outside the window runs now");
    }
}
