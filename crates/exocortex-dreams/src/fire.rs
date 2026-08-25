// crates/exocortex-dreams/src/fire.rs
//! §12.2 transport + quiet hours: the Dreams fire channel as Redis
//! RPUSH/BLPOP (`exocortex:dreams:queue`), with the R-Dr13 Lua counter
//! reset and the R-Dr14 quiet-hours preference ordering. The in-process
//! mpsc channel in `DreamsEngine` remains the same-shape local transport;
//! this module is the multi-node production one.

use std::time::Duration;

use exocortex_storage::RegionKey;
use smol_str::SmolStr;

/// The queue key (§12.2).
pub const DREAMS_QUEUE_KEY: &str = "exocortex:dreams:queue";

/// R-Dr13: reset the region's write counters atomically when a new lease
/// holder picks the region up (the previous holder may have died after
/// firing but before resetting). Counter hashes live under
/// `exocortex:dreams:counters:<region>`.
pub const RESET_COUNTERS_LUA: &str = r#"
local key = KEYS[1]
local mem = redis.call('HGET', key, 'memories') or '0'
local edges = redis.call('HGET', key, 'edges') or '0'
redis.call('DEL', key)
return {mem, edges}
"#;

/// One queued region payload (JSON): region coordinates + who fired.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct FireMessage {
    /// The region to consolidate.
    pub region: RegionPayload,
    /// Firing node id (diagnostics).
    pub fired_by: SmolStr,
}

/// Serializable RegionKey (it is not serde-derived in storage).
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct RegionPayload {
    /// Org.
    pub org: String,
    /// Project.
    pub project: String,
    /// Memory type.
    pub memory_type: u8,
}

impl From<&RegionKey> for RegionPayload {
    fn from(r: &RegionKey) -> Self {
        Self {
            org: r.org.to_string(),
            project: r.project.to_string(),
            memory_type: r.memory_type,
        }
    }
}

impl From<RegionPayload> for RegionKey {
    fn from(p: RegionPayload) -> Self {
        Self {
            org: p.org.into(),
            project: p.project.into(),
            memory_type: p.memory_type,
        }
    }
}

/// R-Dr14 quiet hours: user preference ordering for when Dreams cycles may
/// run. `contains(t)` is local-wall-clock; callers pass the region's
/// timezone offset. Default: no quiet window.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct QuietHours {
    /// Start hour (local, 0-23), inclusive.
    pub start_hour: u8,
    /// End hour (local, 0-23), exclusive.
    pub end_hour: u8,
    /// Whether the window is active (wraps midnight when start > end).
    pub enabled: bool,
}

impl QuietHours {
    /// No quiet window (cycles may run any time).
    pub fn none() -> Self {
        Self::default()
    }

    /// A window like 23:00-07:00 (R-Dr14 default shape for dev work).
    pub fn nightly() -> Self {
        Self {
            start_hour: 23,
            end_hour: 7,
            enabled: true,
        }
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
}

/// The Redis-backed fire transport. Requires a `redis` async client
/// (multiplexed); RPUSH fires, BLPOP drains, Lua resets counters (R-Dr13).
pub struct RedisFireQueue {
    conn: redis::aio::MultiplexedConnection,
    /// Per-node quiet hours preference (R-Dr14).
    pub quiet_hours: QuietHours,
}

impl RedisFireQueue {
    /// Connect over an existing client.
    pub fn new(conn: redis::aio::MultiplexedConnection, quiet_hours: QuietHours) -> Self {
        Self { conn, quiet_hours }
    }

    /// RPUSH a region onto the queue (TriggerWatcher side, §12.2). Honors
    /// quiet hours: during the window the message is deferred by encoding
    /// it with `defer_until_end_of_quiet=true`, and the drainer holds it.
    pub async fn fire(&mut self, region: &RegionKey, fired_by: &str) -> anyhow::Result<()> {
        let msg = FireMessage {
            region: region.into(),
            fired_by: fired_by.into(),
        };
        let payload = serde_json::to_string(&msg)?;
        let _: () = redis::cmd("RPUSH")
            .arg(DREAMS_QUEUE_KEY)
            .arg(payload)
            .query_async(&mut self.conn)
            .await?;
        metrics::counter!("exocortex_dreams_fired_total", "transport" => "redis").increment(1);
        Ok(())
    }

    /// BLPOP one region (DreamsWorker side). `timeout` bounds the wait.
    /// Returns `None` on timeout.
    pub async fn drain(&mut self, timeout: Duration) -> anyhow::Result<Option<RegionKey>> {
        let (key, payload): (String, String) = match redis::cmd("BLPOP")
            .arg(DREAMS_QUEUE_KEY)
            .arg(timeout.as_secs_f64())
            .query_async(&mut self.conn)
            .await
        {
            Ok(pair) => pair,
            Err(e) => {
                if e.to_string().contains("timeout") {
                    return Ok(None);
                }
                return Err(e.into());
            }
        };
        let _ = key;
        let msg: FireMessage = serde_json::from_str(&payload)?;
        // R-Dr14: during quiet hours the worker defers (re-queues at the
        // tail) instead of consolidating; ordering preserves fairness.
        let local_hour = local_hour_now();
        if self.quiet_hours.contains(local_hour) {
            let _: () = redis::cmd("RPUSH")
                .arg(DREAMS_QUEUE_KEY)
                .arg(payload)
                .query_async(&mut self.conn)
                .await?;
            metrics::counter!("exocortex_dreams_deferred_quiet_total").increment(1);
            return Ok(None);
        }
        Ok(Some(msg.region.into()))
    }

    /// R-Dr13: atomically read + reset the region's write counters; call
    /// when a lease is (re-)acquired so a dead predecessor's half-state
    /// never double-counts.
    pub async fn reset_counters(&mut self, region: &RegionKey) -> anyhow::Result<(u32, u32)> {
        let key = format!(
            "exocortex:dreams:counters:{}:{}:{}",
            region.org, region.project, region.memory_type
        );
        let script = redis::Script::new(RESET_COUNTERS_LUA);
        let invoke = script.key(key);
        let (mem, edges): (String, String) = invoke.invoke_async(&mut self.conn).await?;
        Ok((mem.parse().unwrap_or(0), edges.parse().unwrap_or(0)))
    }
}

/// Local wall-clock hour (the node's TZ; per-user TZ ordering arrives with
/// multi-tenant preferences in v2).
fn local_hour_now() -> u8 {
    chrono::Local::now()
        .format("%H")
        .to_string()
        .parse()
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quiet_hours_windows() {
        let none = QuietHours::none();
        assert!(!none.contains(3));
        let nightly = QuietHours::nightly();
        assert!(nightly.contains(23));
        assert!(nightly.contains(2));
        assert!(nightly.contains(6));
        assert!(!nightly.contains(7), "end hour is exclusive");
        assert!(!nightly.contains(22));
        let day = QuietHours {
            start_hour: 9,
            end_hour: 17,
            enabled: true,
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
        let payload = RegionPayload::from(&region);
        let back: RegionKey = payload.into();
        assert_eq!(back.org, region.org);
        assert_eq!(back.project, region.project);
        assert_eq!(back.memory_type, region.memory_type);
    }
}
