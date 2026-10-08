//! D46b: the post-boot store quiesce window.
//!
//! The bundled darwin-arm64 falkordb runtime forks its index GC once
//! ~40-60s after the store boots. No surface disables it: `FORK_GC_*`
//! and `ASYNC_DELETE` are rejected as `--loadmodule` arguments, the
//! runtime `GRAPH.CONFIG` table exposes no GC entry, and the Cypher
//! grammar has no index `OPTIONS`. On darwin that fork corrupts parked
//! thread-pool workers' condition variables and macOS pthread traps
//! the parent with SIGILL whenever queries are in flight around the
//! fork — while a store that is idle through the window survives it
//! (six-for-six isolated runs against the real graph; the supervisor
//! separately sets `ASYNC_DELETE no` to remove the delete-path forks).
//!
//! The supervisor arms this window each time a fresh store answers
//! PING; the falkor adapter's command paths await it first, so the
//! one fork per boot meets an idle store. Hostile only to latency
//! (at most one bounded hold per store boot, background traffic).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

/// Millis after arm before the gate starts holding. The fork has never
/// been observed before ~39s post-boot; 25s leaves margin while keeping
/// the post-boot drain running at full speed.
const HOLD_FROM_MS: u64 = 25_000;

/// Millis after arm when the gate releases. The fork has been observed
/// at 39-56s; 75s closes the window with margin.
const HOLD_UNTIL_MS: u64 = 75_000;

static ARMED_AT_MS: AtomicU64 = AtomicU64::new(0);
static EPOCH: OnceLock<Instant> = OnceLock::new();

fn now_ms() -> u64 {
    EPOCH.get_or_init(Instant::now).elapsed().as_millis() as u64
}

/// Arm the window for a freshly booted store. Called by the supervisor
/// right after the store answers PING (spawn and restart paths alike).
pub fn arm() {
    ARMED_AT_MS.store(now_ms(), Ordering::Release);
}

/// Pure decision core, so the window math is testable without a clock:
/// given the armed-at mark and now (both millis since the epoch), how
/// long must a command hold, if at all?
fn hold_for(armed_at_ms: Option<u64>, now_ms: u64) -> Option<Duration> {
    let armed = armed_at_ms?;
    let elapsed = now_ms.saturating_sub(armed);
    if elapsed < HOLD_FROM_MS {
        return None;
    }
    if elapsed >= HOLD_UNTIL_MS {
        return None;
    }
    Some(Duration::from_millis(HOLD_UNTIL_MS - elapsed))
}

/// How long the calling command must wait before touching the store.
/// Disarms itself once the window has passed.
pub fn hold_remaining() -> Option<Duration> {
    let armed = ARMED_AT_MS.load(Ordering::Acquire);
    let hold = hold_for((armed != 0).then_some(armed), now_ms());
    if hold.is_none() && armed != 0 && now_ms().saturating_sub(armed) >= HOLD_UNTIL_MS {
        ARMED_AT_MS.store(0, Ordering::Release);
    }
    hold
}

/// Await the quiesce window, if one is currently holding. Callers are
/// the adapter's command entry points.
pub async fn await_if_holding() {
    if let Some(wait) = hold_remaining() {
        tokio::time::sleep(wait).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gate_is_open_before_the_window() {
        assert_eq!(hold_for(Some(1_000), 5_000), None);
        assert_eq!(hold_for(Some(1_000), 25_999), None);
    }

    #[test]
    fn gate_holds_through_the_window_and_releases_at_the_end() {
        assert_eq!(
            hold_for(Some(1_000), 26_000),
            Some(Duration::from_millis(HOLD_UNTIL_MS - 26_000 + 1_000))
        );
        // The worst case: a command arriving the instant the window
        // opens waits only the window's remainder.
        assert_eq!(
            hold_for(Some(1_000), 25_000 + 1_000),
            Some(Duration::from_millis(HOLD_UNTIL_MS - 25_000))
        );
        assert_eq!(hold_for(Some(1_000), HOLD_UNTIL_MS + 1_000), None);
    }

    #[test]
    fn unarmed_state_never_holds() {
        assert_eq!(hold_for(None, 60_000), None);
    }
}
