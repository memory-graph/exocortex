// crates/exocortex-adapter-sdk/src/retry.rs
//! Backoff computation with injectable time (R14): delays come from a
//! pure function of the policy and attempt number, and the session's
//! sleep is a pluggable future so tests assert delay sequences without
//! sleeping.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use crate::RetryPolicy;

/// An injectable sleep: takes the delay, returns the future to await.
pub type SleepFn = Arc<dyn Fn(Duration) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

/// Real wall-clock sleep (production).
pub fn real_sleep() -> SleepFn {
    Arc::new(|d| Box::pin(tokio::time::sleep(d)))
}

/// Immediate no-op sleep (tests that don't care about delays).
pub fn instant_sleep() -> SleepFn {
    Arc::new(|_| Box::pin(std::future::ready(())))
}

/// The delay before the retry that follows `attempt` completed attempts:
/// exponential from `base` (attempt 1 → one base delay), capped at `max`,
/// jittered when enabled.
/// Jitter multiplies by `0.5 + rand % 2^32 / 2^33` (±50%) using the
/// caller-supplied entropy so tests can pin exact values.
pub fn next_delay(policy: &RetryPolicy, attempt: u32, entropy: &mut u64) -> Duration {
    let exp = policy
        .base
        .saturating_mul(1u32 << attempt.saturating_sub(1).min(16));
    let capped = exp.min(policy.max);
    if !policy.jitter {
        return capped;
    }
    // xorshift for cheap per-call entropy.
    let mut x = *entropy;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *entropy = x;
    let unit = (x >> 11) as f64 / (1u64 << 53) as f64; // [0, 1)
    let factor = 0.5 + unit; // [0.5, 1.5)
    Duration::from_secs_f64(
        (capped.as_secs_f64() * factor).clamp(0.0, policy.max.as_secs_f64().max(0.001)),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy(jitter: bool) -> RetryPolicy {
        RetryPolicy {
            base: Duration::from_millis(100),
            max: Duration::from_secs(10),
            max_attempts: 8,
            jitter,
        }
    }

    #[test]
    fn delays_grow_and_cap_without_jitter() {
        let p = policy(false);
        let mut e = 1u64;
        // Attempts are 1-based at call sites (attempt 1 = first failure).
        let seq: Vec<Duration> = (1..=8).map(|a| next_delay(&p, a, &mut e)).collect();
        for w in seq.windows(2) {
            assert!(w[1] >= w[0], "monotonic non-decreasing: {seq:?}");
        }
        assert_eq!(seq[0], Duration::from_millis(100), "attempt 1 -> base");
        assert_eq!(seq[3], Duration::from_millis(800));
        assert!(seq.iter().all(|d| *d <= Duration::from_secs(10)));
    }

    #[test]
    fn jitter_stays_within_bounds() {
        let p = policy(true);
        for seed in [1u64, 42, 9999] {
            let mut e = seed;
            for a in 0..10 {
                let base = next_delay(&policy(false), a, &mut 1);
                let d = next_delay(&p, a, &mut e);
                assert!(d >= base / 2 && d <= base + base / 2, "{d:?} vs {base:?}");
                assert!(d <= p.max);
            }
        }
    }
}
