// crates/exocortex-dreams/src/trigger.rs
//! The write-counter trigger model (§12.2): event-driven, never scheduled.

/// Per-region write counters.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RegionWriteCounters {
    /// Commits since the last cycle.
    pub memories_since_last_cycle: u32,
    /// Edge commits since the last cycle.
    pub edges_since_last_cycle: u32,
    /// Wall-clock seconds since the last cycle (updated on read).
    pub seconds_since_last_cycle: u64,
}

/// Trigger thresholds (§12.2 defaults).
#[derive(Clone, Copy, Debug)]
pub struct DreamsTrigger {
    /// Default 1000.
    pub memory_threshold: u32,
    /// Default 5000.
    pub edge_threshold: u32,
    /// Default 30 — forces a cycle on stale-but-live regions.
    pub age_floor_days: u32,
    /// Default 6 — rate limit (R-MT17).
    pub min_interval_hours: u32,
}

impl Default for DreamsTrigger {
    fn default() -> Self {
        Self {
            memory_threshold: 1000,
            edge_threshold: 5000,
            age_floor_days: 30,
            min_interval_hours: 6,
        }
    }
}

impl DreamsTrigger {
    /// §12.2's predicate, verbatim semantics.
    pub fn should_fire(&self, c: &RegionWriteCounters) -> bool {
        let min_interval = (self.min_interval_hours as u64) * 3600;
        if c.seconds_since_last_cycle < min_interval {
            return false;
        }
        c.memories_since_last_cycle >= self.memory_threshold
            || c.edges_since_last_cycle >= self.edge_threshold
            || c.seconds_since_last_cycle >= (self.age_floor_days as u64) * 86400
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fires_on_memory_threshold_after_interval() {
        let t = DreamsTrigger::default();
        let c = RegionWriteCounters {
            memories_since_last_cycle: 1000,
            edges_since_last_cycle: 0,
            seconds_since_last_cycle: 7 * 3600,
        };
        assert!(t.should_fire(&c));
    }

    #[test]
    fn rate_limited_below_min_interval() {
        let t = DreamsTrigger::default();
        let c = RegionWriteCounters {
            memories_since_last_cycle: 5000,
            edges_since_last_cycle: 5000,
            seconds_since_last_cycle: 3600,
        };
        assert!(!t.should_fire(&c), "R-MT17 rate limit");
    }

    #[test]
    fn age_floor_fires_on_stale_region() {
        let t = DreamsTrigger::default();
        let c = RegionWriteCounters {
            memories_since_last_cycle: 0,
            edges_since_last_cycle: 0,
            seconds_since_last_cycle: 31 * 86400,
        };
        assert!(t.should_fire(&c));
    }
}
