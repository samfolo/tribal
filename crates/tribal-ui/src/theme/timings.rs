//! Animation cadence: spinner ticks and progress refresh.

use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Timings {
    pub instant: Duration,
    pub short: Duration,
    pub default: Duration,
    pub long: Duration,
}

impl Default for Timings {
    fn default() -> Self {
        Self {
            instant: Duration::from_millis(0),
            short: Duration::from_millis(150),
            default: Duration::from_millis(300),
            long: Duration::from_millis(500),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------
    // default cadence
    // -----------------------------------------------------------------

    #[test]
    fn test_default_cadence_is_monotonic() {
        let t = Timings::default();
        assert!(t.instant <= t.short);
        assert!(t.short <= t.default);
        assert!(t.default <= t.long);
    }
}
