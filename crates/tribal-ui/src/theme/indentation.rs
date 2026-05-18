//! Nesting-depth ramp, capped at six levels.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Indent {
    L1,
    L2,
    L3,
    L4,
    L5,
    L6,
}

impl Indent {
    /// Step one level deeper, saturating at `L6`. Debug builds
    /// trip a `debug_assert!` when stepping past `L5` so genuine
    /// L7+ nesting bugs surface in tests; release builds saturate
    /// silently to keep deeply-nested rendering robust.
    #[must_use]
    pub const fn saturating_next(self) -> Self {
        match self {
            Self::L1 => Self::L2,
            Self::L2 => Self::L3,
            Self::L3 => Self::L4,
            Self::L4 => Self::L5,
            Self::L5 | Self::L6 => {
                debug_assert!(matches!(self, Self::L5), "indent already saturated at L6");
                Self::L6
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndentRamp {
    pub l1: usize,
    pub l2: usize,
    pub l3: usize,
    pub l4: usize,
    pub l5: usize,
    pub l6: usize,
}

impl IndentRamp {
    #[must_use]
    pub fn chars(&self, level: Indent) -> usize {
        match level {
            Indent::L1 => self.l1,
            Indent::L2 => self.l2,
            Indent::L3 => self.l3,
            Indent::L4 => self.l4,
            Indent::L5 => self.l5,
            Indent::L6 => self.l6,
        }
    }

    #[must_use]
    pub fn prefix(&self, level: Indent) -> String {
        " ".repeat(self.chars(level))
    }
}

impl Default for IndentRamp {
    fn default() -> Self {
        Self {
            l1: 0,
            l2: 4,
            l3: 8,
            l4: 12,
            l5: 16,
            l6: 20,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------
    // saturation
    // -----------------------------------------------------------------

    #[test]
    fn test_saturating_next_climbs_through_levels() {
        let mut level = Indent::L1;
        for _ in 0..4 {
            level = level.saturating_next();
        }
        assert_eq!(level, Indent::L5);
    }

    // -----------------------------------------------------------------
    // ramp lookup
    // -----------------------------------------------------------------

    #[test]
    fn test_default_ramp_widths() {
        let ramp = IndentRamp::default();
        assert_eq!(ramp.chars(Indent::L1), 0);
        assert_eq!(ramp.chars(Indent::L2), 4);
        assert_eq!(ramp.chars(Indent::L6), 20);
    }
}
