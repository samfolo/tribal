//! Inter-element gap ramp.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Spacing {
    None,
    Sm,
    Md,
    Lg,
    Xl,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpacingRamp {
    pub sm: usize,
    pub md: usize,
    pub lg: usize,
    pub xl: usize,
}

impl SpacingRamp {
    #[must_use]
    pub fn chars(&self, size: Spacing) -> usize {
        match size {
            Spacing::None => 0,
            Spacing::Sm => self.sm,
            Spacing::Md => self.md,
            Spacing::Lg => self.lg,
            Spacing::Xl => self.xl,
        }
    }

    #[must_use]
    pub fn prefix(&self, size: Spacing) -> String {
        " ".repeat(self.chars(size))
    }
}

impl Default for SpacingRamp {
    fn default() -> Self {
        Self {
            sm: 2,
            md: 4,
            lg: 6,
            xl: 8,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------
    // ramp lookup
    // -----------------------------------------------------------------

    #[test]
    fn test_default_ramp_widths() {
        let ramp = SpacingRamp::default();
        assert_eq!(ramp.chars(Spacing::None), 0);
        assert_eq!(ramp.chars(Spacing::Sm), 2);
        assert_eq!(ramp.chars(Spacing::Md), 4);
        assert_eq!(ramp.chars(Spacing::Lg), 6);
        assert_eq!(ramp.chars(Spacing::Xl), 8);
    }

    #[test]
    fn test_prefix_matches_chars_count() {
        let ramp = SpacingRamp::default();
        let prefix = ramp.prefix(Spacing::Md);
        assert_eq!(prefix.len(), ramp.chars(Spacing::Md));
        assert!(prefix.chars().all(|c| c == ' '));
    }
}
