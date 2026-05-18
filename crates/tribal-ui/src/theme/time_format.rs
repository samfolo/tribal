//! Time-display defaults the theme prescribes.
//!
//! Each `TimeDisplay` variant pulls its defaults from the matching
//! nested struct: `Elapsed` reads `elapsed`, `Absolute` reads
//! `absolute`, `Relative` reads `relative`. The nesting reserves room
//! for additional knobs per variant (precision profiles, locale
//! overrides) without rippling field renames through call sites.

use crate::format::time::Precision;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum AbsoluteStyle {
    #[default]
    Rfc3339Utc,
    Rfc3339Offset,
    Compact,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RelativeAccuracy {
    #[default]
    Rough,
    Precise,
}

/// Defaults for `TimeDisplay::Elapsed` rendering.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ElapsedFormat {
    pub default_precision: Precision,
}

/// Defaults for `TimeDisplay::Absolute` rendering.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AbsoluteFormat {
    pub style: AbsoluteStyle,
}

/// Defaults for `TimeDisplay::Relative` rendering.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RelativeFormat {
    pub accuracy: RelativeAccuracy,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TimeFormat {
    pub elapsed: ElapsedFormat,
    pub absolute: AbsoluteFormat,
    pub relative: RelativeFormat,
}
