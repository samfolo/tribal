//! Pure string-shape helpers shared across the CLI.
//!
//! No theme, no writer, no component — just typed inputs to `String`
//! outputs. Lives outside the component tree so non-UI callers (log-
//! line formatters, JSON shape constructors) reach for the same
//! vocabulary the UI components use under the hood.

pub mod time;
