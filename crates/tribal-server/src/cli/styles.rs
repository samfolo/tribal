//! Help output styling for the Tribal CLI.
//!
//! Defines a single [`STYLES`] constant consumed by the root `Cli` parser.
//! Only affects `--help` and `--version` output — command handler output is
//! styled independently.

use clap::builder::{styling::AnsiColor, Styles};

/// Colour scheme applied to clap-generated help text.
pub const STYLES: Styles = Styles::styled()
    .header(AnsiColor::Yellow.on_default().bold())
    .usage(AnsiColor::Green.on_default().bold())
    .literal(AnsiColor::Cyan.on_default().bold())
    .placeholder(AnsiColor::Green.on_default());
