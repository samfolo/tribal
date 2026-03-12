//! Well-known filesystem paths used as CLI defaults.

/// Default path to the Tribal configuration file.
///
/// Stored as a raw string — tilde resolution is the responsibility of the
/// configuration loading layer, not the CLI.
pub const DEFAULT_CONFIG_PATH: &str = "~/.config/tribal/tribal.yaml";
