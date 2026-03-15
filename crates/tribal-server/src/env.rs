//! Environment variable names used by the Tribal server binary.
//!
//! Centralised here so that runtime lookups and clap `env` attributes
//! reference the same source of truth.  Clap's `#[arg(env = "...")]`
//! requires a string literal, so each constant has a companion test
//! verifying it matches the attribute.

/// Environment variable for the configuration file path.
pub(crate) const ENV_CONFIG_PATH: &str = "TRIBAL_CONFIG_PATH";

/// Environment variable for project ID override.
pub(crate) const ENV_PROJECT_ID: &str = "TRIBAL_PROJECT_ID";
