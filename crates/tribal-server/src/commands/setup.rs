//! Implementation of the `tribal setup` subcommand.
//!
//! Bootstraps a fresh Tribal installation: creates the config directory,
//! writes default prompt files, connects to the database, runs migrations,
//! creates the `principal:local` identity, generates an initial bearer
//! token, and writes a minimal config file.

mod config_file;
mod outcome;
mod output;
mod run;

pub use config_file::ConfigFileOutcome;
pub use outcome::SetupOutcome;
pub(crate) use run::run;
pub use run::run_async;
