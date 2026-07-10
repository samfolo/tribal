//! Implementation of `tribal config` subcommands.

mod output;
mod revisioned;
mod show;

pub(crate) use revisioned::{get, path, set, validate};
pub(crate) use show::run as show;
