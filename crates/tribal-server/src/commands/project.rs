//! Implementation of `tribal project` subcommands.

mod list;
mod outcome;
mod output;
mod register;

pub(crate) use list::run as list;
pub(crate) use register::run as register;
