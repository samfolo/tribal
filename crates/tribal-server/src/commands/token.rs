//! Implementation of `tribal token` subcommands.

mod create;
mod list;
mod output;
mod revoke;
mod revoke_all;

pub(crate) use create::run as create;
pub(crate) use list::run as list;
pub(crate) use revoke::run as revoke;
pub(crate) use revoke_all::run as revoke_all;
