//! Terminal output for the `tribal bootstrap` command.

mod action_list;
mod handoff;
mod status_block;
mod token_block;

pub(super) use handoff::{Handoff, write_human, write_json};
