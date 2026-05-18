//! Per-check modules and shared internal types.
//!
//! Each check is a leaf module under this namespace producing a typed
//! `CheckOutcome`.  The dispatch path lives in `super::run` (added in
//! a later step); the wire-format conversion lives in `super::output`.

mod types;

pub(super) use types::{CheckDetail, CheckName, CheckOutcome, CheckRemediation, CheckStatus};
