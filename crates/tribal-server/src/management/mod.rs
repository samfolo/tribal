//! Runtime-independent local authority and application services.

pub(crate) mod application;
pub(crate) mod authority;
pub(crate) mod client;
pub(crate) mod config_schema;
pub(crate) mod configuration;
pub(crate) mod custody;
pub(crate) mod lifecycle;
pub(crate) mod probe;
pub(crate) mod product;
pub(crate) mod readiness;
pub(crate) mod runtime_control;
pub(crate) mod socket;
pub(crate) mod worker;
