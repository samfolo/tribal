//! Per-job watch channel entry and shared map type.
//!
//! The job-state signalling seam between the worker and the MCP
//! handlers, expressed as the first instantiation of the entity-keyed
//! [`event hub`](crate::event_hub): job state rides the watch channel;
//! the delta channel is unused today (`()`), present so job progress can
//! grow a delta stream without reshaping the map.

use tribal_domain::{JobId, JobState};

use crate::event_hub::{EntityHub, HubEntry};

/// Per-job hub entry carrying typed state and TTL metadata.
pub type JobWatchEntry = HubEntry<JobState, ()>;

/// Shared map of per-job watch channel entries.
pub type JobStateTxs = EntityHub<JobId, JobState, ()>;
