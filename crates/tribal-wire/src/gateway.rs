//! The gateway wire contract: the DTOs a managed run and the metering gateway
//! exchange across the wire/domain seam.
//!
//! This module authors the gateway crossings — the inference bracket, the
//! acknowledgement, the holds report, job enqueue, cap sync, and the grant set —
//! plus the metering shape the gateway writes. The types are
//! pure serde; forward compatibility rests on two pins: an unknown value on any
//! closed axis is rejected at deserialization, and the vocabulary grows only
//! with [`GATEWAY_CONTRACT_VERSION`].

mod inference;
mod job_plane;
mod metering;
mod reference;
mod vocabulary;

pub use inference::{
    ChatMessage, CompletionChunk, CompletionEnvelope, CompletionTerminal, EmbeddingEnvelope,
    FinishReason, GatewayError, InferenceCall, InferenceRequest, ResponseFormat, ToolCall,
    ToolDefinition,
};
pub use job_plane::{
    Acknowledgement, CapSync, GrantSet, HoldEntry, HoldStatus, HoldsReport, JobEnqueue, JobKind,
};
pub use metering::{GatewayUsage, UsageQuantity};
pub use reference::{AccountReference, ModelId, PositionKey, PrincipalReference};
pub use vocabulary::{CacheTier, Mode, Operation, Quality, UsageDimension};

/// The version of the gateway wire contract. A client stamps it on an inference
/// request; the closed vocabulary sets grow only when it does.
pub const GATEWAY_CONTRACT_VERSION: u16 = 1;
