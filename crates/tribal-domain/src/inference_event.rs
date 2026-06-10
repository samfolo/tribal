//! Streaming events emitted over the lifetime of one completion call.

use crate::CompletionResponse;

/// One event in a completion call's stream.
///
/// Delta events are advisory progress: a consumer may render them live,
/// fold them, or ignore them. Only [`InferenceEvent::Completed`] is
/// authoritative — it carries the full response text and the usage for
/// the call, independent of whatever deltas preceded it. A provider
/// without a native streaming path emits the terminal event alone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InferenceEvent {
    /// A fragment of generated response text.
    TextDelta {
        /// The appended text fragment.
        text: String,
    },
    /// A fragment of model reasoning, where the provider surfaces it.
    ReasoningDelta {
        /// The appended reasoning fragment.
        text: String,
    },
    /// A fragment of a tool call's arguments, where the provider
    /// surfaces tool calls.
    ToolCallDelta {
        /// Position of the tool call within the response, demultiplexing
        /// parallel calls.
        index: u32,
        /// The provider-assigned call identifier, where the fragment
        /// carries it.
        call_id: Option<String>,
        /// The tool name, where the fragment carries it.
        name: Option<String>,
        /// The appended fragment of the call's JSON arguments.
        arguments_fragment: String,
    },
    /// The terminal event: the completed response with its usage.
    Completed {
        /// The full response, equal to the folded deltas.
        response: CompletionResponse,
    },
}
