//! Streaming wire support shared by the provider implementations.
//!
//! Providers stream over two wire framings: server-sent events (Anthropic,
//! `OpenAI`) and newline-delimited JSON (Ollama). Both are line-based, so
//! one incremental [`LineFramer`] feeds either an [`SseAssembler`] or a
//! per-line JSON parse. A provider-specific [`EventTranslator`] turns wire
//! frames into [`InferenceEvent`]s, and [`drive_event_stream`] runs the
//! shared pull loop: framing, translation, error mapping, and fusing after
//! the terminal event.

use std::collections::VecDeque;

use futures_util::{Stream, StreamExt};
use tribal_domain::{CompletionResponse, InferenceEvent};

use crate::{
    InferenceError,
    error::{map_body_read_error, map_json_parse_error},
};

/// The stream of events from one completion call.
///
/// Items are deltas followed by exactly one terminal
/// [`InferenceEvent::Completed`], or an error that ends the stream. The
/// stream is fused: nothing follows the terminal event or an error.
pub type InferenceEventStream = std::pin::Pin<
    Box<dyn Stream<Item = Result<InferenceEvent, InferenceError>> + Send + 'static>,
>;

/// Which wire transport a request body is built for.
///
/// The two modes must build byte-identical request bodies apart from the
/// stream-specific fields; the fold-equivalence tests assert exactly that.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WireMode {
    /// One buffered request/response exchange.
    Buffered,
    /// A provider-native streaming exchange.
    Streaming,
}

// ---------------------------------------------------------------------------
// Line framing
// ---------------------------------------------------------------------------

/// Incremental byte-to-line framer.
///
/// Accumulates wire chunks and yields complete lines, handling lines split
/// across chunk boundaries. Splitting happens on `\n` bytes only, which is
/// safe inside multi-byte UTF-8 sequences; a trailing `\r` is stripped so
/// `\r\n` and `\n` terminators behave identically.
#[derive(Debug, Default)]
pub(crate) struct LineFramer {
    buffer: Vec<u8>,
}

impl LineFramer {
    /// Appends a wire chunk to the internal buffer.
    pub(crate) fn push(&mut self, chunk: &[u8]) {
        self.buffer.extend_from_slice(chunk);
    }

    /// Pops the next complete line, without its terminator.
    pub(crate) fn next_line(&mut self) -> Option<Vec<u8>> {
        let newline = self.buffer.iter().position(|&b| b == b'\n')?;
        let mut line: Vec<u8> = self.buffer.drain(..=newline).collect();
        line.pop();
        if line.last() == Some(&b'\r') {
            line.pop();
        }
        Some(line)
    }

    /// Drains the final unterminated line, if any bytes remain.
    ///
    /// Called once at end of stream so a final line without a trailing
    /// newline is still delivered.
    pub(crate) fn take_remainder(&mut self) -> Option<Vec<u8>> {
        if self.buffer.is_empty() {
            return None;
        }
        Some(std::mem::take(&mut self.buffer))
    }
}

// ---------------------------------------------------------------------------
// Server-sent event assembly
// ---------------------------------------------------------------------------

/// Assembles server-sent events from framed lines, yielding each event's
/// joined `data` payload.
///
/// Only the `data` field matters to the providers that speak SSE here: the
/// payload JSON carries its own discriminator, so `event:` lines are
/// redundant and ignored, as are comments, `id:`, and `retry:`. An event
/// dispatches on a blank line; multi-line data joins with `\n` per the SSE
/// specification.
#[derive(Debug, Default)]
pub(crate) struct SseAssembler {
    data: Vec<String>,
}

impl SseAssembler {
    /// Feeds one framed line, returning a complete data payload when the
    /// line dispatches an event.
    pub(crate) fn on_line(&mut self, line: &str) -> Option<String> {
        if line.is_empty() {
            if self.data.is_empty() {
                return None;
            }
            return Some(std::mem::take(&mut self.data).join("\n"));
        }
        if let Some(rest) = line.strip_prefix("data:") {
            self.data.push(rest.strip_prefix(' ').unwrap_or(rest).to_owned());
        }
        None
    }
}

// ---------------------------------------------------------------------------
// Event translation and the shared pull loop
// ---------------------------------------------------------------------------

/// Translates framed wire lines into [`InferenceEvent`]s.
///
/// One translator instance accumulates the state a terminal event needs
/// (text, usage counters) across the lifetime of one wire exchange.
pub(crate) trait EventTranslator: Send + 'static {
    /// Consumes one complete line from the wire, yielding zero or more
    /// events. The terminal [`InferenceEvent::Completed`] must be the last
    /// event the translator ever yields.
    fn on_line(&mut self, line: &str) -> Result<Vec<InferenceEvent>, InferenceError>;

    /// The wire closed. A translator whose protocol signals the terminal
    /// event in-band treats end-of-stream before that signal as an error.
    fn on_end(&mut self) -> Result<Vec<InferenceEvent>, InferenceError>;
}

/// Runs a provider's wire response through its translator, producing the
/// public event stream.
///
/// Owns the shared loop mechanics: chunk awaiting, line framing, UTF-8
/// validation, chunk-error mapping, and fusing the stream after the
/// terminal event or the first error.
pub(crate) fn drive_event_stream<T: EventTranslator>(
    response: reqwest::Response,
    translator: T,
    provider: &'static str,
) -> InferenceEventStream {
    drive_lines(response.bytes_stream(), translator, provider)
}

/// [`drive_event_stream`] over any chunk stream; the seam unit tests use.
pub(crate) fn drive_lines<S, B, T>(
    body: S,
    translator: T,
    provider: &'static str,
) -> InferenceEventStream
where
    S: Stream<Item = Result<B, reqwest::Error>> + Send + 'static,
    B: AsRef<[u8]>,
    T: EventTranslator,
{
    let state = DriveState {
        body: Box::pin(body),
        framer: LineFramer::default(),
        translator,
        pending: VecDeque::new(),
        provider,
        finished: false,
    };

    Box::pin(futures_util::stream::unfold(state, |mut state| async move {
        loop {
            if state.finished {
                return None;
            }
            if let Some(item) = state.pending.pop_front() {
                if item.is_err() || matches!(item, Ok(InferenceEvent::Completed { .. })) {
                    state.finished = true;
                    state.pending.clear();
                }
                return Some((item, state));
            }

            match state.body.next().await {
                Some(Ok(chunk)) => {
                    state.framer.push(chunk.as_ref());
                    while let Some(line) = state.framer.next_line() {
                        if !state.feed_line(line) {
                            break;
                        }
                    }
                }
                Some(Err(error)) => {
                    state
                        .pending
                        .push_back(Err(map_body_read_error(&error, state.provider)));
                }
                None => {
                    if let Some(line) = state.framer.take_remainder() {
                        state.feed_line(line);
                    }
                    let outcome = state.translator.on_end();
                    state.extend_pending(outcome);
                    // The next pending drain returns the terminal or the
                    // error; an empty drain on a misbehaving translator
                    // would loop forever without this.
                    if state.pending.is_empty() {
                        return None;
                    }
                }
            }
        }
    }))
}

struct DriveState<S, T> {
    body: std::pin::Pin<Box<S>>,
    framer: LineFramer,
    translator: T,
    pending: VecDeque<Result<InferenceEvent, InferenceError>>,
    provider: &'static str,
    finished: bool,
}

impl<S, T: EventTranslator> DriveState<S, T> {
    /// Validates and translates one framed line into pending events.
    /// Returns `false` when translation failed and framing should stop.
    fn feed_line(&mut self, line: Vec<u8>) -> bool {
        let outcome = match String::from_utf8(line) {
            Ok(text) => self.translator.on_line(&text),
            Err(error) => Err(InferenceError::ResponseParseFailed {
                expected_shape: "UTF-8 stream data".to_owned(),
                actual: format!("invalid UTF-8 in stream line: {error}"),
            }),
        };
        self.extend_pending(outcome)
    }

    /// Queues a translation outcome. Returns `false` on error.
    fn extend_pending(&mut self, outcome: Result<Vec<InferenceEvent>, InferenceError>) -> bool {
        match outcome {
            Ok(events) => {
                self.pending.extend(events.into_iter().map(Ok));
                true
            }
            Err(error) => {
                self.pending.push_back(Err(error));
                false
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Terminal await
// ---------------------------------------------------------------------------

/// Consumes an event stream to its terminal event, returning the completed
/// response.
///
/// The consumer for callers that want the result, not the progress: deltas
/// are discarded, errors propagate, and a stream that ends without a
/// terminal event is a protocol violation surfaced as a parse failure.
///
/// # Errors
///
/// Propagates any error item from the stream. Returns
/// [`InferenceError::ResponseParseFailed`] if the stream ends without a
/// terminal [`InferenceEvent::Completed`].
pub async fn collect_completion(
    mut stream: InferenceEventStream,
) -> Result<CompletionResponse, InferenceError> {
    while let Some(event) = stream.next().await {
        if let InferenceEvent::Completed { response } = event? {
            return Ok(response);
        }
    }
    Err(InferenceError::ResponseParseFailed {
        expected_shape: "a terminal Completed event".to_owned(),
        actual: "stream ended without one".to_owned(),
    })
}

/// Parses one streamed JSON frame, mapping failure with the frame preview.
pub(crate) fn parse_frame<D: serde::de::DeserializeOwned>(
    frame: &str,
    expected_shape: &str,
) -> Result<D, InferenceError> {
    serde_json::from_str(frame).map_err(|e| map_json_parse_error(&e, expected_shape, frame))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tribal_domain::CompletionUsage;

    use super::*;

    // -- LineFramer ----------------------------------------------------------

    #[test]
    fn test_line_framer_splits_lines_within_one_chunk() {
        let mut framer = LineFramer::default();
        framer.push(b"alpha\nbeta\n");
        assert_eq!(framer.next_line(), Some(b"alpha".to_vec()));
        assert_eq!(framer.next_line(), Some(b"beta".to_vec()));
        assert_eq!(framer.next_line(), None);
    }

    #[test]
    fn test_line_framer_reassembles_line_split_across_chunks() {
        let mut framer = LineFramer::default();
        framer.push(b"al");
        assert_eq!(framer.next_line(), None);
        framer.push(b"pha\nbe");
        assert_eq!(framer.next_line(), Some(b"alpha".to_vec()));
        assert_eq!(framer.next_line(), None);
        framer.push(b"ta\n");
        assert_eq!(framer.next_line(), Some(b"beta".to_vec()));
    }

    #[test]
    fn test_line_framer_strips_carriage_return() {
        let mut framer = LineFramer::default();
        framer.push(b"alpha\r\nbeta\n");
        assert_eq!(framer.next_line(), Some(b"alpha".to_vec()));
        assert_eq!(framer.next_line(), Some(b"beta".to_vec()));
    }

    #[test]
    fn test_line_framer_remainder_returns_unterminated_tail() {
        let mut framer = LineFramer::default();
        framer.push(b"alpha\ntail");
        assert_eq!(framer.next_line(), Some(b"alpha".to_vec()));
        assert_eq!(framer.next_line(), None);
        assert_eq!(framer.take_remainder(), Some(b"tail".to_vec()));
        assert_eq!(framer.take_remainder(), None);
    }

    #[test]
    fn test_line_framer_preserves_multibyte_utf8_across_chunks() {
        // "£" is 0xC2 0xA3; split it across chunks.
        let mut framer = LineFramer::default();
        framer.push(&[b'a', 0xC2]);
        assert_eq!(framer.next_line(), None);
        framer.push(&[0xA3, b'\n']);
        assert_eq!(framer.next_line(), Some("a£".as_bytes().to_vec()));
    }

    // -- SseAssembler ---------------------------------------------------------

    #[test]
    fn test_sse_assembler_dispatches_data_on_blank_line() {
        let mut sse = SseAssembler::default();
        assert_eq!(sse.on_line("data: {\"a\":1}"), None);
        assert_eq!(sse.on_line(""), Some("{\"a\":1}".to_owned()));
    }

    #[test]
    fn test_sse_assembler_joins_multi_line_data() {
        let mut sse = SseAssembler::default();
        assert_eq!(sse.on_line("data: first"), None);
        assert_eq!(sse.on_line("data: second"), None);
        assert_eq!(sse.on_line(""), Some("first\nsecond".to_owned()));
    }

    #[test]
    fn test_sse_assembler_ignores_event_comment_and_unknown_fields() {
        let mut sse = SseAssembler::default();
        assert_eq!(sse.on_line("event: message_start"), None);
        assert_eq!(sse.on_line(": keep-alive comment"), None);
        assert_eq!(sse.on_line("id: 7"), None);
        assert_eq!(sse.on_line("retry: 100"), None);
        assert_eq!(sse.on_line("data: payload"), None);
        assert_eq!(sse.on_line(""), Some("payload".to_owned()));
    }

    #[test]
    fn test_sse_assembler_blank_line_without_data_dispatches_nothing() {
        let mut sse = SseAssembler::default();
        assert_eq!(sse.on_line("event: ping"), None);
        assert_eq!(sse.on_line(""), None);
    }

    #[test]
    fn test_sse_assembler_data_without_space_after_colon() {
        let mut sse = SseAssembler::default();
        assert_eq!(sse.on_line("data:tight"), None);
        assert_eq!(sse.on_line(""), Some("tight".to_owned()));
    }

    // -- drive_lines ----------------------------------------------------------

    /// Emits one `TextDelta` per non-empty line and the terminal on `END`.
    struct ScriptedTranslator {
        text: String,
    }

    impl ScriptedTranslator {
        fn new() -> Self {
            Self {
                text: String::new(),
            }
        }

        fn terminal(&self) -> InferenceEvent {
            InferenceEvent::Completed {
                response: CompletionResponse {
                    text: self.text.clone(),
                    usage: CompletionUsage {
                        provider: "scripted".to_owned(),
                        model: "scripted-model".to_owned(),
                        input_tokens: 1,
                        output_tokens: 2,
                        cache_read_tokens: 0,
                        cache_write_tokens: 0,
                        total_tokens: 3,
                        latency: Duration::ZERO,
                    },
                },
            }
        }
    }

    impl EventTranslator for ScriptedTranslator {
        fn on_line(&mut self, line: &str) -> Result<Vec<InferenceEvent>, InferenceError> {
            match line {
                "" => Ok(vec![]),
                "END" => Ok(vec![self.terminal()]),
                "FAIL" => Err(InferenceError::ResponseParseFailed {
                    expected_shape: "scripted".to_owned(),
                    actual: "FAIL line".to_owned(),
                }),
                text => {
                    self.text.push_str(text);
                    Ok(vec![InferenceEvent::TextDelta {
                        text: text.to_owned(),
                    }])
                }
            }
        }

        fn on_end(&mut self) -> Result<Vec<InferenceEvent>, InferenceError> {
            Err(InferenceError::ResponseParseFailed {
                expected_shape: "an END line".to_owned(),
                actual: "stream ended without one".to_owned(),
            })
        }
    }

    fn chunks(parts: &[&str]) -> Vec<Result<Vec<u8>, reqwest::Error>> {
        parts.iter().map(|p| Ok(p.as_bytes().to_vec())).collect()
    }

    #[tokio::test]
    async fn test_drive_lines_translates_and_terminates() {
        let body = futures_util::stream::iter(chunks(&["al", "pha\nbeta\nEN", "D\n"]));
        let stream = drive_lines(body, ScriptedTranslator::new(), "scripted");

        let events: Vec<_> = stream.collect().await;
        assert_eq!(events.len(), 3);
        assert!(matches!(
            &events[0],
            Ok(InferenceEvent::TextDelta { text }) if text == "alpha"
        ));
        assert!(matches!(
            &events[1],
            Ok(InferenceEvent::TextDelta { text }) if text == "beta"
        ));
        assert!(matches!(
            &events[2],
            Ok(InferenceEvent::Completed { response }) if response.text == "alphabeta"
        ));
    }

    #[tokio::test]
    async fn test_drive_lines_terminal_without_trailing_newline() {
        let body = futures_util::stream::iter(chunks(&["alpha\nEND"]));
        let stream = drive_lines(body, ScriptedTranslator::new(), "scripted");

        let events: Vec<_> = stream.collect().await;
        assert_eq!(events.len(), 2);
        assert!(matches!(&events[1], Ok(InferenceEvent::Completed { .. })));
    }

    #[tokio::test]
    async fn test_drive_lines_translation_error_fuses_stream() {
        let body = futures_util::stream::iter(chunks(&["alpha\nFAIL\nbeta\nEND\n"]));
        let stream = drive_lines(body, ScriptedTranslator::new(), "scripted");

        let events: Vec<_> = stream.collect().await;
        assert_eq!(events.len(), 2, "nothing follows the first error");
        assert!(matches!(&events[0], Ok(InferenceEvent::TextDelta { .. })));
        assert!(matches!(
            &events[1],
            Err(InferenceError::ResponseParseFailed { actual, .. }) if actual == "FAIL line"
        ));
    }

    #[tokio::test]
    async fn test_drive_lines_eof_without_terminal_errors() {
        let body = futures_util::stream::iter(chunks(&["alpha\n"]));
        let stream = drive_lines(body, ScriptedTranslator::new(), "scripted");

        let events: Vec<_> = stream.collect().await;
        assert_eq!(events.len(), 2);
        assert!(matches!(
            &events[1],
            Err(InferenceError::ResponseParseFailed { expected_shape, .. })
                if expected_shape == "an END line"
        ));
    }

    #[tokio::test]
    async fn test_drive_lines_invalid_utf8_errors() {
        let body = futures_util::stream::iter(vec![Ok::<_, reqwest::Error>(vec![
            0xFF, 0xFE, b'\n',
        ])]);
        let stream = drive_lines(body, ScriptedTranslator::new(), "scripted");

        let events: Vec<_> = stream.collect().await;
        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0],
            Err(InferenceError::ResponseParseFailed { expected_shape, .. })
                if expected_shape == "UTF-8 stream data"
        ));
    }

    #[tokio::test]
    async fn test_drive_lines_chunk_error_maps_and_fuses() {
        let wire_error = reqwest::get("http://127.0.0.1:1").await.unwrap_err();
        let body = futures_util::stream::iter(vec![
            Ok(b"alpha\n".to_vec()),
            Err(wire_error),
            Ok(b"END\n".to_vec()),
        ]);
        let stream = drive_lines(body, ScriptedTranslator::new(), "scripted");

        let events: Vec<_> = stream.collect().await;
        assert_eq!(events.len(), 2, "nothing follows the wire error");
        assert!(matches!(&events[0], Ok(InferenceEvent::TextDelta { .. })));
        assert!(events[1].is_err());
    }

    #[tokio::test]
    async fn test_drive_lines_nothing_follows_terminal() {
        let body = futures_util::stream::iter(chunks(&["END\nafter\n"]));
        let stream = drive_lines(body, ScriptedTranslator::new(), "scripted");

        let events: Vec<_> = stream.collect().await;
        assert_eq!(events.len(), 1);
        assert!(matches!(&events[0], Ok(InferenceEvent::Completed { .. })));
    }

    // -- collect_completion ---------------------------------------------------

    #[tokio::test]
    async fn test_collect_completion_returns_terminal_response() {
        let body = futures_util::stream::iter(chunks(&["alpha\nbeta\nEND\n"]));
        let stream = drive_lines(body, ScriptedTranslator::new(), "scripted");

        let response = collect_completion(stream).await.unwrap();
        assert_eq!(response.text, "alphabeta");
        assert_eq!(response.usage.total_tokens, 3);
    }

    #[tokio::test]
    async fn test_collect_completion_propagates_stream_error() {
        let body = futures_util::stream::iter(chunks(&["FAIL\n"]));
        let stream = drive_lines(body, ScriptedTranslator::new(), "scripted");

        let err = collect_completion(stream).await.unwrap_err();
        assert!(matches!(
            err,
            InferenceError::ResponseParseFailed { ref actual, .. } if actual == "FAIL line"
        ));
    }

    #[tokio::test]
    async fn test_collect_completion_empty_stream_errors() {
        let stream: InferenceEventStream = Box::pin(futures_util::stream::empty());
        let err = collect_completion(stream).await.unwrap_err();
        assert!(matches!(
            err,
            InferenceError::ResponseParseFailed { ref expected_shape, .. }
                if expected_shape == "a terminal Completed event"
        ));
    }
}
