use serde_json::Value;
use tribal_domain::ProviderKind;
use wiremock::{
    Mock, MockBuilder, MockServer, ResponseTemplate,
    matchers::{body_string_contains, method, path},
};

use super::envelope::{chat_path, wrap_completion, wrap_completion_stream};

// ---------------------------------------------------------------------------
// ContentMatcher
// ---------------------------------------------------------------------------

/// Determines how a mock matches incoming request bodies.
pub enum ContentMatcher {
    /// Matches any request body.
    Any,
    /// Matches when the request body contains the substring.
    Contains(String),
}

impl From<&str> for ContentMatcher {
    fn from(s: &str) -> Self {
        Self::Contains(s.to_owned())
    }
}

// ---------------------------------------------------------------------------
// MockResponse
// ---------------------------------------------------------------------------

/// A single entry in a mock response sequence.
#[derive(Clone)]
pub enum MockResponse {
    /// A successful response wrapping a stage fixture.
    Fixture(Value),
    /// An HTTP error response with the given status code.
    Error(u16),
}

impl From<Value> for MockResponse {
    fn from(v: Value) -> Self {
        Self::Fixture(v)
    }
}

/// Creates an HTTP error response for use in mock sequences.
#[must_use]
pub fn error(status: u16) -> MockResponse {
    MockResponse::Error(status)
}

// ---------------------------------------------------------------------------
// StageMountBuilder
// ---------------------------------------------------------------------------

struct MountEntry {
    matcher: ContentMatcher,
    responses: Vec<MockResponse>,
    repeat_last: bool,
    streaming: bool,
    delay_ms: Option<u64>,
}

/// Builder for mounting stage-specific mock responses on a wiremock server.
///
/// The closure-based API ensures every stage uses the same pattern.
pub struct StageMountBuilder<'a> {
    server: &'a MockServer,
    stage: &'static str,
    provider: ProviderKind,
    entries: Vec<MountEntry>,
    has_any: bool,
    has_contains: bool,
}

impl<'a> StageMountBuilder<'a> {
    pub(crate) fn new(server: &'a MockServer, stage: &'static str, provider: ProviderKind) -> Self {
        Self {
            server,
            stage,
            provider,
            entries: Vec::new(),
            has_any: false,
            has_contains: false,
        }
    }

    /// Mounts a single persistent response for any request content.
    ///
    /// Sugar for `on_content_repeat_last(ContentMatcher::Any, &[fixture.into()])`.
    pub fn respond(&mut self, fixture: Value) {
        self.on_content_repeat_last(ContentMatcher::Any, &[MockResponse::Fixture(fixture)]);
    }

    /// Mounts a response sequence matched by content.
    ///
    /// Wiremock returns 404 for requests beyond the sequence length,
    /// which surfaces as a provider error in the worker.
    pub fn on_content(&mut self, matcher: impl Into<ContentMatcher>, responses: &[MockResponse]) {
        self.add_entry(matcher.into(), responses, false, false, None);
    }

    /// Mounts a persistent buffered response that arrives only after the
    /// given delay — a slow stage call, so a test can observe the state a
    /// thread holds while it waits (a verifier child suspending its
    /// parent, say).
    pub fn on_content_delayed(
        &mut self,
        matcher: impl Into<ContentMatcher>,
        responses: &[MockResponse],
        delay_ms: u64,
    ) {
        self.add_entry(matcher.into(), responses, true, false, Some(delay_ms));
    }

    /// Mounts a response sequence matched by content, framed as the
    /// provider's streaming wire body — the agentic loop's turns, whose
    /// only inference entry is the streaming path. A verifier child, which
    /// runs a buffered one-shot, is mounted with [`Self::on_content`]
    /// against the same endpoint; the two are told apart by the `stream`
    /// flag in the request body.
    pub fn on_content_streamed(
        &mut self,
        matcher: impl Into<ContentMatcher>,
        responses: &[MockResponse],
    ) {
        self.add_entry(matcher.into(), responses, false, true, None);
    }

    /// Mounts a response sequence matched by content, repeating the final
    /// entry indefinitely after the sequence is consumed.
    pub fn on_content_repeat_last(
        &mut self,
        matcher: impl Into<ContentMatcher>,
        responses: &[MockResponse],
    ) {
        self.add_entry(matcher.into(), responses, true, false, None);
    }

    fn add_entry(
        &mut self,
        matcher: ContentMatcher,
        responses: &[MockResponse],
        repeat_last: bool,
        streaming: bool,
        delay_ms: Option<u64>,
    ) {
        assert!(
            !responses.is_empty(),
            "{}: response sequence must not be empty",
            self.stage,
        );

        match &matcher {
            ContentMatcher::Any => {
                assert!(
                    !self.has_contains,
                    "{}: `respond()` (or `ContentMatcher::Any`) must be declared before \
                     content-specific matchers — otherwise the catch-all would shadow \
                     them in wiremock's LIFO priority",
                    self.stage,
                );
                assert!(
                    !self.has_any,
                    "{}: duplicate `ContentMatcher::Any` matcher",
                    self.stage,
                );
                self.has_any = true;
            }
            ContentMatcher::Contains(substring) => {
                let duplicate = self
                    .entries
                    .iter()
                    .any(|e| matches!(&e.matcher, ContentMatcher::Contains(s) if s == substring));
                assert!(
                    !duplicate,
                    "{}: duplicate content matcher for '{substring}'",
                    self.stage,
                );
                self.has_contains = true;
            }
        }

        self.entries.push(MountEntry {
            matcher,
            responses: responses.to_vec(),
            repeat_last,
            streaming,
            delay_ms,
        });
    }

    /// Mounts all configured entries on the wiremock server.
    ///
    /// # Panics
    ///
    /// Panics if no entries were configured.
    pub(crate) async fn mount(self) {
        assert!(
            !self.entries.is_empty(),
            "{}: at least one mock response must be configured",
            self.stage,
        );

        let endpoint = chat_path(self.provider);

        for entry in &self.entries {
            let wrapped: Vec<ResponseTemplate> = entry
                .responses
                .iter()
                .map(|r| self.to_template(r, entry.streaming, entry.delay_ms))
                .collect();

            // For repeat_last, mount a persistent fallback with the last response.
            if entry.repeat_last {
                let last = wrapped.last().expect("non-empty responses");
                let mock = self.build_when(endpoint, &entry.matcher);
                mock.respond_with(last.clone()).mount(self.server).await;
            }

            // Mount sequence entries in reverse order (LIFO) with up_to_n_times(1).
            // Skip the last entry if repeat_last — the persistent fallback covers it.
            let sequence = if entry.repeat_last && wrapped.len() > 1 {
                &wrapped[..wrapped.len() - 1]
            } else if entry.repeat_last {
                // Single response with repeat — persistent fallback handles everything.
                continue;
            } else {
                &wrapped[..]
            };

            for template in sequence.iter().rev() {
                let mock = self.build_when(endpoint, &entry.matcher);
                mock.respond_with(template.clone())
                    .up_to_n_times(1)
                    .mount(self.server)
                    .await;
            }
        }
    }

    fn to_template(
        &self,
        response: &MockResponse,
        streaming: bool,
        delay_ms: Option<u64>,
    ) -> ResponseTemplate {
        let template = match response {
            MockResponse::Fixture(v) if streaming => {
                let (body, content_type) = wrap_completion_stream(v, self.provider);
                ResponseTemplate::new(200).set_body_raw(body, content_type)
            }
            MockResponse::Fixture(v) => {
                let wrapped = wrap_completion(v, self.provider);
                ResponseTemplate::new(200).set_body_json(wrapped)
            }
            MockResponse::Error(status) => ResponseTemplate::new(*status),
        };
        match delay_ms {
            Some(ms) => template.set_delay(std::time::Duration::from_millis(ms)),
            None => template,
        }
    }

    fn build_when(&self, endpoint: &str, matcher: &ContentMatcher) -> MockBuilder {
        let base = Mock::given(method("POST")).and(path(endpoint));
        match matcher {
            ContentMatcher::Any => base,
            ContentMatcher::Contains(s) => base.and(body_string_contains(s.clone())),
        }
    }
}
