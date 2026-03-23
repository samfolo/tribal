//! SSE connection lifecycle enforcement layer.
//!
//! Wraps SSE response body streams with max-connection-age and
//! idle-timeout policies.  Non-SSE responses pass through unchanged.

use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll, ready},
    time::Duration,
};

use bytes::Bytes;
use dashmap::DashMap;
use http::header::CONTENT_TYPE;
use http_body::Frame;
use pin_project_lite::pin_project;
use tokio::time::{Instant, Sleep};
use tower::{Layer, Service};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// SSE content-type prefix used to identify SSE responses.
const SSE_CONTENT_TYPE_PREFIX: &str = "text/event-stream";

/// SSE data field prefix.  Lines starting with this carry event payload.
///
/// SSE field names are case-sensitive per the W3C `EventSource`
/// specification (§9.2 "Interpreting an event stream").  No
/// case-insensitive comparison is needed.
const SSE_DATA_PREFIX: &[u8] = b"data:";

/// SSE event-type field prefix.  Lines starting with this set the event
/// type for the subsequent data.
const SSE_EVENT_PREFIX: &[u8] = b"event:";

/// Closure reason logged when max connection age is exceeded.
const CLOSURE_REASON_MAX_AGE: &str = "max_connection_age";

/// Closure reason logged when idle timeout is exceeded.
const CLOSURE_REASON_IDLE: &str = "idle_timeout";

// ---------------------------------------------------------------------------
// Activity tracker
// ---------------------------------------------------------------------------

/// Per-session counter that tracks inbound request activity.
///
/// The SSE lifecycle service increments this on every inbound request
/// that carries the session's `Mcp-Session-Id` header.  The SSE
/// lifecycle body checks it in `poll_frame` and resets the idle
/// deadline when new activity is detected.  This ensures inbound
/// client requests (not just outbound SSE events) prevent the idle
/// timeout from firing.
#[derive(Clone)]
pub(super) struct ActivityTracker(Arc<AtomicU64>);

impl ActivityTracker {
    pub(super) fn new() -> Self {
        Self(Arc::new(AtomicU64::new(0)))
    }

    /// Records inbound activity.  Called by the service on every
    /// inbound request to this session.
    fn record(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }

    /// Returns the current activity count.
    fn count(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// Per-session activity tracker registry.
///
/// Maps MCP session IDs to their activity trackers.  Entries are
/// created when a session is established (initialize response) and
/// looked up on subsequent requests carrying `Mcp-Session-Id`.
/// Entries are removed when the SSE lifecycle body closes (deadline
/// expiry or graceful shutdown).
type SessionRegistry = Arc<DashMap<String, ActivityTracker>>;

/// MCP session ID header name.
const MCP_SESSION_ID_HEADER: &str = "mcp-session-id";

// ---------------------------------------------------------------------------
// Layer
// ---------------------------------------------------------------------------

/// Tower layer that wraps SSE responses with lifecycle policies.
#[derive(Clone)]
pub(super) struct SseLifecycleLayer {
    max_connection_age: Duration,
    idle_timeout: Duration,
    sessions: SessionRegistry,
}

impl SseLifecycleLayer {
    pub(super) fn new(max_connection_age: Duration, idle_timeout: Duration) -> Self {
        Self {
            max_connection_age,
            idle_timeout,
            sessions: Arc::new(DashMap::new()),
        }
    }
}

impl<S> Layer<S> for SseLifecycleLayer {
    type Service = SseLifecycleService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        SseLifecycleService {
            inner,
            max_connection_age: self.max_connection_age,
            idle_timeout: self.idle_timeout,
            sessions: Arc::clone(&self.sessions),
        }
    }
}

// ---------------------------------------------------------------------------
// Service
// ---------------------------------------------------------------------------

/// Tower service that wraps SSE response bodies with lifecycle enforcement.
#[derive(Clone)]
pub(super) struct SseLifecycleService<S> {
    inner: S,
    max_connection_age: Duration,
    idle_timeout: Duration,
    sessions: SessionRegistry,
}

impl<S, ReqBody, ResBody> Service<http::Request<ReqBody>> for SseLifecycleService<S>
where
    S: Service<http::Request<ReqBody>, Response = http::Response<ResBody>>,
    ResBody: http_body::Body<Data = Bytes>,
    S::Future: Send,
{
    type Response = http::Response<MaybeSseLifecycleBody<ResBody>>;
    type Error = S::Error;
    type Future = SseLifecycleFuture<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<ReqBody>) -> Self::Future {
        // Look up the session's tracker if this request carries a
        // session ID (all requests after initialize).  For the initial
        // request, create a fresh tracker — the future will register
        // it once the response reveals the new session ID.
        let session_id = req
            .headers()
            .get(MCP_SESSION_ID_HEADER)
            .and_then(|v| v.to_str().ok())
            .map(String::from);

        let tracker = session_id
            .as_ref()
            .and_then(|id| self.sessions.get(id).map(|entry| entry.value().clone()))
            .unwrap_or_else(ActivityTracker::new);

        tracker.record();

        SseLifecycleFuture {
            inner: self.inner.call(req),
            max_connection_age: self.max_connection_age,
            idle_timeout: self.idle_timeout,
            tracker,
            request_session_id: session_id,
            sessions: Arc::clone(&self.sessions),
        }
    }
}

// ---------------------------------------------------------------------------
// Future
// ---------------------------------------------------------------------------

pin_project! {
    /// Future that resolves the inner service and conditionally wraps the
    /// response body.
    pub(super) struct SseLifecycleFuture<F> {
        #[pin]
        inner: F,
        max_connection_age: Duration,
        idle_timeout: Duration,
        tracker: ActivityTracker,
        request_session_id: Option<String>,
        sessions: SessionRegistry,
    }
}

impl<F, ResBody, E> Future for SseLifecycleFuture<F>
where
    F: Future<Output = Result<http::Response<ResBody>, E>>,
    ResBody: http_body::Body<Data = Bytes>,
{
    type Output = Result<http::Response<MaybeSseLifecycleBody<ResBody>>, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let response = ready!(this.inner.poll(cx))?;

        // Resolve the session ID: either from the inbound request or,
        // for initialize, from the response header.  Register the
        // tracker so subsequent requests to this session find it.
        let session_id = this.request_session_id.take().or_else(|| {
            response
                .headers()
                .get(MCP_SESSION_ID_HEADER)
                .and_then(|v| v.to_str().ok())
                .map(String::from)
        });

        if let Some(id) = &session_id {
            // Insert is idempotent — existing sessions get the same
            // tracker back; new sessions (initialize) are registered.
            this.sessions
                .entry(id.clone())
                .or_insert_with(|| this.tracker.clone());
        }

        let is_sse = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|ct| ct.starts_with(SSE_CONTENT_TYPE_PREFIX));

        let response = if is_sse {
            response.map(|body| MaybeSseLifecycleBody::Sse {
                inner: SseLifecycleBody::new(
                    body,
                    *this.max_connection_age,
                    *this.idle_timeout,
                    this.tracker.clone(),
                    session_id,
                    Arc::clone(this.sessions),
                ),
            })
        } else {
            response.map(|inner| MaybeSseLifecycleBody::Passthrough { inner })
        };

        Poll::Ready(Ok(response))
    }
}

// ---------------------------------------------------------------------------
// Body wrapper (enum dispatch)
// ---------------------------------------------------------------------------

pin_project! {
    /// Response body that is either lifecycle-wrapped (SSE) or passed through
    /// unchanged (non-SSE).
    #[project = MaybeSseLifecycleBodyProj]
    pub(super) enum MaybeSseLifecycleBody<B> {
        Sse { #[pin] inner: SseLifecycleBody<B> },
        Passthrough { #[pin] inner: B },
    }
}

impl<B: http_body::Body<Data = Bytes>> http_body::Body for MaybeSseLifecycleBody<B> {
    type Data = Bytes;
    type Error = B::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        match self.project() {
            MaybeSseLifecycleBodyProj::Sse { inner } => inner.poll_frame(cx),
            MaybeSseLifecycleBodyProj::Passthrough { inner } => inner.poll_frame(cx),
        }
    }

    fn is_end_stream(&self) -> bool {
        match self {
            Self::Sse { inner } => inner.is_end_stream(),
            Self::Passthrough { inner } => inner.is_end_stream(),
        }
    }
}

// ---------------------------------------------------------------------------
// SSE lifecycle body
// ---------------------------------------------------------------------------

pin_project! {
    /// Body wrapper that enforces max connection age and idle timeout on an
    /// SSE stream.
    ///
    /// The idle deadline resets on both outbound real events (detected via
    /// frame content) and inbound client requests (detected via the shared
    /// [`ActivityTracker`]).
    ///
    /// Removes its entry from the session registry on drop, guaranteeing
    /// cleanup regardless of how the body closes (deadline expiry, inner
    /// stream end, error, or abrupt disconnect).
    pub(super) struct SseLifecycleBody<B> {
        #[pin]
        inner: B,
        #[pin]
        max_age_deadline: Sleep,
        #[pin]
        idle_deadline: Sleep,
        idle_timeout: Duration,
        activity_tracker: ActivityTracker,
        last_seen_activity: u64,
        session_id: Option<String>,
        sessions: SessionRegistry,
        closed: bool,
    }

    impl<B> PinnedDrop for SseLifecycleBody<B> {
        fn drop(this: Pin<&mut Self>) {
            let this = this.project();
            if let Some(id) = this.session_id {
                this.sessions.remove(id.as_str());
            }
        }
    }
}

impl<B> SseLifecycleBody<B> {
    fn new(
        inner: B,
        max_connection_age: Duration,
        idle_timeout: Duration,
        activity_tracker: ActivityTracker,
        session_id: Option<String>,
        sessions: SessionRegistry,
    ) -> Self {
        let now = Instant::now();
        let last_seen_activity = activity_tracker.count();
        Self {
            inner,
            max_age_deadline: tokio::time::sleep_until(now + max_connection_age),
            idle_deadline: tokio::time::sleep_until(now + idle_timeout),
            idle_timeout,
            activity_tracker,
            last_seen_activity,
            session_id,
            sessions,
            closed: false,
        }
    }
}

impl<B: http_body::Body<Data = Bytes>> http_body::Body for SseLifecycleBody<B> {
    type Data = Bytes;
    type Error = B::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let mut this = self.project();

        if *this.closed {
            return Poll::Ready(None);
        }

        // Check max connection age deadline.
        if this.max_age_deadline.as_mut().poll(cx).is_ready() {
            tracing::info!(
                closure_reason = CLOSURE_REASON_MAX_AGE,
                "SSE connection closed",
            );
            *this.closed = true;
            return Poll::Ready(None);
        }

        // Reset idle deadline if inbound requests arrived since the
        // last check.  This ensures client-to-server activity (POST
        // requests routed through the session) prevents the idle
        // timeout from firing — not just server-to-client events.
        let current_activity = this.activity_tracker.count();
        if current_activity != *this.last_seen_activity {
            *this.last_seen_activity = current_activity;
            this.idle_deadline
                .as_mut()
                .reset(Instant::now() + *this.idle_timeout);
        }

        // Check idle timeout deadline.
        if this.idle_deadline.as_mut().poll(cx).is_ready() {
            tracing::info!(
                closure_reason = CLOSURE_REASON_IDLE,
                "SSE connection closed",
            );
            *this.closed = true;
            return Poll::Ready(None);
        }

        // Poll the inner body for the next frame.
        match ready!(this.inner.poll_frame(cx)) {
            Some(Ok(frame)) => {
                if let Some(data) = frame.data_ref()
                    && is_real_event(data)
                {
                    this.idle_deadline
                        .as_mut()
                        .reset(Instant::now() + *this.idle_timeout);
                }
                Poll::Ready(Some(Ok(frame)))
            }
            other => Poll::Ready(other),
        }
    }

    fn is_end_stream(&self) -> bool {
        self.closed || self.inner.is_end_stream()
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Returns `true` if the frame bytes represent a real SSE event (not just a
/// keepalive comment).
///
/// Splits the frame into lines and checks whether any line starts with
/// `data:` or `event:`.  SSE field names are case-sensitive per the W3C
/// `EventSource` specification (§9.2), so byte-level prefix matching is
/// the correct approach — no case folding is needed.
///
/// Comment lines (starting with `:`) and empty lines are not real events.
///
/// **Frame-boundary caveat**: this inspects each HTTP body frame
/// independently.  If an SSE event is split across frames such that the
/// `data:` prefix spans the boundary, the match fails and the idle
/// timer is not reset for that event.  In practice rmcp writes complete
/// events as single frames, so splitting only occurs if an intermediary
/// re-chunks the stream.  The next complete frame would still reset
/// the timer, so the worst case is a single missed reset — not a
/// spurious close.
fn is_real_event(data: &Bytes) -> bool {
    data.split(|&b| b == b'\n')
        .any(|line| line.starts_with(SSE_DATA_PREFIX) || line.starts_with(SSE_EVENT_PREFIX))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::{collections::VecDeque, convert::Infallible};

    use http_body::Body;

    use super::*;

    // -- Test constants -----------------------------------------------------

    /// Short lifecycle deadline for tests that verify deadline expiry.
    ///
    /// Paired with `start_paused = true` so elapsed time is simulated.
    /// The exact value is arbitrary — only the relative ordering with
    /// `JUST_BEFORE_DEADLINE` and `JUST_AFTER_DEADLINE` matters.
    const TEST_DEADLINE: Duration = Duration::from_millis(200);

    /// Duration that falls just before `TEST_DEADLINE`, used to advance
    /// time to a point where the deadline has NOT yet fired.
    const JUST_BEFORE_DEADLINE: Duration = Duration::from_millis(150);

    /// Duration that pushes past `TEST_DEADLINE` from the start, used
    /// to advance time beyond the deadline boundary.
    const JUST_AFTER_DEADLINE: Duration = Duration::from_millis(201);

    /// Duration that, when added after `JUST_BEFORE_DEADLINE`, would
    /// exceed the original `TEST_DEADLINE` but NOT a reset deadline.
    /// Used to verify that an idle-timeout reset actually took effect.
    const PAST_ORIGINAL_BUT_WITHIN_RESET: Duration = Duration::from_millis(150);

    /// Duration that, when added after `JUST_BEFORE_DEADLINE`, exceeds
    /// the original `TEST_DEADLINE`.  Used to verify that a keepalive
    /// comment did NOT reset the idle timer.
    const PAST_ORIGINAL_DEADLINE: Duration = Duration::from_millis(60);

    /// Long deadline used as the "other" timeout when only one deadline
    /// is under test, ensuring it never fires.
    const FAR_FUTURE: Duration = Duration::from_secs(60);

    // -- Mock body ----------------------------------------------------------

    /// Minimal body that yields frames from a queue.
    struct MockBody {
        frames: VecDeque<Frame<Bytes>>,
    }

    impl MockBody {
        fn from_frames(frames: Vec<Frame<Bytes>>) -> Self {
            Self {
                frames: frames.into(),
            }
        }
    }

    impl http_body::Body for MockBody {
        type Data = Bytes;
        type Error = Infallible;

        fn poll_frame(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            Poll::Ready(self.frames.pop_front().map(Ok))
        }
    }

    // -- is_real_event ------------------------------------------------------

    #[test]
    fn test_is_real_event_detects_data_line() {
        assert!(is_real_event(&Bytes::from_static(
            b"data: {\"jsonrpc\":\"2.0\"}\n\n"
        )));
    }

    #[test]
    fn test_is_real_event_detects_event_line() {
        assert!(is_real_event(&Bytes::from_static(
            b"event: message\ndata: hello\n\n"
        )));
    }

    #[test]
    fn test_is_real_event_rejects_comment() {
        assert!(!is_real_event(&Bytes::from_static(b":keepalive\n\n")));
    }

    #[test]
    fn test_is_real_event_rejects_comment_containing_data_text() {
        assert!(!is_real_event(&Bytes::from_static(
            b": data: debug info\n\n"
        )));
    }

    #[test]
    fn test_is_real_event_rejects_empty_frame() {
        assert!(!is_real_event(&Bytes::from_static(b"\n\n")));
    }

    // -- Helpers ------------------------------------------------------------

    /// Creates a lifecycle body with an empty session registry.
    ///
    /// Body-level tests exercise deadline and event-detection logic
    /// in isolation — session eviction is not under test here.
    fn test_body(
        inner: MockBody,
        max_connection_age: Duration,
        idle_timeout: Duration,
        tracker: ActivityTracker,
    ) -> SseLifecycleBody<MockBody> {
        SseLifecycleBody::new(
            inner,
            max_connection_age,
            idle_timeout,
            tracker,
            None,
            Arc::new(DashMap::new()),
        )
    }

    // -- SseLifecycleBody deadlines -----------------------------------------

    #[tokio::test(start_paused = true)]
    async fn test_body_closes_on_max_age() {
        let body = MockBody::from_frames(vec![]);
        let mut body = Box::pin(test_body(
            body,
            TEST_DEADLINE,
            FAR_FUTURE,
            ActivityTracker::new(),
        ));

        tokio::time::advance(JUST_AFTER_DEADLINE).await;

        let frame = std::future::poll_fn(|cx| Pin::as_mut(&mut body).poll_frame(cx)).await;
        assert!(frame.is_none(), "stream should be closed after max age");
    }

    #[tokio::test(start_paused = true)]
    async fn test_body_closes_on_idle_timeout() {
        let body = MockBody::from_frames(vec![]);
        let mut body = Box::pin(test_body(
            body,
            FAR_FUTURE,
            TEST_DEADLINE,
            ActivityTracker::new(),
        ));

        tokio::time::advance(JUST_AFTER_DEADLINE).await;

        let frame = std::future::poll_fn(|cx| Pin::as_mut(&mut body).poll_frame(cx)).await;
        assert!(
            frame.is_none(),
            "stream should be closed after idle timeout",
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_body_resets_idle_on_data_event() {
        let frames = vec![Frame::data(Bytes::from_static(b"data: hello\n\n"))];
        let body = MockBody::from_frames(frames);
        let mut body = Box::pin(test_body(
            body,
            FAR_FUTURE,
            TEST_DEADLINE,
            ActivityTracker::new(),
        ));

        // Advance to just before the idle deadline, then receive a data
        // frame — this resets the idle timer.
        tokio::time::advance(JUST_BEFORE_DEADLINE).await;
        let frame = std::future::poll_fn(|cx| Pin::as_mut(&mut body).poll_frame(cx)).await;
        assert!(frame.is_some(), "data frame should be yielded");

        // Advance past the original deadline.  The reset should keep the
        // stream alive — the inner body is exhausted (returns None) but
        // the lifecycle layer did not close it.
        tokio::time::advance(PAST_ORIGINAL_BUT_WITHIN_RESET).await;
        let frame = std::future::poll_fn(|cx| Pin::as_mut(&mut body).poll_frame(cx)).await;
        assert!(frame.is_none(), "inner body exhausted");
        assert!(
            !body.closed,
            "lifecycle layer should not have closed the stream",
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_body_does_not_reset_idle_on_comment() {
        let frames = vec![Frame::data(Bytes::from_static(b":keepalive\n\n"))];
        let body = MockBody::from_frames(frames);
        let mut body = Box::pin(test_body(
            body,
            FAR_FUTURE,
            TEST_DEADLINE,
            ActivityTracker::new(),
        ));

        // Advance to just before the idle deadline, receive a comment
        // frame — this should NOT reset the idle timer.
        tokio::time::advance(JUST_BEFORE_DEADLINE).await;
        let frame = std::future::poll_fn(|cx| Pin::as_mut(&mut body).poll_frame(cx)).await;
        assert!(frame.is_some(), "comment frame should be yielded");

        // A small advance past the original deadline — the comment did
        // not reset the timer, so idle timeout fires.
        tokio::time::advance(PAST_ORIGINAL_DEADLINE).await;
        let frame = std::future::poll_fn(|cx| Pin::as_mut(&mut body).poll_frame(cx)).await;
        assert!(
            frame.is_none(),
            "stream should be closed after idle timeout"
        );
        assert!(body.closed, "body should be closed by idle timeout");
    }

    #[tokio::test(start_paused = true)]
    async fn test_body_resets_idle_on_inbound_activity() {
        let body = MockBody::from_frames(vec![]);
        let tracker = ActivityTracker::new();
        let mut body = Box::pin(test_body(body, FAR_FUTURE, TEST_DEADLINE, tracker.clone()));

        // Advance to just before the idle deadline, then simulate an
        // inbound request by recording activity on the tracker.
        tokio::time::advance(JUST_BEFORE_DEADLINE).await;
        tracker.record();

        // Poll to let the body observe the tracker update and reset
        // the idle deadline.
        let frame = std::future::poll_fn(|cx| Pin::as_mut(&mut body).poll_frame(cx)).await;
        assert!(frame.is_none(), "inner body exhausted");

        // Advance past the original deadline.  The tracker reset
        // should keep the stream alive.
        tokio::time::advance(PAST_ORIGINAL_BUT_WITHIN_RESET).await;
        let frame = std::future::poll_fn(|cx| Pin::as_mut(&mut body).poll_frame(cx)).await;
        assert!(frame.is_none(), "inner body still exhausted");
        assert!(
            !body.closed,
            "lifecycle layer should not have closed the stream",
        );
    }

    // -- MaybeSseLifecycleBody dispatch -------------------------------------

    #[tokio::test]
    async fn test_non_sse_response_passes_through() {
        let layer = SseLifecycleLayer::new(FAR_FUTURE, FAR_FUTURE);
        let mut service = layer.layer(tower::service_fn(move |_req: http::Request<()>| {
            let resp = http::Response::builder()
                .header(CONTENT_TYPE, "application/json")
                .body(MockBody::from_frames(vec![Frame::data(
                    Bytes::from_static(b"{}"),
                )]))
                .unwrap();
            std::future::ready(Ok::<_, Infallible>(resp))
        }));

        let req = http::Request::new(());
        let response = service.call(req).await.unwrap();

        assert!(
            matches!(response.body(), MaybeSseLifecycleBody::Passthrough { .. }),
            "non-SSE response should use passthrough variant",
        );
    }
}
