//! SSE connection lifecycle enforcement layer.
//!
//! Wraps SSE response body streams with max-connection-age and
//! idle-timeout policies.  Non-SSE responses pass through unchanged.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll, ready},
    time::Duration,
};

use bytes::Bytes;
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
/// SSE field names are case-sensitive per the W3C EventSource
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
// Layer
// ---------------------------------------------------------------------------

/// Tower layer that wraps SSE responses with lifecycle policies.
#[derive(Debug, Clone)]
pub(super) struct SseLifecycleLayer {
    max_connection_age: Duration,
    idle_timeout: Duration,
}

impl SseLifecycleLayer {
    pub(super) fn new(max_connection_age: Duration, idle_timeout: Duration) -> Self {
        Self {
            max_connection_age,
            idle_timeout,
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
        }
    }
}

// ---------------------------------------------------------------------------
// Service
// ---------------------------------------------------------------------------

/// Tower service that wraps SSE response bodies with lifecycle enforcement.
#[derive(Debug, Clone)]
pub(super) struct SseLifecycleService<S> {
    inner: S,
    max_connection_age: Duration,
    idle_timeout: Duration,
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
        SseLifecycleFuture {
            inner: self.inner.call(req),
            max_connection_age: self.max_connection_age,
            idle_timeout: self.idle_timeout,
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

        let is_sse = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|ct| ct.starts_with(SSE_CONTENT_TYPE_PREFIX));

        let response = if is_sse {
            response.map(|body| {
                MaybeSseLifecycleBody::Sse(SseLifecycleBody::new(
                    body,
                    *this.max_connection_age,
                    *this.idle_timeout,
                ))
            })
        } else {
            response.map(MaybeSseLifecycleBody::Passthrough)
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
    pub(super) struct SseLifecycleBody<B> {
        #[pin]
        inner: B,
        #[pin]
        max_age_deadline: Sleep,
        #[pin]
        idle_deadline: Sleep,
        idle_timeout: Duration,
        closed: bool,
    }
}

impl<B> SseLifecycleBody<B> {
    fn new(inner: B, max_connection_age: Duration, idle_timeout: Duration) -> Self {
        let now = Instant::now();
        Self {
            inner,
            max_age_deadline: tokio::time::sleep_until(now + max_connection_age),
            idle_deadline: tokio::time::sleep_until(now + idle_timeout),
            idle_timeout,
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
        let this = self.project();

        if *this.closed {
            return Poll::Ready(None);
        }

        // Check max connection age deadline.
        if this.max_age_deadline.poll(cx).is_ready() {
            tracing::info!(
                closure_reason = CLOSURE_REASON_MAX_AGE,
                "SSE connection closed",
            );
            *this.closed = true;
            return Poll::Ready(None);
        }

        // Check idle timeout deadline.
        if this.idle_deadline.poll(cx).is_ready() {
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
                if let Some(data) = frame.data_ref() {
                    if is_real_event(data) {
                        this.idle_deadline
                            .reset(Instant::now() + *this.idle_timeout);
                    }
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
/// EventSource specification (§9.2), so byte-level prefix matching is
/// the correct approach — no case folding is needed.
///
/// Comment lines (starting with `:`) and empty lines are not real events.
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

    // -- SseLifecycleBody deadlines -----------------------------------------

    #[tokio::test(start_paused = true)]
    async fn test_body_closes_on_max_age() {
        let body = MockBody::from_frames(vec![]);
        let mut body = Box::pin(SseLifecycleBody::new(body, TEST_DEADLINE, FAR_FUTURE));

        tokio::time::advance(JUST_AFTER_DEADLINE).await;

        let frame = std::future::poll_fn(|cx| Pin::as_mut(&mut body).poll_frame(cx)).await;
        assert!(frame.is_none(), "stream should be closed after max age");
    }

    #[tokio::test(start_paused = true)]
    async fn test_body_closes_on_idle_timeout() {
        let body = MockBody::from_frames(vec![]);
        let mut body = Box::pin(SseLifecycleBody::new(body, FAR_FUTURE, TEST_DEADLINE));

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
        let mut body = Box::pin(SseLifecycleBody::new(body, FAR_FUTURE, TEST_DEADLINE));

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
        let mut body = Box::pin(SseLifecycleBody::new(body, FAR_FUTURE, TEST_DEADLINE));

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
