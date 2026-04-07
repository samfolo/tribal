//! Writer-backed span exporter.
//!
//! [`WriterSpanExporter`] serialises span data as OTLP JSON lines to any
//! [`std::io::Write`] target.  Used for console export (stderr) and file
//! export ([`RollingFileAppender`](tracing_appender::rolling::RollingFileAppender)).
//!
//! Span data is converted to the official OTLP proto [`Span`] type via the
//! [`From<SpanData>`] implementation in `opentelemetry-proto`, then serialised
//! with `serde_json`.  This ensures the output conforms to the OTLP JSON
//! specification without hand-rolled serialisation.
//!
//! [`Span`]: opentelemetry_proto::tonic::trace::v1::Span

use std::{
    fmt,
    io::Write,
    sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use opentelemetry_proto::tonic::trace::v1::Span;
use opentelemetry_sdk::{
    error::{OTelSdkError, OTelSdkResult},
    trace::{SpanData, SpanExporter},
};

// ---------------------------------------------------------------------------
// WriterSpanExporter
// ---------------------------------------------------------------------------

/// A span exporter that writes OTLP JSON lines to an arbitrary [`Write`]
/// target.
///
/// Each exported span is serialised as a single JSON object (the official
/// OTLP proto [`Span`] representation) followed by a newline.  The writer
/// is protected by a [`Mutex`] so the exporter is safe to share across
/// threads.
pub(crate) struct WriterSpanExporter<W: Write + Send> {
    writer: Mutex<W>,
    is_shutdown: AtomicBool,
}

impl<W: Write + Send> WriterSpanExporter<W> {
    /// Creates a new exporter writing to the given target.
    pub(crate) fn new(writer: W) -> Self {
        Self {
            writer: Mutex::new(writer),
            is_shutdown: AtomicBool::new(false),
        }
    }
}

// `RollingFileAppender` does not implement `Debug`, so derive is not
// possible for all writer types.  Print the struct name only.
impl<W: Write + Send> fmt::Debug for WriterSpanExporter<W> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WriterSpanExporter")
            .field("is_shutdown", &self.is_shutdown.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl<W: Write + Send + 'static> SpanExporter for WriterSpanExporter<W> {
    fn export(
        &self,
        batch: Vec<SpanData>,
    ) -> impl std::future::Future<Output = OTelSdkResult> + Send {
        // All work is synchronous — lock, serialise, flush.  Returning
        // `std::future::ready` keeps the `MutexGuard` out of the future's
        // state machine, satisfying the `Send` requirement.
        let result = self.export_sync(batch);
        std::future::ready(result)
    }

    fn shutdown_with_timeout(&mut self, _timeout: Duration) -> OTelSdkResult {
        self.is_shutdown.store(true, Ordering::SeqCst);
        if let Ok(mut w) = self.writer.lock() {
            let _ = w.flush();
        }
        Ok(())
    }

    fn force_flush(&mut self) -> OTelSdkResult {
        if let Ok(mut w) = self.writer.lock() {
            w.flush()
                .map_err(|e| OTelSdkError::InternalFailure(format!("flush failed: {e}")))?;
        }
        Ok(())
    }
}

impl<W: Write + Send> WriterSpanExporter<W> {
    fn export_sync(&self, batch: Vec<SpanData>) -> OTelSdkResult {
        if self.is_shutdown.load(Ordering::SeqCst) {
            return Err(OTelSdkError::AlreadyShutdown);
        }

        let mut writer = self
            .writer
            .lock()
            .map_err(|e| OTelSdkError::InternalFailure(format!("writer lock poisoned: {e}")))?;

        for span_data in batch {
            let span: Span = span_data.into();
            serde_json::to_writer(&mut *writer, &span)
                .map_err(|e| OTelSdkError::InternalFailure(format!("serialisation failed: {e}")))?;
            writer
                .write_all(b"\n")
                .map_err(|e| OTelSdkError::InternalFailure(format!("write failed: {e}")))?;
        }

        writer
            .flush()
            .map_err(|e| OTelSdkError::InternalFailure(format!("flush failed: {e}")))?;

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::time::SystemTime;

    use opentelemetry::InstrumentationScope;
    use opentelemetry::KeyValue;
    use opentelemetry::trace::{
        Event, Link, SpanContext, SpanId, SpanKind, Status, TraceFlags, TraceId, TraceState,
    };
    use opentelemetry_sdk::trace::{SpanEvents, SpanLinks};

    use super::*;

    fn test_span_data() -> SpanData {
        let trace_id =
            TraceId::from_hex("0af7651916cd43dd8448eb211c80319c").expect("valid trace id");
        let span_id = SpanId::from_hex("00f067aa0ba902b7").expect("valid span id");
        let parent_span_id = SpanId::from_hex("b7ad6b7169203331").expect("valid parent span id");

        let span_context = SpanContext::new(
            trace_id,
            span_id,
            TraceFlags::SAMPLED,
            false,
            TraceState::default(),
        );

        let mut events = SpanEvents::default();
        events.events.push(Event::new(
            "exception",
            SystemTime::UNIX_EPOCH,
            vec![KeyValue::new("exception.message", "something went wrong")],
            0,
        ));

        let mut links = SpanLinks::default();
        links.links.push(Link::new(
            SpanContext::new(
                TraceId::from_hex("aaaabbbbccccddddeeee111122223333").expect("valid"),
                SpanId::from_hex("1122334455667788").expect("valid"),
                TraceFlags::default(),
                false,
                TraceState::default(),
            ),
            Vec::new(),
            0,
        ));

        SpanData {
            span_context,
            parent_span_id,
            parent_span_is_remote: false,
            span_kind: SpanKind::Server,
            name: Cow::Borrowed("test-span"),
            start_time: SystemTime::UNIX_EPOCH,
            end_time: SystemTime::UNIX_EPOCH,
            attributes: vec![
                KeyValue::new("http.method", "GET"),
                KeyValue::new("http.status_code", 200),
            ],
            dropped_attributes_count: 0,
            events,
            links,
            status: Status::Error {
                description: "test error".into(),
            },
            instrumentation_scope: InstrumentationScope::builder("test").build(),
        }
    }

    #[test]
    fn test_export_produces_valid_otlp_json() {
        let mut buf = Vec::new();
        {
            let exporter = WriterSpanExporter::new(&mut buf as &mut Vec<u8>);
            exporter
                .export_sync(vec![test_span_data()])
                .expect("export should succeed");
        }

        let output = String::from_utf8(buf).expect("valid UTF-8");
        let parsed: serde_json::Value = serde_json::from_str(output.trim()).expect("valid JSON");

        // Trace and span IDs are hex-serialised by the proto serde support.
        assert_eq!(parsed["traceId"], "0af7651916cd43dd8448eb211c80319c");
        assert_eq!(parsed["spanId"], "00f067aa0ba902b7");
        assert_eq!(parsed["parentSpanId"], "b7ad6b7169203331");
        assert_eq!(parsed["name"], "test-span");
    }

    #[test]
    fn test_export_serialises_attributes() {
        let mut buf = Vec::new();
        {
            let exporter = WriterSpanExporter::new(&mut buf as &mut Vec<u8>);
            exporter
                .export_sync(vec![test_span_data()])
                .expect("export should succeed");
        }

        let output = String::from_utf8(buf).expect("valid UTF-8");
        let parsed: serde_json::Value = serde_json::from_str(output.trim()).expect("valid JSON");

        let attrs = parsed["attributes"]
            .as_array()
            .expect("attributes is an array");
        assert!(
            attrs.iter().any(|a| a["key"] == "http.method"),
            "expected http.method attribute in {attrs:?}",
        );
    }

    #[test]
    fn test_export_serialises_events() {
        let mut buf = Vec::new();
        {
            let exporter = WriterSpanExporter::new(&mut buf as &mut Vec<u8>);
            exporter
                .export_sync(vec![test_span_data()])
                .expect("export should succeed");
        }

        let output = String::from_utf8(buf).expect("valid UTF-8");
        let parsed: serde_json::Value = serde_json::from_str(output.trim()).expect("valid JSON");

        let events = parsed["events"].as_array().expect("events is an array");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["name"], "exception");
    }

    #[test]
    fn test_export_serialises_links() {
        let mut buf = Vec::new();
        {
            let exporter = WriterSpanExporter::new(&mut buf as &mut Vec<u8>);
            exporter
                .export_sync(vec![test_span_data()])
                .expect("export should succeed");
        }

        let output = String::from_utf8(buf).expect("valid UTF-8");
        let parsed: serde_json::Value = serde_json::from_str(output.trim()).expect("valid JSON");

        let links = parsed["links"].as_array().expect("links is an array");
        assert_eq!(links.len(), 1);
        assert_eq!(links[0]["traceId"], "aaaabbbbccccddddeeee111122223333");
        assert_eq!(links[0]["spanId"], "1122334455667788");
    }

    #[test]
    fn test_export_serialises_error_status() {
        let mut buf = Vec::new();
        {
            let exporter = WriterSpanExporter::new(&mut buf as &mut Vec<u8>);
            exporter
                .export_sync(vec![test_span_data()])
                .expect("export should succeed");
        }

        let output = String::from_utf8(buf).expect("valid UTF-8");
        let parsed: serde_json::Value = serde_json::from_str(output.trim()).expect("valid JSON");

        let status = &parsed["status"];
        assert_eq!(status["message"], "test error");
    }

    #[test]
    fn test_shutdown_prevents_further_export() {
        let buf = Vec::new();
        let mut exporter = WriterSpanExporter::new(buf);

        exporter.shutdown().expect("shutdown should succeed");

        let result = exporter.export_sync(vec![test_span_data()]);
        assert!(
            matches!(result, Err(OTelSdkError::AlreadyShutdown)),
            "export after shutdown should fail, got {result:?}",
        );
    }

    #[test]
    fn test_debug_impl() {
        let exporter = WriterSpanExporter::new(Vec::<u8>::new());
        let debug = format!("{exporter:?}");
        assert!(
            debug.contains("WriterSpanExporter"),
            "debug output should contain struct name: {debug}",
        );
    }
}
