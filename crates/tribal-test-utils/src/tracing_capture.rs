//! A thread-local capturing `tracing` subscriber for asserting that spans
//! and events fire.
//!
//! Production code records observability through `tracing`; these tests
//! install a subscriber over the current thread, run the instrumented code,
//! and read back the spans (with their fields and contextual parent) and
//! events. The capture is the registry plus one layer, so span parenting
//! resolves the same way `tracing-opentelemetry` would see it.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, PoisonError},
};

use tracing::{
    Event, Subscriber,
    field::{Field, Visit},
    span,
    subscriber::DefaultGuard,
};
use tracing_subscriber::{
    Layer,
    layer::{Context, SubscriberExt},
    registry::LookupSpan,
};

/// One captured span: its name, the name of the span it was created under,
/// and its fields (including any recorded after creation).
#[derive(Clone, Debug)]
pub struct CapturedSpan {
    /// The static span name.
    pub name: String,
    /// The contextually enclosing span's name, when one was entered.
    pub parent: Option<String>,
    /// The span's fields, rendered to strings.
    pub fields: HashMap<String, String>,
}

impl CapturedSpan {
    /// The string value of one field, when present.
    #[must_use]
    pub fn field(&self, key: &str) -> Option<&str> {
        self.fields.get(key).map(String::as_str)
    }
}

/// One captured event: its message and its fields.
#[derive(Clone, Debug)]
pub struct CapturedEvent {
    /// The event's message (the `message` field).
    pub message: String,
    /// The event's other fields, rendered to strings.
    pub fields: HashMap<String, String>,
}

impl CapturedEvent {
    /// The string value of one field, when present.
    #[must_use]
    pub fn field(&self, key: &str) -> Option<&str> {
        self.fields.get(key).map(String::as_str)
    }
}

#[derive(Default)]
struct Store {
    spans: Vec<CapturedSpan>,
    /// Span id (as `u64`) to its index in `spans`, so a later field
    /// recording merges into the right span.
    index: HashMap<u64, usize>,
    events: Vec<CapturedEvent>,
}

/// A handle to the captured spans and events of one installed subscriber.
pub struct TracingCapture {
    store: Arc<Mutex<Store>>,
}

impl TracingCapture {
    /// Installs the capture as the current thread's default subscriber for
    /// the returned guard's lifetime, and returns the handle to query.
    #[must_use]
    pub fn install() -> (Self, DefaultGuard) {
        let store = Arc::new(Mutex::new(Store::default()));
        let layer = CaptureLayer {
            store: Arc::clone(&store),
        };
        let subscriber = tracing_subscriber::registry().with(layer);
        let guard = tracing::subscriber::set_default(subscriber);
        (Self { store }, guard)
    }

    /// Every captured span, in creation order.
    #[must_use]
    pub fn spans(&self) -> Vec<CapturedSpan> {
        self.store
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .spans
            .clone()
    }

    /// Every captured event, in emission order.
    #[must_use]
    pub fn events(&self) -> Vec<CapturedEvent> {
        self.store
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .events
            .clone()
    }

    /// The first captured span with the given name.
    #[must_use]
    pub fn span(&self, name: &str) -> Option<CapturedSpan> {
        self.spans().into_iter().find(|span| span.name == name)
    }

    /// Every captured span with the given name.
    #[must_use]
    pub fn spans_named(&self, name: &str) -> Vec<CapturedSpan> {
        self.spans()
            .into_iter()
            .filter(|span| span.name == name)
            .collect()
    }

    /// The first captured event whose message equals `message`.
    #[must_use]
    pub fn event(&self, message: &str) -> Option<CapturedEvent> {
        self.events()
            .into_iter()
            .find(|event| event.message == message)
    }
}

struct CaptureLayer {
    store: Arc<Mutex<Store>>,
}

impl<S> Layer<S> for CaptureLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &span::Attributes<'_>, id: &span::Id, ctx: Context<'_, S>) {
        let mut visitor = FieldVisitor::default();
        attrs.record(&mut visitor);
        let parent = ctx
            .current_span()
            .id()
            .and_then(|parent_id| ctx.span(parent_id))
            .map(|parent| parent.name().to_owned());
        let captured = CapturedSpan {
            name: attrs.metadata().name().to_owned(),
            parent,
            fields: visitor.fields,
        };
        let mut store = self.store.lock().unwrap_or_else(PoisonError::into_inner);
        let position = store.spans.len();
        store.spans.push(captured);
        store.index.insert(id.into_u64(), position);
    }

    fn on_record(&self, id: &span::Id, values: &span::Record<'_>, _ctx: Context<'_, S>) {
        let mut visitor = FieldVisitor::default();
        values.record(&mut visitor);
        let mut store = self.store.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(&position) = store.index.get(&id.into_u64()) {
            store.spans[position].fields.extend(visitor.fields);
        }
    }

    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let mut visitor = FieldVisitor::default();
        event.record(&mut visitor);
        let message = visitor.fields.remove("message").unwrap_or_default();
        self.store
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .events
            .push(CapturedEvent {
                message,
                fields: visitor.fields,
            });
    }
}

/// Renders every field value to a string, so `%Display`, `?Debug`, and the
/// primitive recordings all read back uniformly.
#[derive(Default)]
struct FieldVisitor {
    fields: HashMap<String, String>,
}

impl Visit for FieldVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.fields
            .insert(field.name().to_owned(), format!("{value:?}"));
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.fields
            .insert(field.name().to_owned(), value.to_owned());
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.fields
            .insert(field.name().to_owned(), value.to_string());
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.fields
            .insert(field.name().to_owned(), value.to_string());
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.fields
            .insert(field.name().to_owned(), value.to_string());
    }

    fn record_f64(&mut self, field: &Field, value: f64) {
        self.fields
            .insert(field.name().to_owned(), value.to_string());
    }
}
