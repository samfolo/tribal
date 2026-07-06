//! The live-reload substrate for the subscriber's level filter.
//!
//! The subscriber initialises once; what changes at runtime is the
//! [`EnvFilter`] mounted behind a [`reload::Layer`], swapped through the
//! [`LogFilterHandle`] initialisation returns. Reloading touches only the
//! filter — never the subscriber, its outputs, or the one-shot init barrier.

use tracing_subscriber::{EnvFilter, Registry, reload};

use crate::error::TelemetryError;

/// A reloadable level filter: the layer to mount first in the subscriber
/// stack, and the handle that swaps its directive while the subscriber runs.
///
/// # Errors
///
/// Returns [`TelemetryError::InvalidFilterDirective`] when `directive` does
/// not parse.
pub fn reloadable_env_filter(
    directive: &str,
) -> Result<(reload::Layer<EnvFilter, Registry>, LogFilterHandle), TelemetryError> {
    let filter = parse_directive(directive)?;
    let (layer, handle) = reload::Layer::new(filter);
    Ok((layer, LogFilterHandle { handle }))
}

/// Parses a filter directive, mapping the failure to the crate error.
fn parse_directive(directive: &str) -> Result<EnvFilter, TelemetryError> {
    EnvFilter::try_new(directive).map_err(|source| TelemetryError::InvalidFilterDirective {
        directive: directive.to_owned(),
        source,
    })
}

/// A live handle to the subscriber's level filter, the `logging.level`
/// hot-reload substrate: reloading swaps the [`EnvFilter`] in place, so the
/// process filters at the new level without re-initialising.
#[derive(Clone, Debug)]
pub struct LogFilterHandle {
    handle: reload::Handle<EnvFilter, Registry>,
}

impl LogFilterHandle {
    /// Parses `directive` and makes it the active filter.
    ///
    /// # Errors
    ///
    /// Returns [`TelemetryError::InvalidFilterDirective`] when the directive
    /// does not parse — the previous filter stays in force — and
    /// [`TelemetryError::FilterReload`] when the subscriber owning the filter
    /// is gone.
    pub fn reload(&self, directive: &str) -> Result<(), TelemetryError> {
        let filter = parse_directive(directive)?;
        self.handle
            .reload(filter)
            .map_err(|source| TelemetryError::FilterReload { source })
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::broadcast;
    use tracing_subscriber::layer::SubscriberExt;

    use super::*;
    use crate::log_ring::{LogRing, LogRingLayer};

    /// A ring-capturing subscriber behind a reloadable filter, and the pieces
    /// the tests assert through.
    fn captured_stack(directive: &str) -> (impl tracing::Subscriber, LogFilterHandle, LogRing) {
        let (filter_layer, handle) =
            reloadable_env_filter(directive).expect("the directive parses");
        let (events, _) = broadcast::channel(1);
        let (log_layer, ring) = LogRingLayer::new(events);
        let subscriber = Registry::default().with(filter_layer).with(log_layer);
        (subscriber, handle, ring)
    }

    fn captured_messages(ring: &LogRing) -> Vec<String> {
        ring.tail(usize::MAX)
            .into_iter()
            .map(|line| line.message)
            .collect()
    }

    #[test]
    fn test_reload_swaps_the_active_filter() {
        let (subscriber, handle, ring) = captured_stack("info");

        tracing::subscriber::with_default(subscriber, || {
            tracing::debug!("below before");
            tracing::info!("above before");
            handle.reload("debug").expect("the reload succeeds");
            tracing::debug!("below after");
        });

        assert_eq!(
            captured_messages(&ring),
            vec!["above before", "below after"],
            "debug is filtered before the reload and admitted after",
        );
    }

    #[test]
    fn test_an_unparseable_directive_is_refused_and_the_old_filter_stands() {
        let (subscriber, handle, ring) = captured_stack("info");

        tracing::subscriber::with_default(subscriber, || {
            let error = handle.reload("not valid [[").expect_err("the parse fails");
            assert!(matches!(
                error,
                TelemetryError::InvalidFilterDirective { .. }
            ));
            tracing::debug!("still below");
            tracing::info!("still above");
        });

        assert_eq!(
            captured_messages(&ring),
            vec!["still above"],
            "the previous filter keeps filtering after a refused reload",
        );
    }

    #[test]
    fn test_a_reload_after_the_subscriber_is_gone_is_a_typed_error() {
        let (filter_layer, handle) = reloadable_env_filter("info").expect("the directive parses");
        drop(filter_layer);

        let error = handle.reload("debug").expect_err("the subscriber is gone");
        assert!(matches!(error, TelemetryError::FilterReload { .. }));
    }
}
