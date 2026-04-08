//! OTLP exporter setup for traces and metrics.
//!
//! Provides building blocks for constructing [`SdkTracerProvider`] and
//! [`SdkMeterProvider`] from [`TelemetryConfig`].  The trace exporter and
//! resource are exposed separately so the subscriber can compose them with
//! other exporters (console, file).

use opentelemetry::KeyValue;
use opentelemetry_otlp::{MetricExporter, SpanExporter, WithExportConfig};
use opentelemetry_sdk::{
    Resource,
    metrics::{PeriodicReader, SdkMeterProvider},
};
use opentelemetry_semantic_conventions::attribute::SERVICE_NAME;
use tribal_config::TelemetryConfig;

use crate::error::TelemetryError;

// ---------------------------------------------------------------------------
// Trace exporter
// ---------------------------------------------------------------------------

/// Builds an OTLP span exporter from the given configuration.
///
/// Returns the exporter without wrapping it in a provider — the caller
/// assembles the [`SdkTracerProvider`](opentelemetry_sdk::trace::SdkTracerProvider)
/// with this exporter alongside any other exporters (console, file).
///
/// # Errors
///
/// Returns [`TelemetryError::UnrecognisedOtlpProtocol`] if
/// `config.otlp_protocol` is not `"grpc"` or `"http"`.
/// Returns [`TelemetryError::OtlpTracePipelineInit`] if the exporter
/// fails to initialise.
pub(crate) fn build_otlp_span_exporter(
    config: &TelemetryConfig,
) -> Result<SpanExporter, TelemetryError> {
    let endpoint = config
        .otlp_endpoint
        .as_deref()
        .ok_or(TelemetryError::OtlpEndpointMissing)?;

    match config.otlp_protocol.as_str() {
        "grpc" => SpanExporter::builder()
            .with_tonic()
            .with_endpoint(endpoint)
            .build()
            .map_err(|source| TelemetryError::OtlpTracePipelineInit { source }),
        "http" => SpanExporter::builder()
            .with_http()
            .with_endpoint(endpoint)
            .build()
            .map_err(|source| TelemetryError::OtlpTracePipelineInit { source }),
        other => Err(TelemetryError::UnrecognisedOtlpProtocol {
            protocol: other.to_owned(),
        }),
    }
}

// ---------------------------------------------------------------------------
// Resource
// ---------------------------------------------------------------------------

/// Builds the shared OTLP resource with `service.name`.
pub(crate) fn build_resource(config: &TelemetryConfig) -> Resource {
    Resource::builder()
        .with_attribute(KeyValue::new(SERVICE_NAME, config.service_name.clone()))
        .build()
}

// ---------------------------------------------------------------------------
// Meter provider
// ---------------------------------------------------------------------------

/// Builds an OTLP metric exporter and wraps it in a [`SdkMeterProvider`].
///
/// The provider is returned without being installed as the global default.
///
/// # Errors
///
/// Returns [`TelemetryError::UnrecognisedOtlpProtocol`] if
/// `config.otlp_protocol` is not `"grpc"` or `"http"`.
/// Returns [`TelemetryError::MetricsPipelineInit`] if the exporter
/// or provider fails to initialise.
pub(crate) fn build_meter_provider(
    config: &TelemetryConfig,
) -> Result<SdkMeterProvider, TelemetryError> {
    let endpoint = config
        .otlp_endpoint
        .as_deref()
        .ok_or(TelemetryError::OtlpEndpointMissing)?;

    let exporter = match config.otlp_protocol.as_str() {
        "grpc" => MetricExporter::builder()
            .with_tonic()
            .with_endpoint(endpoint)
            .build()
            .map_err(|source| TelemetryError::MetricsPipelineInit { source })?,
        "http" => MetricExporter::builder()
            .with_http()
            .with_endpoint(endpoint)
            .build()
            .map_err(|source| TelemetryError::MetricsPipelineInit { source })?,
        other => {
            return Err(TelemetryError::UnrecognisedOtlpProtocol {
                protocol: other.to_owned(),
            });
        }
    };

    let reader = PeriodicReader::builder(exporter).build();

    let provider = SdkMeterProvider::builder()
        .with_resource(build_resource(config))
        .with_reader(reader)
        .build();

    Ok(provider)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unrecognised_protocol_returns_error() {
        let config = TelemetryConfig {
            otlp_endpoint: Some("http://localhost:4317".to_owned()),
            otlp_protocol: "quic".to_owned(),
            ..TelemetryConfig::default()
        };
        let result = build_otlp_span_exporter(&config);
        assert!(
            matches!(
                result,
                Err(TelemetryError::UnrecognisedOtlpProtocol { ref protocol }) if protocol == "quic"
            ),
            "expected UnrecognisedOtlpProtocol, got {result:?}",
        );
    }
}
