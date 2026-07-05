//! The managed-platform gateway clients: the inference bracket and the
//! job-plane metering endpoints.

mod inference;
mod metering;

pub use inference::PlatformInferenceProvider;
pub use metering::GatewayMeteringClient;
#[cfg(feature = "test-helpers")]
pub use metering::{ACK_PATH, HOLDS_PATH};
