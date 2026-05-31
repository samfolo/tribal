//! Embedding profile — one activation of one embedding geometry.
//!
//! `embedding_profiles` is an append-only, epoch-ordered activation log. The
//! active profile is the highest-epoch `complete` row, derived at read time and
//! never stored as a mutable pointer. Identity is not unique; epoch is.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;

use crate::{EmbeddingProfileId, ProviderKind};

/// Lifecycle state of an embedding profile.
///
/// `building → complete` on a successful reindex (the profile becomes active),
/// `building → failed` when a run exhausts retries, and `complete`/`failed →
/// superseded` on prune. Stored as `TEXT`; the string form is the single source
/// of truth for the database `CHECK`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EmbeddingProfileState {
    /// The profile's index is being built; it is never active.
    Building,
    /// The build completed; the highest-epoch such profile is active.
    Complete,
    /// The build was abandoned (retry exhaustion or cancellation).
    Failed,
    /// Retired by prune; its rows are reclaimable.
    Superseded,
}

enum_text_conversions!(EmbeddingProfileState {
    EmbeddingProfileState::Building => "building",
    EmbeddingProfileState::Complete => "complete",
    EmbeddingProfileState::Failed => "failed",
    EmbeddingProfileState::Superseded => "superseded",
});

/// The vector distance metric a profile's index is built for.
///
/// Part of a profile's declared identity. Cosine is the only supported metric;
/// the enum keeps the database `CHECK` honest and leaves room for L2 or inner
/// product later.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DistanceMetric {
    /// Cosine distance (`halfvec_cosine_ops`).
    #[default]
    Cosine,
}

enum_text_conversions!(DistanceMetric {
    DistanceMetric::Cosine => "cosine",
});

/// One activation of one embedding geometry.
///
/// The declared identity `(provider_kind, normalised_base_url, model,
/// dimensions, distance_metric)` plus the drift signals (`revision_token`,
/// `probe_digest`) drive the no-op-versus-rebuild decision. `completed_at` is
/// set on the transition out of `building` and preserved thereafter.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TypedBuilder)]
pub struct EmbeddingProfile {
    /// Unique identifier with `eprof_` prefix.
    id: EmbeddingProfileId,
    /// Sequence-backed activation order; the only structural unique key.
    epoch: i64,
    /// The provider that produced this geometry.
    provider_kind: ProviderKind,
    /// The provider endpoint, normalised for credential and registry lookup.
    normalised_base_url: String,
    /// The embedding model name.
    model: String,
    /// Vector dimension (`1..=4000`).
    dimensions: u32,
    /// The distance metric the index is built for.
    distance_metric: DistanceMetric,
    /// A provider-exposed revision signal (digest or dated snapshot), empty
    /// when the provider exposes none.
    revision_token: String,
    /// A quantised-then-hashed probe digest, the cross-provider drift backstop.
    probe_digest: Option<String>,
    /// Lifecycle state.
    state: EmbeddingProfileState,
    /// Stable non-unique handle over the identity tuple, for logs and the CLI.
    fingerprint_hash: String,
    /// When the activation row was created.
    created_at: DateTime<Utc>,
    /// When the profile left `building`; `None` only while `building`.
    completed_at: Option<DateTime<Utc>>,
}

impl EmbeddingProfile {
    /// Returns the profile identifier.
    #[must_use]
    pub fn id(&self) -> EmbeddingProfileId {
        self.id
    }

    /// Returns the activation epoch.
    #[must_use]
    pub fn epoch(&self) -> i64 {
        self.epoch
    }

    /// Returns the provider that produced this geometry.
    #[must_use]
    pub fn provider_kind(&self) -> ProviderKind {
        self.provider_kind
    }

    /// Returns the normalised provider endpoint.
    #[must_use]
    pub fn normalised_base_url(&self) -> &str {
        &self.normalised_base_url
    }

    /// Returns the embedding model name.
    #[must_use]
    pub fn model(&self) -> &str {
        &self.model
    }

    /// Returns the vector dimension.
    #[must_use]
    pub fn dimensions(&self) -> u32 {
        self.dimensions
    }

    /// Returns the distance metric.
    #[must_use]
    pub fn distance_metric(&self) -> DistanceMetric {
        self.distance_metric
    }

    /// Returns the provider revision signal (empty when none).
    #[must_use]
    pub fn revision_token(&self) -> &str {
        &self.revision_token
    }

    /// Returns the probe digest, if one was resolved.
    #[must_use]
    pub fn probe_digest(&self) -> Option<&str> {
        self.probe_digest.as_deref()
    }

    /// Returns the lifecycle state.
    #[must_use]
    pub fn state(&self) -> EmbeddingProfileState {
        self.state
    }

    /// Returns the stable fingerprint handle.
    #[must_use]
    pub fn fingerprint_hash(&self) -> &str {
        &self.fingerprint_hash
    }

    /// Returns when the activation row was created.
    #[must_use]
    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    /// Returns when the profile left `building`, if it has.
    #[must_use]
    pub fn completed_at(&self) -> Option<DateTime<Utc>> {
        self.completed_at
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{enum_serde_tests, enum_text_tests};

    enum_serde_tests!(test_embedding_profile_state_serde_roundtrip, EmbeddingProfileState {
        EmbeddingProfileState::Building => "building",
        EmbeddingProfileState::Complete => "complete",
        EmbeddingProfileState::Failed => "failed",
        EmbeddingProfileState::Superseded => "superseded",
    });

    enum_text_tests!(test_embedding_profile_state_text_roundtrip, EmbeddingProfileState {
        EmbeddingProfileState::Building => "building",
        EmbeddingProfileState::Complete => "complete",
        EmbeddingProfileState::Failed => "failed",
        EmbeddingProfileState::Superseded => "superseded",
    });

    enum_serde_tests!(test_distance_metric_serde_roundtrip, DistanceMetric {
        DistanceMetric::Cosine => "cosine",
    });

    enum_text_tests!(test_distance_metric_text_roundtrip, DistanceMetric {
        DistanceMetric::Cosine => "cosine",
    });
}
