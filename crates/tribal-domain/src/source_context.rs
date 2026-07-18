//! Typed source context — the provenance account written at ingest and
//! completed at extraction commit.
//!
//! Client name/version and claimed provider/model are declarations by the
//! MCP client; principal, project, transport, and the committed extraction
//! binding are observed by the server. The two never mix: a claim lives
//! under [`ClaimedActor`], an observation on the context itself.

use serde::{Deserialize, Deserializer, Serialize};

use crate::SourceType;

// ---------------------------------------------------------------------------
// SourceContextV1
// ---------------------------------------------------------------------------

/// Version discriminator for [`SourceContextV1`].
const VERSION: u8 = 1;

/// The typed source context stored as JSONB on jobs and knowledge items.
///
/// Constructed at ingest with `extraction: None`; the extraction identity
/// is committed exactly once, inside the extraction commit transaction,
/// through [`SourceContextV1::commit_extraction_identity`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceContextV1 {
    /// Schema version; always `1`, refused on deserialize otherwise.
    #[serde(deserialize_with = "expect_version_one")]
    version: u8,
    /// The asserted capture path, never an authentication claim.
    #[serde(rename = "type")]
    source_type: SourceType,
    /// The transport the ingest arrived over. `None` only on contexts
    /// migrated from rows whose transport was never observed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    channel: Option<IngestChannel>,
    /// The connected client's declared identity, when it made one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    claimed_actor: Option<ClaimedActor>,
    /// The binding the engine actually extracted with, committed once.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    extraction: Option<InferenceIdentity>,
}

impl SourceContextV1 {
    /// Builds the context an ingest writes: classification follows the
    /// claim — a claimed inference provider makes the source
    /// `agent_mediated`, anything else `manual_capture`.
    #[must_use]
    pub fn from_claims(channel: IngestChannel, claimed_actor: Option<ClaimedActor>) -> Self {
        let claims_provider = claimed_actor
            .as_ref()
            .and_then(|actor| actor.inference.as_ref())
            .is_some_and(|inference| inference.provider().is_some());
        let source_type = if claims_provider {
            SourceType::AgentMediated
        } else {
            SourceType::ManualCapture
        };
        Self {
            version: VERSION,
            source_type,
            channel: Some(channel),
            claimed_actor,
            extraction: None,
        }
    }

    /// Returns the asserted capture path.
    pub fn source_type(&self) -> SourceType {
        self.source_type
    }

    /// Returns the transport the ingest was observed on.
    pub fn channel(&self) -> Option<IngestChannel> {
        self.channel
    }

    /// Returns the client's declared identity.
    pub fn claimed_actor(&self) -> Option<&ClaimedActor> {
        self.claimed_actor.as_ref()
    }

    /// Returns the committed extraction identity.
    pub fn extraction(&self) -> Option<&InferenceIdentity> {
        self.extraction.as_ref()
    }

    /// Commits the extraction identity: sets it when absent, accepts an
    /// identical value as a no-op, and refuses a different one — a job
    /// extracted under one binding can never be re-attributed to another.
    ///
    /// # Errors
    ///
    /// [`SourceContextError::ConflictingExtractionIdentity`] when an
    /// identity differing from the committed one is offered.
    pub fn commit_extraction_identity(
        &mut self,
        identity: &InferenceIdentity,
    ) -> Result<ExtractionCommitOutcome, SourceContextError> {
        match &self.extraction {
            None => {
                self.extraction = Some(identity.clone());
                Ok(ExtractionCommitOutcome::Recorded)
            }
            Some(existing) if existing == identity => Ok(ExtractionCommitOutcome::AlreadyRecorded),
            Some(existing) => Err(SourceContextError::ConflictingExtractionIdentity {
                existing: existing.clone(),
                offered: identity.clone(),
            }),
        }
    }
}

/// Refuses any stored version other than the one this type models, so a
/// future schema revision can never be silently misread as V1.
fn expect_version_one<'de, D>(deserializer: D) -> Result<u8, D::Error>
where
    D: Deserializer<'de>,
{
    let version = u8::deserialize(deserializer)?;
    if version == VERSION {
        Ok(version)
    } else {
        Err(serde::de::Error::custom(format!(
            "unsupported source context version {version}"
        )))
    }
}

/// Outcome of committing an extraction identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtractionCommitOutcome {
    /// The identity was absent and is now recorded.
    Recorded,
    /// An identical identity was already recorded; nothing changed.
    AlreadyRecorded,
}

// ---------------------------------------------------------------------------
// Claimed identity
// ---------------------------------------------------------------------------

/// The identity a connected client declares for itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimedActor {
    /// The client implementation named at MCP initialization.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client: Option<ClaimedClientIdentity>,
    /// The inference identity the client asserts it runs under.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inference: Option<ClaimedInferenceIdentity>,
}

/// A client's declared name and version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimedClientIdentity {
    /// Declared client name.
    pub name: String,
    /// Declared client version.
    pub version: String,
}

/// A client's declared inference provider and/or model — a claim, so it
/// may be partial, but never empty.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "ClaimedInferenceIdentityUnchecked")]
pub struct ClaimedInferenceIdentity {
    #[serde(skip_serializing_if = "Option::is_none")]
    provider: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    model: Option<String>,
}

impl ClaimedInferenceIdentity {
    /// Builds a claim carrying at least one of provider and model.
    ///
    /// # Errors
    ///
    /// [`SourceContextError::EmptyInferenceClaim`] when both are absent.
    pub fn new(
        provider: Option<String>,
        model: Option<String>,
    ) -> Result<Self, SourceContextError> {
        if provider.is_none() && model.is_none() {
            return Err(SourceContextError::EmptyInferenceClaim);
        }
        Ok(Self { provider, model })
    }

    /// Returns the claimed provider.
    pub fn provider(&self) -> Option<&str> {
        self.provider.as_deref()
    }

    /// Returns the claimed model.
    pub fn model(&self) -> Option<&str> {
        self.model.as_deref()
    }
}

/// Wire shadow routing deserialization through the at-least-one check.
#[derive(Deserialize)]
struct ClaimedInferenceIdentityUnchecked {
    #[serde(default)]
    provider: Option<String>,
    #[serde(default)]
    model: Option<String>,
}

impl TryFrom<ClaimedInferenceIdentityUnchecked> for ClaimedInferenceIdentity {
    type Error = SourceContextError;

    fn try_from(raw: ClaimedInferenceIdentityUnchecked) -> Result<Self, Self::Error> {
        Self::new(raw.provider, raw.model)
    }
}

// ---------------------------------------------------------------------------
// Confirmed identity
// ---------------------------------------------------------------------------

/// The complete binding the engine actually ran an extraction under.
///
/// Both fields are required — this records an observation, so it never
/// reuses the partial claimed shape.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InferenceIdentity {
    /// Provider the binding resolved to.
    pub provider: String,
    /// Model the binding resolved to.
    pub model: String,
}

impl std::fmt::Display for InferenceIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.provider, self.model)
    }
}

// ---------------------------------------------------------------------------
// IngestChannel
// ---------------------------------------------------------------------------

/// Durable provenance vocabulary for the transport an ingest arrived
/// over. Deliberately separate from the network-facing transport enums,
/// so config evolution cannot rewrite stored provenance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IngestChannel {
    McpHttp,
    McpSse,
    McpStdio,
}

// ---------------------------------------------------------------------------
// Lenient stored reads
// ---------------------------------------------------------------------------

/// Reads the source type out of any stored context shape: the typed V1
/// discriminator, the flat shape's PascalCase and snake_case
/// discriminators, and typeless flat contexts — which, like unknown
/// values, read as `manual_capture` rather than inventing a stronger
/// classification.
#[must_use]
pub fn stored_source_type(context: &serde_json::Value) -> SourceType {
    let Some(discriminator) = context.get("type").and_then(serde_json::Value::as_str) else {
        return SourceType::ManualCapture;
    };
    if let Ok(source_type) = discriminator.parse::<SourceType>() {
        return source_type;
    }
    match discriminator {
        "AgentMediated" => SourceType::AgentMediated,
        "ManualCapture" => SourceType::ManualCapture,
        "Derived" => SourceType::Derived,
        "FileWatch" => SourceType::FileWatch,
        _ => SourceType::ManualCapture,
    }
}

// ---------------------------------------------------------------------------
// SourceContextError
// ---------------------------------------------------------------------------

/// Invariant failures of the typed source context.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SourceContextError {
    /// A claimed inference identity must carry provider, model, or both.
    #[error("claimed inference identity carries neither provider nor model")]
    EmptyInferenceClaim,

    /// The job was already attributed to a different extraction binding.
    #[error("extraction identity already committed as {existing}, refusing {offered}")]
    ConflictingExtractionIdentity {
        /// The identity the context already carries.
        existing: InferenceIdentity,
        /// The differing identity the caller offered.
        offered: InferenceIdentity,
    },
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn an_inference_identity() -> InferenceIdentity {
        InferenceIdentity {
            provider: "anthropic".into(),
            model: "claude-opus-4-6".into(),
        }
    }

    fn a_full_claim() -> ClaimedActor {
        ClaimedActor {
            client: Some(ClaimedClientIdentity {
                name: "tribal-mac".into(),
                version: "1.2.0".into(),
            }),
            inference: Some(
                ClaimedInferenceIdentity::new(Some("anthropic".into()), None)
                    .expect("provider-only claim is valid"),
            ),
        }
    }

    #[test]
    fn test_a_claimed_provider_classifies_agent_mediated() {
        let context = SourceContextV1::from_claims(IngestChannel::McpHttp, Some(a_full_claim()));

        assert_eq!(context.source_type(), SourceType::AgentMediated);
        assert_eq!(context.channel(), Some(IngestChannel::McpHttp));
        assert!(context.extraction().is_none());
    }

    #[test]
    fn test_no_claim_classifies_manual_capture() {
        let context = SourceContextV1::from_claims(IngestChannel::McpStdio, None);

        assert_eq!(context.source_type(), SourceType::ManualCapture);
    }

    #[test]
    fn test_a_model_only_claim_classifies_manual_capture() {
        let claim = ClaimedActor {
            client: None,
            inference: Some(
                ClaimedInferenceIdentity::new(None, Some("claude-opus-4-6".into()))
                    .expect("model-only claim is valid"),
            ),
        };

        let context = SourceContextV1::from_claims(IngestChannel::McpSse, Some(claim));

        assert_eq!(context.source_type(), SourceType::ManualCapture);
    }

    #[test]
    fn test_serialized_context_uses_snake_case_discriminators() {
        let context = SourceContextV1::from_claims(IngestChannel::McpHttp, Some(a_full_claim()));

        let value = serde_json::to_value(&context).expect("context serialises");

        assert_eq!(value["version"], json!(1));
        assert_eq!(value["type"], json!("agent_mediated"));
        assert_eq!(value["channel"], json!("mcp_http"));
        assert_eq!(value["claimed_actor"]["client"]["name"], json!("tribal-mac"));
        assert_eq!(value.get("extraction"), None);
    }

    #[test]
    fn test_a_context_round_trips_through_json() {
        let mut context = SourceContextV1::from_claims(IngestChannel::McpSse, Some(a_full_claim()));
        context
            .commit_extraction_identity(&an_inference_identity())
            .expect("first commit records");

        let value = serde_json::to_value(&context).expect("context serialises");
        let restored: SourceContextV1 =
            serde_json::from_value(value).expect("context deserialises");

        assert_eq!(restored, context);
    }

    #[test]
    fn test_an_unsupported_version_is_refused() {
        let value = json!({ "version": 2, "type": "manual_capture" });

        let err = serde_json::from_value::<SourceContextV1>(value).unwrap_err();

        assert!(err.to_string().contains("unsupported source context version 2"));
    }

    #[test]
    fn test_an_empty_inference_claim_is_refused() {
        let err = ClaimedInferenceIdentity::new(None, None).unwrap_err();

        assert!(matches!(err, SourceContextError::EmptyInferenceClaim));
    }

    #[test]
    fn test_deserialize_refuses_an_empty_inference_claim() {
        let value = json!({
            "version": 1,
            "type": "agent_mediated",
            "claimed_actor": { "inference": {} },
        });

        assert!(serde_json::from_value::<SourceContextV1>(value).is_err());
    }

    #[test]
    fn test_first_extraction_commit_records_the_identity() {
        let mut context = SourceContextV1::from_claims(IngestChannel::McpHttp, None);

        let outcome = context
            .commit_extraction_identity(&an_inference_identity())
            .expect("first commit records");

        assert_eq!(outcome, ExtractionCommitOutcome::Recorded);
        assert_eq!(context.extraction(), Some(&an_inference_identity()));
    }

    #[test]
    fn test_an_identical_recommit_is_a_no_op() {
        let mut context = SourceContextV1::from_claims(IngestChannel::McpHttp, None);
        context
            .commit_extraction_identity(&an_inference_identity())
            .expect("first commit records");

        let outcome = context
            .commit_extraction_identity(&an_inference_identity())
            .expect("identical recommit is accepted");

        assert_eq!(outcome, ExtractionCommitOutcome::AlreadyRecorded);
    }

    #[test]
    fn test_a_differing_extraction_identity_is_refused() {
        let mut context = SourceContextV1::from_claims(IngestChannel::McpHttp, None);
        context
            .commit_extraction_identity(&an_inference_identity())
            .expect("first commit records");
        let other = InferenceIdentity {
            provider: "openai".into(),
            model: "gpt-6".into(),
        };

        let err = context.commit_extraction_identity(&other).unwrap_err();

        assert!(matches!(
            err,
            SourceContextError::ConflictingExtractionIdentity { .. }
        ));
        assert_eq!(context.extraction(), Some(&an_inference_identity()));
    }

    #[test]
    fn test_stored_source_type_reads_every_stored_shape() {
        let v1 = json!({ "version": 1, "type": "agent_mediated" });
        let flat_pascal = json!({ "type": "AgentMediated", "provider": "anthropic" });
        let flat_typeless = json!({ "provider": "anthropic", "model": "claude-opus-4-6" });
        let unknown = json!({ "type": "Telepathy" });

        assert_eq!(stored_source_type(&v1), SourceType::AgentMediated);
        assert_eq!(stored_source_type(&flat_pascal), SourceType::AgentMediated);
        assert_eq!(stored_source_type(&flat_typeless), SourceType::ManualCapture);
        assert_eq!(stored_source_type(&unknown), SourceType::ManualCapture);
    }
}
