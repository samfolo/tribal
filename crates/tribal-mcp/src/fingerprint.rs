//! System fingerprint computation: a content-addressed SHA-256 over the
//! inference-affecting configuration.
//!
//! The composite names ingest-time intent: a configuration or prompt flip
//! between ingest and first claim leaves it naming a binding version no
//! thread ran, and the thread's recorded binding (not this composite) is the
//! truth of what executed.

use std::collections::HashMap;

use sqlx::PgConnection;
use tribal_common::sha256_hex;
use tribal_config::AgentsConfig;
use tribal_db::{DbError, NewSystemFingerprint};
use tribal_domain::{
    McpErrorCode, PipelineParameters, PromptClass, PromptRole, PromptStage, PromptVersion,
    PromptVersionId, TaskType,
};
use tribal_inference::{CompletionStageSpec, CompletionStageSpecs, ProviderIdentity};
use tribal_worker::{
    DefinitionError, StagePromptHashes, derive_stage_definition, resolve_agentic_prompt_hashes,
};

use crate::{
    error::{IntoMcpError, McpToolError},
    server_handler::{ActivePromptVersions, ConnectionRepositories},
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

pub(crate) const MISSING_PROMPT_VERSIONS: &str =
    "one or more active prompt versions could not be found in the database";

// ---------------------------------------------------------------------------
// PromptContentHashes
// ---------------------------------------------------------------------------

/// Each stage's prompt content hashes, resolved the way the worker resolves
/// them at claim so the fingerprint cannot drift from the recorded binding.
pub(crate) struct PromptContentHashes {
    pub extraction: StagePromptHashes,
    pub triage: StagePromptHashes,
    pub relation: StagePromptHashes,
}

impl PromptContentHashes {
    /// Resolves each stage's hashes from the active version IDs and their
    /// `PromptVersion` records, returning `None` when a required version (a
    /// one-shot pair, or a slot the stage's executor needs) is absent.
    pub(crate) fn from_active(
        active: &ActivePromptVersions,
        versions: &[PromptVersion],
        agents: &AgentsConfig,
    ) -> Option<Self> {
        let by_id: HashMap<PromptVersionId, &str> = versions
            .iter()
            .map(|v| (v.id(), v.content_hash()))
            .collect();

        let stage = |stage: TaskType,
                     system_id: PromptVersionId,
                     user_id: PromptVersionId|
         -> Option<StagePromptHashes> {
            let agentic =
                resolve_agentic_prompt_hashes(stage, agents, |prompt_stage, class, role| {
                    active
                        .get_version(prompt_stage, class, role)
                        .and_then(|id| by_id.get(&id))
                        .map(|hash| (*hash).to_owned())
                        .ok_or(())
                })
                .ok()?;
            Some(StagePromptHashes {
                system: by_id.get(&system_id)?.to_string(),
                user: by_id.get(&user_id)?.to_string(),
                agentic,
            })
        };

        Some(Self {
            extraction: stage(
                TaskType::Extraction,
                active.extraction_system_prompt_version_id,
                active.extraction_user_prompt_version_id,
            )?,
            triage: stage(
                TaskType::Triage,
                active.triage_system_prompt_version_id,
                active.triage_user_prompt_version_id,
            )?,
            relation: stage(
                TaskType::Relation,
                active.relation_system_prompt_version_id,
                active.relation_user_prompt_version_id,
            )?,
        })
    }
}

// ---------------------------------------------------------------------------
// FingerprintInputs
// ---------------------------------------------------------------------------

/// The boot-static fingerprint inputs.
///
/// The stage specs carry post-reconcile sampling parameters, so the binding
/// hashes derived from them record the effective request shape.
#[derive(Debug, Clone)]
pub(crate) struct FingerprintInputs {
    pub specs: CompletionStageSpecs,
    pub agents: AgentsConfig,
    pub embedding: ProviderIdentity,
    pub embedding_dimensions: u32,
    pub pipeline: PipelineParameters,
}

/// The three stage binding hashes the fingerprint composes.
struct StageBindingHashes {
    extraction: String,
    triage: String,
    relation: String,
}

/// Derives the three stage binding hashes from the boot-time endpoint
/// specs, the active prompt content hashes, and the agentic
/// configuration, through the same derivation the worker uses at claim.
fn stage_binding_hashes(
    specs: &CompletionStageSpecs,
    hashes: &PromptContentHashes,
    agents: &AgentsConfig,
) -> Result<StageBindingHashes, FingerprintError> {
    Ok(StageBindingHashes {
        extraction: binding_hash(
            TaskType::Extraction,
            &specs.extraction,
            &hashes.extraction,
            agents,
        )?,
        triage: binding_hash(TaskType::Triage, &specs.triage, &hashes.triage, agents)?,
        relation: binding_hash(
            TaskType::Relation,
            &specs.relation,
            &hashes.relation,
            agents,
        )?,
    })
}

/// One stage's binding hash: the content address over the canonically
/// serialised definition the shared derivation produces.
fn binding_hash(
    stage: TaskType,
    spec: &CompletionStageSpec,
    prompts: &StagePromptHashes,
    agents: &AgentsConfig,
) -> Result<String, FingerprintError> {
    let definition = derive_stage_definition(stage, spec, prompts, agents)?;
    Ok(sha256_hex(&definition.canonical_json()?))
}

// ---------------------------------------------------------------------------
// Hash computation
// ---------------------------------------------------------------------------

/// Hashes the stage binding hashes together with the inputs no stage binding
/// subsumes: the build version, the embedding identity and dimensions, and
/// the pipeline parameters. Newline separation keeps the concatenation
/// unambiguous so two distinct input sets cannot collide.
fn compute_fingerprint_hash(
    bindings: &StageBindingHashes,
    build_version: &str,
    inputs: &FingerprintInputs,
) -> String {
    let dimensions = inputs.embedding_dimensions.to_string();
    let parts = [
        bindings.extraction.as_str(),
        bindings.triage.as_str(),
        bindings.relation.as_str(),
        build_version,
        inputs.embedding.name.as_str(),
        inputs.embedding.model.as_str(),
        dimensions.as_str(),
    ];

    let mut concatenated = parts.join("\n");
    concatenated.push('\n');
    concatenated.push_str(&inputs.pipeline.to_canonical_json());

    sha256_hex(&concatenated)
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors from fingerprint computation and upsert.
#[derive(Debug, thiserror::Error)]
pub(crate) enum FingerprintError {
    #[error(transparent)]
    Db(#[from] DbError),

    #[error("{message} (expected {expected}, found {found})")]
    MissingPromptVersions {
        /// Describes which invariant was violated.
        message: &'static str,
        /// Number of prompt versions expected.
        expected: usize,
        /// Number of prompt versions found in the database.
        found: usize,
    },

    #[error("serialising a stage definition for the fingerprint failed")]
    DefinitionSerialisation {
        #[from]
        source: serde_json::Error,
    },

    #[error("deriving a stage binding for the fingerprint failed")]
    BindingDerivation {
        #[from]
        source: DefinitionError,
    },
}

impl IntoMcpError for FingerprintError {
    fn into_mcp_error(self) -> McpToolError {
        match self {
            Self::Db(e) => e.into_mcp_error(),
            Self::MissingPromptVersions {
                message,
                expected,
                found,
            } => McpToolError {
                code: McpErrorCode::Internal,
                message: format!("{message} (expected {expected}, found {found})"),
                details: serde_json::json!({
                    "expected": expected,
                    "found": found,
                }),
            },
            Self::DefinitionSerialisation { source } => McpToolError {
                code: McpErrorCode::Internal,
                message: format!(
                    "serialising a stage definition for the fingerprint failed: {source}"
                ),
                details: serde_json::Value::Null,
            },
            Self::BindingDerivation { source } => McpToolError {
                code: McpErrorCode::Internal,
                message: format!("deriving a stage binding for the fingerprint failed: {source}"),
                details: serde_json::Value::Null,
            },
        }
    }
}

// ---------------------------------------------------------------------------
// Combined compute + upsert
// ---------------------------------------------------------------------------

/// Looks up prompt content hashes, derives the stage binding hashes,
/// computes the fingerprint hash, upserts the fingerprint record, and
/// returns the content hash.
pub(crate) async fn compute_and_upsert_fingerprint(
    conn: &mut PgConnection,
    repositories: &ConnectionRepositories,
    active_prompts: &ActivePromptVersions,
    build_version: &str,
    inputs: &FingerprintInputs,
) -> Result<String, FingerprintError> {
    let mut version_ids = active_prompts.version_ids().to_vec();
    // Every active agentic slot any stage might bind, so the looked-up
    // records cover whatever the shared resolver decides each stage needs.
    // A slot with no active prompt is simply absent; the resolver errors
    // only if a stage's executor actually requires the missing one.
    for stage in [
        PromptStage::Extraction,
        PromptStage::Triage,
        PromptStage::Relation,
    ] {
        for (class, role) in [
            (PromptClass::Loop, PromptRole::System),
            (PromptClass::Loop, PromptRole::User),
            (PromptClass::Verifier, PromptRole::System),
            (PromptClass::Verifier, PromptRole::User),
        ] {
            if let Some(id) = active_prompts.get_version(stage, class, role) {
                version_ids.push(id);
            }
        }
    }

    let prompt_versions = repositories
        .prompt_version
        .find_by_ids(conn, &version_ids)
        .await?;

    let expected = version_ids.len();
    let found = prompt_versions.len();
    let content_hashes =
        PromptContentHashes::from_active(active_prompts, &prompt_versions, &inputs.agents).ok_or(
            FingerprintError::MissingPromptVersions {
                message: MISSING_PROMPT_VERSIONS,
                expected,
                found,
            },
        )?;

    let bindings = stage_binding_hashes(&inputs.specs, &content_hashes, &inputs.agents)?;
    let fingerprint_hash = compute_fingerprint_hash(&bindings, build_version, inputs);

    let new_fingerprint = NewSystemFingerprint::builder()
        .content_hash(fingerprint_hash.clone())
        .build_version(build_version.to_owned())
        .extraction_binding_hash(bindings.extraction)
        .triage_binding_hash(bindings.triage)
        .relation_binding_hash(bindings.relation)
        .embedding_provider(inputs.embedding.name.clone())
        .embedding_model(inputs.embedding.model.clone())
        .embedding_dimensions(inputs.embedding_dimensions)
        .pipeline_parameters(serde_json::to_value(&inputs.pipeline)?)
        .build();

    repositories
        .system_fingerprint
        .upsert(conn, &new_fingerprint)
        .await?;

    Ok(fingerprint_hash)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use tribal_common::SHA256_HEX_LENGTH;
    use tribal_domain::StageParameters;
    use tribal_test_utils::a_completion_stage_spec;
    use tribal_worker::{AgenticPromptHashes, PromptHashPair};

    use super::*;

    fn a_loop_pair(system: &str, user: &str) -> AgenticPromptHashes {
        AgenticPromptHashes {
            loop_pair: Some(PromptHashPair {
                system: system.repeat(64),
                user: user.repeat(64),
            }),
            verifier_pair: None,
        }
    }

    fn a_stage(system: &str, user: &str, loop_system: &str, loop_user: &str) -> StagePromptHashes {
        StagePromptHashes {
            system: system.repeat(64),
            user: user.repeat(64),
            agentic: a_loop_pair(loop_system, loop_user),
        }
    }

    // Every stage carries a loop pair, so a test that flips any stage to the
    // loop executor finds its prompts; a one-shot stage ignores them.
    fn test_hashes() -> PromptContentHashes {
        PromptContentHashes {
            extraction: a_stage("a", "b", "1", "2"),
            triage: a_stage("c", "d", "3", "4"),
            relation: a_stage("e", "f", "5", "6"),
        }
    }

    fn a_spec(model: &str, temperature: Option<f64>) -> CompletionStageSpec {
        CompletionStageSpec {
            model: model.to_owned(),
            parameters: StageParameters {
                temperature,
                max_tokens: Some(2048),
            },
            ..a_completion_stage_spec()
        }
    }

    fn test_inputs() -> FingerprintInputs {
        FingerprintInputs {
            specs: CompletionStageSpecs {
                extraction: a_spec("llama3:70b", Some(0.2)),
                triage: a_spec("llama3:8b", Some(0.1)),
                relation: a_spec("llama3:8b", Some(0.1)),
            },
            embedding: ProviderIdentity {
                name: "ollama".into(),
                model: "nomic-embed-text".into(),
            },
            embedding_dimensions: 768,
            pipeline: PipelineParameters {
                max_candidates_per_job: 20,
                triage_search_limit: 10,
                tag_similarity_threshold: 0.85,
            },
            agents: AgentsConfig::default(),
        }
    }

    fn hash_of(inputs: &FingerprintInputs, build_version: &str) -> String {
        let bindings = stage_binding_hashes(&inputs.specs, &test_hashes(), &inputs.agents)
            .expect("definitions serialise");
        compute_fingerprint_hash(&bindings, build_version, inputs)
    }

    #[test]
    fn test_hash_produces_valid_hex() {
        let hash = hash_of(&test_inputs(), "v0.1.0-abc123");

        assert_eq!(hash.len(), SHA256_HEX_LENGTH);
        assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_hash_is_deterministic() {
        assert_eq!(
            hash_of(&test_inputs(), "v0.1.0"),
            hash_of(&test_inputs(), "v0.1.0")
        );
    }

    #[test]
    fn test_changing_build_version_changes_hash() {
        assert_ne!(
            hash_of(&test_inputs(), "v0.1.0"),
            hash_of(&test_inputs(), "v0.2.0")
        );
    }

    #[test]
    fn test_changing_prompt_hash_changes_fingerprint() {
        let inputs = test_inputs();
        let a = hash_of(&inputs, "v0.1.0");

        let mut hashes = test_hashes();
        hashes.extraction.system = "0".repeat(64);
        let bindings = stage_binding_hashes(&inputs.specs, &hashes, &inputs.agents)
            .expect("definitions serialise");
        let b = compute_fingerprint_hash(&bindings, "v0.1.0", &inputs);

        assert_ne!(a, b);
    }

    #[test]
    fn test_changing_model_changes_hash() {
        let mut inputs = test_inputs();
        let a = hash_of(&inputs, "v0.1.0");

        inputs.specs.triage.model = "different-model".into();
        let b = hash_of(&inputs, "v0.1.0");

        assert_ne!(a, b);
    }

    #[test]
    fn test_a_sampling_parameter_edit_changes_the_binding_and_the_fingerprint() {
        // The composite changes when the binding does: a temperature edit
        // is a new binding version, and the fingerprint follows it.
        let inputs = test_inputs();
        let bindings_before = stage_binding_hashes(&inputs.specs, &test_hashes(), &inputs.agents)
            .expect("serialises");
        let a = hash_of(&inputs, "v0.1.0");

        let mut edited = inputs.clone();
        edited.specs.extraction.parameters.temperature = Some(0.9);
        let bindings_after = stage_binding_hashes(&edited.specs, &test_hashes(), &edited.agents)
            .expect("serialises");
        let b = hash_of(&edited, "v0.1.0");

        assert_ne!(bindings_before.extraction, bindings_after.extraction);
        assert_eq!(bindings_before.triage, bindings_after.triage);
        assert_ne!(a, b);
    }

    #[test]
    fn test_changing_pipeline_parameters_changes_hash() {
        let mut inputs = test_inputs();
        let a = hash_of(&inputs, "v0.1.0");

        inputs.pipeline.triage_search_limit = 25;
        let b = hash_of(&inputs, "v0.1.0");

        assert_ne!(a, b);
    }

    #[test]
    fn test_enabling_the_loop_executor_changes_the_triage_binding_and_the_composite() {
        // Flipping the executor is a binding change by construction: the
        // ingest-time composite moves with it, naming the agentic
        // binding a claim under the same configuration will resolve.
        let inputs = test_inputs();
        let bindings_before =
            stage_binding_hashes(&inputs.specs, &test_hashes(), &inputs.agents).expect("derives");
        let a = hash_of(&inputs, "v0.1.0");

        let mut flipped = inputs.clone();
        flipped.agents.triage.executor = tribal_config::ExecutorChoice::Loop;
        let bindings_after =
            stage_binding_hashes(&flipped.specs, &test_hashes(), &flipped.agents).expect("derives");
        let b = hash_of(&flipped, "v0.1.0");

        assert_ne!(bindings_before.triage, bindings_after.triage);
        assert_eq!(bindings_before.extraction, bindings_after.extraction);
        assert_ne!(a, b);
    }

    #[test]
    fn test_enabling_the_extraction_loop_derives_and_moves_only_its_binding() {
        // The ingest-path regression guard: enabling the extraction loop must
        // derive (not fail for want of loop prompts, as it once did) and move
        // the extraction binding alone.
        let inputs = test_inputs();
        let before =
            stage_binding_hashes(&inputs.specs, &test_hashes(), &inputs.agents).expect("derives");

        let mut flipped = inputs.clone();
        flipped.agents.extraction.executor = tribal_config::ExecutorChoice::Loop;
        let after = stage_binding_hashes(&flipped.specs, &test_hashes(), &flipped.agents)
            .expect("the extraction loop derives");

        assert_ne!(before.extraction, after.extraction);
        assert_eq!(before.triage, after.triage);
        assert_eq!(before.relation, after.relation);
    }

    #[test]
    fn test_enabling_the_relation_loop_derives_and_moves_only_its_binding() {
        let inputs = test_inputs();
        let before =
            stage_binding_hashes(&inputs.specs, &test_hashes(), &inputs.agents).expect("derives");

        let mut flipped = inputs.clone();
        flipped.agents.relation.executor = tribal_config::ExecutorChoice::Loop;
        let after = stage_binding_hashes(&flipped.specs, &test_hashes(), &flipped.agents)
            .expect("the relation loop derives");

        assert_ne!(before.relation, after.relation);
        assert_eq!(before.extraction, after.extraction);
        assert_eq!(before.triage, after.triage);
    }

    #[test]
    fn test_the_verifier_pair_is_part_of_the_triage_loop_binding() {
        // The verifier the binding runs is named in its hash: a triage loop
        // with a verifier binds differently from one without, and a verifier
        // rubric edit is a new binding version.
        let mut inputs = test_inputs();
        inputs.agents.triage.executor = tribal_config::ExecutorChoice::Loop;

        let triage_binding = |verifier_pair: Option<PromptHashPair>| {
            let mut hashes = test_hashes();
            hashes.triage.agentic.verifier_pair = verifier_pair;
            stage_binding_hashes(&inputs.specs, &hashes, &inputs.agents)
                .expect("derives")
                .triage
        };

        let without = triage_binding(None);
        let with = triage_binding(Some(PromptHashPair {
            system: "7".repeat(64),
            user: "8".repeat(64),
        }));
        let edited = triage_binding(Some(PromptHashPair {
            system: "9".repeat(64),
            user: "8".repeat(64),
        }));

        assert_ne!(without, with, "the verifier is part of the binding");
        assert_ne!(
            with, edited,
            "a verifier prompt edit is a new binding version"
        );
    }

    #[test]
    fn test_changing_embedding_dimensions_changes_hash() {
        let mut inputs = test_inputs();
        let a = hash_of(&inputs, "v0.1.0");

        inputs.embedding_dimensions = 1024;
        let b = hash_of(&inputs, "v0.1.0");

        assert_ne!(a, b);
    }
}
