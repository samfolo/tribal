//! Prompt file management and loading.
//!
//! Embedded defaults are compiled into the binary via `include_str!`.
//! On first run, prompt files are written to disk from embedded defaults.
//! On every startup, files are read, hashed, and upserted into the database.

use std::{collections::HashMap, path::Path};

use sqlx::PgPool;
use tribal_common::sha256_hex;
use tribal_db::{NewPromptVersion, PgPromptVersionRepository, PromptVersionRepository};
use tribal_domain::{PromptRole, PromptStage, PromptVersionId};
use tribal_mcp::ActivePromptVersions;

use crate::error::AppError;

// ---------------------------------------------------------------------------
// Embedded defaults
// ---------------------------------------------------------------------------

const EXTRACTION_SYSTEM: &str = include_str!("../../../../prompts/extraction/system.tera");
const EXTRACTION_USER: &str = include_str!("../../../../prompts/extraction/user.tera");
const TRIAGE_SYSTEM: &str = include_str!("../../../../prompts/triage/system.tera");
const TRIAGE_USER: &str = include_str!("../../../../prompts/triage/user.tera");
const RELATION_SYSTEM: &str = include_str!("../../../../prompts/relation/system.tera");
const RELATION_USER: &str = include_str!("../../../../prompts/relation/user.tera");

/// Returns the embedded default content for a given stage and role.
fn embedded_default(stage: PromptStage, role: PromptRole) -> &'static str {
    match (stage, role) {
        (PromptStage::Extraction, PromptRole::System) => EXTRACTION_SYSTEM,
        (PromptStage::Extraction, PromptRole::User) => EXTRACTION_USER,
        (PromptStage::Triage, PromptRole::System) => TRIAGE_SYSTEM,
        (PromptStage::Triage, PromptRole::User) => TRIAGE_USER,
        (PromptStage::Relation, PromptRole::System) => RELATION_SYSTEM,
        (PromptStage::Relation, PromptRole::User) => RELATION_USER,
    }
}

// ---------------------------------------------------------------------------
// Stage/role iteration
// ---------------------------------------------------------------------------

/// All (stage, role) pairs in canonical order.
const PROMPT_PAIRS: [(PromptStage, PromptRole); 6] = [
    (PromptStage::Extraction, PromptRole::System),
    (PromptStage::Extraction, PromptRole::User),
    (PromptStage::Triage, PromptRole::System),
    (PromptStage::Triage, PromptRole::User),
    (PromptStage::Relation, PromptRole::System),
    (PromptStage::Relation, PromptRole::User),
];

// ---------------------------------------------------------------------------
// Expect messages
// ---------------------------------------------------------------------------

const EXPECT_VERSION: &str = "all prompt pairs must be loaded";

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Writes any missing prompt files from embedded defaults.
///
/// Creates the stage subdirectories under `prompts_dir` if they do not
/// exist.  Existing files are left untouched.
pub(crate) async fn ensure_prompt_files(prompts_dir: &Path) -> Result<(), AppError> {
    for (stage, role) in &PROMPT_PAIRS {
        let stage_dir = prompts_dir.join(stage.as_str());
        tokio::fs::create_dir_all(&stage_dir)
            .await
            .map_err(|source| AppError::PromptIo {
                context: format!("create directory {}", stage_dir.display()),
                source,
            })?;

        let file_path = stage_dir.join(format!("{}.tera", role.as_str()));
        if !file_path.exists() {
            let content = embedded_default(*stage, *role);
            tokio::fs::write(&file_path, content)
                .await
                .map_err(|source| AppError::PromptIo {
                    context: format!("write default {}", file_path.display()),
                    source,
                })?;
            tracing::info!(path = %file_path.display(), "wrote default prompt file");
        }
    }

    Ok(())
}

/// Reads all prompt files, computes SHA-256 hashes, and upserts into the
/// database.
///
/// Returns the [`ActivePromptVersions`] struct populated with the version
/// IDs of all six prompts.
pub(crate) async fn load_prompts(
    pool: &PgPool,
    prompts_dir: &Path,
) -> Result<ActivePromptVersions, AppError> {
    let repo = PgPromptVersionRepository;
    let mut conn = pool.acquire().await.map_err(|e| AppError::Database {
        source: tribal_db::DbError::QueryFailed {
            context: "acquire connection for prompt loading".into(),
            source: e,
        },
    })?;

    let mut versions: HashMap<(PromptStage, PromptRole), PromptVersionId> =
        HashMap::with_capacity(PROMPT_PAIRS.len());

    for (stage, role) in &PROMPT_PAIRS {
        let file_path = prompts_dir
            .join(stage.as_str())
            .join(format!("{}.tera", role.as_str()));

        let content = tokio::fs::read_to_string(&file_path)
            .await
            .map_err(|source| AppError::PromptIo {
                context: format!("read {}", file_path.display()),
                source,
            })?;

        let content_hash = sha256_hex(&content);

        let new = NewPromptVersion::builder()
            .stage(*stage)
            .role(*role)
            .content_hash(content_hash)
            .content(content)
            .build();

        let version =
            repo.upsert(&mut conn, &new)
                .await
                .map_err(|source| AppError::PromptLoading {
                    context: format!("upsert {} {} prompt", stage, role),
                    source,
                })?;

        tracing::info!(
            stage = stage.as_str(),
            role = role.as_str(),
            version_id = %version.id(),
            "loaded prompt version",
        );

        versions.insert((*stage, *role), version.id());
    }

    Ok(ActivePromptVersions::new(
        versions
            .remove(&(PromptStage::Extraction, PromptRole::System))
            .expect(EXPECT_VERSION),
        versions
            .remove(&(PromptStage::Extraction, PromptRole::User))
            .expect(EXPECT_VERSION),
        versions
            .remove(&(PromptStage::Triage, PromptRole::System))
            .expect(EXPECT_VERSION),
        versions
            .remove(&(PromptStage::Triage, PromptRole::User))
            .expect(EXPECT_VERSION),
        versions
            .remove(&(PromptStage::Relation, PromptRole::System))
            .expect(EXPECT_VERSION),
        versions
            .remove(&(PromptStage::Relation, PromptRole::User))
            .expect(EXPECT_VERSION),
    ))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_embedded_defaults_are_non_empty() {
        for (stage, role) in &PROMPT_PAIRS {
            let content = embedded_default(*stage, *role);
            assert!(
                !content.is_empty(),
                "embedded default for {stage}/{role} is empty",
            );
        }
    }
}
