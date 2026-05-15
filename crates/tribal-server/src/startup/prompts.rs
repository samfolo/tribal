//! Prompt file management and loading.
//!
//! Embedded defaults are compiled into the binary via `include_str!`.
//! On first run, prompt files are written to disk from embedded defaults.
//! On every startup, files are read, hashed, and upserted into the database.

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    str::FromStr,
};

use sqlx::PgPool;
use tribal_common::sha256_hex;
use tribal_db::{NewPromptVersion, PgPromptVersionRepository, PromptVersionRepository};
use tribal_domain::{PromptRole, PromptStage, PromptVersionId};
use tribal_mcp::ActivePromptVersions;

use super::POOL_NAME_MCP;
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

/// Returns the embedded default content for a given location.
fn embedded_default(location: PromptTemplateLocation) -> &'static str {
    match (location.stage, location.role) {
        (PromptStage::Extraction, PromptRole::System) => EXTRACTION_SYSTEM,
        (PromptStage::Extraction, PromptRole::User) => EXTRACTION_USER,
        (PromptStage::Triage, PromptRole::System) => TRIAGE_SYSTEM,
        (PromptStage::Triage, PromptRole::User) => TRIAGE_USER,
        (PromptStage::Relation, PromptRole::System) => RELATION_SYSTEM,
        (PromptStage::Relation, PromptRole::User) => RELATION_USER,
    }
}

// ---------------------------------------------------------------------------
// PromptTemplateLocation
// ---------------------------------------------------------------------------

/// File extension used for prompt template files.
const PROMPT_FILE_EXTENSION: &str = "tera";

const EXPECT_VERSION: &str = "all prompt pairs must be loaded";

/// A prompt template identified by its pipeline stage and role.
///
/// Encapsulates the mapping from (stage, role) to filesystem path,
/// providing a single source of truth for the directory layout and
/// file extension convention.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct PromptTemplateLocation {
    stage: PromptStage,
    role: PromptRole,
}

impl PromptTemplateLocation {
    /// All (stage, role) pairs in canonical order.
    pub(crate) const ALL: [Self; 6] = [
        Self {
            stage: PromptStage::Extraction,
            role: PromptRole::System,
        },
        Self {
            stage: PromptStage::Extraction,
            role: PromptRole::User,
        },
        Self {
            stage: PromptStage::Triage,
            role: PromptRole::System,
        },
        Self {
            stage: PromptStage::Triage,
            role: PromptRole::User,
        },
        Self {
            stage: PromptStage::Relation,
            role: PromptRole::System,
        },
        Self {
            stage: PromptStage::Relation,
            role: PromptRole::User,
        },
    ];

    /// Returns the pipeline stage.
    pub(crate) fn stage(self) -> PromptStage {
        self.stage
    }

    /// Returns the prompt role.
    pub(crate) fn role(self) -> PromptRole {
        self.role
    }

    /// Resolves the full file path under the given prompts directory.
    pub(crate) fn resolve(self, prompts_dir: &Path) -> PathBuf {
        prompts_dir
            .join(self.stage.as_str())
            .join(format!("{}.{PROMPT_FILE_EXTENSION}", self.role.as_str()))
    }

    /// Attempts to parse a file path back into a location.
    ///
    /// Returns `None` for paths that do not match the expected layout
    /// (including editor swap files and unrecognised stage/role names).
    pub(crate) fn from_path(prompts_dir: &Path, path: &Path) -> Option<Self> {
        let relative = path.strip_prefix(prompts_dir).ok()?;

        if relative.extension()?.to_str()? != PROMPT_FILE_EXTENSION {
            return None;
        }

        let role_str = relative.file_stem()?.to_str()?;
        let stage_dir = relative.parent()?;
        let stage_str = stage_dir.file_name()?.to_str()?;

        // Reject nested paths: stage_dir must be a single component.
        if stage_dir.parent()? != Path::new("") {
            return None;
        }

        let stage = PromptStage::from_str(stage_str).ok()?;
        let role = PromptRole::from_str(role_str).ok()?;

        Some(Self { stage, role })
    }
}

impl From<(PromptStage, PromptRole)> for PromptTemplateLocation {
    fn from((stage, role): (PromptStage, PromptRole)) -> Self {
        Self { stage, role }
    }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Writes any missing prompt files from embedded defaults.
///
/// Creates the stage subdirectories under `prompts_dir` if they do not
/// exist.  Existing files are left untouched.
pub(crate) async fn ensure_prompt_files(prompts_dir: &Path) -> Result<(), AppError> {
    for location in &PromptTemplateLocation::ALL {
        let file_path = location.resolve(prompts_dir);
        let stage_dir = file_path
            .parent()
            .expect("resolve always produces a parent");

        tokio::fs::create_dir_all(stage_dir)
            .await
            .map_err(|source| AppError::PromptIo {
                context: format!("create directory {}", stage_dir.display()),
                source,
            })?;

        let exists =
            tokio::fs::try_exists(&file_path)
                .await
                .map_err(|source| AppError::PromptIo {
                    context: format!("check existence of {}", file_path.display()),
                    source,
                })?;
        if !exists {
            let content = embedded_default(*location);
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
    let mut contents = Vec::with_capacity(PromptTemplateLocation::ALL.len());
    for location in PromptTemplateLocation::ALL {
        let file_path = location.resolve(prompts_dir);
        let content = tokio::fs::read_to_string(&file_path)
            .await
            .map_err(|source| AppError::PromptIo {
                context: format!("read {}", file_path.display()),
                source,
            })?;
        contents.push((location, content));
    }
    upsert_prompt_versions(pool, contents).await
}

/// Hashes and upserts the six prompts compiled into the binary.
///
/// Used when [`PromptSource::Embedded`](tribal_config::PromptSource::Embedded)
/// is in effect: no filesystem IO, no user-editable files. The on-disk
/// equivalent is [`load_prompts`].
pub(crate) async fn load_prompts_embedded(
    pool: &PgPool,
) -> Result<ActivePromptVersions, AppError> {
    let contents = PromptTemplateLocation::ALL
        .into_iter()
        .map(|location| (location, embedded_default(location).to_owned()));
    upsert_prompt_versions(pool, contents).await
}

/// Hashes each `(location, content)` pair and upserts via the prompt-version
/// repository.
///
/// Shared by [`load_prompts`] and [`load_prompts_embedded`] so the disk and
/// embedded paths produce byte-identical database rows when the on-disk
/// files contain the embedded defaults.
async fn upsert_prompt_versions(
    pool: &PgPool,
    contents: impl IntoIterator<Item = (PromptTemplateLocation, String)>,
) -> Result<ActivePromptVersions, AppError> {
    let repo = PgPromptVersionRepository;
    let mut conn = pool
        .acquire()
        .await
        .map_err(|e| AppError::pool_acquire(POOL_NAME_MCP, "prompt loading", e))?;

    let mut versions: HashMap<(PromptStage, PromptRole), PromptVersionId> =
        HashMap::with_capacity(PromptTemplateLocation::ALL.len());

    for (location, content) in contents {
        let content_hash = sha256_hex(&content);
        let stage = location.stage();
        let role = location.role();

        let new = NewPromptVersion::builder()
            .stage(stage)
            .role(role)
            .content_hash(content_hash)
            .content(content)
            .build();

        let version =
            repo.upsert(&mut conn, &new)
                .await
                .map_err(|source| AppError::PromptLoading {
                    context: format!("upsert {stage} {role} prompt"),
                    source,
                })?;

        tracing::info!(
            stage = stage.as_str(),
            role = role.as_str(),
            version_id = %version.id(),
            "loaded prompt version",
        );

        versions.insert((stage, role), version.id());
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
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn test_embedded_defaults_are_non_empty() {
        for location in &PromptTemplateLocation::ALL {
            let content = embedded_default(*location);
            assert!(
                !content.is_empty(),
                "embedded default for {}/{} is empty",
                location.stage(),
                location.role(),
            );
        }
    }

    #[test]
    fn test_all_covers_every_stage_role_combination() {
        let all_stages = [
            PromptStage::Extraction,
            PromptStage::Triage,
            PromptStage::Relation,
        ];
        let all_roles = [PromptRole::System, PromptRole::User];

        let expected: HashSet<(PromptStage, PromptRole)> = all_stages
            .iter()
            .flat_map(|s| all_roles.iter().map(move |r| (*s, *r)))
            .collect();

        let actual: HashSet<(PromptStage, PromptRole)> = PromptTemplateLocation::ALL
            .iter()
            .map(|l| (l.stage(), l.role()))
            .collect();

        assert_eq!(
            actual, expected,
            "ALL must cover all stage×role combinations"
        );
        assert_eq!(
            PromptTemplateLocation::ALL.len(),
            6,
            "ALL must contain exactly 6 entries"
        );
    }

    #[test]
    fn test_from_path_is_inverse_of_resolve() {
        let dir = Path::new("/prompts");
        for location in &PromptTemplateLocation::ALL {
            let path = location.resolve(dir);
            let parsed = PromptTemplateLocation::from_path(dir, &path);
            assert_eq!(
                parsed,
                Some(*location),
                "from_path should invert resolve for {}/{}",
                location.stage(),
                location.role(),
            );
        }
    }

    #[test]
    fn test_from_path_ignores_swap_files() {
        let dir = Path::new("/prompts");
        let cases = [
            "extraction/system.tera.swp",
            "extraction/.system.tera.swp",
            "extraction/system.tera~",
            "extraction/system.tmp",
            "extraction/#system.tera#",
        ];
        for case in &cases {
            let path = dir.join(case);
            assert!(
                PromptTemplateLocation::from_path(dir, &path).is_none(),
                "should ignore {case}",
            );
        }
    }

    #[test]
    fn test_from_path_ignores_invalid_stage() {
        let dir = Path::new("/prompts");
        let path = dir.join("unknown/system.tera");
        assert!(PromptTemplateLocation::from_path(dir, &path).is_none());
    }

    #[test]
    fn test_from_path_ignores_invalid_role() {
        let dir = Path::new("/prompts");
        let path = dir.join("extraction/unknown.tera");
        assert!(PromptTemplateLocation::from_path(dir, &path).is_none());
    }

    #[test]
    fn test_from_path_ignores_shallow_path() {
        let dir = Path::new("/prompts");
        let path = dir.join("system.tera");
        assert!(PromptTemplateLocation::from_path(dir, &path).is_none());
    }

    #[test]
    fn test_from_path_ignores_deep_path() {
        let dir = Path::new("/prompts");
        let path = dir.join("extraction/nested/system.tera");
        assert!(PromptTemplateLocation::from_path(dir, &path).is_none());
    }

    #[tokio::test]
    async fn test_ensure_prompt_files_writes_defaults_to_tempdir() {
        let tmp = tempfile::tempdir().expect("should create tempdir");
        let prompts_dir = tmp.path();

        ensure_prompt_files(prompts_dir)
            .await
            .expect("should write defaults");

        for location in &PromptTemplateLocation::ALL {
            let file_path = location.resolve(prompts_dir);
            assert!(
                file_path.exists(),
                "expected {}/{}.tera to exist",
                location.stage(),
                location.role(),
            );

            let content = std::fs::read_to_string(&file_path).expect("should read file");
            let expected = embedded_default(*location);
            assert_eq!(
                content,
                expected,
                "content mismatch for {}/{}",
                location.stage(),
                location.role(),
            );
        }
    }

    #[tokio::test]
    async fn test_ensure_prompt_files_does_not_overwrite_existing() {
        let tmp = tempfile::tempdir().expect("should create tempdir");
        let prompts_dir = tmp.path();

        ensure_prompt_files(prompts_dir)
            .await
            .expect("initial write");

        let custom = "custom content";
        let target = PromptTemplateLocation::from((PromptStage::Extraction, PromptRole::System));
        let custom_path = target.resolve(prompts_dir);
        std::fs::write(&custom_path, custom).expect("should overwrite");

        ensure_prompt_files(prompts_dir).await.expect("second run");

        let content = std::fs::read_to_string(&custom_path).expect("should read");
        assert_eq!(content, custom, "existing file must not be overwritten");
    }
}
