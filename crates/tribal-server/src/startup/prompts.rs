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
use tribal_domain::{PromptClass, PromptRole, PromptStage, PromptVersionId};
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
const EXTRACTION_LOOP_SYSTEM: &str =
    include_str!("../../../../prompts/extraction/loop_system.tera");
const EXTRACTION_LOOP_USER: &str = include_str!("../../../../prompts/extraction/loop_user.tera");
const TRIAGE_LOOP_SYSTEM: &str = include_str!("../../../../prompts/triage/loop_system.tera");
const TRIAGE_LOOP_USER: &str = include_str!("../../../../prompts/triage/loop_user.tera");
const RELATION_LOOP_SYSTEM: &str = include_str!("../../../../prompts/relation/loop_system.tera");
const RELATION_LOOP_USER: &str = include_str!("../../../../prompts/relation/loop_user.tera");
const TRIAGE_VERIFIER_SYSTEM: &str =
    include_str!("../../../../prompts/triage/verifier_system.tera");
const TRIAGE_VERIFIER_USER: &str = include_str!("../../../../prompts/triage/verifier_user.tera");

/// Returns the embedded default content for a given location.
///
/// Nested per axis: the class arm owns which stages it serves, so a
/// class that gains a stage extends one arm here and the vocabulary in
/// [`PromptTemplateLocation::ALL`]. A pairing outside the vocabulary
/// (constructible from a raw tuple) is a clean error, never a panic.
fn embedded_default(location: PromptTemplateLocation) -> Result<&'static str, AppError> {
    let slot_error = || AppError::PromptValidation {
        context: format!(
            "no embedded default exists for {}/{}/{}",
            location.stage.as_str(),
            location.class.as_str(),
            location.role.as_str(),
        ),
    };
    match location.class {
        PromptClass::OneShot => Ok(match (location.stage, location.role) {
            (PromptStage::Extraction, PromptRole::System) => EXTRACTION_SYSTEM,
            (PromptStage::Extraction, PromptRole::User) => EXTRACTION_USER,
            (PromptStage::Triage, PromptRole::System) => TRIAGE_SYSTEM,
            (PromptStage::Triage, PromptRole::User) => TRIAGE_USER,
            (PromptStage::Relation, PromptRole::System) => RELATION_SYSTEM,
            (PromptStage::Relation, PromptRole::User) => RELATION_USER,
        }),
        PromptClass::Loop => Ok(match (location.stage, location.role) {
            (PromptStage::Extraction, PromptRole::System) => EXTRACTION_LOOP_SYSTEM,
            (PromptStage::Extraction, PromptRole::User) => EXTRACTION_LOOP_USER,
            (PromptStage::Triage, PromptRole::System) => TRIAGE_LOOP_SYSTEM,
            (PromptStage::Triage, PromptRole::User) => TRIAGE_LOOP_USER,
            (PromptStage::Relation, PromptRole::System) => RELATION_LOOP_SYSTEM,
            (PromptStage::Relation, PromptRole::User) => RELATION_LOOP_USER,
        }),
        PromptClass::Verifier => match (location.stage, location.role) {
            (PromptStage::Triage, PromptRole::System) => Ok(TRIAGE_VERIFIER_SYSTEM),
            (PromptStage::Triage, PromptRole::User) => Ok(TRIAGE_VERIFIER_USER),
            (PromptStage::Extraction | PromptStage::Relation, _) => Err(slot_error()),
        },
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
    class: PromptClass,
    role: PromptRole,
}

impl PromptTemplateLocation {
    /// Every template slot in canonical order: the single authority on
    /// which (stage, class) pairings exist. The loop class serves every
    /// stage, the verifier class triage only in this release; admitting
    /// another stage is an addition here, never a new match arm elsewhere.
    pub(crate) const ALL: [Self; 14] = [
        Self::one_shot(PromptStage::Extraction, PromptRole::System),
        Self::one_shot(PromptStage::Extraction, PromptRole::User),
        Self::one_shot(PromptStage::Triage, PromptRole::System),
        Self::one_shot(PromptStage::Triage, PromptRole::User),
        Self::one_shot(PromptStage::Relation, PromptRole::System),
        Self::one_shot(PromptStage::Relation, PromptRole::User),
        Self::new(
            PromptStage::Extraction,
            PromptClass::Loop,
            PromptRole::System,
        ),
        Self::new(PromptStage::Extraction, PromptClass::Loop, PromptRole::User),
        Self::new(PromptStage::Triage, PromptClass::Loop, PromptRole::System),
        Self::new(PromptStage::Triage, PromptClass::Loop, PromptRole::User),
        Self::new(PromptStage::Relation, PromptClass::Loop, PromptRole::System),
        Self::new(PromptStage::Relation, PromptClass::Loop, PromptRole::User),
        Self::new(
            PromptStage::Triage,
            PromptClass::Verifier,
            PromptRole::System,
        ),
        Self::new(PromptStage::Triage, PromptClass::Verifier, PromptRole::User),
    ];

    /// Builds a location.
    pub(crate) const fn new(stage: PromptStage, class: PromptClass, role: PromptRole) -> Self {
        Self { stage, class, role }
    }

    /// Builds a launched one-shot location.
    pub(crate) const fn one_shot(stage: PromptStage, role: PromptRole) -> Self {
        Self::new(stage, PromptClass::OneShot, role)
    }

    /// Returns the pipeline stage.
    pub(crate) fn stage(self) -> PromptStage {
        self.stage
    }

    /// Returns the executor class.
    pub(crate) fn class(self) -> PromptClass {
        self.class
    }

    /// Returns the prompt role.
    pub(crate) fn role(self) -> PromptRole {
        self.role
    }

    /// Returns the file stem encoding this slot: the launched one-shot
    /// pair keeps its bare role names, other classes prefix theirs.
    /// [`Self::from_path`] is the inverse; the stem grammar lives only
    /// in these two functions.
    fn file_stem(self) -> String {
        match self.class {
            PromptClass::OneShot => self.role.as_str().to_owned(),
            PromptClass::Loop | PromptClass::Verifier => {
                format!("{}_{}", self.class.as_str(), self.role.as_str())
            }
        }
    }

    /// Resolves the full file path under the given prompts directory.
    pub(crate) fn resolve(self, prompts_dir: &Path) -> PathBuf {
        prompts_dir
            .join(self.stage.as_str())
            .join(format!("{}.{PROMPT_FILE_EXTENSION}", self.file_stem()))
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
        let (class, role) = match role_str.split_once('_') {
            None => (PromptClass::OneShot, PromptRole::from_str(role_str).ok()?),
            Some((class_str, rest)) => (
                PromptClass::from_str(class_str).ok()?,
                PromptRole::from_str(rest).ok()?,
            ),
        };

        // Membership in the vocabulary is the guard: a stray file pairing
        // a stage with a class it does not serve parses but is not a
        // recognised template.
        let location = Self { stage, class, role };
        Self::ALL.contains(&location).then_some(location)
    }
}

impl From<(PromptStage, PromptClass, PromptRole)> for PromptTemplateLocation {
    fn from((stage, class, role): (PromptStage, PromptClass, PromptRole)) -> Self {
        Self { stage, class, role }
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
            let content = embedded_default(*location)?;
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

/// Hashes and upserts the embedded prompts compiled into the binary.
///
/// Used when [`PromptSource::Embedded`](tribal_config::PromptSource::Embedded)
/// is in effect: no filesystem IO, no user-editable files. The on-disk
/// equivalent is [`load_prompts`].
pub(crate) async fn load_prompts_embedded(pool: &PgPool) -> Result<ActivePromptVersions, AppError> {
    let contents = PromptTemplateLocation::ALL
        .into_iter()
        .map(|location| Ok((location, embedded_default(location)?.to_owned())))
        .collect::<Result<Vec<_>, AppError>>()?;
    upsert_prompt_versions(pool, contents).await
}

/// Removes a launched one-shot slot's version from the loaded map.
fn take_launched(
    versions: &mut HashMap<PromptTemplateLocation, PromptVersionId>,
    stage: PromptStage,
    role: PromptRole,
) -> PromptVersionId {
    versions
        .remove(&PromptTemplateLocation::one_shot(stage, role))
        .expect(EXPECT_VERSION)
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

    let mut versions: HashMap<PromptTemplateLocation, PromptVersionId> =
        HashMap::with_capacity(PromptTemplateLocation::ALL.len());

    for (location, content) in contents {
        let content_hash = sha256_hex(&content);
        let stage = location.stage();
        let role = location.role();

        let new = NewPromptVersion::builder()
            .stage(stage)
            .class(location.class())
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
            class = location.class().as_str(),
            role = role.as_str(),
            version_id = %version.id(),
            "loaded prompt version",
        );

        versions.insert(location, version.id());
    }

    let mut active = ActivePromptVersions::new(
        take_launched(&mut versions, PromptStage::Extraction, PromptRole::System),
        take_launched(&mut versions, PromptStage::Extraction, PromptRole::User),
        take_launched(&mut versions, PromptStage::Triage, PromptRole::System),
        take_launched(&mut versions, PromptStage::Triage, PromptRole::User),
        take_launched(&mut versions, PromptStage::Relation, PromptRole::System),
        take_launched(&mut versions, PromptStage::Relation, PromptRole::User),
    );
    for (location, id) in versions {
        active.set_version(location.stage(), location.class(), location.role(), id);
    }
    Ok(active)
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
            let content = embedded_default(*location).expect("every vocabulary slot has a default");
            assert!(
                !content.is_empty(),
                "embedded default for {}/{}/{} is empty",
                location.stage(),
                location.class(),
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

        let one_shot: HashSet<(PromptStage, PromptRole)> = PromptTemplateLocation::ALL
            .iter()
            .filter(|l| l.class() == PromptClass::OneShot)
            .map(|l| (l.stage(), l.role()))
            .collect();

        assert_eq!(
            one_shot, expected,
            "the one-shot class must cover all stage×role combinations"
        );
        let class_coverage = |class: PromptClass| -> HashSet<(PromptStage, PromptRole)> {
            PromptTemplateLocation::ALL
                .iter()
                .filter(|l| l.class() == class)
                .map(|l| (l.stage(), l.role()))
                .collect()
        };
        // The loop class serves every stage; the verifier class serves
        // triage only in this release.
        assert_eq!(
            class_coverage(PromptClass::Loop),
            expected,
            "the loop class serves every stage×role combination",
        );
        assert_eq!(
            class_coverage(PromptClass::Verifier),
            HashSet::from([
                (PromptStage::Triage, PromptRole::System),
                (PromptStage::Triage, PromptRole::User),
            ]),
            "the verifier class serves the triage pair in this release",
        );
        assert_eq!(
            PromptTemplateLocation::ALL.len(),
            14,
            "ALL must contain exactly 14 entries"
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
                "from_path should invert resolve for {}/{}/{}",
                location.stage(),
                location.class(),
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
                location.file_stem(),
            );

            let content = std::fs::read_to_string(&file_path).expect("should read file");
            let expected =
                embedded_default(*location).expect("every vocabulary slot has a default");
            assert_eq!(
                content,
                expected,
                "content mismatch for {}/{}/{}",
                location.stage(),
                location.class(),
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
        let target = PromptTemplateLocation::one_shot(PromptStage::Extraction, PromptRole::System);
        let custom_path = target.resolve(prompts_dir);
        std::fs::write(&custom_path, custom).expect("should overwrite");

        ensure_prompt_files(prompts_dir).await.expect("second run");

        let content = std::fs::read_to_string(&custom_path).expect("should read");
        assert_eq!(content, custom, "existing file must not be overwritten");
    }

    #[tokio::test]
    async fn test_embedded_and_disk_paths_produce_equal_version_ids() {
        use tribal_test_utils::{serial_lock, test_context, truncate_all_tables};

        let _guard = serial_lock().await;
        let ctx = test_context().await;
        let pool = ctx.create_pool().await.expect("create pool");

        let mut conn = pool.acquire().await.expect("acquire connection");
        truncate_all_tables(&mut conn).await;
        drop(conn);

        // `ensure_prompt_files` writes the embedded defaults to disk, so
        // both paths receive byte-identical content. Equal `content_hash`
        // values yield equal `PromptVersionId`s through the upsert.
        let prompts_dir = tempfile::tempdir().expect("create prompts dir");
        ensure_prompt_files(prompts_dir.path())
            .await
            .expect("write defaults");

        let embedded = load_prompts_embedded(&pool)
            .await
            .expect("load embedded prompts");
        let disk = load_prompts(&pool, prompts_dir.path())
            .await
            .expect("load disk prompts");

        for location in &PromptTemplateLocation::ALL {
            let stage = location.stage();
            let class = location.class();
            let role = location.role();
            assert_eq!(
                embedded.get_version(stage, class, role),
                disk.get_version(stage, class, role),
                "embedded and disk paths must produce equal version IDs for \
                 {stage}/{class}/{role}",
            );
        }
    }
}
