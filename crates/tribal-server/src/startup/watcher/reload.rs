//! Template validation and single-prompt reload.

use std::{path::Path, sync::Arc};

use sqlx::PgPool;
use tokio::sync::RwLock;
use tracing::{info, warn};
use tribal_common::sha256_hex;
use tribal_db::{NewPromptVersion, PgPromptVersionRepository, PromptVersionRepository};
use tribal_domain::{PromptRole, PromptStage};
use tribal_mcp::ActivePromptVersions;
use tribal_worker::synthetic_validation_context;

use super::{
    LOG_PROMPT_READ_FAILED, LOG_PROMPT_RELOADED, LOG_PROMPT_UPSERT_FAILED,
    LOG_PROMPT_VALIDATION_FAILED,
};
use crate::startup::PromptTemplateLocation;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

pub(crate) const VALIDATION_EMPTY_CONTENT: &str = "prompt content must not be empty";

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

/// Validates that a prompt template can render against the production
/// context shape.
///
/// Delegates to the same context builders that the production
/// `assemble_*_prompt` functions use, via
/// [`tribal_worker::synthetic_validation_context`].
pub(crate) fn validate_prompt_template(
    stage: PromptStage,
    role: PromptRole,
    content: &str,
) -> Result<(), String> {
    if content.trim().is_empty() {
        return Err(VALIDATION_EMPTY_CONTENT.to_owned());
    }

    let tera_ctx = synthetic_validation_context(stage, role);

    let Err(error) = tera::Tera::one_off(content, &tera_ctx, false) else {
        return Ok(());
    };

    Err(error.to_string())
}

// ---------------------------------------------------------------------------
// Single-prompt reload
// ---------------------------------------------------------------------------

/// Reads, validates, hashes, upserts, and swaps a single prompt version.
///
/// All errors are logged and swallowed — the watcher never crashes.
pub(crate) async fn reload_single_prompt(
    location: PromptTemplateLocation,
    file_path: &Path,
    pool: &PgPool,
    active_prompt_versions: &Arc<RwLock<ActivePromptVersions>>,
) {
    let stage = location.stage();
    let role = location.role();

    let content = match tokio::fs::read_to_string(file_path).await {
        Ok(c) => c,
        Err(error) => {
            warn!(
                %error,
                stage = stage.as_str(),
                role = role.as_str(),
                path = %file_path.display(),
                LOG_PROMPT_READ_FAILED,
            );
            return;
        }
    };

    if let Err(reason) = validate_prompt_template(stage, role, &content) {
        warn!(
            %reason,
            stage = stage.as_str(),
            role = role.as_str(),
            path = %file_path.display(),
            LOG_PROMPT_VALIDATION_FAILED,
        );
        return;
    }

    let content_hash = sha256_hex(&content);

    let mut conn = match pool.acquire().await {
        Ok(c) => c,
        Err(error) => {
            warn!(
                context = "acquire connection for prompt reload",
                %error,
                LOG_PROMPT_UPSERT_FAILED,
            );
            return;
        }
    };

    let repo = PgPromptVersionRepository;
    let new = NewPromptVersion::builder()
        .stage(stage)
        .role(role)
        .content_hash(content_hash)
        .content(content)
        .build();

    let version = match repo.upsert(&mut conn, &new).await {
        Ok(v) => v,
        Err(error) => {
            warn!(
                %error,
                stage = stage.as_str(),
                role = role.as_str(),
                LOG_PROMPT_UPSERT_FAILED,
            );
            return;
        }
    };

    active_prompt_versions
        .write()
        .await
        .set_version(stage, role, version.id());

    info!(
        stage = stage.as_str(),
        role = role.as_str(),
        version_id = %version.id(),
        LOG_PROMPT_RELOADED,
    );
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::sync::RwLock;
    use tribal_mcp::ActivePromptVersions;
    use tribal_test_utils::{serial_lock, test_context, truncate_all_tables};

    use super::*;
    use crate::startup::{PromptTemplateLocation, ensure_prompt_files, load_prompts};

    #[test]
    fn test_validate_prompt_template_rejects_empty() {
        let result = validate_prompt_template(PromptStage::Extraction, PromptRole::System, "");
        assert_eq!(result.unwrap_err(), VALIDATION_EMPTY_CONTENT);
    }

    #[test]
    fn test_validate_prompt_template_rejects_whitespace_only() {
        let result =
            validate_prompt_template(PromptStage::Extraction, PromptRole::System, "   \n\t  ");
        assert_eq!(result.unwrap_err(), VALIDATION_EMPTY_CONTENT);
    }

    #[test]
    fn test_validate_prompt_template_rejects_syntax_error() {
        let result = validate_prompt_template(
            PromptStage::Extraction,
            PromptRole::System,
            "{% if true %}unclosed",
        );
        assert!(result.is_err());
        assert_ne!(result.unwrap_err(), VALIDATION_EMPTY_CONTENT);
    }

    #[test]
    fn test_validate_prompt_template_rejects_unknown_variable() {
        let result = validate_prompt_template(
            PromptStage::Extraction,
            PromptRole::System,
            "{{ nonexistent }}",
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_prompt_template_rejects_misspelled_nested_path() {
        let result = validate_prompt_template(
            PromptStage::Triage,
            PromptRole::User,
            "{{ candidate.nmae }}",
        );
        assert!(result.is_err());
    }

    // -- reload_single_prompt -----------------------------------------------

    async fn reload_test_harness() -> (
        Arc<RwLock<ActivePromptVersions>>,
        tempfile::TempDir,
        sqlx::PgPool,
        tokio::sync::MutexGuard<'static, ()>,
    ) {
        let guard = serial_lock().await;
        let ctx = test_context().await;
        let pool = ctx.create_pool().await.expect("create per-test pool");

        let mut conn = pool.acquire().await.expect("acquire connection");
        truncate_all_tables(&mut conn).await;
        drop(conn);

        let prompts_dir = tempfile::tempdir().expect("create prompts tempdir");

        ensure_prompt_files(prompts_dir.path())
            .await
            .expect("write default prompts");

        let initial = load_prompts(&pool, prompts_dir.path())
            .await
            .expect("load initial prompts");

        let active = Arc::new(RwLock::new(initial));

        (active, prompts_dir, pool, guard)
    }

    #[tokio::test]
    async fn test_reload_updates_active_prompt_version() {
        let (active, prompts_dir, pool, _guard) = reload_test_harness().await;

        let target = PromptTemplateLocation::from((PromptStage::Extraction, PromptRole::System));
        let file_path = target.resolve(prompts_dir.path());

        let original = tokio::fs::read_to_string(&file_path)
            .await
            .expect("read original");
        let modified = format!("{original}\n{{# hot-reload test #}}");
        tokio::fs::write(&file_path, &modified)
            .await
            .expect("write modified prompt");

        let snapshot_before = format!("{:?}", *active.read().await);

        reload_single_prompt(target, &file_path, &pool, &active).await;

        let snapshot_after = format!("{:?}", *active.read().await);

        assert_ne!(
            snapshot_before, snapshot_after,
            "active prompt versions should change after reload",
        );
    }

    #[tokio::test]
    async fn test_reload_same_content_is_idempotent() {
        let (active, prompts_dir, pool, _guard) = reload_test_harness().await;

        let target = PromptTemplateLocation::from((PromptStage::Extraction, PromptRole::System));
        let file_path = target.resolve(prompts_dir.path());

        let snapshot_before = format!("{:?}", *active.read().await);

        reload_single_prompt(target, &file_path, &pool, &active).await;

        let snapshot_after = format!("{:?}", *active.read().await);

        assert_eq!(
            snapshot_before, snapshot_after,
            "identical content should not change the active version",
        );
    }

    #[tokio::test]
    async fn test_reload_empty_content_keeps_current_version() {
        let (active, prompts_dir, pool, _guard) = reload_test_harness().await;

        let target = PromptTemplateLocation::from((PromptStage::Triage, PromptRole::User));
        let file_path = target.resolve(prompts_dir.path());
        tokio::fs::write(&file_path, "")
            .await
            .expect("write empty content");

        let snapshot_before = format!("{:?}", *active.read().await);

        reload_single_prompt(target, &file_path, &pool, &active).await;

        let snapshot_after = format!("{:?}", *active.read().await);

        assert_eq!(
            snapshot_before, snapshot_after,
            "empty content should not change the active version",
        );
    }

    #[tokio::test]
    async fn test_reload_invalid_template_keeps_current_version() {
        let (active, prompts_dir, pool, _guard) = reload_test_harness().await;

        let target = PromptTemplateLocation::from((PromptStage::Extraction, PromptRole::System));
        let file_path = target.resolve(prompts_dir.path());
        tokio::fs::write(&file_path, "{{ nonexistent_variable }}")
            .await
            .expect("write invalid template");

        let snapshot_before = format!("{:?}", *active.read().await);

        reload_single_prompt(target, &file_path, &pool, &active).await;

        let snapshot_after = format!("{:?}", *active.read().await);

        assert_eq!(
            snapshot_before, snapshot_after,
            "invalid template should not change the active version",
        );
    }

    #[tokio::test]
    async fn test_reload_only_affects_targeted_prompt() {
        let (active, prompts_dir, pool, _guard) = reload_test_harness().await;

        let target = PromptTemplateLocation::from((PromptStage::Relation, PromptRole::User));
        let file_path = target.resolve(prompts_dir.path());
        let original = tokio::fs::read_to_string(&file_path)
            .await
            .expect("read original");
        let modified = format!("{original}\n{{# isolation test #}}");
        tokio::fs::write(&file_path, &modified)
            .await
            .expect("write modified prompt");

        let snapshot_before = format!("{:?}", *active.read().await);

        reload_single_prompt(target, &file_path, &pool, &active).await;

        let snapshot_after = format!("{:?}", *active.read().await);

        assert_ne!(
            snapshot_before, snapshot_after,
            "relation/user should have changed",
        );

        // Second reload of same content should be idempotent.
        reload_single_prompt(target, &file_path, &pool, &active).await;

        let snapshot_third = format!("{:?}", *active.read().await);
        assert_eq!(
            snapshot_after, snapshot_third,
            "second reload of same content should be idempotent",
        );
    }

    // -- validate_prompt_template (embedded defaults) -----------------------

    #[test]
    fn test_validate_prompt_template_accepts_embedded_defaults() {
        let pairs: [(PromptStage, PromptRole, &str); 6] = [
            (
                PromptStage::Extraction,
                PromptRole::System,
                include_str!("../../../../../prompts/extraction/system.tera"),
            ),
            (
                PromptStage::Extraction,
                PromptRole::User,
                include_str!("../../../../../prompts/extraction/user.tera"),
            ),
            (
                PromptStage::Triage,
                PromptRole::System,
                include_str!("../../../../../prompts/triage/system.tera"),
            ),
            (
                PromptStage::Triage,
                PromptRole::User,
                include_str!("../../../../../prompts/triage/user.tera"),
            ),
            (
                PromptStage::Relation,
                PromptRole::System,
                include_str!("../../../../../prompts/relation/system.tera"),
            ),
            (
                PromptStage::Relation,
                PromptRole::User,
                include_str!("../../../../../prompts/relation/user.tera"),
            ),
        ];
        for (stage, role, content) in &pairs {
            let result = validate_prompt_template(*stage, *role, content);
            assert!(
                result.is_ok(),
                "embedded default for {stage}/{role} failed validation: {}",
                result.unwrap_err(),
            );
        }
    }
}
