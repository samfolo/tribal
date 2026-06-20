//! Template validation and single-prompt reload.

use std::{path::Path, sync::Arc};

use sqlx::PgPool;
use tokio::sync::RwLock;
use tracing::{info, warn};
use tribal_common::sha256_hex;
use tribal_db::{NewPromptVersion, PgPromptVersionRepository, PromptVersionRepository};
use tribal_domain::{PromptClass, PromptRole, PromptStage};
use tribal_mcp::ActivePromptVersions;
use tribal_worker::{reserved_keys, synthetic_validation_context};

use super::constants::{
    LOG_PROMPT_READ_FAILED, LOG_PROMPT_RELOADED, LOG_PROMPT_UPSERT_FAILED,
    LOG_PROMPT_VALIDATION_FAILED,
};
use crate::startup::PromptTemplateLocation;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

pub(crate) const VALIDATION_EMPTY_CONTENT: &str = "prompt content must not be empty";
pub(crate) const VALIDATION_MISSING_REFERENCE: &str =
    "prompt content does not reference required variable";
pub(crate) const VALIDATION_CONTEXT_PANIC: &str =
    "synthetic validation context construction panicked";

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

/// Validates that a prompt template can render against the production
/// context shape and references all required variables.
///
/// Three checks:
/// 1. Non-empty content.
/// 2. Successful Tera render against the full synthetic context (catches
///    syntax errors, unknown variables, and type mismatches).
/// 3. For each top-level context variable, render with that variable
///    removed — if the render still succeeds, the template does not
///    actually reference it (prevents silently dropping required inputs).
///
/// The required variable set is derived from the synthetic context's
/// keys — the same builders production uses — so it is self-maintaining.
pub(crate) fn validate_prompt_template(
    stage: PromptStage,
    class: PromptClass,
    role: PromptRole,
    content: &str,
) -> Result<(), String> {
    if content.trim().is_empty() {
        return Err(VALIDATION_EMPTY_CONTENT.to_owned());
    }

    // synthetic_validation_context panics only if the hardcoded JSON
    // cannot deserialise into domain types — a programming error.
    // Catching the unwind preserves the "watcher never crashes" contract.
    let Ok(tera_ctx) =
        std::panic::catch_unwind(|| synthetic_validation_context(stage, class, role))
    else {
        return Err(VALIDATION_CONTEXT_PANIC.to_owned());
    };

    let reserved = reserved_keys();

    let Err(error) = tera::Tera::one_off(content, &tera_ctx, false) else {
        // Full render succeeded. Now verify every required variable is
        // actually referenced by rendering with each key removed in turn.
        // Reserved variables are excluded — they are available in all
        // prompts but only referenced by user templates.
        for key in context_keys(&tera_ctx) {
            if reserved.contains(&key.as_str()) {
                continue;
            }
            let reduced = context_without_key(&tera_ctx, &key);
            if tera::Tera::one_off(content, &reduced, false).is_ok() {
                return Err(format!("{VALIDATION_MISSING_REFERENCE}: {key}"));
            }
        }
        return Ok(());
    };

    Err(error.to_string())
}

/// Extracts the top-level variable names from a Tera context.
fn context_keys(ctx: &tera::Context) -> Vec<String> {
    let json = ctx.clone().into_json();
    match json {
        serde_json::Value::Object(map) => map.keys().cloned().collect(),
        _ => vec![],
    }
}

/// Returns a copy of the context with one top-level key removed.
fn context_without_key(ctx: &tera::Context, key: &str) -> tera::Context {
    let serde_json::Value::Object(mut map) = ctx.clone().into_json() else {
        return ctx.clone();
    };
    map.remove(key);
    tera::Context::from_value(serde_json::Value::Object(map))
        .expect("reduced context is valid JSON")
}

/// Reloads a single prompt version.
///
/// All errors are logged and swallowed — the watcher never crashes.
pub(crate) async fn reload_single_prompt(
    location: PromptTemplateLocation,
    file_path: &Path,
    pool: &PgPool,
    active_prompt_versions: &Arc<RwLock<ActivePromptVersions>>,
) {
    let stage = location.stage();
    let class = location.class();
    let role = location.role();

    // -- Read ----------------------------------------------------------------

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

    // -- Validate ------------------------------------------------------------

    if let Err(reason) = validate_prompt_template(stage, class, role, &content) {
        warn!(
            %reason,
            stage = stage.as_str(),
            role = role.as_str(),
            path = %file_path.display(),
            LOG_PROMPT_VALIDATION_FAILED,
        );
        return;
    }

    // -- Upsert --------------------------------------------------------------

    let content_hash = sha256_hex(&content);

    let mut conn = match pool.acquire().await {
        Ok(c) => c,
        Err(error) => {
            warn!(
                %error,
                stage = stage.as_str(),
                role = role.as_str(),
                path = %file_path.display(),
                LOG_PROMPT_UPSERT_FAILED,
            );
            return;
        }
    };

    let repo = PgPromptVersionRepository;
    let new = NewPromptVersion::builder()
        .stage(stage)
        .class(class)
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
                path = %file_path.display(),
                LOG_PROMPT_UPSERT_FAILED,
            );
            return;
        }
    };

    // -- Swap ----------------------------------------------------------------

    let current_id = active_prompt_versions
        .read()
        .await
        .get_version(stage, class, role);
    if current_id == Some(version.id()) {
        return;
    }

    active_prompt_versions
        .write()
        .await
        .set_version(stage, class, role, version.id());

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
        let result = validate_prompt_template(
            PromptStage::Extraction,
            PromptClass::OneShot,
            PromptRole::System,
            "",
        );
        assert_eq!(result.unwrap_err(), VALIDATION_EMPTY_CONTENT);
    }

    #[test]
    fn test_validate_prompt_template_rejects_whitespace_only() {
        let result = validate_prompt_template(
            PromptStage::Extraction,
            PromptClass::OneShot,
            PromptRole::System,
            "   \n\t  ",
        );
        assert_eq!(result.unwrap_err(), VALIDATION_EMPTY_CONTENT);
    }

    #[test]
    fn test_validate_prompt_template_rejects_dropped_required_variable() {
        let result = validate_prompt_template(
            PromptStage::Extraction,
            PromptClass::OneShot,
            PromptRole::User,
            "Static text with no variable references",
        );
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.starts_with(VALIDATION_MISSING_REFERENCE),
            "expected missing reference error, got: {err}",
        );
    }

    #[test]
    fn test_validate_prompt_template_rejects_partial_variable_drop() {
        // Uses tags but drops raw_input — should fail for extraction/user.
        let result = validate_prompt_template(
            PromptStage::Extraction,
            PromptClass::OneShot,
            PromptRole::User,
            "{{ tags }} but no raw input reference",
        );
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.contains("raw_input"),
            "error should name the missing variable, got: {err}",
        );
    }

    #[test]
    fn test_validate_prompt_template_rejects_syntax_error() {
        // The unclosed block triggers a Tera parse error regardless of
        // which variables are available.
        let result = validate_prompt_template(
            PromptStage::Extraction,
            PromptClass::OneShot,
            PromptRole::System,
            "{% if true %}unclosed",
        );
        let err = result.unwrap_err();
        assert_ne!(err, VALIDATION_EMPTY_CONTENT);
        assert!(
            !err.starts_with(VALIDATION_MISSING_REFERENCE),
            "should be a Tera parse error, not a reference error: {err}",
        );
    }

    #[test]
    fn test_validate_prompt_template_rejects_unknown_variable() {
        // The unknown variable triggers a Tera render error regardless
        // of which variables are available.
        let result = validate_prompt_template(
            PromptStage::Extraction,
            PromptClass::OneShot,
            PromptRole::System,
            "{{ nonexistent }}",
        );
        let err = result.unwrap_err();
        assert!(
            !err.starts_with(VALIDATION_MISSING_REFERENCE),
            "should be a Tera render error, not a reference error: {err}",
        );
    }

    #[test]
    fn test_validate_prompt_template_rejects_comment_only_reference() {
        let result = validate_prompt_template(
            PromptStage::Extraction,
            PromptClass::OneShot,
            PromptRole::User,
            "{# raw_input #}\n{{ tags }}",
        );
        let err = result.unwrap_err();
        assert!(
            err.contains("raw_input"),
            "should reject comment-only reference: {err}",
        );
    }

    #[test]
    fn test_validate_prompt_template_rejects_prose_only_reference() {
        let result = validate_prompt_template(
            PromptStage::Triage,
            PromptClass::OneShot,
            PromptRole::User,
            "Consider the candidate carefully.\n{{ similar_items }} {{ tags }}",
        );
        let err = result.unwrap_err();
        assert!(
            err.contains("candidate"),
            "should reject prose-only reference: {err}",
        );
    }

    #[test]
    fn test_validate_prompt_template_rejects_misspelled_nested_path() {
        // Includes all required top-level variables so the reference
        // check passes; Tera rejects the invalid nested path.
        let result = validate_prompt_template(
            PromptStage::Triage,
            PromptClass::OneShot,
            PromptRole::User,
            "{{ candidate.nmae }} {{ similar_items }} {{ tags }}",
        );
        let err = result.unwrap_err();
        assert!(
            !err.starts_with(VALIDATION_MISSING_REFERENCE),
            "should be a Tera render error, not a reference error: {err}",
        );
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

        let stage = PromptStage::Extraction;
        let role = PromptRole::System;
        let target = PromptTemplateLocation::one_shot(stage, role);
        let file_path = target.resolve(prompts_dir.path());

        let id_before = active
            .read()
            .await
            .get_version(stage, PromptClass::OneShot, role);

        let original = tokio::fs::read_to_string(&file_path)
            .await
            .expect("read original");
        let modified = format!("{original}\n{{# hot-reload test #}}");
        tokio::fs::write(&file_path, &modified)
            .await
            .expect("write modified prompt");

        reload_single_prompt(target, &file_path, &pool, &active).await;

        let id_after = active
            .read()
            .await
            .get_version(stage, PromptClass::OneShot, role);

        assert_ne!(id_before, id_after, "version ID should change after reload");
    }

    #[tokio::test]
    async fn test_reload_same_content_is_idempotent() {
        let (active, prompts_dir, pool, _guard) = reload_test_harness().await;

        let stage = PromptStage::Extraction;
        let role = PromptRole::System;
        let target = PromptTemplateLocation::one_shot(stage, role);
        let file_path = target.resolve(prompts_dir.path());

        let id_before = active
            .read()
            .await
            .get_version(stage, PromptClass::OneShot, role);

        reload_single_prompt(target, &file_path, &pool, &active).await;

        let id_after = active
            .read()
            .await
            .get_version(stage, PromptClass::OneShot, role);

        assert_eq!(
            id_before, id_after,
            "identical content should not change the version ID",
        );
    }

    #[tokio::test]
    async fn test_reload_empty_content_keeps_current_version() {
        let (active, prompts_dir, pool, _guard) = reload_test_harness().await;

        let stage = PromptStage::Triage;
        let role = PromptRole::User;
        let target = PromptTemplateLocation::one_shot(stage, role);
        let file_path = target.resolve(prompts_dir.path());
        tokio::fs::write(&file_path, "")
            .await
            .expect("write empty content");

        let id_before = active
            .read()
            .await
            .get_version(stage, PromptClass::OneShot, role);

        reload_single_prompt(target, &file_path, &pool, &active).await;

        let id_after = active
            .read()
            .await
            .get_version(stage, PromptClass::OneShot, role);

        assert_eq!(
            id_before, id_after,
            "empty content should not change the version ID",
        );
    }

    #[tokio::test]
    async fn test_reload_invalid_template_keeps_current_version() {
        let (active, prompts_dir, pool, _guard) = reload_test_harness().await;

        let stage = PromptStage::Extraction;
        let role = PromptRole::System;
        let target = PromptTemplateLocation::one_shot(stage, role);
        let file_path = target.resolve(prompts_dir.path());
        tokio::fs::write(&file_path, "{{ nonexistent_variable }}")
            .await
            .expect("write invalid template");

        let id_before = active
            .read()
            .await
            .get_version(stage, PromptClass::OneShot, role);

        reload_single_prompt(target, &file_path, &pool, &active).await;

        let id_after = active
            .read()
            .await
            .get_version(stage, PromptClass::OneShot, role);

        assert_eq!(
            id_before, id_after,
            "invalid template should not change the version ID",
        );
    }

    #[tokio::test]
    async fn test_reload_updates_and_is_idempotent() {
        let (active, prompts_dir, pool, _guard) = reload_test_harness().await;

        let stage = PromptStage::Relation;
        let role = PromptRole::User;
        let target = PromptTemplateLocation::one_shot(stage, role);
        let file_path = target.resolve(prompts_dir.path());
        let original = tokio::fs::read_to_string(&file_path)
            .await
            .expect("read original");
        let modified = format!("{original}\n{{# idempotency test #}}");
        tokio::fs::write(&file_path, &modified)
            .await
            .expect("write modified prompt");

        let id_before = active
            .read()
            .await
            .get_version(stage, PromptClass::OneShot, role);

        reload_single_prompt(target, &file_path, &pool, &active).await;

        let id_after = active
            .read()
            .await
            .get_version(stage, PromptClass::OneShot, role);

        assert_ne!(id_before, id_after, "version ID should change after reload");

        // Second reload of same content should be idempotent.
        reload_single_prompt(target, &file_path, &pool, &active).await;

        let id_third = active
            .read()
            .await
            .get_version(stage, PromptClass::OneShot, role);
        assert_eq!(
            id_after, id_third,
            "second reload of same content should be idempotent",
        );
    }

    // -- validate_prompt_template (embedded defaults) -----------------------

    #[test]
    fn test_validate_prompt_template_accepts_embedded_defaults() {
        let pairs: [(PromptStage, PromptClass, PromptRole, &str); 16] = [
            (
                PromptStage::Extraction,
                PromptClass::OneShot,
                PromptRole::System,
                include_str!("../../../../../prompts/extraction/system.tera"),
            ),
            (
                PromptStage::Extraction,
                PromptClass::OneShot,
                PromptRole::User,
                include_str!("../../../../../prompts/extraction/user.tera"),
            ),
            (
                PromptStage::Triage,
                PromptClass::OneShot,
                PromptRole::System,
                include_str!("../../../../../prompts/triage/system.tera"),
            ),
            (
                PromptStage::Triage,
                PromptClass::OneShot,
                PromptRole::User,
                include_str!("../../../../../prompts/triage/user.tera"),
            ),
            (
                PromptStage::Relation,
                PromptClass::OneShot,
                PromptRole::System,
                include_str!("../../../../../prompts/relation/system.tera"),
            ),
            (
                PromptStage::Relation,
                PromptClass::OneShot,
                PromptRole::User,
                include_str!("../../../../../prompts/relation/user.tera"),
            ),
            (
                PromptStage::Extraction,
                PromptClass::Loop,
                PromptRole::System,
                include_str!("../../../../../prompts/extraction/loop_system.tera"),
            ),
            (
                PromptStage::Extraction,
                PromptClass::Loop,
                PromptRole::User,
                include_str!("../../../../../prompts/extraction/loop_user.tera"),
            ),
            (
                PromptStage::Triage,
                PromptClass::Loop,
                PromptRole::System,
                include_str!("../../../../../prompts/triage/loop_system.tera"),
            ),
            (
                PromptStage::Triage,
                PromptClass::Loop,
                PromptRole::User,
                include_str!("../../../../../prompts/triage/loop_user.tera"),
            ),
            (
                PromptStage::Relation,
                PromptClass::Loop,
                PromptRole::System,
                include_str!("../../../../../prompts/relation/loop_system.tera"),
            ),
            (
                PromptStage::Relation,
                PromptClass::Loop,
                PromptRole::User,
                include_str!("../../../../../prompts/relation/loop_user.tera"),
            ),
            (
                PromptStage::Triage,
                PromptClass::Verifier,
                PromptRole::System,
                include_str!("../../../../../prompts/triage/verifier_system.tera"),
            ),
            (
                PromptStage::Triage,
                PromptClass::Verifier,
                PromptRole::User,
                include_str!("../../../../../prompts/triage/verifier_user.tera"),
            ),
            (
                PromptStage::Relation,
                PromptClass::Verifier,
                PromptRole::System,
                include_str!("../../../../../prompts/relation/verifier_system.tera"),
            ),
            (
                PromptStage::Relation,
                PromptClass::Verifier,
                PromptRole::User,
                include_str!("../../../../../prompts/relation/verifier_user.tera"),
            ),
        ];
        for (stage, class, role, content) in &pairs {
            let result = validate_prompt_template(*stage, *class, *role, content);
            assert!(
                result.is_ok(),
                "embedded default for {stage}/{class}/{role} failed validation: {}",
                result.unwrap_err(),
            );
        }
    }
}
