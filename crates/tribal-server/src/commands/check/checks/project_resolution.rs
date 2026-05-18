//! Outcome constructors and probe for the `project_resolution` check.

use tribal_domain::ProjectId;

use super::{
    context::CheckContext,
    types::{CheckDetail, CheckName, CheckOutcome, CheckRemediation},
};
use crate::{error::AppError, startup::resolve_project};

impl CheckOutcome {
    pub(in crate::commands::check) fn project_found(project_id: ProjectId, name: String) -> Self {
        Self::Pass {
            name: CheckName::ProjectResolution,
            detail: CheckDetail::ProjectFound { project_id, name },
        }
    }

    pub(in crate::commands::check) fn project_cascade_missing() -> Self {
        Self::Warn {
            name: CheckName::ProjectResolution,
            detail: CheckDetail::ProjectCascadeMissing,
            remediation: Some(CheckRemediation::RegisterProjectOrSetEnv),
        }
    }

    pub(in crate::commands::check) fn project_not_found(error: String) -> Self {
        Self::Fail {
            name: CheckName::ProjectResolution,
            detail: CheckDetail::ProjectNotFound { error },
            remediation: None,
        }
    }

    pub(in crate::commands::check) fn project_query_failed(error: String) -> Self {
        Self::Fail {
            name: CheckName::ProjectResolution,
            detail: CheckDetail::ProjectQueryFailed { error },
            remediation: Some(CheckRemediation::CheckPgIsready),
        }
    }
}

/// Runs the project-resolution cascade: CLI override → env var → git
/// remote → `None`.
pub(in crate::commands::check) async fn run(ctx: &CheckContext) -> CheckOutcome {
    match resolve_project(&ctx.pool, ctx.project_override.clone()).await {
        Ok(Some(project)) => CheckOutcome::project_found(project.id(), project.name().to_owned()),
        Ok(None) => CheckOutcome::project_cascade_missing(),
        Err(AppError::ProjectResolution { context }) => CheckOutcome::project_not_found(context),
        Err(other) => CheckOutcome::project_query_failed(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_project_id() -> ProjectId {
        "proj_550e8400-e29b-41d4-a716-446655440000"
            .parse()
            .expect("valid project id")
    }

    #[test]
    fn test_project_found_is_pass() {
        let id = sample_project_id();
        let outcome = CheckOutcome::project_found(id, "tribal".to_owned());
        assert!(matches!(
            &outcome,
            CheckOutcome::Pass {
                name: CheckName::ProjectResolution,
                detail: CheckDetail::ProjectFound { project_id, name },
            } if *project_id == id && name == "tribal",
        ));
    }

    #[test]
    fn test_project_cascade_missing_is_warn_with_remediation() {
        assert!(matches!(
            &CheckOutcome::project_cascade_missing(),
            CheckOutcome::Warn {
                name: CheckName::ProjectResolution,
                detail: CheckDetail::ProjectCascadeMissing,
                remediation: Some(CheckRemediation::RegisterProjectOrSetEnv),
            },
        ));
    }

    #[test]
    fn test_project_not_found_is_fail_without_remediation() {
        let outcome = CheckOutcome::project_not_found("project proj_xxx not found".into());
        assert!(matches!(
            &outcome,
            CheckOutcome::Fail {
                name: CheckName::ProjectResolution,
                detail: CheckDetail::ProjectNotFound { error },
                remediation: None,
            } if error == "project proj_xxx not found",
        ));
    }

    #[test]
    fn test_project_query_failed_is_fail_with_pg_isready_remediation() {
        let outcome = CheckOutcome::project_query_failed("pool exhausted".into());
        assert!(matches!(
            &outcome,
            CheckOutcome::Fail {
                name: CheckName::ProjectResolution,
                detail: CheckDetail::ProjectQueryFailed { error },
                remediation: Some(CheckRemediation::CheckPgIsready),
            } if error == "pool exhausted",
        ));
    }
}
