//! Exhaustive readiness policy over operator check observations.

use tribal_wire::management::{
    CheckName, CheckObservation, CheckResult, CheckSubject, HealthVerdict, ReadinessReport,
    ReadinessScope, StartVerdict,
};

/// Lifecycle policy for one check kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CheckPolicy {
    pub(crate) scope: ReadinessScope,
    pub(crate) subject: Option<CheckSubjectKind>,
}

/// Static subject class used when a check has no field-specific diagnostic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CheckSubjectKind {
    Credentials,
    Project,
    Runtime,
}

/// One exhaustive policy catalogue for automatic and explicit checks.
pub(crate) const fn policy(name: CheckName) -> CheckPolicy {
    match name {
        CheckName::ConfigParse | CheckName::ConfigValidate => CheckPolicy {
            scope: ReadinessScope::StartAndHealth,
            subject: None,
        },
        CheckName::DatabaseReachable
        | CheckName::MigrationsCurrent
        | CheckName::EmbeddingProfile => CheckPolicy {
            scope: ReadinessScope::StartAndHealth,
            subject: Some(CheckSubjectKind::Project),
        },
        CheckName::ProjectResolution | CheckName::ValidTokenExists => CheckPolicy {
            scope: ReadinessScope::Start,
            subject: Some(CheckSubjectKind::Project),
        },
        CheckName::AdvertisedUrlReachable => CheckPolicy {
            scope: ReadinessScope::Health,
            subject: Some(CheckSubjectKind::Runtime),
        },
        CheckName::BinaryUniqueness => CheckPolicy {
            scope: ReadinessScope::Advisory,
            subject: None,
        },
        CheckName::ProviderEmbedding
        | CheckName::ProviderExtraction
        | CheckName::ProviderTriage
        | CheckName::ProviderRelation => CheckPolicy {
            scope: ReadinessScope::Health,
            subject: Some(CheckSubjectKind::Credentials),
        },
    }
}

/// Attaches static policy to a check row while preserving specific subjects.
pub(crate) fn observation(result: CheckResult, subjects: Vec<CheckSubject>) -> CheckObservation {
    let check_policy = policy(result_name(&result));
    let subjects = if subjects.is_empty() {
        check_policy
            .subject
            .map(subject)
            .into_iter()
            .collect::<Vec<_>>()
    } else {
        subjects
    };
    CheckObservation {
        result,
        scope: check_policy.scope,
        subjects,
    }
}

/// Derives both verdict axes from the same ordered observations.
pub(crate) fn derive(checks: Vec<CheckObservation>, runtime_present: bool) -> ReadinessReport {
    let mut start_failures = Vec::new();
    let mut health_failures = Vec::new();
    for check in &checks {
        if !is_failure(&check.result) {
            continue;
        }
        let name = result_name(&check.result);
        if matches!(
            check.scope,
            ReadinessScope::Start | ReadinessScope::StartAndHealth
        ) {
            start_failures.push(name);
        }
        if matches!(
            check.scope,
            ReadinessScope::Health | ReadinessScope::StartAndHealth
        ) {
            health_failures.push(name);
        }
    }
    ReadinessReport {
        start: verdict(start_failures),
        health: if runtime_present {
            health_verdict(health_failures)
        } else {
            HealthVerdict::NotApplicable
        },
        checks,
    }
}

pub(crate) fn from_results(results: Vec<CheckResult>, runtime_present: bool) -> ReadinessReport {
    derive(
        results
            .into_iter()
            .map(|result| observation(result, Vec::new()))
            .collect(),
        runtime_present,
    )
}

fn result_name(result: &CheckResult) -> CheckName {
    match result {
        CheckResult::Pass { name, .. }
        | CheckResult::Warn { name, .. }
        | CheckResult::Fail { name, .. }
        | CheckResult::Skip { name, .. } => *name,
    }
}

fn is_failure(result: &CheckResult) -> bool {
    matches!(result, CheckResult::Fail { .. })
}

fn verdict(mut failures: Vec<CheckName>) -> StartVerdict {
    if failures.is_empty() {
        StartVerdict::Clear
    } else {
        let first = failures.remove(0);
        StartVerdict::Blocked {
            first,
            rest: failures,
        }
    }
}

fn health_verdict(mut failures: Vec<CheckName>) -> HealthVerdict {
    if failures.is_empty() {
        HealthVerdict::Clear
    } else {
        let first = failures.remove(0);
        HealthVerdict::Degraded {
            first,
            rest: failures,
        }
    }
}

fn subject(kind: CheckSubjectKind) -> CheckSubject {
    match kind {
        CheckSubjectKind::Credentials => CheckSubject::Credentials,
        CheckSubjectKind::Project => CheckSubject::Project,
        CheckSubjectKind::Runtime => CheckSubject::Runtime,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALL_CHECKS: [CheckName; 13] = [
        CheckName::ConfigParse,
        CheckName::ConfigValidate,
        CheckName::DatabaseReachable,
        CheckName::MigrationsCurrent,
        CheckName::ProjectResolution,
        CheckName::ValidTokenExists,
        CheckName::AdvertisedUrlReachable,
        CheckName::BinaryUniqueness,
        CheckName::EmbeddingProfile,
        CheckName::ProviderEmbedding,
        CheckName::ProviderExtraction,
        CheckName::ProviderTriage,
        CheckName::ProviderRelation,
    ];

    #[test]
    fn test_every_check_has_scope_and_subject_policy() {
        for name in ALL_CHECKS {
            let check_policy = policy(name);
            assert!(matches!(
                check_policy.scope,
                ReadinessScope::Advisory
                    | ReadinessScope::Start
                    | ReadinessScope::Health
                    | ReadinessScope::StartAndHealth
            ));
        }
    }

    #[test]
    fn test_start_and_health_are_derived_independently() {
        let checks = vec![
            observation(
                CheckResult::Fail {
                    name: CheckName::ConfigValidate,
                    detail: "invalid".to_owned(),
                    remediation: "repair".to_owned(),
                },
                Vec::new(),
            ),
            observation(
                CheckResult::Fail {
                    name: CheckName::AdvertisedUrlReachable,
                    detail: "offline".to_owned(),
                    remediation: "start".to_owned(),
                },
                Vec::new(),
            ),
        ];
        let stopped = derive(checks.clone(), false);
        assert!(matches!(stopped.start, StartVerdict::Blocked { .. }));
        assert_eq!(stopped.health, HealthVerdict::NotApplicable);

        let running = derive(checks, true);
        assert!(matches!(running.start, StartVerdict::Blocked { .. }));
        assert!(matches!(running.health, HealthVerdict::Degraded { .. }));
    }
}
