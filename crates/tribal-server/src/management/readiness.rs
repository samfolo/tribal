//! Exhaustive readiness policy over operator check observations.

use tribal_wire::management::{
    CheckName, CheckObservation, CheckResult, CheckSubject, HealthVerdict, ProbeOutcome,
    ProbeReceipt, ProbeReceiptFreshness, ProbeSubject, ProviderProbeCapability, ReadinessReport,
    ReadinessScope, StartVerdict,
};

use super::{
    configuration::ConfigAuthorityError,
    probe::{ProbeError, ProbeService},
    worker::ConfigWorkerClient,
};

const AUTOMATIC_OBSERVATION_ATTEMPTS: usize = 3;

/// Failure obtaining one revision-coherent automatic readiness report.
#[derive(Debug, thiserror::Error)]
pub(crate) enum AutomaticReadinessError {
    #[error("configuration is unavailable for readiness: {source}")]
    Config {
        #[from]
        source: ConfigAuthorityError,
    },
    #[error("readiness checks failed: {source}")]
    Check {
        #[from]
        source: crate::error::AppError,
    },
    #[error("retained probe evidence is unavailable: {source}")]
    Probe {
        #[from]
        source: ProbeError,
    },
    #[error("configuration changed throughout the bounded readiness observation")]
    ChangedDuringObservation,
}

/// Runs non-provider checks against one stable revision and merges retained evidence.
pub(crate) async fn automatic(
    config: &ConfigWorkerClient,
    probe: &ProbeService,
    runtime_present: bool,
) -> Result<ReadinessReport, AutomaticReadinessError> {
    for _ in 0..AUTOMATIC_OBSERVATION_ATTEMPTS {
        let snapshot = config.check_snapshot().await?;
        let revision = snapshot.revision.clone();
        let report = crate::management::operator_check::run_report_async(
            crate::management::operator_check::CheckReportOptions {
                config_path: &snapshot.path,
                source: crate::management::operator_check::CheckConfigSource::ProvenBytes(
                    snapshot.bytes,
                ),
                providers: false,
                project: None,
                token: None,
            },
        )
        .await?;
        if config.check_snapshot().await?.revision != revision {
            continue;
        }
        let receipts = probe.receipts().await?;
        if config.check_snapshot().await?.revision != revision {
            continue;
        }
        return Ok(from_results_with_receipts(
            report.checks,
            runtime_present,
            receipts,
        ));
    }
    Err(AutomaticReadinessError::ChangedDuringObservation)
}

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
pub(crate) fn derive(
    mut checks: Vec<CheckObservation>,
    runtime_present: bool,
    probe_receipts: Vec<ProbeReceipt>,
) -> ReadinessReport {
    checks.extend(
        probe_receipts
            .iter()
            .filter(|receipt| matches!(receipt.freshness, ProbeReceiptFreshness::Current))
            .map(|receipt| observation(probe_result(receipt), Vec::new())),
    );
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
        probe_receipts,
    }
}

fn probe_result(receipt: &ProbeReceipt) -> CheckResult {
    let name = match &receipt.subject {
        ProbeSubject::Database => CheckName::DatabaseReachable,
        ProbeSubject::Provider {
            capability: ProviderProbeCapability::Embedding,
            ..
        } => CheckName::ProviderEmbedding,
        ProbeSubject::Provider {
            capability: ProviderProbeCapability::Extraction,
            ..
        } => CheckName::ProviderExtraction,
        ProbeSubject::Provider {
            capability: ProviderProbeCapability::Triage,
            ..
        } => CheckName::ProviderTriage,
        ProbeSubject::Provider {
            capability: ProviderProbeCapability::Relation,
            ..
        } => CheckName::ProviderRelation,
    };
    match &receipt.result {
        ProbeOutcome::Pass { detail } => CheckResult::Pass {
            name,
            detail: detail.clone(),
        },
        ProbeOutcome::Warn {
            detail,
            remediation,
        } => CheckResult::Warn {
            name,
            detail: detail.clone(),
            remediation: remediation.clone(),
        },
        ProbeOutcome::Fail {
            detail,
            remediation,
        } => CheckResult::Fail {
            name,
            detail: detail.clone(),
            remediation: remediation.clone(),
        },
        ProbeOutcome::Skip { detail } => CheckResult::Skip {
            name,
            detail: detail.clone(),
        },
    }
}

pub(crate) fn from_results(results: Vec<CheckResult>, runtime_present: bool) -> ReadinessReport {
    from_results_with_receipts(results, runtime_present, Vec::new())
}

pub(crate) fn from_results_with_receipts(
    results: Vec<CheckResult>,
    runtime_present: bool,
    probe_receipts: Vec<ProbeReceipt>,
) -> ReadinessReport {
    derive(
        results
            .into_iter()
            .map(|result| observation(result, Vec::new()))
            .collect(),
        runtime_present,
        probe_receipts,
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
    use tribal_wire::management::{
        ConfigDigest, ConfigRevision, ProbeOutcome, ProbeReceiptFreshness, ProbeSubject,
        ProviderProbeCapability,
    };

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
        let stopped = derive(checks.clone(), false, Vec::new());
        assert!(matches!(stopped.start, StartVerdict::Blocked { .. }));
        assert_eq!(stopped.health, HealthVerdict::NotApplicable);

        let running = derive(checks, true, Vec::new());
        assert!(matches!(running.start, StartVerdict::Blocked { .. }));
        assert!(matches!(running.health, HealthVerdict::Degraded { .. }));
    }

    #[test]
    fn test_stale_probe_receipts_remain_visible_without_affecting_health() {
        let result = CheckResult::Fail {
            name: CheckName::ProviderExtraction,
            detail: "provider refused the probe".to_owned(),
            remediation: "repair the credential".to_owned(),
        };
        let revision = ConfigRevision::from_digest(&ConfigDigest::from_bytes(b"observed"));
        let stale = ProbeReceipt {
            observed_at_unix_ms: 1,
            revision: revision.clone(),
            subject: ProbeSubject::Provider {
                capability: ProviderProbeCapability::Extraction,
                provider: tribal_domain::ProviderKind::OpenAi,
            },
            result: ProbeOutcome::Fail {
                detail: "provider refused the probe".to_owned(),
                remediation: "repair the credential".to_owned(),
            },
            freshness: ProbeReceiptFreshness::Stale {
                current_revision: Some(ConfigRevision::from_digest(&ConfigDigest::from_bytes(
                    b"current",
                ))),
            },
        };
        let report = derive(Vec::new(), true, vec![stale]);
        assert_eq!(report.health, HealthVerdict::Clear);
        assert!(report.checks.is_empty());
        assert_eq!(report.probe_receipts.len(), 1);

        let current = ProbeReceipt {
            freshness: ProbeReceiptFreshness::Current,
            ..report.probe_receipts[0].clone()
        };
        let report = derive(Vec::new(), true, vec![current]);
        assert!(matches!(report.health, HealthVerdict::Degraded { .. }));
        assert_eq!(report.checks.len(), 1);
        assert_eq!(report.checks[0].result, result);
    }

    #[tokio::test]
    async fn test_automatic_readiness_reports_invalid_proven_bytes() {
        let temp = tempfile::tempdir().expect("temporary config root");
        let path = temp.path().join("tribal.yaml");
        std::fs::write(&path, "database: [").expect("invalid config writes");
        let (config, runtime) =
            super::super::worker::spawn(super::super::configuration::ConfigAuthority::new(path))
                .expect("config worker starts");
        let probe = ProbeService::new(config.clone());

        let report = automatic(&config, &probe, false)
            .await
            .expect("invalid bytes still produce readiness");

        assert!(matches!(report.start, StartVerdict::Blocked { .. }));
        assert_eq!(report.health, HealthVerdict::NotApplicable);
        assert!(matches!(
            report.checks.first().map(|check| &check.result),
            Some(CheckResult::Fail {
                name: CheckName::ConfigParse,
                ..
            })
        ));
        assert!(report.probe_receipts.is_empty());
        drop(probe);
        drop(config);
        runtime.join().expect("config worker joins");
    }
}
