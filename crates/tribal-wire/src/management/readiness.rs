//! Readiness observations and their phase-safe verdict refinements.

use serde::{Deserialize, Serialize};
use tribal_domain::ConfigFieldPath;

use crate::operator_check::{CheckName, CheckResult};

/// Readiness derived from one set of typed check observations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ReadinessReport {
    /// Whether a runtime may start.
    pub start: StartVerdict,
    /// Whether a running runtime is healthy.
    pub health: HealthVerdict,
    /// The observations from which both verdicts were derived.
    pub checks: Vec<CheckObservation>,
}

/// Readiness narrowed to a start-blocked verdict.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct StartBlockedReadinessReport {
    /// The blocking check set.
    pub start: StartBlockedVerdict,
    /// The independently derived health verdict.
    pub health: HealthVerdict,
    /// The observations from which both verdicts were derived.
    pub checks: Vec<CheckObservation>,
}

/// Readiness narrowed to a start-clear verdict.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct StartClearReadinessReport {
    /// The start-clear verdict.
    pub start: StartClearVerdict,
    /// The independently derived health verdict.
    pub health: HealthVerdict,
    /// The observations from which both verdicts were derived.
    pub checks: Vec<CheckObservation>,
}

/// Readiness narrowed to a health-degraded verdict.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct HealthDegradedReadinessReport {
    /// The independently derived start verdict.
    pub start: StartVerdict,
    /// The degrading check set.
    pub health: HealthDegradedVerdict,
    /// The observations from which both verdicts were derived.
    pub checks: Vec<CheckObservation>,
}

/// Whether local evidence permits starting a runtime.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "verdict", content = "data", rename_all = "snake_case")]
pub enum StartVerdict {
    /// Every start-gating check passed.
    Clear,
    /// One or more start-gating checks failed.
    Blocked {
        /// The first blocking check in report order.
        first: CheckName,
        /// Remaining blocking checks in report order.
        rest: Vec<CheckName>,
    },
}

/// The one legal start verdict for an unconfigured snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "verdict", content = "data", rename_all = "snake_case")]
pub enum StartBlockedVerdict {
    /// One or more start-gating checks failed.
    Blocked {
        /// The first blocking check in report order.
        first: CheckName,
        /// Remaining blocking checks in report order.
        rest: Vec<CheckName>,
    },
}

/// The one legal start verdict for a ready stopped snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "verdict", rename_all = "snake_case")]
pub enum StartClearVerdict {
    /// Every start-gating check passed.
    Clear,
}

/// Whether a running runtime is healthy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "verdict", content = "data", rename_all = "snake_case")]
pub enum HealthVerdict {
    /// Every health-gating check passed.
    Clear,
    /// One or more health-gating checks failed.
    Degraded {
        /// The first degrading check in report order.
        first: CheckName,
        /// Remaining degrading checks in report order.
        rest: Vec<CheckName>,
    },
    /// Health has no meaning while no runtime is serving.
    NotApplicable,
}

/// The one legal health verdict for a readiness-driven degraded snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "verdict", content = "data", rename_all = "snake_case")]
pub enum HealthDegradedVerdict {
    /// One or more health-gating checks failed.
    Degraded {
        /// The first degrading check in report order.
        first: CheckName,
        /// Remaining degrading checks in report order.
        rest: Vec<CheckName>,
    },
}

/// One check result with its lifecycle policy and repair subjects.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct CheckObservation {
    /// The observed check result.
    pub result: CheckResult,
    /// The lifecycle axes this observation affects.
    pub scope: ReadinessScope,
    /// The typed operator-facing repair subjects.
    pub subjects: Vec<CheckSubject>,
}

/// Which lifecycle verdicts one check may affect.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum ReadinessScope {
    /// The check informs the operator but gates no lifecycle transition.
    Advisory,
    /// The check gates only runtime start.
    Start,
    /// The check gates only running health.
    Health,
    /// The check gates both runtime start and running health.
    StartAndHealth,
}

/// A typed subject an operator can repair after a check result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "kind", content = "data", rename_all = "snake_case")]
pub enum CheckSubject {
    /// A location within configuration input.
    Configuration {
        /// The configuration location that needs attention.
        location: ConfigDiagnosticLocation,
    },
    /// Credential availability or validity.
    Credentials,
    /// Project resolution or registration.
    Project,
    /// The managed runtime process.
    Runtime,
}

/// Configuration input associated with a diagnostic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "kind", content = "data", rename_all = "snake_case")]
pub enum ConfigDiagnosticLocation {
    /// A statically defined schema field.
    Field {
        /// The validated schema-field identity.
        path: ConfigFieldPath,
    },
    /// An entry in the dynamic credential catalogue.
    CredentialEntry {
        /// The raw user-authored map key.
        key: String,
        /// The invalid member, or none when the key itself is invalid.
        member: Option<CredentialEntryMember>,
    },
}

/// A member of a dynamic credential entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum CredentialEntryMember {
    /// Credential provider kind.
    ProviderKind,
    /// Credential endpoint URL.
    BaseUrl,
    /// Credential API key.
    ApiKey,
}

/// The active configuration file's location.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ConfigFilePath {
    /// Filesystem path read by the authority.
    pub path: String,
}

/// A broad readiness report did not satisfy the requested phase refinement.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ReadinessRefinementError {
    /// The start verdict was clear where blocked was required.
    #[error("readiness report is start-clear")]
    StartIsClear,
    /// The start verdict was blocked where clear was required.
    #[error("readiness report is start-blocked")]
    StartIsBlocked,
    /// The health verdict was not degraded.
    #[error("readiness report is not health-degraded")]
    HealthIsNotDegraded,
}

impl TryFrom<ReadinessReport> for StartBlockedReadinessReport {
    type Error = ReadinessRefinementError;

    fn try_from(report: ReadinessReport) -> Result<Self, Self::Error> {
        let StartVerdict::Blocked { first, rest } = report.start else {
            return Err(ReadinessRefinementError::StartIsClear);
        };
        Ok(Self {
            start: StartBlockedVerdict::Blocked { first, rest },
            health: report.health,
            checks: report.checks,
        })
    }
}

impl TryFrom<ReadinessReport> for StartClearReadinessReport {
    type Error = ReadinessRefinementError;

    fn try_from(report: ReadinessReport) -> Result<Self, Self::Error> {
        if !matches!(report.start, StartVerdict::Clear) {
            return Err(ReadinessRefinementError::StartIsBlocked);
        }
        Ok(Self {
            start: StartClearVerdict::Clear,
            health: report.health,
            checks: report.checks,
        })
    }
}

impl TryFrom<ReadinessReport> for HealthDegradedReadinessReport {
    type Error = ReadinessRefinementError;

    fn try_from(report: ReadinessReport) -> Result<Self, Self::Error> {
        let HealthVerdict::Degraded { first, rest } = report.health else {
            return Err(ReadinessRefinementError::HealthIsNotDegraded);
        };
        Ok(Self {
            start: report.start,
            health: HealthDegradedVerdict::Degraded { first, rest },
            checks: report.checks,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn report(start: StartVerdict, health: HealthVerdict) -> ReadinessReport {
        ReadinessReport {
            start,
            health,
            checks: Vec::new(),
        }
    }

    #[test]
    fn test_start_blocked_refinement_rejects_a_clear_report() {
        let result = StartBlockedReadinessReport::try_from(report(
            StartVerdict::Clear,
            HealthVerdict::NotApplicable,
        ));
        assert_eq!(result.unwrap_err(), ReadinessRefinementError::StartIsClear);
    }

    #[test]
    fn test_start_clear_refinement_rejects_a_blocked_report() {
        let result = StartClearReadinessReport::try_from(report(
            StartVerdict::Blocked {
                first: CheckName::ConfigParse,
                rest: Vec::new(),
            },
            HealthVerdict::NotApplicable,
        ));
        assert_eq!(
            result.unwrap_err(),
            ReadinessRefinementError::StartIsBlocked,
        );
    }

    #[test]
    fn test_health_degraded_refinement_rejects_not_applicable() {
        let result = HealthDegradedReadinessReport::try_from(report(
            StartVerdict::Clear,
            HealthVerdict::NotApplicable,
        ));
        assert_eq!(
            result.unwrap_err(),
            ReadinessRefinementError::HealthIsNotDegraded,
        );
    }

    #[test]
    fn test_dynamic_credential_keys_remain_data_not_field_paths() {
        let location = ConfigDiagnosticLocation::CredentialEntry {
            key: "openai.prod-west".to_owned(),
            member: Some(CredentialEntryMember::ApiKey),
        };
        let encoded = serde_json::to_value(&location).expect("location serialises");
        let decoded: ConfigDiagnosticLocation =
            serde_json::from_value(encoded).expect("location deserialises");
        assert_eq!(decoded, location);
    }
}
