//! Agentic execution configuration: the opt-in per-stage executor switch.
//!
//! Absent configuration reproduces launched behaviour exactly: every stage
//! runs one-shot. Setting a stage's executor to `loop` routes it through
//! the in-process turn loop with finite default budgets, every one of
//! which is overridable here. The section admits only the triage stage in
//! this release, so no other stage can select the loop: an
//! `agents.extraction` key is an unknown-field error at load.

use serde::{Deserialize, Serialize};

use crate::validation::{ConfigPath, Diagnostics, ValidationError};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Default cap on an agentic thread's turns.
pub const DEFAULT_AGENTIC_MAX_TURNS: u32 = 8;

/// Default cap on an agentic thread's token spend: input, output, and
/// cache-write tokens. Cache-read is excluded, since on some providers it
/// is a subset of the input count and counting it would double up.
pub const DEFAULT_AGENTIC_MAX_TOTAL_TOKENS: u64 = 200_000;

/// Default cap on verifier rounds per submission, doubling as the
/// thread's child-launch cap.
pub const DEFAULT_AGENTIC_VERIFY_ROUNDS: u32 = 2;

/// Default seconds between budget-exhaustion re-checks while suspended.
pub const DEFAULT_AGENTIC_RECHECK_DELAY_SECONDS: u32 = 300;

/// Default bound on unchanged budget re-checks before the thread fails.
pub const DEFAULT_AGENTIC_RECHECK_BOUND: u32 = 3;

/// Default wall-clock bound on an agentic thread's whole execution,
/// measured from creation and so inclusive of suspended time. It must
/// exceed the budget-recheck window (the delay times the bound) so a
/// thread suspended on spend exhaustion completes its bounded re-checks
/// before the deadline pre-empts them; the surplus is headroom for the
/// active execution a rescued thread resumes into.
pub const DEFAULT_AGENTIC_EXECUTION_DEADLINE_SECONDS: u32 =
    DEFAULT_AGENTIC_RECHECK_DELAY_SECONDS * DEFAULT_AGENTIC_RECHECK_BOUND + 300;

// The deadline must outlast the recheck window, or a thread suspended on
// spend exhaustion is pre-empted before its bounded re-checks complete.
// Enforced at compile time so a future drift in either constant fails the
// build rather than silently reopening the unreachable-recheck defect.
const _: () = assert!(
    DEFAULT_AGENTIC_EXECUTION_DEADLINE_SECONDS
        > DEFAULT_AGENTIC_RECHECK_DELAY_SECONDS * DEFAULT_AGENTIC_RECHECK_BOUND,
);

/// Advisory raised when the verifier is enabled under the one-shot
/// executor, where there is no submission loop for it to check, so the
/// setting is inert. Non-fatal: the stage runs one-shot regardless.
pub const VERIFIER_INERT_ADVISORY: &str = "agents.triage.verifier is set but agents.triage.executor is one_shot; \
     the verifier runs only under the loop executor, so this setting is inert";

// ---------------------------------------------------------------------------
// Executor choice
// ---------------------------------------------------------------------------

/// Which executor a stage's binding selects.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutorChoice {
    /// The launched single-call behaviour.
    #[default]
    OneShot,
    /// The in-process turn loop with stage-scoped tools.
    Loop,
}

// ---------------------------------------------------------------------------
// AgentsConfig
// ---------------------------------------------------------------------------

/// The `[agents]` section: per-stage executor selection and budgets.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentsConfig {
    /// The triage stage's agentic configuration.
    #[serde(default)]
    pub triage: StageAgentConfig,
}

/// One stage's agentic configuration.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StageAgentConfig {
    /// The executor the stage runs under.
    #[serde(default)]
    pub executor: ExecutorChoice,
    /// Whether an accepted submission is verified by a child execution.
    /// Absent leaves the loop's default in force (verification on). It is
    /// honoured only under the loop executor; setting it under one-shot,
    /// where there is no submission loop to verify, is inert and surfaced
    /// as a startup advisory.
    #[serde(default)]
    pub verifier: Option<bool>,
    /// Override for the turn cap; the named default applies when absent.
    #[serde(default)]
    pub max_turns: Option<u32>,
    /// Override for the token cap; the named default applies when absent.
    #[serde(default)]
    pub max_total_tokens: Option<u64>,
    /// Override for the execution deadline, in seconds; the named default
    /// applies when absent.
    #[serde(default)]
    pub execution_deadline_seconds: Option<u32>,
}

impl AgentsConfig {
    /// Validates the section's value constraints.
    pub(crate) fn validate(&self, diags: &mut Diagnostics) {
        let stage = &self.triage;
        for (field, value) in [
            ("agents.triage.max_turns", stage.max_turns),
            (
                "agents.triage.execution_deadline_seconds",
                stage.execution_deadline_seconds,
            ),
        ] {
            if value == Some(0) {
                diags.push(ValidationError::must_be_positive(ConfigPath::from_static(
                    field,
                )));
            }
        }
        if stage.max_total_tokens == Some(0) {
            diags.push(ValidationError::must_be_positive(ConfigPath::from_static(
                "agents.triage.max_total_tokens",
            )));
        }
    }

    /// Non-fatal advisories about inert or surprising combinations that
    /// validation admits but the operator may not have intended.
    pub(crate) fn advisories(&self) -> Vec<&'static str> {
        let stage = &self.triage;
        let mut advisories = Vec::new();
        if stage.verifier == Some(true) && stage.executor == ExecutorChoice::OneShot {
            advisories.push(VERIFIER_INERT_ADVISORY);
        }
        advisories
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_defaults_reproduce_launched_behaviour() {
        let config = AgentsConfig::default();
        assert_eq!(config.triage.executor, ExecutorChoice::OneShot);
        assert_eq!(config.triage.max_turns, None);
    }

    #[test]
    fn test_loop_executor_parses_with_overrides() {
        let config: AgentsConfig =
            serde_yaml::from_str("triage:\n  executor: loop\n  verifier: false\n  max_turns: 4\n")
                .expect("parse");
        assert_eq!(config.triage.executor, ExecutorChoice::Loop);
        assert_eq!(config.triage.verifier, Some(false));
        assert_eq!(config.triage.max_turns, Some(4));
    }

    #[test]
    fn test_zero_caps_are_rejected() {
        let config: AgentsConfig =
            serde_yaml::from_str("triage:\n  executor: loop\n  max_turns: 0\n").expect("parse");
        let mut diags = Diagnostics::default();
        config.validate(&mut diags);
        assert!(!diags.is_empty(), "a zero turn cap must fail validation");
    }

    #[test]
    fn test_verifier_under_one_shot_is_an_advisory_not_an_error() {
        // The verifier is inert under one-shot, so it warns rather than
        // refusing to start: an explicit verifier there is surfaced as an
        // advisory, and validation still passes.
        let config: AgentsConfig =
            serde_yaml::from_str("triage:\n  verifier: true\n").expect("parse");
        let mut diags = Diagnostics::default();
        config.validate(&mut diags);
        assert!(
            diags.is_empty(),
            "an inert verifier must not fail validation"
        );
        assert_eq!(config.advisories(), vec![VERIFIER_INERT_ADVISORY]);
    }

    #[test]
    fn test_verifier_under_loop_raises_no_advisory() {
        let config: AgentsConfig =
            serde_yaml::from_str("triage:\n  executor: loop\n  verifier: true\n").expect("parse");
        assert!(
            config.advisories().is_empty(),
            "the verifier belongs to the loop"
        );
    }

    #[test]
    fn test_defaults_raise_no_advisory() {
        // The verifier is left unset by default, so a pure-default config
        // (one-shot) raises nothing: only an explicit verifier under
        // one-shot is the inert combination worth surfacing.
        assert!(AgentsConfig::default().advisories().is_empty());
    }
}
