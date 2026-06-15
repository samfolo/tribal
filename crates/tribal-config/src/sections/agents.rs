//! Agentic execution configuration: the opt-in per-stage executor switch.
//!
//! Absent configuration reproduces launched behaviour exactly: every stage
//! runs one-shot. Setting a stage's executor to `loop` routes it through
//! the in-process turn loop with finite default budgets, every one of
//! which is overridable here. The section admits the triage and relation
//! stages; an `agents.extraction` key is an unknown-field error at load
//! until extraction gains an executor of its own.

use serde::{Deserialize, Serialize};

use crate::validation::{ConfigPath, Diagnostics, ValidationError};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Default cap on an agentic thread's turns.
pub const DEFAULT_AGENTIC_MAX_TURNS: u32 = 8;

/// Default cap on an agentic thread's token spend: input, output, and
/// cache-write tokens. Cache-read is not counted. No provider populates
/// cache-read today, so the exclusion is currently a no-op; before
/// provider prompt caching is enabled, the cap, the ledger's cache-read
/// subset check, and the per-provider usage mappings must be reconciled to
/// one provider-independent cache-accounting model.
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

// The deadline must exceed the recheck window, or a thread that suspends on
// spend exhaustion early in its life is pre-empted before its bounded
// re-checks complete. Enforced at compile time so a future drift in either
// constant fails the build. (A thread that has already spent more than the
// surplus on active execution before it suspends can still reach the
// deadline first; that is a clean termination on a different cause, not the
// recheck window failing to fit.)
const _: () = assert!(
    DEFAULT_AGENTIC_EXECUTION_DEADLINE_SECONDS
        > DEFAULT_AGENTIC_RECHECK_DELAY_SECONDS * DEFAULT_AGENTIC_RECHECK_BOUND,
);

/// Advisory raised when the verifier is enabled under the one-shot
/// executor, where there is no submission loop for it to check, so the
/// setting is inert. Non-fatal: the stage runs one-shot regardless.
pub const VERIFIER_INERT_ADVISORY: &str = "agents.triage.verifier is set but agents.triage.executor is one_shot; \
     the verifier runs only under the loop executor, so this setting is inert";

/// Advisory raised when the relation verifier is set. The relation loop
/// ships without a verifier in this release, so the setting is inert under
/// either executor. Non-fatal: the stage runs regardless.
pub const RELATION_VERIFIER_UNAVAILABLE_ADVISORY: &str = "agents.relation.verifier is set, but the relation stage has no verifier in this release, \
     so this setting is inert";

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
// Verifier sub-config
// ---------------------------------------------------------------------------

/// Whether an accepted submission is checked by a fresh-context child
/// execution, and (in time) under what model and budget.
///
/// A struct rather than a bare bool so the verifier's model and budget can
/// be added as fields without a config-type break. A bare bool still
/// deserialises (`verifier: false`), so the launched ergonomics hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct VerifierConfig {
    /// Whether the verifier runs.
    pub enabled: bool,
}

impl<'de> Deserialize<'de> for VerifierConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Detailed {
            enabled: bool,
        }

        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Shim {
            Bare(bool),
            Detailed(Detailed),
        }

        Ok(match Shim::deserialize(deserializer)? {
            Shim::Bare(enabled) | Shim::Detailed(Detailed { enabled }) => Self { enabled },
        })
    }
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
    /// The relation stage's agentic configuration.
    #[serde(default)]
    pub relation: StageAgentConfig,
}

/// One stage's agentic configuration.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StageAgentConfig {
    /// The executor the stage runs under.
    #[serde(default)]
    pub executor: ExecutorChoice,
    /// Whether an accepted submission is verified by a child execution.
    /// Absent leaves the stage's loop default in force (triage verifies by
    /// default, relation does not). It is honoured only under the loop
    /// executor; setting it under one-shot, where there is no submission
    /// loop to verify, is inert and surfaced as a startup advisory.
    #[serde(default)]
    pub verifier: Option<VerifierConfig>,
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

impl StageAgentConfig {
    /// Whether the verifier runs, resolving the three-state config: an
    /// explicit setting wins, absence takes the stage's loop default.
    #[must_use]
    pub fn verifier_enabled(&self, default_when_absent: bool) -> bool {
        self.verifier.map_or(default_when_absent, |v| v.enabled)
    }
}

impl AgentsConfig {
    /// The configurable stages, paired with their config-path prefix.
    fn stages(&self) -> [(&'static str, &StageAgentConfig); 2] {
        [
            ("agents.triage", &self.triage),
            ("agents.relation", &self.relation),
        ]
    }

    /// Validates the section's value constraints.
    pub(crate) fn validate(&self, diags: &mut Diagnostics) {
        for (prefix, stage) in self.stages() {
            let positive = [
                ("max_turns", stage.max_turns.map(u64::from)),
                (
                    "execution_deadline_seconds",
                    stage.execution_deadline_seconds.map(u64::from),
                ),
                ("max_total_tokens", stage.max_total_tokens),
            ];
            for (field, value) in positive {
                if value == Some(0) {
                    diags.push(ValidationError::must_be_positive(ConfigPath::child(
                        prefix, field,
                    )));
                }
            }
        }
    }

    /// Non-fatal advisories about inert or surprising combinations that
    /// validation admits but the operator may not have intended.
    pub(crate) fn advisories(&self) -> Vec<&'static str> {
        let mut advisories = Vec::new();
        // Triage's verifier runs under the loop, so it is inert only when
        // set under the one-shot executor.
        if self.triage.verifier.is_some_and(|v| v.enabled)
            && self.triage.executor == ExecutorChoice::OneShot
        {
            advisories.push(VERIFIER_INERT_ADVISORY);
        }
        // The relation stage has no verifier yet, so any setting is inert.
        if self.relation.verifier.is_some_and(|v| v.enabled) {
            advisories.push(RELATION_VERIFIER_UNAVAILABLE_ADVISORY);
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
        assert_eq!(
            config.triage.verifier,
            Some(VerifierConfig { enabled: false })
        );
        assert_eq!(config.triage.max_turns, Some(4));
    }

    #[test]
    fn test_verifier_accepts_both_the_bare_bool_and_the_struct_form() {
        // The bare bool is the launched ergonomics; the struct form is the
        // shape #213 extends with model and budget. Both must parse to the
        // same value, and an unknown field in the struct form is rejected.
        let bare: AgentsConfig =
            serde_yaml::from_str("triage:\n  verifier: true\n").expect("bare bool parses");
        let structured: AgentsConfig =
            serde_yaml::from_str("triage:\n  verifier:\n    enabled: true\n")
                .expect("struct form parses");
        assert_eq!(bare.triage.verifier, Some(VerifierConfig { enabled: true }));
        assert_eq!(bare.triage.verifier, structured.triage.verifier);

        let unknown: Result<AgentsConfig, _> =
            serde_yaml::from_str("triage:\n  verifier:\n    enabledd: true\n");
        assert!(unknown.is_err(), "an unknown verifier field is rejected");
    }

    #[test]
    fn test_verifier_enabled_resolves_the_three_states() {
        let mut stage = StageAgentConfig::default();
        assert!(
            stage.verifier_enabled(true),
            "absent takes the supplied default",
        );
        assert!(!stage.verifier_enabled(false));
        stage.verifier = Some(VerifierConfig { enabled: false });
        assert!(
            !stage.verifier_enabled(true),
            "an explicit setting overrides the default",
        );
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

    #[test]
    fn test_relation_stage_parses_independently_of_triage() {
        let config: AgentsConfig = serde_yaml::from_str(
            "relation:\n  executor: loop\n  verifier: false\n  max_turns: 6\n",
        )
        .expect("parse");
        assert_eq!(config.relation.executor, ExecutorChoice::Loop);
        assert_eq!(config.triage.executor, ExecutorChoice::OneShot);
        assert_eq!(
            config.relation.verifier,
            Some(VerifierConfig { enabled: false }),
        );
        assert_eq!(config.relation.max_turns, Some(6));
    }

    #[test]
    fn test_relation_zero_caps_are_rejected() {
        let config: AgentsConfig =
            serde_yaml::from_str("relation:\n  executor: loop\n  max_total_tokens: 0\n")
                .expect("parse");
        let mut diags = Diagnostics::default();
        config.validate(&mut diags);
        assert!(
            !diags.is_empty(),
            "a zero relation token cap must fail validation",
        );
    }

    #[test]
    fn test_relation_verifier_is_inert_under_either_executor() {
        // The relation loop has no verifier yet, so the setting is inert
        // whether the stage runs one-shot or under the loop.
        for yaml in [
            "relation:\n  verifier: true\n",
            "relation:\n  executor: loop\n  verifier: true\n",
        ] {
            let config: AgentsConfig = serde_yaml::from_str(yaml).expect("parse");
            assert_eq!(
                config.advisories(),
                vec![RELATION_VERIFIER_UNAVAILABLE_ADVISORY],
            );
        }
    }
}
