//! Money-driven run dispositions: how a gateway refusal, under the account's cap
//! setting, decides a run's next move.
//!
//! The gateway's typed refusals are the money seam's only signal to a managed
//! run. This module turns one into a [`RunDisposition`] the driver acts on — a
//! cap breach becomes a defined behaviour rather than a crash, and a refusal no
//! wait can clear fails the run cleanly.

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use tribal_wire::gateway::GatewayError;

/// The account's configured response to a cap breach — the setting the control
/// plane holds, carried to the run so an over-cap refusal resolves to a defined
/// behaviour.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CapBehaviour {
    /// Suspend until credit returns — a period rollover or a top-up — giving up
    /// once the deadline passes.
    HardStop,
    /// Requeue to retry after a backoff.
    ThrottleQueue,
}

/// What a run does next in response to a gateway refusal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunDisposition {
    /// Suspend awaiting a credit signal, failing cleanly once `give_up_at` passes
    /// with credit still short.
    Suspend {
        /// The instant past which a still-uncredited run fails rather than
        /// waiting further.
        give_up_at: DateTime<Utc>,
    },
    /// Requeue to retry after the backoff.
    Requeue,
    /// Fail the run cleanly to a terminal state, its holds resolved — a refusal
    /// no wait or retry can clear.
    Fail,
    /// Not a run disposition: the refusal is the bracket's own concern — a
    /// bounded wait it re-presents (`InFlight`), or a per-attempt failure the run
    /// records and retries per position key (`Failed`) — leaving the run's
    /// trajectory unchanged.
    Bracket,
}

/// Maps a gateway refusal, under the account's cap setting, to the run's next
/// move. `now` and `give_up_after` fix a hard-stop suspension's give-up instant.
#[must_use]
pub fn cap_disposition(
    error: &GatewayError,
    behaviour: CapBehaviour,
    now: DateTime<Utc>,
    give_up_after: Duration,
) -> RunDisposition {
    match error {
        GatewayError::OverCap => match behaviour {
            CapBehaviour::HardStop => RunDisposition::Suspend {
                give_up_at: now + give_up_after,
            },
            CapBehaviour::ThrottleQueue => RunDisposition::Requeue,
        },
        GatewayError::NotEntitled | GatewayError::Unpriceable => RunDisposition::Fail,
        GatewayError::InFlight { .. } | GatewayError::Failed => RunDisposition::Bracket,
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    fn instant() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 7, 5, 12, 0, 0).unwrap()
    }

    #[test]
    fn test_over_cap_hard_stops_to_a_suspension_with_a_give_up_deadline() {
        let now = instant();
        let disposition = cap_disposition(
            &GatewayError::OverCap,
            CapBehaviour::HardStop,
            now,
            Duration::hours(6),
        );
        assert_eq!(
            disposition,
            RunDisposition::Suspend {
                give_up_at: now + Duration::hours(6),
            },
        );
    }

    #[test]
    fn test_over_cap_throttle_queues_to_a_requeue() {
        let disposition = cap_disposition(
            &GatewayError::OverCap,
            CapBehaviour::ThrottleQueue,
            instant(),
            Duration::hours(6),
        );
        assert_eq!(disposition, RunDisposition::Requeue);
    }

    #[test]
    fn test_a_not_entitled_refusal_fails_the_run_whatever_the_cap_setting() {
        for behaviour in [CapBehaviour::HardStop, CapBehaviour::ThrottleQueue] {
            let disposition = cap_disposition(
                &GatewayError::NotEntitled,
                behaviour,
                instant(),
                Duration::hours(6),
            );
            assert_eq!(disposition, RunDisposition::Fail);
        }
    }

    #[test]
    fn test_an_unpriceable_refusal_fails_the_run() {
        let disposition = cap_disposition(
            &GatewayError::Unpriceable,
            CapBehaviour::HardStop,
            instant(),
            Duration::hours(6),
        );
        assert_eq!(disposition, RunDisposition::Fail);
    }

    #[test]
    fn test_a_bracket_owned_refusal_leaves_the_run_untouched() {
        for error in [
            GatewayError::InFlight {
                retry_after_ms: 500,
            },
            GatewayError::Failed,
        ] {
            let disposition = cap_disposition(
                &error,
                CapBehaviour::HardStop,
                instant(),
                Duration::hours(6),
            );
            assert_eq!(disposition, RunDisposition::Bracket);
        }
    }
}
