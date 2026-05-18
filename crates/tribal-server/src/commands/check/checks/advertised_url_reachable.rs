//! Outcome constructors and probe for the `advertised_url_reachable`
//! check.

use std::time::Duration;

use tribal_config::TransportKind;

use super::{
    context::CheckContext,
    types::{CheckDetail, CheckName, CheckOutcome, CheckRemediation},
};
use crate::output::resolved_advertised_url;

/// Maximum time the probe waits for any response.  The advertised URL
/// is local to the host or behind a reverse proxy in front of it — a
/// healthy server responds in single-digit milliseconds.  Two seconds
/// is generous enough to absorb a cold-start delay without dragging
/// out a check run.
const PROBE_TIMEOUT: Duration = Duration::from_secs(2);

impl CheckOutcome {
    pub(in crate::commands::check) fn advertised_url_skipped_stdio() -> Self {
        Self::Skip {
            name: CheckName::AdvertisedUrlReachable,
            detail: CheckDetail::AdvertisedUrlSkippedStdio,
        }
    }

    pub(in crate::commands::check) fn advertised_url_reachable(url: String, status: u16) -> Self {
        Self::Pass {
            name: CheckName::AdvertisedUrlReachable,
            detail: CheckDetail::AdvertisedUrlReachable { url, status },
        }
    }

    pub(in crate::commands::check) fn advertised_url_unreachable(
        url: String,
        error: String,
    ) -> Self {
        Self::Fail {
            name: CheckName::AdvertisedUrlReachable,
            detail: CheckDetail::AdvertisedUrlUnreachable { url, error },
            remediation: Some(CheckRemediation::StartServeOnAdvertisedUrl),
        }
    }
}

/// Probes the advertised URL with a HEAD request.  Any HTTP response
/// (including 4xx and 5xx) is treated as `Pass` — the server is up.
/// Connection refused, DNS failures, and timeouts produce `Fail`.
pub(in crate::commands::check) async fn run(ctx: &CheckContext) -> CheckOutcome {
    if matches!(ctx.config.server.transport, TransportKind::Stdio) {
        return CheckOutcome::advertised_url_skipped_stdio();
    }

    let url = resolved_advertised_url(&ctx.config);
    match ctx
        .http_client
        .head(&url)
        .timeout(PROBE_TIMEOUT)
        .send()
        .await
    {
        Ok(response) => CheckOutcome::advertised_url_reachable(url, response.status().as_u16()),
        Err(err) => CheckOutcome::advertised_url_unreachable(url, err.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_advertised_url_skipped_stdio_is_skip() {
        assert!(matches!(
            &CheckOutcome::advertised_url_skipped_stdio(),
            CheckOutcome::Skip {
                name: CheckName::AdvertisedUrlReachable,
                detail: CheckDetail::AdvertisedUrlSkippedStdio,
            },
        ));
    }

    #[test]
    fn test_advertised_url_reachable_is_pass_with_status() {
        let outcome =
            CheckOutcome::advertised_url_reachable("http://localhost:8080/mcp".into(), 200);
        assert!(matches!(
            &outcome,
            CheckOutcome::Pass {
                name: CheckName::AdvertisedUrlReachable,
                detail: CheckDetail::AdvertisedUrlReachable { url, status: 200 },
            } if url == "http://localhost:8080/mcp",
        ));
    }

    #[test]
    fn test_advertised_url_unreachable_is_fail_with_remediation() {
        let outcome = CheckOutcome::advertised_url_unreachable(
            "http://localhost:8080/mcp".into(),
            "connection refused".into(),
        );
        assert!(matches!(
            &outcome,
            CheckOutcome::Fail {
                name: CheckName::AdvertisedUrlReachable,
                detail: CheckDetail::AdvertisedUrlUnreachable { url, error },
                remediation: Some(CheckRemediation::StartServeOnAdvertisedUrl),
            } if url == "http://localhost:8080/mcp" && error == "connection refused",
        ));
    }
}
