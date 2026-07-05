//! The managed-platform metering client.
//!
//! The job-plane teardown reads a run's outstanding holds and acknowledges
//! their settled positions over the same control-plane gateway the inference
//! bracket uses: the fleet bearer authenticates the deployment, and the
//! run-scoped grant set rides the [`GRANT_SET_HEADER`] the gateway derives
//! tenancy from. The client is grant-agnostic — the caller supplies the run's
//! grant per call — so one client serves every run a worker tears down.

use tribal_wire::gateway::{Acknowledgement, GRANT_SET_HEADER, GrantSet, HoldsReport, PositionKey};

use crate::{InferenceError, error::map_send_error, http::normalise_base_url};

/// The provider name recorded on error contexts.
const PROVIDER_NAME: &str = "platform";

/// The gateway's outstanding-holds endpoint.
pub const HOLDS_PATH: &str = "/v1/holds";

/// The gateway's position-acknowledgement endpoint.
pub const ACK_PATH: &str = "/v1/ack";

/// A client for the gateway's job-plane metering endpoints, holding the fleet
/// bearer and reusing one connection pool across every run.
#[derive(Clone)]
pub struct GatewayMeteringClient {
    client: reqwest::Client,
    base_url: String,
    bearer: String,
}

impl GatewayMeteringClient {
    /// Builds a client against a gateway base URL, authenticating with the
    /// fleet bearer token.
    #[must_use]
    pub fn new(
        client: reqwest::Client,
        base_url: impl Into<String>,
        bearer: impl Into<String>,
    ) -> Self {
        Self {
            client,
            base_url: normalise_base_url(base_url),
            bearer: bearer.into(),
        }
    }

    /// Reads a run's outstanding holds, scoped to the grant's account.
    ///
    /// # Errors
    ///
    /// Returns [`InferenceError::ProviderUnavailable`] on a transport failure,
    /// a non-success status, or an undecodable report.
    pub async fn holds_report(
        &self,
        run_key: &str,
        grant: &GrantSet,
    ) -> Result<HoldsReport, InferenceError> {
        let url = reqwest::Url::parse_with_params(
            &format!("{}{HOLDS_PATH}", self.base_url),
            &[("run_key", run_key)],
        )
        .map_err(|source| {
            InferenceError::provider_unavailable(
                PROVIDER_NAME,
                format!("building the holds URL for run {run_key} failed: {source}"),
            )
        })?;
        let response = self
            .client
            .get(url)
            .bearer_auth(&self.bearer)
            .header(GRANT_SET_HEADER, grant_header(grant)?)
            .send()
            .await
            .map_err(|source| map_send_error(&source, PROVIDER_NAME))?;

        let status = response.status();
        if !status.is_success() {
            return Err(InferenceError::provider_unavailable(
                PROVIDER_NAME,
                format!(
                    "the gateway refused the holds read for run {run_key} with status {status}"
                ),
            ));
        }
        response.json::<HoldsReport>().await.map_err(|source| {
            InferenceError::provider_unavailable(
                PROVIDER_NAME,
                format!("the holds report for run {run_key} was undecodable: {source}"),
            )
        })
    }

    /// Acknowledges a settled position. Idempotent: a re-presented ack after a
    /// crash mid-teardown is a no-op the gateway absorbs.
    ///
    /// # Errors
    ///
    /// Returns [`InferenceError::ProviderUnavailable`] on a transport failure
    /// or a non-success status.
    pub async fn acknowledge(
        &self,
        position_key: &PositionKey,
        grant: &GrantSet,
    ) -> Result<(), InferenceError> {
        let url = format!("{}{ACK_PATH}", self.base_url);
        let response = self
            .client
            .post(&url)
            .bearer_auth(&self.bearer)
            .header(GRANT_SET_HEADER, grant_header(grant)?)
            .json(&Acknowledgement {
                position_key: position_key.clone(),
            })
            .send()
            .await
            .map_err(|source| map_send_error(&source, PROVIDER_NAME))?;

        let status = response.status();
        if !status.is_success() {
            return Err(InferenceError::provider_unavailable(
                PROVIDER_NAME,
                format!("the gateway refused the ack of {position_key} with status {status}"),
            ));
        }
        Ok(())
    }
}

/// Serialises a grant set into its header value.
fn grant_header(grant: &GrantSet) -> Result<String, InferenceError> {
    serde_json::to_string(grant).map_err(|source| {
        InferenceError::provider_unavailable(
            PROVIDER_NAME,
            format!("serialising the grant set for its header failed: {source}"),
        )
    })
}
