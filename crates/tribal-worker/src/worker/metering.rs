//! The metering-gateway adapter.
//!
//! The job plane's teardown talks to the gateway through the
//! [`MeteringGateway`] trait (owned by `tribal-runtime-db`), while the HTTP
//! transport is `tribal-inference`'s [`GatewayMeteringClient`]. Both are
//! foreign here, so the orphan rule forbids implementing the one for the
//! other directly; this local type is the seam, binding one run's grant to
//! the shared client and presenting the trait.

use async_trait::async_trait;
use tribal_inference::{GatewayMeteringClient, InferenceError};
use tribal_runtime_db::{MeteringGateway, TeardownError};
use tribal_wire::gateway::{GrantSet, HoldsReport, PositionKey};

/// Presents the shared metering client under one run's grant. Built per run:
/// the client clones cheaply — its connection pool is shared — and the grant
/// scopes every crossing to the run's account.
pub struct MeteringTransport {
    client: GatewayMeteringClient,
    grant: GrantSet,
}

impl MeteringTransport {
    /// Binds a run's grant to the metering client.
    #[must_use]
    pub fn new(client: GatewayMeteringClient, grant: GrantSet) -> Self {
        Self { client, grant }
    }
}

#[async_trait]
impl MeteringGateway for MeteringTransport {
    async fn holds_report(&self, run_key: &str) -> Result<HoldsReport, TeardownError> {
        self.client
            .holds_report(run_key, &self.grant)
            .await
            .map_err(|source| gateway_error(&source))
    }

    async fn acknowledge(&self, position_key: &PositionKey) -> Result<(), TeardownError> {
        self.client
            .acknowledge(position_key, &self.grant)
            .await
            .map_err(|source| gateway_error(&source))
    }
}

/// Folds a transport failure into the teardown's gateway-crossing error.
fn gateway_error(source: &InferenceError) -> TeardownError {
    TeardownError::Gateway {
        context: source.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use tribal_inference::{METERING_ACK_PATH, METERING_HOLDS_PATH};
    use tribal_wire::gateway::{
        AccountReference, Acknowledgement, GRANT_SET_HEADER, HoldEntry, HoldStatus,
    };
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path, query_param},
    };

    use super::*;

    const BEARER: &str = "fleet-token";

    fn a_transport(base_url: String) -> MeteringTransport {
        let client = GatewayMeteringClient::new(reqwest::Client::new(), base_url, BEARER);
        let grant = GrantSet {
            account: AccountReference::new("acct_01"),
            principal: None,
            tools: vec![],
        };
        MeteringTransport::new(client, grant)
    }

    fn header<'a>(request: &'a wiremock::Request, name: &str) -> &'a str {
        request
            .headers
            .get(name)
            .unwrap_or_else(|| panic!("the {name} header is present"))
            .to_str()
            .expect("an ascii header value")
    }

    #[tokio::test]
    async fn test_holds_report_reads_the_run_holds_under_bearer_and_grant() {
        let server = MockServer::start().await;
        let report = HoldsReport {
            holds: vec![HoldEntry {
                position_key: PositionKey::new("job_x:1"),
                status: HoldStatus::Settled,
            }],
        };
        Mock::given(method("GET"))
            .and(path(METERING_HOLDS_PATH))
            .and(query_param("run_key", "job_x"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&report))
            .mount(&server)
            .await;

        let got = a_transport(server.uri())
            .holds_report("job_x")
            .await
            .expect("holds report");
        assert_eq!(got, report, "the report round-trips through the transport");

        let requests = server.received_requests().await.expect("recorded");
        let request = requests.first().expect("one call reached the gateway");
        assert_eq!(header(request, "authorization"), format!("Bearer {BEARER}"));
        let grant: GrantSet = serde_json::from_str(header(request, GRANT_SET_HEADER))
            .expect("the grant rides its header");
        assert_eq!(grant.account.as_str(), "acct_01");
    }

    #[tokio::test]
    async fn test_acknowledge_posts_the_position_key_under_bearer_and_grant() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(METERING_ACK_PATH))
            .respond_with(ResponseTemplate::new(204))
            .mount(&server)
            .await;

        a_transport(server.uri())
            .acknowledge(&PositionKey::new("job_x:1"))
            .await
            .expect("ack");

        let requests = server.received_requests().await.expect("recorded");
        let request = requests.first().expect("one call reached the gateway");
        assert_eq!(header(request, "authorization"), format!("Bearer {BEARER}"));
        assert!(
            request.headers.get(GRANT_SET_HEADER).is_some(),
            "the ack carries its grant",
        );
        let ack: Acknowledgement =
            serde_json::from_slice(&request.body).expect("the body is an Acknowledgement");
        assert_eq!(ack.position_key.as_str(), "job_x:1");
    }

    #[tokio::test]
    async fn test_a_refused_crossing_maps_to_a_gateway_teardown_error() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(METERING_HOLDS_PATH))
            .respond_with(ResponseTemplate::new(403))
            .mount(&server)
            .await;

        let err = a_transport(server.uri())
            .holds_report("job_x")
            .await
            .expect_err("a refused crossing is a gateway error");
        assert!(matches!(err, TeardownError::Gateway { .. }));
    }
}
