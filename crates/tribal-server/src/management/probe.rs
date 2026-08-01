//! Revision-bound retention for explicitly requested external probes.

use std::{sync::Arc, time::SystemTime};

use tokio::sync::Mutex;
use tribal_config::TribalConfig;
use tribal_domain::{ApiKey, ProviderConnectionName};
use tribal_inference::{
    ProviderConnectionFailure as InferenceProviderConnectionFailure,
    ProviderConnectionFailureKind as InferenceProviderConnectionFailureKind,
    ProviderConnectionProbeOutcome as InferenceProviderConnectionProbeOutcome,
    ProviderConnectionProbeSkipReason as InferenceProviderConnectionProbeSkipReason,
    probe_provider_connection,
};
use tribal_wire::management::{
    CandidateProviderProbeObservation, CheckName, CheckResult, ConfigDigest, ConfigDocument,
    ConfigRevision, DatabaseProbeRequest, ProbeOutcome, ProbeReceipt, ProbeReceiptFreshness,
    ProbeSubject, ProviderConnectionFailure, ProviderConnectionFailureKind,
    ProviderConnectionProbeReceipt, ProviderConnectionProbeResult,
    ProviderConnectionProbeSkipReason, ProviderProbeCapability, ProviderProbeRequest,
    ProviderProbeResponse, ProviderProbeTarget,
};

use super::{
    configuration::{ConfigAuthorityError, ConfigProbeSnapshot},
    observed_at_unix_ms,
    worker::ConfigWorkerClient,
};

#[derive(Clone)]
pub(crate) struct ProbeService {
    config: ConfigWorkerClient,
    receipts: Arc<Mutex<Vec<StoredReceipt>>>,
    provider_receipts: Arc<Mutex<Vec<StoredProviderReceipt>>>,
}

struct StoredReceipt {
    receipt: ProbeReceipt,
    input_digest: ConfigDigest,
}

struct StoredProviderReceipt {
    receipt: ProviderConnectionProbeReceipt,
    input_digest: ConfigDigest,
}

/// Failure preparing or executing an explicit probe.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ProbeError {
    #[error("configuration is unavailable for probing: {source}")]
    Config {
        #[from]
        source: ConfigAuthorityError,
    },
    #[error("probe execution failed: {source}")]
    Execution {
        #[from]
        source: crate::error::AppError,
    },
    #[error("probe input encoding failed: {source}")]
    Encoding {
        #[from]
        source: serde_json::Error,
    },
    #[error("system time is unavailable: {source}")]
    Clock {
        #[from]
        source: std::time::SystemTimeError,
    },
    #[error("probe observation time exceeds the wire representation: {source}")]
    TimeOverflow {
        #[from]
        source: std::num::TryFromIntError,
    },
    #[error("the requested probe produced no typed observation")]
    MissingObservation,
    #[error("configuration changed during the probe")]
    RevisionConflict {
        expected: ConfigRevision,
        actual: ConfigRevision,
    },
}

impl std::fmt::Debug for ProbeService {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ProbeService { receipts: <retained> }")
    }
}

impl ProbeService {
    pub(crate) fn new(config: ConfigWorkerClient) -> Self {
        Self {
            config,
            receipts: Arc::new(Mutex::new(Vec::new())),
            provider_receipts: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Runs the stored database probe under a before/after revision fence.
    pub(crate) async fn database(
        &self,
        request: DatabaseProbeRequest,
    ) -> Result<ProbeReceipt, ProbeError> {
        let snapshot = self.config.probe_snapshot().await?;
        ensure_revision(&request.expected_revision, &snapshot.revision)?;
        let result = run_report(&snapshot)
            .await?
            .into_iter()
            .find(|result| result_name(result) == CheckName::DatabaseReachable)
            .ok_or(ProbeError::MissingObservation)?;
        let subject = ProbeSubject::Database;
        let outcome = probe_outcome(result);
        ensure_revision(
            &request.expected_revision,
            &self.config.probe_snapshot().await?.revision,
        )?;
        self.retain(&snapshot, subject.clone(), outcome).await?;
        self.receipts()
            .await?
            .into_iter()
            .find(|receipt| receipt.subject == subject)
            .ok_or(ProbeError::MissingObservation)
    }

    /// Probes one stored or candidate provider connection without persisting candidates.
    pub(crate) async fn provider(
        &self,
        request: ProviderProbeRequest,
    ) -> Result<ProviderProbeResponse, ProbeError> {
        let snapshot = self.config.probe_snapshot().await?;
        ensure_revision(&request.expected_revision, &snapshot.revision)?;
        let (name, connection, retain) = match request.target {
            ProviderProbeTarget::Stored { name } => {
                let connection = snapshot
                    .config
                    .provider_connections
                    .get(name.as_str())
                    .cloned()
                    .ok_or(ProbeError::MissingObservation)?;
                (name, connection, true)
            }
            ProviderProbeTarget::Candidate {
                existing,
                connection,
            } => {
                let name = match existing {
                    Some(name) => name,
                    None => ProviderConnectionName::parse("probe_candidate")
                        .map_err(|_| ProbeError::MissingObservation)?,
                };
                let current = snapshot.config.provider_connections.get(name.as_str());
                let connection = super::product::provider_input(connection, current)
                    .map_err(|_| ProbeError::MissingObservation)?;
                (name, connection, false)
            }
        };
        let provider = connection.provider();
        let outcome = provider_probe_result(
            probe_provider_connection(
                provider,
                connection.base_url(),
                connection.api_key().map(ApiKey::as_str),
            )
            .await,
        );
        ensure_revision(
            &request.expected_revision,
            &self.config.probe_snapshot().await?.revision,
        )?;
        if retain {
            let receipt = ProviderConnectionProbeReceipt {
                connection: name.clone(),
                provider,
                observed_at_unix_ms: observed_at_unix_ms(),
                revision: request.expected_revision,
                result: outcome,
                freshness: ProbeReceiptFreshness::Current,
            };
            self.retain_provider(&snapshot, receipt).await?;
            let receipt = self
                .provider_receipts()
                .await?
                .into_iter()
                .find(|receipt| receipt.connection == name && receipt.provider == provider)
                .ok_or(ProbeError::MissingObservation)?;
            Ok(ProviderProbeResponse::Stored { receipt })
        } else {
            Ok(ProviderProbeResponse::Candidate {
                observation: CandidateProviderProbeObservation {
                    observed_at_unix_ms: observed_at_unix_ms(),
                    validated_against_revision: request.expected_revision,
                    result: outcome,
                },
            })
        }
    }

    /// Projects named provider evidence against current exact connection inputs.
    pub(crate) async fn provider_receipts(
        &self,
    ) -> Result<Vec<ProviderConnectionProbeReceipt>, ProbeError> {
        let current = self.config.probe_snapshot().await?;
        let stored = self.provider_receipts.lock().await;
        let mut receipts = stored
            .iter()
            .map(|stored| {
                let mut receipt = stored.receipt.clone();
                receipt.freshness = match current
                    .config
                    .provider_connections
                    .get(receipt.connection.as_str())
                {
                    Some(connection)
                        if provider_connection_digest(&receipt.connection, connection)?
                            == stored.input_digest =>
                    {
                        ProbeReceiptFreshness::Current
                    }
                    Some(_) | None => ProbeReceiptFreshness::Stale {
                        current_revision: Some(current.revision.clone()),
                    },
                };
                Ok(receipt)
            })
            .collect::<Result<Vec<_>, ProbeError>>()?;
        receipts.sort_by(|left, right| left.connection.as_str().cmp(right.connection.as_str()));
        Ok(receipts)
    }

    /// Projects retained evidence against the current relevant inputs.
    pub(crate) async fn receipts(&self) -> Result<Vec<ProbeReceipt>, ProbeError> {
        let current = self.config.probe_snapshot().await;
        let current_revision = match &current {
            Ok(snapshot) => Some(snapshot.revision.clone()),
            Err(_) => self
                .config
                .document()
                .await
                .ok()
                .and_then(document_revision),
        };
        let stored = self.receipts.lock().await;
        let mut receipts = stored
            .iter()
            .map(|stored| {
                let mut receipt = stored.receipt.clone();
                receipt.freshness = match &current {
                    Ok(snapshot)
                        if subject_digest(&receipt.subject, &snapshot.config)?
                            == stored.input_digest =>
                    {
                        ProbeReceiptFreshness::Current
                    }
                    Ok(_) | Err(_) => ProbeReceiptFreshness::Stale {
                        current_revision: current_revision.clone(),
                    },
                };
                Ok(receipt)
            })
            .collect::<Result<Vec<_>, ProbeError>>()?;
        receipts.sort_by_key(|receipt| subject_order(&receipt.subject));
        Ok(receipts)
    }

    async fn retain(
        &self,
        snapshot: &ConfigProbeSnapshot,
        subject: ProbeSubject,
        result: ProbeOutcome,
    ) -> Result<(), ProbeError> {
        let observed_at_unix_ms = u64::try_from(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)?
                .as_millis(),
        )?;
        let input_digest = subject_digest(&subject, &snapshot.config)?;
        let stored = StoredReceipt {
            receipt: ProbeReceipt {
                observed_at_unix_ms,
                revision: snapshot.revision.clone(),
                subject: subject.clone(),
                result,
                freshness: ProbeReceiptFreshness::Current,
            },
            input_digest,
        };
        let mut receipts = self.receipts.lock().await;
        if let Some(existing) = receipts
            .iter_mut()
            .find(|receipt| receipt.receipt.subject == subject)
        {
            *existing = stored;
        } else {
            receipts.push(stored);
        }
        Ok(())
    }

    async fn retain_provider(
        &self,
        snapshot: &ConfigProbeSnapshot,
        receipt: ProviderConnectionProbeReceipt,
    ) -> Result<(), ProbeError> {
        let connection = snapshot
            .config
            .provider_connections
            .get(receipt.connection.as_str())
            .ok_or(ProbeError::MissingObservation)?;
        let stored = StoredProviderReceipt {
            input_digest: provider_connection_digest(&receipt.connection, connection)?,
            receipt,
        };
        let mut receipts = self.provider_receipts.lock().await;
        if let Some(existing) = receipts.iter_mut().find(|existing| {
            existing.receipt.connection == stored.receipt.connection
                && existing.receipt.provider == stored.receipt.provider
        }) {
            *existing = stored;
        } else {
            receipts.push(stored);
        }
        Ok(())
    }
}

fn ensure_revision(expected: &ConfigRevision, actual: &ConfigRevision) -> Result<(), ProbeError> {
    if expected == actual {
        Ok(())
    } else {
        Err(ProbeError::RevisionConflict {
            expected: expected.clone(),
            actual: actual.clone(),
        })
    }
}

fn provider_connection_digest(
    name: &ProviderConnectionName,
    connection: &tribal_config::ProviderConnectionConfig,
) -> Result<ConfigDigest, ProbeError> {
    digest(&(name, connection))
}

async fn run_report(snapshot: &ConfigProbeSnapshot) -> Result<Vec<CheckResult>, ProbeError> {
    let output = crate::management::operator_check::run_report_async(
        crate::management::operator_check::CheckReportOptions {
            config_path: &snapshot.path,
            source: crate::management::operator_check::CheckConfigSource::Parsed(Box::new(
                snapshot.config.clone(),
            )),
            providers: false,
            project: None,
            token: None,
        },
    )
    .await?;
    Ok(output.checks)
}

fn provider_probe_result(
    outcome: InferenceProviderConnectionProbeOutcome,
) -> ProviderConnectionProbeResult {
    match outcome {
        InferenceProviderConnectionProbeOutcome::Reachable => {
            ProviderConnectionProbeResult::Reachable
        }
        InferenceProviderConnectionProbeOutcome::Skipped(reason) => {
            ProviderConnectionProbeResult::Skipped {
                reason: match reason {
                    InferenceProviderConnectionProbeSkipReason::ManagedConnection => {
                        ProviderConnectionProbeSkipReason::ManagedConnection
                    }
                },
            }
        }
        InferenceProviderConnectionProbeOutcome::Failed(failure) => {
            ProviderConnectionProbeResult::Failed {
                failure: provider_connection_failure(failure),
            }
        }
    }
}

fn provider_connection_failure(
    failure: InferenceProviderConnectionFailure,
) -> ProviderConnectionFailure {
    ProviderConnectionFailure {
        kind: match failure.kind {
            InferenceProviderConnectionFailureKind::Authentication => {
                ProviderConnectionFailureKind::Authentication
            }
            InferenceProviderConnectionFailureKind::Permission => {
                ProviderConnectionFailureKind::Permission
            }
            InferenceProviderConnectionFailureKind::QuotaExhausted => {
                ProviderConnectionFailureKind::QuotaExhausted
            }
            InferenceProviderConnectionFailureKind::RateLimited => {
                ProviderConnectionFailureKind::RateLimited
            }
            InferenceProviderConnectionFailureKind::Overloaded => {
                ProviderConnectionFailureKind::Overloaded
            }
            InferenceProviderConnectionFailureKind::UpstreamUnavailable => {
                ProviderConnectionFailureKind::UpstreamUnavailable
            }
            InferenceProviderConnectionFailureKind::EndpointUnreachable => {
                ProviderConnectionFailureKind::EndpointUnreachable
            }
            InferenceProviderConnectionFailureKind::TimedOut => {
                ProviderConnectionFailureKind::TimedOut
            }
            InferenceProviderConnectionFailureKind::InvalidRequest => {
                ProviderConnectionFailureKind::InvalidRequest
            }
            InferenceProviderConnectionFailureKind::Unknown => {
                ProviderConnectionFailureKind::Unknown
            }
        },
        provider_code: failure.provider_code,
        http_status: failure.http_status,
        message: failure.message,
        request_id: failure.request_id,
        retry_after_seconds: failure.retry_after_seconds,
    }
}

fn subject_digest(
    subject: &ProbeSubject,
    config: &TribalConfig,
) -> Result<ConfigDigest, ProbeError> {
    match subject {
        ProbeSubject::Database => digest(&config.database),
        ProbeSubject::Provider {
            capability: ProviderProbeCapability::Embedding,
            ..
        } => digest(&(&config.init.embedding, &config.provider_connections)),
        ProbeSubject::Provider {
            capability: ProviderProbeCapability::Extraction,
            ..
        } => digest(&(
            &config.inference.extraction,
            config
                .provider_connections
                .get(config.inference.extraction.connection.as_str()),
        )),
        ProbeSubject::Provider {
            capability: ProviderProbeCapability::Triage,
            ..
        } => digest(&(
            &config.inference.triage,
            config
                .provider_connections
                .get(config.inference.triage.connection.as_str()),
        )),
        ProbeSubject::Provider {
            capability: ProviderProbeCapability::Relation,
            ..
        } => digest(&(
            &config.inference.relation,
            config
                .provider_connections
                .get(config.inference.relation.connection.as_str()),
        )),
    }
}

fn digest(value: &impl serde::Serialize) -> Result<ConfigDigest, ProbeError> {
    let bytes = zeroize::Zeroizing::new(serde_json::to_vec(value)?);
    Ok(ConfigDigest::from_bytes(&bytes))
}

fn result_name(result: &CheckResult) -> CheckName {
    match result {
        CheckResult::Pass { name, .. }
        | CheckResult::Warn { name, .. }
        | CheckResult::Fail { name, .. }
        | CheckResult::Skip { name, .. } => *name,
    }
}

fn probe_outcome(result: CheckResult) -> ProbeOutcome {
    match result {
        CheckResult::Pass { detail, .. } => ProbeOutcome::Pass { detail },
        CheckResult::Warn {
            detail,
            remediation,
            ..
        } => ProbeOutcome::Warn {
            detail,
            remediation,
        },
        CheckResult::Fail {
            detail,
            remediation,
            ..
        } => ProbeOutcome::Fail {
            detail,
            remediation,
        },
        CheckResult::Skip { detail, .. } => ProbeOutcome::Skip { detail },
    }
}

fn document_revision(document: ConfigDocument) -> Option<ConfigRevision> {
    match document {
        ConfigDocument::DurableValid { revision, .. }
        | ConfigDocument::DurableInvalid { revision } => Some(revision),
        ConfigDocument::UncertainValid { .. }
        | ConfigDocument::UncertainInvalid { .. }
        | ConfigDocument::Unreadable { .. } => None,
    }
}

fn subject_order(subject: &ProbeSubject) -> u8 {
    match subject {
        ProbeSubject::Database => 0,
        ProbeSubject::Provider {
            capability: ProviderProbeCapability::Embedding,
            ..
        } => 1,
        ProbeSubject::Provider {
            capability: ProviderProbeCapability::Extraction,
            ..
        } => 2,
        ProbeSubject::Provider {
            capability: ProviderProbeCapability::Triage,
            ..
        } => 3,
        ProbeSubject::Provider {
            capability: ProviderProbeCapability::Relation,
            ..
        } => 4,
    }
}

#[cfg(test)]
mod tests {
    use tribal_wire::management::{ConfigLiteral, ConfigSetRequest};
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    use super::*;

    #[test]
    fn test_relevant_input_digest_ignores_unrelated_changes() {
        let mut config = TribalConfig::minimum_valid("postgres://user:pass@localhost:5432/tribal");
        let subject = ProbeSubject::Database;
        let original = subject_digest(&subject, &config).expect("database digest computes");

        config.auth.token_ttl_hours += 1;
        assert_eq!(
            subject_digest(&subject, &config).expect("unrelated digest computes"),
            original,
        );

        config.database.url = "postgres://user:pass@localhost:5432/other".to_owned();
        assert_ne!(
            subject_digest(&subject, &config).expect("changed digest computes"),
            original,
        );
    }

    #[test]
    fn test_provider_digest_is_scoped_to_its_effective_inputs() {
        let mut config = TribalConfig::minimum_valid("postgres://user:pass@localhost:5432/tribal");
        let subject = ProbeSubject::Provider {
            capability: ProviderProbeCapability::Extraction,
            provider: config
                .provider_connections
                .get(config.inference.extraction.connection.as_str())
                .unwrap()
                .provider(),
        };
        let original = subject_digest(&subject, &config).expect("provider digest computes");

        config.inference.triage.model = "unrelated".to_owned();
        config.init.embedding.model = "also-unrelated".to_owned();
        config.provider_connections.insert(
            tribal_domain::ProviderConnectionName::parse("unrelated").unwrap(),
            tribal_config::ProviderConnectionConfig::OpenAi {
                base_url: "https://api.openai.com".to_owned(),
                api_key: Some("sk-test".parse().unwrap()),
            },
        );
        assert_eq!(
            subject_digest(&subject, &config).expect("unrelated digest computes"),
            original,
        );

        config.inference.extraction.model = "changed".to_owned();
        assert_ne!(
            subject_digest(&subject, &config).expect("changed digest computes"),
            original,
        );
    }

    #[tokio::test]
    async fn test_provider_probe_uses_discovery_without_selecting_a_model() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/tags"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({ "models": [] })),
            )
            .expect(1)
            .mount(&server)
            .await;

        let temp = tempfile::tempdir().expect("temporary config root");
        let path = temp.path().join("tribal.yaml");
        let mut config = TribalConfig::minimum_valid("postgres://user:pass@localhost:5432/tribal");
        let name = config.inference.extraction.connection.clone();
        config.provider_connections.insert(
            name.clone(),
            tribal_config::ProviderConnectionConfig::Ollama {
                base_url: server.uri(),
            },
        );
        std::fs::write(
            &path,
            serde_yaml::to_string(&config).expect("config serialises"),
        )
        .expect("config writes");
        let (config, runtime) =
            super::super::worker::spawn(super::super::configuration::ConfigAuthority::new(path))
                .expect("config worker starts");
        let service = ProbeService::new(config.clone());
        let revision = config
            .probe_snapshot()
            .await
            .expect("probe snapshot is valid")
            .revision;

        let response = service
            .provider(ProviderProbeRequest {
                target: ProviderProbeTarget::Stored { name },
                expected_revision: revision,
            })
            .await
            .expect("provider probe completes");

        assert!(matches!(
            response,
            ProviderProbeResponse::Stored {
                receipt: ProviderConnectionProbeReceipt {
                    result: ProviderConnectionProbeResult::Reachable,
                    ..
                }
            }
        ));
        drop(service);
        drop(config);
        runtime.join().expect("config worker joins");
    }

    #[tokio::test]
    async fn test_relevant_change_marks_retained_receipt_stale_then_probe_replaces_it() {
        let temp = tempfile::tempdir().expect("temporary config root");
        let path = temp.path().join("tribal.yaml");
        let config = TribalConfig::minimum_valid("postgres://user:pass@localhost:5432/tribal");
        std::fs::write(
            &path,
            serde_yaml::to_string(&config).expect("config serialises"),
        )
        .expect("config writes");
        let (config, runtime) =
            super::super::worker::spawn(super::super::configuration::ConfigAuthority::new(path))
                .expect("config worker starts");
        let service = ProbeService::new(config.clone());
        let observed = config
            .probe_snapshot()
            .await
            .expect("probe snapshot is valid");
        service
            .retain(
                &observed,
                ProbeSubject::Database,
                ProbeOutcome::Pass {
                    detail: "reachable".to_owned(),
                },
            )
            .await
            .expect("receipt retains");
        assert!(matches!(
            service.receipts().await.expect("receipts project")[0].freshness,
            ProbeReceiptFreshness::Current
        ));

        let outcome = config
            .set(ConfigSetRequest {
                key: tribal_domain::ConfigFieldPath::parse("database.url")
                    .expect("field path is valid"),
                value: ConfigLiteral::new(serde_json::Value::String(
                    "postgres://user:pass@localhost:5432/other".to_owned(),
                )),
                expected_revision: observed.revision,
            })
            .await
            .expect("database change commits");
        let receipts = service.receipts().await.expect("receipts project");
        assert_eq!(receipts.len(), 1);
        assert!(matches!(
            &receipts[0].freshness,
            ProbeReceiptFreshness::Stale {
                current_revision: Some(revision),
            } if revision == &outcome.revision
        ));

        let current = config
            .probe_snapshot()
            .await
            .expect("changed snapshot is valid");
        service
            .retain(
                &current,
                ProbeSubject::Database,
                ProbeOutcome::Pass {
                    detail: "reachable again".to_owned(),
                },
            )
            .await
            .expect("new receipt replaces old receipt");
        let receipts = service.receipts().await.expect("receipts project");
        assert_eq!(receipts.len(), 1);
        assert_eq!(receipts[0].revision, outcome.revision);
        assert!(matches!(
            receipts[0].freshness,
            ProbeReceiptFreshness::Current
        ));

        drop(service);
        drop(config);
        runtime.join().expect("config worker joins");
    }
}
