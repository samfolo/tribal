//! Revision-bound retention for explicitly requested external probes.

use std::{sync::Arc, time::SystemTime};

use tokio::sync::Mutex;
use tribal_config::TribalConfig;
use tribal_domain::ProviderKind;
use tribal_wire::management::{
    CheckName, CheckResult, ConfigDigest, ConfigDocument, ConfigRevision, ProbeOutcome,
    ProbeReceipt, ProbeReceiptFreshness, ProbeSubject, ProviderProbeCapability,
};

use super::{
    configuration::{ConfigAuthorityError, ConfigProbeSnapshot},
    worker::ConfigWorkerClient,
};

#[derive(Clone)]
pub(crate) struct ProbeService {
    config: ConfigWorkerClient,
    receipts: Arc<Mutex<Vec<StoredReceipt>>>,
}

struct StoredReceipt {
    receipt: ProbeReceipt,
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
        }
    }

    /// Runs the named database probe and retains its revision-bound result.
    pub(crate) async fn database(&self) -> Result<ProbeReceipt, ProbeError> {
        let snapshot = self.config.probe_snapshot().await?;
        let result = run_report(&snapshot, false)
            .await?
            .into_iter()
            .find(|result| result_name(result) == CheckName::DatabaseReachable)
            .ok_or(ProbeError::MissingObservation)?;
        let subject = ProbeSubject::Database;
        self.retain(&snapshot, subject.clone(), probe_outcome(result))
            .await?;
        self.receipts()
            .await?
            .into_iter()
            .find(|receipt| receipt.subject == subject)
            .ok_or(ProbeError::MissingObservation)
    }

    /// Runs only explicitly enabled provider checks and retains each result.
    pub(crate) async fn credentials(&self) -> Result<Vec<ProbeReceipt>, ProbeError> {
        let snapshot = self.config.probe_snapshot().await?;
        let mut subjects = Vec::new();
        for result in run_report(&snapshot, true).await? {
            let check = result_name(&result);
            let Some((capability, provider)) = provider_for(check, &snapshot.config) else {
                continue;
            };
            let subject = ProbeSubject::Provider {
                capability,
                provider,
            };
            self.retain(&snapshot, subject.clone(), probe_outcome(result))
                .await?;
            subjects.push(subject);
        }
        if subjects.is_empty() {
            return Err(ProbeError::MissingObservation);
        }
        Ok(self
            .receipts()
            .await?
            .into_iter()
            .filter(|receipt| subjects.contains(&receipt.subject))
            .collect())
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
}

async fn run_report(
    snapshot: &ConfigProbeSnapshot,
    providers: bool,
) -> Result<Vec<CheckResult>, ProbeError> {
    let output = crate::commands::run_report_async(crate::commands::CheckReportOptions {
        config_path: &snapshot.path,
        source: crate::commands::CheckConfigSource::Parsed(Box::new(snapshot.config.clone())),
        providers,
        project: None,
        token: None,
    })
    .await?;
    Ok(output.checks)
}

fn provider_for(
    check: CheckName,
    config: &TribalConfig,
) -> Option<(ProviderProbeCapability, ProviderKind)> {
    match check {
        CheckName::ProviderEmbedding => Some((
            ProviderProbeCapability::Embedding,
            config.init.embedding.provider,
        )),
        CheckName::ProviderExtraction => Some((
            ProviderProbeCapability::Extraction,
            config.inference.extraction.provider,
        )),
        CheckName::ProviderTriage => Some((
            ProviderProbeCapability::Triage,
            config.inference.triage.provider,
        )),
        CheckName::ProviderRelation => Some((
            ProviderProbeCapability::Relation,
            config.inference.relation.provider,
        )),
        CheckName::ConfigParse
        | CheckName::ConfigValidate
        | CheckName::DatabaseReachable
        | CheckName::MigrationsCurrent
        | CheckName::ProjectResolution
        | CheckName::ValidTokenExists
        | CheckName::AdvertisedUrlReachable
        | CheckName::BinaryUniqueness
        | CheckName::EmbeddingProfile => None,
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
        } => digest(&(&config.init.embedding, &config.credentials)),
        ProbeSubject::Provider {
            capability: ProviderProbeCapability::Extraction,
            ..
        } => digest(&config.inference.extraction),
        ProbeSubject::Provider {
            capability: ProviderProbeCapability::Triage,
            ..
        } => digest(&config.inference.triage),
        ProbeSubject::Provider {
            capability: ProviderProbeCapability::Relation,
            ..
        } => digest(&config.inference.relation),
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
            provider: config.inference.extraction.provider,
        };
        let original = subject_digest(&subject, &config).expect("provider digest computes");

        config.inference.triage.model = "unrelated".to_owned();
        config.init.embedding.model = "also-unrelated".to_owned();
        config.credentials = serde_json::from_value(serde_json::json!({
            "embedding": {
                "provider_kind": "openai",
                "base_url": "https://api.openai.com/v1"
            }
        }))
        .expect("credential catalogue parses");
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
