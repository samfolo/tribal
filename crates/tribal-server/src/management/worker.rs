//! Capacity-one blocking configuration worker and panic terminal channel.

use std::{
    cell::Cell,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::OnceLock,
};

use base64::Engine as _;
use hmac::Mac as _;
use tokio::sync::{broadcast, mpsc, oneshot};
use tribal_wire::management::{
    ConfigChangeEvent, ConfigChangeSource, ConfigDocument, ConfigFilePath, ConfigGetRequest,
    ConfigPatchOutcome, ConfigPatchRequest, ConfigSetRequest, ConfigValue, ConfigWriteEffect,
    ConfigWriteOutcome, PanicCorrelationId,
};

use super::configuration::{
    ConfigAuthority, ConfigAuthorityError, ConfigCheckSnapshot, ConfigProbeSnapshot,
    CredentialMaterial, ResolvedConfigSnapshot,
};

const COMMAND_CAPACITY: usize = 1;
static PANIC_HOOK_INSTALLED: OnceLock<()> = OnceLock::new();

thread_local! {
    static SENSITIVE_PANIC_SCOPE: Cell<bool> = const { Cell::new(false) };
}

/// Cloneable async handle to the synchronous configuration authority.
#[derive(Debug, Clone)]
pub(crate) struct ConfigWorkerClient {
    sender: mpsc::Sender<ConfigCommand>,
    changes: broadcast::Sender<ConfigChangeEvent>,
}

/// Terminal state of the dedicated configuration worker.
#[derive(Debug)]
pub(crate) enum ConfigWorkerExit {
    InputClosed,
    Panicked {
        correlation: Option<PanicCorrelationId>,
    },
}

/// Tracked execution resources for the dedicated worker.
pub(crate) struct ConfigWorkerRuntime {
    terminal: Option<oneshot::Receiver<ConfigWorkerExit>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl ConfigWorkerRuntime {
    /// Transfers the fatal channel to the lifecycle reducer.
    pub(crate) fn take_terminal(&mut self) -> Option<oneshot::Receiver<ConfigWorkerExit>> {
        self.terminal.take()
    }

    /// Joins the worker after its terminal channel has resolved.
    pub(crate) fn join(mut self) -> std::thread::Result<()> {
        self.thread
            .take()
            .map_or(Ok(()), std::thread::JoinHandle::join)
    }
}

/// Failure initialising panic-safe worker supervision.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ConfigWorkerStartError {
    #[error("operating-system entropy is unavailable: {source}")]
    Entropy {
        #[source]
        source: getrandom::Error,
    },
    #[error("starting the dedicated configuration thread: {source}")]
    Thread {
        #[source]
        source: std::io::Error,
    },
}

/// Fail-closed result of a one-shot sensitive configuration call.
#[derive(Debug, thiserror::Error)]
pub(crate) enum OneShotConfigError {
    #[error("operating-system entropy is unavailable: {source}")]
    Entropy {
        #[source]
        source: getrandom::Error,
    },
    #[error("configuration operation panicked; correlation: {correlation:?}")]
    Panicked {
        correlation: Option<PanicCorrelationId>,
    },
}

enum ConfigCommand {
    Path(oneshot::Sender<ConfigFilePath>),
    Document(oneshot::Sender<Result<ConfigDocument, ConfigAuthorityError>>),
    Get {
        request: ConfigGetRequest,
        response: oneshot::Sender<Result<ConfigValue, ConfigAuthorityError>>,
    },
    Set {
        request: ConfigSetRequest,
        response: oneshot::Sender<Result<ConfigWriteOutcome, ConfigAuthorityError>>,
    },
    Patch {
        request: ConfigPatchRequest,
        response: oneshot::Sender<Result<ConfigPatchOutcome, ConfigAuthorityError>>,
    },
    Validate {
        key: String,
        value: serde_json::Value,
        response:
            oneshot::Sender<Result<Vec<tribal_config::ConfigViolation>, ConfigAuthorityError>>,
    },
    CredentialMaterials(oneshot::Sender<Result<Vec<CredentialMaterial>, ConfigAuthorityError>>),
    ProbeSnapshot(oneshot::Sender<Result<ConfigProbeSnapshot, ConfigAuthorityError>>),
    CheckSnapshot(oneshot::Sender<Result<ConfigCheckSnapshot, ConfigAuthorityError>>),
    ResolvedSnapshot(oneshot::Sender<Result<ResolvedConfigSnapshot, ConfigAuthorityError>>),
    #[cfg(test)]
    Panic,
}

struct PanicReporter {
    key: zeroize::Zeroizing<[u8; 32]>,
    opaque_sequence: Cell<u64>,
}

impl std::fmt::Debug for PanicReporter {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("<redacted config worker panic reporter>")
    }
}

impl PanicReporter {
    fn generate() -> Result<Self, getrandom::Error> {
        let mut key = zeroize::Zeroizing::new([0_u8; 32]);
        getrandom::fill(key.as_mut())?;
        Ok(Self {
            key,
            opaque_sequence: Cell::new(0),
        })
    }

    fn correlate(&self, panic: Box<dyn std::any::Any + Send>) -> Option<PanicCorrelationId> {
        let (kind, payload) = match panic.downcast::<String>() {
            Ok(value) => (
                b"string".as_slice(),
                zeroize::Zeroizing::new((*value).into_bytes()),
            ),
            Err(panic) => match panic.downcast::<&'static str>() {
                Ok(value) => (
                    b"static-str".as_slice(),
                    zeroize::Zeroizing::new(value.as_bytes().to_vec()),
                ),
                Err(panic) => {
                    let sequence = self.opaque_sequence.get().checked_add(1)?;
                    self.opaque_sequence.set(sequence);
                    std::mem::forget(panic);
                    (
                        b"opaque".as_slice(),
                        zeroize::Zeroizing::new(sequence.to_be_bytes().to_vec()),
                    )
                }
            },
        };
        let mut mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(self.key.as_ref()).ok()?;
        update_frame(&mut mac, b"tribal.config-worker-panic.v1");
        update_frame(&mut mac, kind);
        update_frame(&mut mac, &payload);
        let digest = mac.finalize().into_bytes();
        let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(digest);
        PanicCorrelationId::parse(&format!("pcorr_{encoded}")).ok()
    }
}

/// Starts the sole blocking configuration worker and terminal signal.
pub(crate) fn spawn(
    authority: ConfigAuthority,
) -> Result<(ConfigWorkerClient, ConfigWorkerRuntime), ConfigWorkerStartError> {
    let reporter =
        PanicReporter::generate().map_err(|source| ConfigWorkerStartError::Entropy { source })?;
    let (sender, mut receiver) = mpsc::channel(COMMAND_CAPACITY);
    let (changes, _) = broadcast::channel(16);
    let worker_changes = changes.clone();
    let (terminal_sender, terminal_receiver) = oneshot::channel();
    let thread = std::thread::Builder::new()
        .name("tribal-config-authority".to_owned())
        .spawn(move || {
            let terminal = loop {
                let Some(command) = receiver.blocking_recv() else {
                    break ConfigWorkerExit::InputClosed;
                };
                if let Err(panic) = catch_sensitive(|| {
                    dispatch(&authority, command, &worker_changes);
                }) {
                    break ConfigWorkerExit::Panicked {
                        correlation: reporter.correlate(panic),
                    };
                }
            };
            let _ = terminal_sender.send(terminal);
        })
        .map_err(|source| ConfigWorkerStartError::Thread { source })?;
    Ok((
        ConfigWorkerClient { sender, changes },
        ConfigWorkerRuntime {
            terminal: Some(terminal_receiver),
            thread: Some(thread),
        },
    ))
}

fn catch_sensitive<T>(operation: impl FnOnce() -> T) -> Result<T, Box<dyn std::any::Any + Send>> {
    install_sensitive_panic_hook();
    let previous = SENSITIVE_PANIC_SCOPE.replace(true);
    let outcome = catch_unwind(AssertUnwindSafe(operation));
    SENSITIVE_PANIC_SCOPE.set(previous);
    outcome
}

fn install_sensitive_panic_hook() {
    PANIC_HOOK_INSTALLED.get_or_init(|| {
        let inherited = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |panic| {
            if !SENSITIVE_PANIC_SCOPE.get() {
                inherited(panic);
            }
        }));
    });
}

fn update_frame(mac: &mut hmac::Hmac<sha2::Sha256>, value: &[u8]) {
    mac.update(&(value.len() as u64).to_be_bytes());
    mac.update(value);
}

pub(crate) fn run_one_shot<T>(operation: impl FnOnce() -> T) -> Result<T, OneShotConfigError> {
    let reporter =
        PanicReporter::generate().map_err(|source| OneShotConfigError::Entropy { source })?;
    catch_sensitive(operation).map_err(|panic| OneShotConfigError::Panicked {
        correlation: reporter.correlate(panic),
    })
}

fn dispatch(
    authority: &ConfigAuthority,
    command: ConfigCommand,
    changes: &broadcast::Sender<ConfigChangeEvent>,
) {
    match command {
        ConfigCommand::Path(response) => {
            let _ = response.send(authority.path());
        }
        ConfigCommand::Document(response) => {
            let _ = response.send(authority.document());
        }
        ConfigCommand::Get { request, response } => {
            let _ = response.send(authority.get(request));
        }
        ConfigCommand::Set { request, response } => {
            let key = request.key.clone();
            let result = authority.set(request);
            if let Ok(outcome) = &result
                && !matches!(outcome.effect, ConfigWriteEffect::Unchanged)
            {
                let _ = changes.send(ConfigChangeEvent {
                    revision: outcome.revision.clone(),
                    source: ConfigChangeSource::Managed,
                    changed: vec![key],
                });
            }
            let _ = response.send(result);
        }
        ConfigCommand::Patch { request, response } => {
            let result = authority.patch(request);
            if let Ok(outcome) = &result {
                let changed_fields = outcome
                    .fields
                    .iter()
                    .filter(|field| !matches!(field.effect, ConfigWriteEffect::Unchanged))
                    .map(|field| field.key.clone())
                    .collect::<Vec<_>>();
                if !changed_fields.is_empty() {
                    let _ = changes.send(ConfigChangeEvent {
                        revision: outcome.revision.clone(),
                        source: ConfigChangeSource::Managed,
                        changed: changed_fields,
                    });
                }
            }
            let _ = response.send(result);
        }
        ConfigCommand::Validate {
            key,
            value,
            response,
        } => {
            let _ = response.send(authority.validate_value(&key, value));
        }
        ConfigCommand::CredentialMaterials(response) => {
            let _ = response.send(authority.credential_materials());
        }
        ConfigCommand::ProbeSnapshot(response) => {
            let _ = response.send(authority.probe_snapshot());
        }
        ConfigCommand::CheckSnapshot(response) => {
            let _ = response.send(authority.check_snapshot());
        }
        ConfigCommand::ResolvedSnapshot(response) => {
            let _ = response.send(authority.resolved_snapshot());
        }
        #[cfg(test)]
        ConfigCommand::Panic => panic!("config-worker-test-panic"),
    }
}

impl ConfigWorkerClient {
    pub(crate) fn subscribe(&self) -> broadcast::Receiver<ConfigChangeEvent> {
        self.changes.subscribe()
    }

    pub(crate) fn publish_raw_change(&self, revision: tribal_wire::management::ConfigRevision) {
        let _ = self.changes.send(ConfigChangeEvent {
            revision,
            source: ConfigChangeSource::RawFile,
            changed: Vec::new(),
        });
    }

    #[cfg(test)]
    pub(crate) async fn panic_for_test(&self) -> bool {
        self.sender.send(ConfigCommand::Panic).await.is_ok()
    }

    pub(crate) async fn path(&self) -> Result<ConfigFilePath, ConfigAuthorityError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(ConfigCommand::Path(response))
            .await
            .map_err(|_| ConfigAuthorityError::WorkerUnavailable)?;
        receiver
            .await
            .map_err(|_| ConfigAuthorityError::WorkerUnavailable)
    }

    pub(crate) async fn document(&self) -> Result<ConfigDocument, ConfigAuthorityError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(ConfigCommand::Document(response))
            .await
            .map_err(|_| ConfigAuthorityError::WorkerUnavailable)?;
        receiver
            .await
            .unwrap_or(Err(ConfigAuthorityError::WorkerUnavailable))
    }

    pub(crate) async fn get(
        &self,
        request: ConfigGetRequest,
    ) -> Result<ConfigValue, ConfigAuthorityError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(ConfigCommand::Get { request, response })
            .await
            .map_err(|_| ConfigAuthorityError::WorkerUnavailable)?;
        receiver
            .await
            .unwrap_or(Err(ConfigAuthorityError::WorkerUnavailable))
    }

    pub(crate) async fn set(
        &self,
        request: ConfigSetRequest,
    ) -> Result<ConfigWriteOutcome, ConfigAuthorityError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(ConfigCommand::Set { request, response })
            .await
            .map_err(|_| ConfigAuthorityError::WorkerUnavailable)?;
        receiver
            .await
            .unwrap_or(Err(ConfigAuthorityError::WorkerUnavailable))
    }

    pub(crate) async fn patch(
        &self,
        request: ConfigPatchRequest,
    ) -> Result<ConfigPatchOutcome, ConfigAuthorityError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(ConfigCommand::Patch { request, response })
            .await
            .map_err(|_| ConfigAuthorityError::WorkerUnavailable)?;
        receiver
            .await
            .unwrap_or(Err(ConfigAuthorityError::WorkerUnavailable))
    }

    pub(crate) async fn validate(
        &self,
        key: String,
        value: serde_json::Value,
    ) -> Result<Vec<tribal_config::ConfigViolation>, ConfigAuthorityError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(ConfigCommand::Validate {
                key,
                value,
                response,
            })
            .await
            .map_err(|_| ConfigAuthorityError::WorkerUnavailable)?;
        receiver
            .await
            .unwrap_or(Err(ConfigAuthorityError::WorkerUnavailable))
    }

    pub(crate) async fn credential_materials(
        &self,
    ) -> Result<Vec<CredentialMaterial>, ConfigAuthorityError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(ConfigCommand::CredentialMaterials(response))
            .await
            .map_err(|_| ConfigAuthorityError::WorkerUnavailable)?;
        receiver
            .await
            .unwrap_or(Err(ConfigAuthorityError::WorkerUnavailable))
    }

    pub(crate) async fn database_url(
        &self,
    ) -> Result<zeroize::Zeroizing<String>, ConfigAuthorityError> {
        let snapshot = self.resolved_snapshot().await?;
        Ok(zeroize::Zeroizing::new(
            snapshot.config.database.url.clone(),
        ))
    }

    pub(crate) async fn probe_snapshot(&self) -> Result<ConfigProbeSnapshot, ConfigAuthorityError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(ConfigCommand::ProbeSnapshot(response))
            .await
            .map_err(|_| ConfigAuthorityError::WorkerUnavailable)?;
        receiver
            .await
            .unwrap_or(Err(ConfigAuthorityError::WorkerUnavailable))
    }

    pub(crate) async fn check_snapshot(&self) -> Result<ConfigCheckSnapshot, ConfigAuthorityError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(ConfigCommand::CheckSnapshot(response))
            .await
            .map_err(|_| ConfigAuthorityError::WorkerUnavailable)?;
        receiver
            .await
            .unwrap_or(Err(ConfigAuthorityError::WorkerUnavailable))
    }

    pub(crate) async fn resolved_snapshot(
        &self,
    ) -> Result<ResolvedConfigSnapshot, ConfigAuthorityError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(ConfigCommand::ResolvedSnapshot(response))
            .await
            .map_err(|_| ConfigAuthorityError::WorkerUnavailable)?;
        receiver
            .await
            .unwrap_or(Err(ConfigAuthorityError::WorkerUnavailable))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_worker_serves_document_through_capacity_one_channel() {
        let temp = tempfile::tempdir().expect("temporary config root");
        let path = temp.path().join("tribal.yaml");
        let config = tribal_config::TribalConfig::minimum_valid(
            "postgres://user:pass@localhost:5432/tribal",
        );
        std::fs::write(
            &path,
            serde_yaml::to_string(&config).expect("config serialises"),
        )
        .expect("config writes");
        let (worker, _terminal) = spawn(ConfigAuthority::new(path)).expect("worker starts");
        assert!(matches!(
            worker.document().await.expect("document returns"),
            ConfigDocument::DurableValid { .. }
        ));
    }

    #[test]
    fn test_panic_reporter_debug_and_correlation_hide_payload() {
        let reporter = PanicReporter::generate().expect("panic reporter starts");
        let sentinel = "sentinel-panic-secret".to_owned();
        let correlation = reporter
            .correlate(Box::new(sentinel.clone()))
            .expect("panic correlation succeeds");
        assert!(!format!("{reporter:?}").contains(&sentinel));
        assert!(!correlation.as_str().contains(&sentinel));
    }

    #[test]
    fn test_correlation_is_stable_per_key_and_separated_between_keys() {
        let first = PanicReporter {
            key: zeroize::Zeroizing::new([1_u8; 32]),
            opaque_sequence: Cell::new(0),
        };
        let second = PanicReporter {
            key: zeroize::Zeroizing::new([2_u8; 32]),
            opaque_sequence: Cell::new(0),
        };
        let left = first
            .correlate(Box::new("same".to_owned()))
            .expect("first correlation succeeds");
        let repeated = first
            .correlate(Box::new("same".to_owned()))
            .expect("repeated correlation succeeds");
        let separated = second
            .correlate(Box::new("same".to_owned()))
            .expect("second-key correlation succeeds");
        assert_eq!(left, repeated);
        assert_ne!(left, separated);
    }

    #[test]
    fn test_opaque_panic_payload_is_quarantined_without_running_its_destructor() {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };

        struct Opaque(Arc<AtomicUsize>);

        impl Drop for Opaque {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
                panic!("opaque destructor must be quarantined");
            }
        }

        let drops = Arc::new(AtomicUsize::new(0));
        let reporter = PanicReporter {
            key: zeroize::Zeroizing::new([3_u8; 32]),
            opaque_sequence: Cell::new(0),
        };
        let correlation = reporter
            .correlate(Box::new(Opaque(Arc::clone(&drops))))
            .expect("opaque correlation succeeds");
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert!(correlation.as_str().starts_with("pcorr_"));
    }

    #[test]
    fn test_sensitive_scope_catches_a_panic_without_crossing_threads() {
        let panic = catch_sensitive(|| panic!("sentinel-sensitive-panic"));
        assert!(panic.is_err());

        let child_scope = catch_sensitive(|| {
            std::thread::spawn(|| SENSITIVE_PANIC_SCOPE.get())
                .join()
                .expect("child thread joins")
        })
        .expect("outer sensitive call returns");
        assert!(!child_scope, "sensitive suppression remains thread-local");
    }

    #[test]
    fn test_one_shot_panic_returns_only_an_opaque_correlation() {
        let error = run_one_shot(|| panic!("one-shot-secret"))
            .expect_err("one-shot panic is returned as a typed error");
        assert!(matches!(
            error,
            OneShotConfigError::Panicked {
                correlation: Some(ref correlation),
            } if correlation.as_str().starts_with("pcorr_")
                && !correlation.as_str().contains("one-shot-secret")
        ));
    }
}
