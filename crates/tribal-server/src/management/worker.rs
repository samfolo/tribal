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
    ConfigWriteOutcome, ManagementResponseError, PanicCorrelationId,
};

use super::{
    application::operation::{self, OperationContext, OperationError},
    configuration::{
        ConfigAuthority, ConfigAuthorityError, ConfigCheckSnapshot, ConfigProbeSnapshot,
        CredentialMaterial, ResolvedConfigSnapshot, management_error,
    },
};

const COMMAND_CAPACITY: usize = 1;
static PANIC_HOOK_INSTALLED: OnceLock<()> = OnceLock::new();

thread_local! {
    static SENSITIVE_PANIC_SCOPE: Cell<bool> = const { Cell::new(false) };
}

/// Cloneable async handle to the synchronous configuration authority.
#[derive(Debug, Clone)]
pub(crate) struct ConfigWorkerClient {
    sender: mpsc::Sender<ConfigWorkerCommand>,
    changes: broadcast::Sender<ConfigChangeEvent>,
}

/// Configuration access bound to one management operation.
pub(in crate::management) struct ConfigWorkerOperation<'a> {
    client: &'a ConfigWorkerClient,
    operation: &'a OperationContext,
}

/// Failure admitting or completing an operation-bound configuration command.
#[derive(Debug, thiserror::Error)]
pub(in crate::management) enum ConfigWorkerRequestError {
    #[error(transparent)]
    Operation(#[from] OperationError),
    #[error(transparent)]
    Authority(#[from] ConfigAuthorityError),
}

impl ConfigWorkerRequestError {
    pub(in crate::management) fn into_public_error(self) -> ManagementResponseError {
        match self {
            Self::Operation(error) => operation::public_error(error),
            Self::Authority(error) => management_error(error),
        }
    }
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

type ConfigResponse<T> = oneshot::Sender<Result<T, OperationError>>;

struct ConfigWorkerCommand {
    operation: Option<OperationContext>,
    command: ConfigCommand,
}

enum ConfigCommand {
    Path(ConfigResponse<ConfigFilePath>),
    Document(ConfigResponse<Result<ConfigDocument, ConfigAuthorityError>>),
    Get {
        request: ConfigGetRequest,
        response: ConfigResponse<Result<ConfigValue, ConfigAuthorityError>>,
    },
    Set {
        request: ConfigSetRequest,
        response: ConfigResponse<Result<ConfigWriteOutcome, ConfigAuthorityError>>,
    },
    Patch {
        request: ConfigPatchRequest,
        response: ConfigResponse<Result<ConfigPatchOutcome, ConfigAuthorityError>>,
    },
    Validate {
        key: String,
        value: serde_json::Value,
        response: ConfigResponse<Result<Vec<tribal_config::ConfigViolation>, ConfigAuthorityError>>,
    },
    CredentialMaterials(ConfigResponse<Result<Vec<CredentialMaterial>, ConfigAuthorityError>>),
    ProbeSnapshot(ConfigResponse<Result<ConfigProbeSnapshot, ConfigAuthorityError>>),
    CheckSnapshot(ConfigResponse<Result<ConfigCheckSnapshot, ConfigAuthorityError>>),
    ResolvedSnapshot(ConfigResponse<Result<ResolvedConfigSnapshot, ConfigAuthorityError>>),
    #[cfg(test)]
    Block {
        entered: oneshot::Sender<()>,
        release: std::sync::mpsc::Receiver<()>,
    },
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

fn dispatch(
    authority: &ConfigAuthority,
    envelope: ConfigWorkerCommand,
    changes: &broadcast::Sender<ConfigChangeEvent>,
) {
    let admitted = envelope
        .operation
        .as_ref()
        .map_or(Ok(()), OperationContext::checkpoint);
    let command = envelope.command;
    match command {
        ConfigCommand::Path(response) => {
            let _ = response.send(admitted.map(|()| authority.path()));
        }
        ConfigCommand::Document(response) => {
            let _ = response.send(admitted.map(|()| authority.document()));
        }
        ConfigCommand::Get { request, response } => {
            let _ = response.send(admitted.map(|()| authority.get(request)));
        }
        ConfigCommand::Set { request, response } => {
            let key = request.key.clone();
            let result = admitted.map(|()| authority.set(request));
            if let Ok(Ok(outcome)) = &result
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
            let result = admitted.map(|()| authority.patch(request));
            if let Ok(Ok(outcome)) = &result {
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
            let _ = response.send(admitted.map(|()| authority.validate_value(&key, value)));
        }
        ConfigCommand::CredentialMaterials(response) => {
            let _ = response.send(admitted.map(|()| authority.credential_materials()));
        }
        ConfigCommand::ProbeSnapshot(response) => {
            let _ = response.send(admitted.map(|()| authority.probe_snapshot()));
        }
        ConfigCommand::CheckSnapshot(response) => {
            let _ = response.send(admitted.map(|()| authority.check_snapshot()));
        }
        ConfigCommand::ResolvedSnapshot(response) => {
            let _ = response.send(admitted.map(|()| authority.resolved_snapshot()));
        }
        #[cfg(test)]
        ConfigCommand::Block { entered, release } => {
            let _ = entered.send(());
            let _ = release.recv();
        }
        #[cfg(test)]
        ConfigCommand::Panic => panic!("config-worker-test-panic"),
    }
}

impl ConfigWorkerClient {
    async fn request<T>(
        &self,
        command: impl FnOnce(ConfigResponse<T>) -> ConfigCommand,
    ) -> Result<T, ConfigAuthorityError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(ConfigWorkerCommand {
                operation: None,
                command: command(response),
            })
            .await
            .map_err(|_| ConfigAuthorityError::WorkerUnavailable)?;
        receiver
            .await
            .map_err(|_| ConfigAuthorityError::WorkerUnavailable)?
            .map_err(|_| ConfigAuthorityError::WorkerUnavailable)
    }

    pub(in crate::management) fn for_operation<'a>(
        &'a self,
        operation: &'a OperationContext,
    ) -> ConfigWorkerOperation<'a> {
        ConfigWorkerOperation {
            client: self,
            operation,
        }
    }

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
        self.sender
            .send(ConfigWorkerCommand {
                operation: None,
                command: ConfigCommand::Panic,
            })
            .await
            .is_ok()
    }

    #[cfg(test)]
    async fn block_for_test(
        &self,
        entered: oneshot::Sender<()>,
        release: std::sync::mpsc::Receiver<()>,
    ) {
        self.sender
            .send(ConfigWorkerCommand {
                operation: None,
                command: ConfigCommand::Block { entered, release },
            })
            .await
            .expect("config worker accepts blocking fixture");
    }

    #[cfg(test)]
    async fn enqueue_path_for_test(
        &self,
    ) -> oneshot::Receiver<Result<ConfigFilePath, OperationError>> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(ConfigWorkerCommand {
                operation: None,
                command: ConfigCommand::Path(response),
            })
            .await
            .expect("config worker accepts capacity fixture");
        receiver
    }

    pub(crate) async fn document(&self) -> Result<ConfigDocument, ConfigAuthorityError> {
        self.request(ConfigCommand::Document).await?
    }

    #[cfg(test)]
    pub(crate) async fn set(
        &self,
        request: ConfigSetRequest,
    ) -> Result<ConfigWriteOutcome, ConfigAuthorityError> {
        self.request(|response| ConfigCommand::Set { request, response })
            .await?
    }

    pub(crate) async fn probe_snapshot(&self) -> Result<ConfigProbeSnapshot, ConfigAuthorityError> {
        self.request(ConfigCommand::ProbeSnapshot).await?
    }

    pub(crate) async fn check_snapshot(&self) -> Result<ConfigCheckSnapshot, ConfigAuthorityError> {
        self.request(ConfigCommand::CheckSnapshot).await?
    }

    #[cfg(test)]
    pub(crate) async fn resolved_snapshot(
        &self,
    ) -> Result<ResolvedConfigSnapshot, ConfigAuthorityError> {
        self.request(ConfigCommand::ResolvedSnapshot).await?
    }
}

#[derive(Clone, Copy)]
enum ConfigCompletion {
    CancelSafe,
    Terminal,
}

impl ConfigWorkerOperation<'_> {
    async fn request<T>(
        &self,
        completion: ConfigCompletion,
        command: impl FnOnce(ConfigResponse<T>) -> ConfigCommand,
    ) -> Result<T, ConfigWorkerRequestError> {
        let (response, receiver) = oneshot::channel();
        let permit = self
            .operation
            .cancel_safe(self.client.sender.reserve())
            .await?
            .map_err(|_| ConfigAuthorityError::WorkerUnavailable)?;
        permit.send(ConfigWorkerCommand {
            operation: Some(self.operation.clone()),
            command: command(response),
        });
        let response = match completion {
            ConfigCompletion::CancelSafe => self
                .operation
                .cancel_safe(receiver)
                .await?
                .map_err(|_| ConfigAuthorityError::WorkerUnavailable)?,
            ConfigCompletion::Terminal => receiver
                .await
                .map_err(|_| ConfigAuthorityError::WorkerUnavailable)?,
        };
        response.map_err(ConfigWorkerRequestError::from)
    }

    pub(in crate::management) async fn path(
        &self,
    ) -> Result<ConfigFilePath, ConfigWorkerRequestError> {
        self.request(ConfigCompletion::CancelSafe, ConfigCommand::Path)
            .await
    }

    pub(in crate::management) async fn document(
        &self,
    ) -> Result<ConfigDocument, ConfigWorkerRequestError> {
        self.request(ConfigCompletion::CancelSafe, ConfigCommand::Document)
            .await?
            .map_err(ConfigWorkerRequestError::from)
    }

    pub(in crate::management) async fn get(
        &self,
        request: ConfigGetRequest,
    ) -> Result<ConfigValue, ConfigWorkerRequestError> {
        self.request(ConfigCompletion::CancelSafe, |response| {
            ConfigCommand::Get { request, response }
        })
        .await?
        .map_err(ConfigWorkerRequestError::from)
    }

    pub(in crate::management) async fn set(
        &self,
        request: ConfigSetRequest,
    ) -> Result<ConfigWriteOutcome, ConfigWorkerRequestError> {
        self.request(ConfigCompletion::Terminal, |response| ConfigCommand::Set {
            request,
            response,
        })
        .await?
        .map_err(ConfigWorkerRequestError::from)
    }

    pub(in crate::management) async fn patch(
        &self,
        request: ConfigPatchRequest,
    ) -> Result<ConfigPatchOutcome, ConfigWorkerRequestError> {
        self.request(ConfigCompletion::Terminal, |response| {
            ConfigCommand::Patch { request, response }
        })
        .await?
        .map_err(ConfigWorkerRequestError::from)
    }

    pub(in crate::management) async fn validate(
        &self,
        key: String,
        value: serde_json::Value,
    ) -> Result<Vec<tribal_config::ConfigViolation>, ConfigWorkerRequestError> {
        self.request(ConfigCompletion::CancelSafe, |response| {
            ConfigCommand::Validate {
                key,
                value,
                response,
            }
        })
        .await?
        .map_err(ConfigWorkerRequestError::from)
    }

    pub(in crate::management) async fn credential_materials(
        &self,
    ) -> Result<Vec<CredentialMaterial>, ConfigWorkerRequestError> {
        self.request(
            ConfigCompletion::CancelSafe,
            ConfigCommand::CredentialMaterials,
        )
        .await?
        .map_err(ConfigWorkerRequestError::from)
    }

    pub(in crate::management) async fn resolved_snapshot(
        &self,
    ) -> Result<ResolvedConfigSnapshot, ConfigWorkerRequestError> {
        self.request(
            ConfigCompletion::CancelSafe,
            ConfigCommand::ResolvedSnapshot,
        )
        .await?
        .map_err(ConfigWorkerRequestError::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::management::application::operation::ACTIVE_WINDOW;

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

    #[tokio::test(start_paused = true)]
    async fn test_expired_write_never_enters_a_saturated_worker() {
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
        let (worker, runtime) = spawn(ConfigAuthority::new(path)).expect("worker starts");
        let before = worker
            .resolved_snapshot()
            .await
            .expect("initial snapshot resolves");
        let (entered_sender, entered_receiver) = oneshot::channel();
        let (release_sender, release_receiver) = std::sync::mpsc::channel();
        worker
            .block_for_test(entered_sender, release_receiver)
            .await;
        entered_receiver.await.expect("blocking fixture enters");
        let capacity = worker.enqueue_path_for_test().await;
        let operation = OperationContext::new(tokio_util::sync::CancellationToken::new());
        let write_worker = worker.clone();
        let expected_revision = before.revision.clone();
        let write = tokio::spawn(async move {
            write_worker
                .for_operation(&operation)
                .set(ConfigSetRequest {
                    key: tribal_domain::ConfigFieldPath::parse("database.url")
                        .expect("field path is valid"),
                    value: tribal_wire::management::ConfigLiteral::new(serde_json::Value::String(
                        "postgres://user:pass@localhost:5432/other".to_owned(),
                    )),
                    expected_revision,
                })
                .await
        });
        tokio::task::yield_now().await;

        tokio::time::advance(ACTIVE_WINDOW).await;
        let failure = write.await.expect("write task joins").unwrap_err();

        assert!(matches!(
            failure,
            ConfigWorkerRequestError::Operation(OperationError::DeadlineElapsed)
        ));
        release_sender.send(()).expect("blocking fixture releases");
        capacity
            .await
            .expect("capacity fixture responds")
            .expect("capacity fixture remains admitted");
        let after = worker
            .resolved_snapshot()
            .await
            .expect("final snapshot resolves");
        assert_eq!(after.revision, before.revision);
        drop(worker);
        runtime.join().expect("config worker joins");
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
}
