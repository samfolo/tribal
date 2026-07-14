//! Process-only manager launch and attachment.

use std::{
    ffi::{OsStr, OsString},
    io,
    path::PathBuf,
    process::Stdio,
};

use tokio::{io::AsyncReadExt as _, process::Command};
use tribal_wire::management::{
    ManagementCall, ManagerAnnouncement, ManagerLaunchDisposition, ManagerLaunchFailure,
    ManagerLaunchRecord,
};

use super::client::{ManagementClient, ManagementClientError};

const MAX_LAUNCH_RECORD_BYTES: usize = 64 * 1024;
const LAUNCH_RECORD_DEADLINE: std::time::Duration = std::time::Duration::from_secs(20);
const ATTACH_EXIT_GRACE: std::time::Duration = std::time::Duration::from_millis(100);

/// Launches or discovers the manager for one configuration path.
#[derive(Debug, Clone)]
pub struct ManagerConnector {
    executable: PathBuf,
    config_path: PathBuf,
    environment: Vec<(OsString, OsString)>,
}

/// One compatible connection and the launch evidence that established it.
pub struct ManagerConnection {
    client: ManagementClient,
    announcement: ManagerAnnouncement,
    disposition: ManagerLaunchDisposition,
}

/// Failure launching, discovering, or attaching to the manager.
#[derive(Debug, thiserror::Error)]
pub enum ManagerConnectorError {
    #[error("resolving the current executable: {source}")]
    CurrentExecutable {
        #[source]
        source: io::Error,
    },
    #[error("launching the management authority: {source}")]
    Launch {
        #[source]
        source: io::Error,
    },
    #[error("the management authority did not expose launch output")]
    MissingLaunchOutput,
    #[error("reading the management launch record: {source}")]
    LaunchRecordRead {
        #[source]
        source: io::Error,
    },
    #[error("the management authority did not announce within the launch deadline")]
    LaunchRecordTimeout,
    #[error("the management launch record exceeds its size limit")]
    LaunchRecordTooLarge,
    #[error("decoding the management launch record: {source}")]
    LaunchRecordInvalid {
        #[source]
        source: serde_json::Error,
    },
    #[error("manager launch refused: {failure:?}")]
    LaunchRefused { failure: ManagerLaunchFailure },
    #[error("the launched manager disappeared before attachment")]
    ManagerDisappeared,
    #[error("attaching to the management authority: {source}")]
    Attach {
        #[source]
        source: ManagementClientError,
    },
}

impl ManagerConnector {
    /// Uses the current executable to establish a manager session.
    ///
    /// # Errors
    ///
    /// Returns an error when the current executable cannot be resolved.
    pub fn new(config_path: impl Into<PathBuf>) -> Result<Self, ManagerConnectorError> {
        let executable = std::env::current_exe()
            .map_err(|source| ManagerConnectorError::CurrentExecutable { source })?;
        Ok(Self {
            executable,
            config_path: config_path.into(),
            environment: Vec::new(),
        })
    }

    /// Uses an explicit executable while retaining the production launch protocol.
    #[must_use]
    pub fn with_executable(
        executable: impl Into<PathBuf>,
        config_path: impl Into<PathBuf>,
    ) -> Self {
        Self {
            executable: executable.into(),
            config_path: config_path.into(),
            environment: Vec::new(),
        }
    }

    /// Adds one environment value to the launched manager process.
    #[must_use]
    pub fn environment(mut self, key: impl AsRef<OsStr>, value: impl AsRef<OsStr>) -> Self {
        self.environment
            .push((key.as_ref().to_owned(), value.as_ref().to_owned()));
        self
    }

    /// Launches or joins the manager and verifies its announced identity.
    ///
    /// # Errors
    ///
    /// Returns an error when launch is refused or a compatible manager cannot be attached.
    pub async fn connect(self) -> Result<ManagerConnection, ManagerConnectorError> {
        self.connect_once().await
    }

    async fn connect_once(&self) -> Result<ManagerConnection, ManagerConnectorError> {
        let mut command = Command::new(&self.executable);
        command
            .arg("--config")
            .arg(&self.config_path)
            .arg("manager")
            .arg("run")
            .arg("--announce-json")
            .envs(self.environment.iter().cloned())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        let mut child = command
            .spawn()
            .map_err(|source| ManagerConnectorError::Launch { source })?;
        let stdout = child
            .stdout
            .take()
            .ok_or(ManagerConnectorError::MissingLaunchOutput)?;
        let mut bytes = Vec::new();
        let mut bounded = stdout.take((MAX_LAUNCH_RECORD_BYTES + 1) as u64);
        let read = bounded.read_to_end(&mut bytes);
        match tokio::time::timeout(LAUNCH_RECORD_DEADLINE, read).await {
            Ok(Ok(_)) => {}
            Ok(Err(source)) => {
                stop_child(&mut child).await;
                return Err(ManagerConnectorError::LaunchRecordRead { source });
            }
            Err(_) => {
                stop_child(&mut child).await;
                return Err(ManagerConnectorError::LaunchRecordTimeout);
            }
        }
        if bytes.len() > MAX_LAUNCH_RECORD_BYTES {
            stop_child(&mut child).await;
            return Err(ManagerConnectorError::LaunchRecordTooLarge);
        }
        let record: ManagerLaunchRecord = match serde_json::from_slice(&bytes) {
            Ok(record) => record,
            Err(source) => {
                stop_child(&mut child).await;
                return Err(ManagerConnectorError::LaunchRecordInvalid { source });
            }
        };
        let (announcement, disposition) = match record {
            ManagerLaunchRecord::Ready {
                announcement,
                disposition,
            } => (announcement, disposition),
            ManagerLaunchRecord::Failed { failure } => {
                reap_child(child);
                return Err(ManagerConnectorError::LaunchRefused { failure });
            }
        };

        let client = match ManagementClient::connect_announcement(&announcement).await {
            Ok(client) => client,
            Err(source) => {
                if disposition == ManagerLaunchDisposition::ManagerContinues {
                    match tokio::time::timeout(ATTACH_EXIT_GRACE, child.wait()).await {
                        Ok(Ok(_)) => return Err(ManagerConnectorError::ManagerDisappeared),
                        Ok(Err(source)) => return Err(ManagerConnectorError::Launch { source }),
                        Err(_) => {}
                    }
                }
                reap_child(child);
                return Err(ManagerConnectorError::Attach { source });
            }
        };
        reap_child(child);
        Ok(ManagerConnection {
            client,
            announcement,
            disposition,
        })
    }
}

fn reap_child(mut child: tokio::process::Child) {
    tokio::spawn(async move {
        let _ = child.wait().await;
    });
}

async fn stop_child(child: &mut tokio::process::Child) {
    let _ = child.kill().await;
    let _ = child.wait().await;
}

impl ManagerConnection {
    /// Returns the manager announcement used for identity verification.
    #[must_use]
    pub fn announcement(&self) -> &ManagerAnnouncement {
        &self.announcement
    }

    /// Returns whether this launch won authority or joined a prior manager.
    #[must_use]
    pub fn disposition(&self) -> &ManagerLaunchDisposition {
        &self.disposition
    }

    /// Borrows the typed management client.
    pub fn client_mut(&mut self) -> &mut ManagementClient {
        &mut self.client
    }

    /// Invokes one typed call through the admitted manager session.
    ///
    /// # Errors
    ///
    /// Returns an error when the request cannot be framed or the manager refuses it.
    pub async fn call<C>(
        &mut self,
        request: &C::Request,
    ) -> Result<C::Response, ManagementClientError>
    where
        C: ManagementCall,
        C::Request: serde::Serialize,
        C::Response: serde::de::DeserializeOwned,
    {
        self.client.call::<C>(request).await
    }

    /// Consumes the launch evidence and returns the typed client.
    #[must_use]
    pub fn into_client(self) -> ManagementClient {
        self.client
    }
}
