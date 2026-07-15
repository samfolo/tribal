//! Typed local client used by headless management CLI projections.

use std::io;

use tokio::{
    io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _, BufReader},
    net::{
        UnixStream,
        unix::{OwnedReadHalf, OwnedWriteHalf},
    },
    time::{Duration, timeout},
};
use tribal_wire::management::{
    BootstrapShutdownRefusal, MANAGEMENT_CONTRACT_VERSION, MANAGEMENT_REQUEST_TIMEOUT_SECONDS,
    ManagementBootstrapRequest, ManagementBootstrapResponse, ManagementCall, ManagementClientHello,
    ManagementEvent, ManagementMethod, ManagementResponseError, ManagerAnnouncement,
};

use super::authority::AuthorityDescriptor;

const MAX_FRAME_BYTES: usize = 64 * 1024;
const BOOTSTRAP_TIMEOUT: Duration = Duration::from_secs(5);
pub(super) const CALL_TIMEOUT: Duration = Duration::from_secs(MANAGEMENT_REQUEST_TIMEOUT_SECONDS);

/// Compatible full-protocol connection to one discovered manager.
pub struct ManagementClient {
    reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
    next_id: u64,
}

/// Failure discovering or speaking to a manager.
#[derive(Debug, thiserror::Error)]
pub enum ManagementClientError {
    #[error("authority descriptor does not contain a manager socket")]
    MissingSocket,
    #[error("connecting to management socket: {source}")]
    Connect {
        #[source]
        source: io::Error,
    },
    #[error("management protocol frame failed: {source}")]
    Frame {
        #[source]
        source: io::Error,
    },
    #[error("management protocol version mismatch")]
    VersionMismatch,
    #[error("manager refused restricted shutdown: {reason:?}")]
    ShutdownRefused { reason: BootstrapShutdownRefusal },
    #[error("manager instance differs from its authority descriptor")]
    InstanceMismatch,
    #[error("management connection closed before a response")]
    Closed,
    #[error("management request timed out")]
    TimedOut,
    #[error("management request failed: {error:?}")]
    Request { error: ManagementResponseError },
}

impl ManagementClient {
    /// Connects and verifies the discovered manager identity.
    pub(crate) async fn connect(
        descriptor: &AuthorityDescriptor,
    ) -> Result<Self, ManagementClientError> {
        let socket = descriptor
            .socket_path
            .as_ref()
            .ok_or(ManagementClientError::MissingSocket)?;
        Self::connect_identity(socket, &descriptor.instance_id).await
    }

    /// Connects and verifies a manager launch announcement.
    ///
    /// # Errors
    ///
    /// Returns an error when the socket, frame, protocol, or announced identity is invalid.
    pub async fn connect_announcement(
        announcement: &ManagerAnnouncement,
    ) -> Result<Self, ManagementClientError> {
        Self::connect_identity(
            std::path::Path::new(&announcement.socket_path),
            &announcement.instance_id,
        )
        .await
    }

    async fn connect_identity(
        socket: &std::path::Path,
        expected_instance_id: &str,
    ) -> Result<Self, ManagementClientError> {
        timeout(
            BOOTSTRAP_TIMEOUT,
            Self::connect_identity_inner(socket, expected_instance_id),
        )
        .await
        .map_err(|_| ManagementClientError::TimedOut)?
    }

    async fn connect_identity_inner(
        socket: &std::path::Path,
        expected_instance_id: &str,
    ) -> Result<Self, ManagementClientError> {
        let stream = UnixStream::connect(socket)
            .await
            .map_err(|source| ManagementClientError::Connect { source })?;
        let (read, write) = stream.into_split();
        let mut client = Self {
            reader: BufReader::new(read),
            writer: write,
            next_id: 1,
        };
        client
            .write(&ManagementBootstrapRequest::Handshake {
                hello: ManagementClientHello {
                    protocol_version: MANAGEMENT_CONTRACT_VERSION,
                },
            })
            .await?;
        let response: ManagementBootstrapResponse =
            client.read().await?.ok_or(ManagementClientError::Closed)?;
        match response {
            ManagementBootstrapResponse::Compatible { hello }
                if hello.manager_instance_id == expected_instance_id =>
            {
                Ok(client)
            }
            ManagementBootstrapResponse::Compatible { .. } => {
                Err(ManagementClientError::InstanceMismatch)
            }
            ManagementBootstrapResponse::VersionMismatch { .. }
            | ManagementBootstrapResponse::ShutdownAccepted
            | ManagementBootstrapResponse::ShutdownRefused { .. } => {
                Err(ManagementClientError::VersionMismatch)
            }
        }
    }

    /// Requests shutdown through the version-stable bootstrap envelope.
    pub(crate) async fn request_shutdown(
        descriptor: &AuthorityDescriptor,
    ) -> Result<(), ManagementClientError> {
        timeout(BOOTSTRAP_TIMEOUT, Self::request_shutdown_inner(descriptor))
            .await
            .map_err(|_| ManagementClientError::TimedOut)?
    }

    async fn request_shutdown_inner(
        descriptor: &AuthorityDescriptor,
    ) -> Result<(), ManagementClientError> {
        let socket = descriptor
            .socket_path
            .as_ref()
            .ok_or(ManagementClientError::MissingSocket)?;
        let stream = UnixStream::connect(socket)
            .await
            .map_err(|source| ManagementClientError::Connect { source })?;
        let (read, write) = stream.into_split();
        let mut client = Self {
            reader: BufReader::new(read),
            writer: write,
            next_id: 1,
        };
        client.write(&ManagementBootstrapRequest::Shutdown).await?;
        match client
            .read::<ManagementBootstrapResponse>()
            .await?
            .ok_or(ManagementClientError::Closed)?
        {
            ManagementBootstrapResponse::ShutdownAccepted => Ok(()),
            ManagementBootstrapResponse::ShutdownRefused { reason } => {
                Err(ManagementClientError::ShutdownRefused { reason })
            }
            ManagementBootstrapResponse::Compatible { .. }
            | ManagementBootstrapResponse::VersionMismatch { .. } => {
                Err(ManagementClientError::VersionMismatch)
            }
        }
    }

    /// Calls one full-protocol method and decodes its typed result.
    ///
    /// # Errors
    ///
    /// Returns an error when transport, framing, decoding, or the management call fails.
    pub async fn call<C>(
        &mut self,
        request: &C::Request,
    ) -> Result<C::Response, ManagementClientError>
    where
        C: ManagementCall,
        C::Request: serde::Serialize,
        C::Response: serde::de::DeserializeOwned,
    {
        if let Ok(result) = timeout(CALL_TIMEOUT, self.call_inner::<C>(request)).await {
            result
        } else {
            let _ = self.writer.shutdown().await;
            Err(ManagementClientError::TimedOut)
        }
    }

    async fn call_inner<C>(
        &mut self,
        request: &C::Request,
    ) -> Result<C::Response, ManagementClientError>
    where
        C: ManagementCall,
        C::Request: serde::Serialize,
        C::Response: serde::de::DeserializeOwned,
    {
        let id = self.next_id;
        self.next_id = self.next_id.saturating_add(1);
        let params =
            serde_json::to_value(request).map_err(|source| ManagementClientError::Frame {
                source: io::Error::new(io::ErrorKind::InvalidData, source),
            })?;
        self.write(&ClientRequest {
            id,
            method: C::METHOD,
            params: (!params.is_null()).then_some(params),
        })
        .await?;
        loop {
            let incoming: ClientIncoming =
                self.read().await?.ok_or(ManagementClientError::Closed)?;
            match incoming {
                ClientIncoming::Response(ClientResponse::Success {
                    id: response_id,
                    result,
                }) if response_id == id => {
                    return serde_json::from_value(result).map_err(|source| {
                        ManagementClientError::Frame {
                            source: io::Error::new(io::ErrorKind::InvalidData, source),
                        }
                    });
                }
                ClientIncoming::Response(ClientResponse::Failure {
                    id: response_id,
                    error,
                }) if response_id == id => {
                    return Err(ManagementClientError::Request { error });
                }
                ClientIncoming::Event(_event) => {}
                ClientIncoming::Response(_) => {
                    return Err(ManagementClientError::Frame {
                        source: io::Error::new(io::ErrorKind::InvalidData, "response id mismatch"),
                    });
                }
            }
        }
    }

    async fn write(&mut self, value: &impl serde::Serialize) -> Result<(), ManagementClientError> {
        let mut bytes =
            serde_json::to_vec(value).map_err(|source| ManagementClientError::Frame {
                source: io::Error::new(io::ErrorKind::InvalidData, source),
            })?;
        bytes.push(b'\n');
        if bytes.len() > MAX_FRAME_BYTES {
            return Err(ManagementClientError::Frame {
                source: io::Error::new(io::ErrorKind::InvalidData, "frame exceeds size limit"),
            });
        }
        self.writer
            .write_all(&bytes)
            .await
            .map_err(|source| ManagementClientError::Frame { source })
    }

    async fn read<T: serde::de::DeserializeOwned>(
        &mut self,
    ) -> Result<Option<T>, ManagementClientError> {
        let mut bytes = Vec::new();
        let read = (&mut self.reader)
            .take((MAX_FRAME_BYTES + 1) as u64)
            .read_until(b'\n', &mut bytes)
            .await
            .map_err(|source| ManagementClientError::Frame { source })?;
        if read == 0 {
            return Ok(None);
        }
        if read > MAX_FRAME_BYTES || bytes.last() != Some(&b'\n') {
            return Err(ManagementClientError::Frame {
                source: io::Error::new(io::ErrorKind::InvalidData, "invalid management frame"),
            });
        }
        bytes.pop();
        serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|source| ManagementClientError::Frame {
                source: io::Error::new(io::ErrorKind::InvalidData, source),
            })
    }
}

#[derive(serde::Serialize)]
struct ClientRequest {
    id: u64,
    method: ManagementMethod,
    #[serde(skip_serializing_if = "Option::is_none")]
    params: Option<serde_json::Value>,
}

#[derive(serde::Deserialize)]
#[serde(untagged)]
enum ClientResponse {
    Success {
        id: u64,
        result: serde_json::Value,
    },
    Failure {
        id: u64,
        error: ManagementResponseError,
    },
}

#[derive(serde::Deserialize)]
#[serde(untagged)]
enum ClientIncoming {
    Response(ClientResponse),
    Event(ManagementEvent),
}

#[cfg(test)]
mod tests {
    use tokio::{
        io::{AsyncReadExt as _, AsyncWriteExt as _},
        net::UnixListener,
    };
    use tribal_wire::management::ConfigSchemaCall;

    use super::*;

    #[tokio::test(start_paused = true)]
    async fn test_handshake_without_response_times_out() {
        let directory = tempfile::tempdir().expect("temporary socket directory");
        let socket = directory.path().join("manager.sock");
        let listener = UnixListener::bind(&socket).expect("bind management socket");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept client");
            let mut request = [0; 256];
            let _ = stream.read(&mut request).await.expect("read handshake");
            std::future::pending::<()>().await;
        });

        let result = ManagementClient::connect_identity(&socket, "manager").await;

        assert!(matches!(result, Err(ManagementClientError::TimedOut)));
        server.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn test_call_without_response_times_out() {
        let (client_stream, mut server_stream) = UnixStream::pair().expect("socket pair");
        let (read, write) = client_stream.into_split();
        let mut client = ManagementClient {
            reader: BufReader::new(read),
            writer: write,
            next_id: 1,
        };
        let server = tokio::spawn(async move {
            let mut request = [0; 256];
            let _ = server_stream
                .read(&mut request)
                .await
                .expect("read request");
            std::future::pending::<()>().await;
        });

        let result = client.call::<ConfigSchemaCall>(&()).await;

        assert!(matches!(result, Err(ManagementClientError::TimedOut)));
        server.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn test_call_waits_past_the_bootstrap_deadline_for_a_manager_result() {
        struct DelayedCall;

        impl ManagementCall for DelayedCall {
            type Request = ();
            type Response = ();

            const METHOD: ManagementMethod = ManagementMethod::ConfigSchema;
        }

        let (client_stream, mut server_stream) = UnixStream::pair().expect("socket pair");
        let (read, write) = client_stream.into_split();
        let mut client = ManagementClient {
            reader: BufReader::new(read),
            writer: write,
            next_id: 1,
        };
        let server = tokio::spawn(async move {
            let mut request = [0; 256];
            let _ = server_stream
                .read(&mut request)
                .await
                .expect("read request");
            tokio::time::sleep(Duration::from_secs(6)).await;
            server_stream
                .write_all(b"{\"id\":1,\"result\":null}\n")
                .await
                .expect("write response");
        });

        let call = client.call::<DelayedCall>(&());
        tokio::pin!(call);
        tokio::select! {
            result = &mut call => panic!("call completed before the manager answered: {result:?}"),
            () = tokio::task::yield_now() => {}
        }
        tokio::time::advance(Duration::from_secs(6)).await;
        let result = call.await;

        assert!(matches!(result, Ok(())), "{result:?}");
        server.await.expect("server joins");
    }
}
