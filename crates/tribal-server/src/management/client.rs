//! Typed local client used by headless management CLI projections.

use std::io;

use tokio::{
    io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _, BufReader},
    net::{
        UnixStream,
        unix::{OwnedReadHalf, OwnedWriteHalf},
    },
};
use tribal_wire::management::{
    MANAGEMENT_CONTRACT_VERSION, ManagementBootstrapRequest, ManagementBootstrapResponse,
    ManagementClientHello, ManagementEvent, ManagementResponseError,
};

use super::authority::AuthorityDescriptor;

const MAX_FRAME_BYTES: usize = 64 * 1024;

/// Compatible full-protocol connection to one discovered manager.
pub(crate) struct ManagementClient {
    reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
    next_id: u64,
}

/// Failure discovering or speaking to a manager.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ManagementClientError {
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
    #[error("manager instance differs from its authority descriptor")]
    InstanceMismatch,
    #[error("management connection closed before a response")]
    Closed,
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
                if hello.manager_instance_id == descriptor.instance_id =>
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

    /// Calls one full-protocol method and decodes its typed result.
    pub(crate) async fn call<T: serde::de::DeserializeOwned>(
        &mut self,
        method: &str,
        params: Option<serde_json::Value>,
    ) -> Result<T, ManagementClientError> {
        let id = self.next_id;
        self.next_id = self.next_id.saturating_add(1);
        self.write(&ClientRequest { id, method, params }).await?;
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
                ClientIncoming::Event(_event) => continue,
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
struct ClientRequest<'a> {
    id: u64,
    method: &'a str,
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
