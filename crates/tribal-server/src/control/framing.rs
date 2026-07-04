//! `Content-Length` framing over the control socket.
//!
//! Each JSON-RPC frame is a `Content-Length: <n>\r\n\r\n` header followed by
//! exactly `n` bytes of JSON — the same envelope the Language Server Protocol
//! uses, chosen so a reader never has to scan the payload for a delimiter. The
//! codec is payload-agnostic: it moves bytes, and the caller deserialises them
//! into the wire DTO the frame carries.

use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// The largest frame the codec will read. The control crossings — a config
/// document, a bounded log tail — are small; a larger declared length is a
/// malformed or hostile peer, refused rather than allocated.
const MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;

/// A failure reading or writing a control frame.
#[derive(Debug, Error)]
pub(crate) enum FramingError {
    /// The underlying socket read or write failed.
    #[error("control frame I/O failed: {source}")]
    Io {
        /// The underlying I/O failure.
        #[source]
        source: std::io::Error,
    },
    /// The frame header carried no `Content-Length`.
    #[error("control frame is missing its Content-Length header")]
    MissingContentLength,
    /// The `Content-Length` value was not a non-negative integer.
    #[error("control frame Content-Length {value:?} is not a length")]
    InvalidContentLength {
        /// The unparseable header value.
        value: String,
    },
    /// The declared length exceeds [`MAX_FRAME_BYTES`].
    #[error("control frame length {length} exceeds the {max}-byte limit")]
    TooLarge {
        /// The declared frame length.
        length: usize,
        /// The limit it broke.
        max: usize,
    },
    /// The connection ended partway through a frame the header promised.
    #[error("control frame ended before its declared {expected} bytes arrived")]
    Truncated {
        /// The byte count the header declared.
        expected: usize,
    },
    /// The payload was not the JSON the caller expected.
    #[error("control frame payload is not valid JSON: {source}")]
    Json {
        /// The underlying deserialisation failure.
        #[source]
        source: serde_json::Error,
    },
}

impl From<std::io::Error> for FramingError {
    fn from(source: std::io::Error) -> Self {
        Self::Io { source }
    }
}

/// Serialises `value` to JSON and writes it as one `Content-Length` frame.
pub(crate) async fn write_frame<W, T>(writer: &mut W, value: &T) -> Result<(), FramingError>
where
    W: AsyncWrite + Unpin,
    T: Serialize,
{
    let frame = encode_frame(value)?;
    writer.write_all(&frame).await?;
    writer.flush().await?;
    Ok(())
}

/// Encodes `value` as one `Content-Length` frame's bytes, header and body — for
/// a caller that queues frames from several producers onto one writer.
pub(crate) fn encode_frame<T: Serialize>(value: &T) -> Result<Vec<u8>, FramingError> {
    let body = serde_json::to_vec(value).map_err(|source| FramingError::Json { source })?;
    let mut frame = format!("Content-Length: {}\r\n\r\n", body.len()).into_bytes();
    frame.extend_from_slice(&body);
    Ok(frame)
}

/// Reads one frame's raw JSON bytes, or `None` on a clean end of stream before a
/// frame begins.
async fn read_frame<R>(reader: &mut R) -> Result<Option<Vec<u8>>, FramingError>
where
    R: AsyncBufRead + Unpin,
{
    let mut content_length: Option<usize> = None;
    let mut saw_header_byte = false;
    let mut line = String::new();
    loop {
        line.clear();
        let read = reader.read_line(&mut line).await?;
        if read == 0 {
            // End of stream. Between frames it is a clean close; partway through
            // a header it is a truncated frame.
            return if saw_header_byte {
                Err(FramingError::MissingContentLength)
            } else {
                Ok(None)
            };
        }
        saw_header_byte = true;
        let header = line.trim_end_matches(['\r', '\n']);
        if header.is_empty() {
            break;
        }
        if let Some((name, value)) = header.split_once(':')
            && name.trim().eq_ignore_ascii_case("content-length")
        {
            let value = value.trim();
            content_length =
                Some(
                    value
                        .parse()
                        .map_err(|_| FramingError::InvalidContentLength {
                            value: value.to_owned(),
                        })?,
                );
        }
    }

    let length = content_length.ok_or(FramingError::MissingContentLength)?;
    if length > MAX_FRAME_BYTES {
        return Err(FramingError::TooLarge {
            length,
            max: MAX_FRAME_BYTES,
        });
    }
    let mut body = vec![0u8; length];
    reader.read_exact(&mut body).await.map_err(|source| {
        if source.kind() == std::io::ErrorKind::UnexpectedEof {
            FramingError::Truncated { expected: length }
        } else {
            FramingError::Io { source }
        }
    })?;
    Ok(Some(body))
}

/// Reads one frame and deserialises it into `T`, or `None` on a clean end of
/// stream before a frame begins.
pub(crate) async fn read_typed_frame<R, T>(reader: &mut R) -> Result<Option<T>, FramingError>
where
    R: AsyncBufRead + Unpin,
    T: DeserializeOwned,
{
    match read_frame(reader).await? {
        Some(body) => {
            let value =
                serde_json::from_slice(&body).map_err(|source| FramingError::Json { source })?;
            Ok(Some(value))
        }
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use tribal_wire::control::{
        ControlEvent, ControlNotification, ControlRequest, ControlResponse, JsonRpcVersion,
        RequestId, ResponseResult, WriteEffect,
    };

    use super::*;

    /// Round-trips a value through `write_frame` and `read_typed_frame` over an
    /// in-memory buffer.
    async fn round_trip<T>(value: &T) -> T
    where
        T: Serialize + DeserializeOwned,
    {
        let mut buffer = Vec::new();
        write_frame(&mut buffer, value).await.expect("frame writes");
        let mut reader = Cursor::new(buffer);
        read_typed_frame(&mut reader)
            .await
            .expect("frame reads")
            .expect("a frame is present")
    }

    #[tokio::test]
    async fn test_a_request_frame_round_trips() {
        let request = ControlRequest {
            jsonrpc: JsonRpcVersion,
            id: RequestId(7),
            method: "config.get".to_owned(),
            params: Some(serde_json::json!({ "key": "logging.level" })),
        };
        assert_eq!(round_trip(&request).await, request);
    }

    #[tokio::test]
    async fn test_a_response_frame_round_trips() {
        let response = ControlResponse {
            jsonrpc: JsonRpcVersion,
            id: RequestId(7),
            outcome: ResponseResult::Success {
                result: serde_json::json!({ "path": "/tmp/tribal.yaml" }),
            },
        };
        assert_eq!(round_trip(&response).await, response);
    }

    #[tokio::test]
    async fn test_a_server_initiated_event_frame_round_trips() {
        // A server-initiated event projects to a JSON-RPC notification frame:
        // its `method`/`params` are the event's adjacently-tagged serialisation.
        let event = ControlEvent::ConfigChanged {
            keys: vec!["logging.level".to_owned()],
            effect: WriteEffect::NeedsRestart,
        };
        let projected = serde_json::to_value(&event).unwrap();
        let notification = ControlNotification {
            jsonrpc: JsonRpcVersion,
            method: projected["method"].as_str().unwrap().to_owned(),
            params: projected.get("params").cloned(),
        };
        let decoded = round_trip(&notification).await;
        assert_eq!(decoded, notification);
        assert_eq!(decoded.method, "config.changed");
    }

    #[tokio::test]
    async fn test_a_clean_end_of_stream_reads_as_no_frame() {
        let mut reader = Cursor::new(Vec::new());
        let frame: Option<ControlRequest> = read_typed_frame(&mut reader)
            .await
            .expect("clean EOF is not an error");
        assert!(frame.is_none(), "an empty stream yields no frame");
    }

    #[tokio::test]
    async fn test_a_frame_without_content_length_is_refused() {
        let mut reader = Cursor::new(b"Content-Type: application/json\r\n\r\n".to_vec());
        let outcome: Result<Option<ControlRequest>, _> = read_typed_frame(&mut reader).await;
        assert!(
            matches!(outcome, Err(FramingError::MissingContentLength)),
            "a frame with no Content-Length must be refused",
        );
    }

    #[tokio::test]
    async fn test_an_oversized_frame_is_refused_before_allocation() {
        let header = format!("Content-Length: {}\r\n\r\n", MAX_FRAME_BYTES + 1);
        let mut reader = Cursor::new(header.into_bytes());
        let outcome: Result<Option<ControlRequest>, _> = read_typed_frame(&mut reader).await;
        assert!(
            matches!(outcome, Err(FramingError::TooLarge { .. })),
            "a frame over the limit must be refused, never allocated",
        );
    }
}
