//! The JSON-RPC 2.0 framing the control bridge speaks, and the connect-time
//! handshake that gates it.
//!
//! One frame carries request, response, or a server-initiated notification; a
//! response holds exactly one of a result or an error, never both. The frozen
//! `"2.0"` marker rides every frame as [`JsonRpcVersion`]; the control
//! contract's own version is exchanged once in the [`ClientHello`] /
//! [`ServerHello`] handshake, before any method is dispatched.

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Protocol markers
// ---------------------------------------------------------------------------

/// The JSON-RPC protocol marker, always the frozen string `"2.0"`. A frame
/// carrying any other value is refused at deserialization.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct JsonRpcVersion;

/// The wire literal every JSON-RPC 2.0 frame carries.
const JSON_RPC_VERSION: &str = "2.0";

impl Serialize for JsonRpcVersion {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(JSON_RPC_VERSION)
    }
}

impl<'de> Deserialize<'de> for JsonRpcVersion {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(deserializer)?;
        if raw == JSON_RPC_VERSION {
            Ok(Self)
        } else {
            Err(serde::de::Error::custom(format!(
                "unsupported JSON-RPC version {raw:?}; only {JSON_RPC_VERSION:?} is spoken"
            )))
        }
    }
}

#[cfg(feature = "schema")]
impl schemars::JsonSchema for JsonRpcVersion {
    fn schema_name() -> String {
        "JsonRpcVersion".to_owned()
    }

    fn json_schema(_generator: &mut schemars::r#gen::SchemaGenerator) -> schemars::schema::Schema {
        let mut schema = schemars::schema::SchemaObject {
            instance_type: Some(schemars::schema::InstanceType::String.into()),
            ..Default::default()
        };
        schema.const_value = Some(serde_json::Value::String(JSON_RPC_VERSION.to_owned()));
        schema.into()
    }
}

/// A request's correlation id, echoed on its response so a client can pair the
/// two over one multiplexed connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(
    feature = "schema",
    derive(schemars::JsonSchema),
    schemars(transparent)
)]
#[serde(transparent)]
pub struct RequestId(pub u64);

// ---------------------------------------------------------------------------
// Frames
// ---------------------------------------------------------------------------

/// A method call from client to server. `params` is absent for a method that
/// takes none; the typed payload lives in this module's siblings.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ControlRequest {
    /// The frozen JSON-RPC 2.0 marker.
    pub jsonrpc: JsonRpcVersion,
    /// The correlation id echoed on the matching response.
    pub id: RequestId,
    /// The dotted method name, e.g. `config.get`.
    pub method: String,
    /// The method's typed parameters, absent when it takes none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<serde_json::Value>,
}

/// A method's failure, in the JSON-RPC error shape.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ResponseError {
    /// The numeric error code.
    pub code: i32,
    /// A human-readable one-line summary.
    pub message: String,
    /// Structured detail a client can branch on, absent when there is none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

/// The body of a response: exactly one of a result or an error, distinguished
/// by which key is present, never both.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(untagged)]
pub enum ResponseResult {
    /// The method succeeded, carrying its typed result.
    Success {
        /// The method's typed result payload.
        result: serde_json::Value,
    },
    /// The method failed.
    Failure {
        /// What went wrong.
        error: ResponseError,
    },
}

/// A response to a [`ControlRequest`], pairing on the echoed [`RequestId`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ControlResponse {
    /// The frozen JSON-RPC 2.0 marker.
    pub jsonrpc: JsonRpcVersion,
    /// The id of the request this answers.
    pub id: RequestId,
    /// The result or the error.
    #[serde(flatten)]
    pub outcome: ResponseResult,
}

/// A server-initiated notification — an event with no id and no reply. Its
/// `method` and `params` are the wire projection of a
/// [`ControlEvent`](super::event::ControlEvent).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ControlNotification {
    /// The frozen JSON-RPC 2.0 marker.
    pub jsonrpc: JsonRpcVersion,
    /// The dotted event name, e.g. `config.changed`.
    pub method: String,
    /// The event's typed payload, absent for an event that carries none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<serde_json::Value>,
}

// ---------------------------------------------------------------------------
// Connect handshake
// ---------------------------------------------------------------------------

/// The first frame a client sends, naming the control-contract version it
/// speaks. The server refuses a version it does not support before dispatching
/// any method.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ClientHello {
    /// The [`CONTROL_CONTRACT_VERSION`](super::CONTROL_CONTRACT_VERSION) the
    /// client was built against.
    pub protocol_version: u16,
}

/// The server's answer to an accepted [`ClientHello`], confirming the contract
/// version in force and identifying the binary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ServerHello {
    /// The control-contract version the server speaks.
    pub protocol_version: u16,
    /// The binary's build version.
    pub binary_version: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_a_request_round_trips_with_params() {
        let request = ControlRequest {
            jsonrpc: JsonRpcVersion,
            id: RequestId(7),
            method: "config.get".to_owned(),
            params: Some(serde_json::json!({ "key": "logging.level" })),
        };
        let parsed: ControlRequest =
            serde_json::from_str(&serde_json::to_string(&request).unwrap()).unwrap();
        assert_eq!(parsed, request);
    }

    #[test]
    fn test_the_jsonrpc_marker_serialises_as_the_frozen_literal() {
        let json = serde_json::to_value(JsonRpcVersion).unwrap();
        assert_eq!(json, serde_json::json!("2.0"));
    }

    #[test]
    fn test_an_unknown_jsonrpc_version_is_rejected() {
        assert!(
            serde_json::from_value::<JsonRpcVersion>(serde_json::json!("1.0")).is_err(),
            "only JSON-RPC 2.0 is spoken",
        );
    }

    #[test]
    fn test_a_request_without_params_omits_the_field() {
        let request = ControlRequest {
            jsonrpc: JsonRpcVersion,
            id: RequestId(1),
            method: "server.status".to_owned(),
            params: None,
        };
        let json = serde_json::to_value(&request).unwrap();
        assert!(
            json.get("params").is_none(),
            "an absent params field is omitted, not null",
        );
    }

    #[test]
    fn test_a_success_response_round_trips() {
        let response = ControlResponse {
            jsonrpc: JsonRpcVersion,
            id: RequestId(7),
            outcome: ResponseResult::Success {
                result: serde_json::json!({ "path": "/home/op/.config/tribal/tribal.yaml" }),
            },
        };
        let json = serde_json::to_value(&response).unwrap();
        assert_eq!(json["id"], serde_json::json!(7));
        assert!(json.get("result").is_some(), "a success carries result");
        assert!(json.get("error").is_none(), "a success carries no error");
        let parsed: ControlResponse = serde_json::from_value(json).unwrap();
        assert_eq!(parsed, response);
    }

    #[test]
    fn test_a_failure_response_carries_the_error_not_a_result() {
        let response = ControlResponse {
            jsonrpc: JsonRpcVersion,
            id: RequestId(9),
            outcome: ResponseResult::Failure {
                error: ResponseError {
                    code: -32602,
                    message: "no such config key".to_owned(),
                    data: None,
                },
            },
        };
        let json = serde_json::to_value(&response).unwrap();
        assert!(json.get("error").is_some(), "a failure carries error");
        assert!(json.get("result").is_none(), "a failure carries no result");
        let parsed: ControlResponse = serde_json::from_value(json).unwrap();
        assert_eq!(parsed, response);
    }
}
