//! Routing a control request to the surface that answers it, and back.
//!
//! Dispatch maps a JSON-RPC method name onto the config-native
//! [`tribal_config`] operations, the live status introspection, and the
//! token metadata in [`tribal_db`], then maps their answers onto the
//! [`tribal_wire::control`] DTOs the client speaks — the wire crate stays pure,
//! and this binding is the one place those vocabularies meet. An `Ok` carries
//! the result payload; an `Err` carries the JSON-RPC error the caller frames.

use std::path::Path;

use serde::de::DeserializeOwned;
use serde_json::Value;
use sqlx::PgPool;
use tribal_auth::AuthenticatedPrincipal;
use tribal_config::{TransportKind, TribalConfig};
use tribal_db::{AuthTokenRepository, PgAuthTokenRepository};
use tribal_domain::AuthToken;
use tribal_wire::control::{self as wire, CONTROL_CONTRACT_VERSION};

use super::ControlContext;

/// JSON-RPC reserved code: the method name is not one this server dispatches.
const METHOD_NOT_FOUND: i32 = -32601;
/// JSON-RPC reserved code: the params were absent, ill-typed, or rejected.
const INVALID_PARAMS: i32 = -32602;
/// JSON-RPC reserved code: the server failed to complete a valid request.
const INTERNAL_ERROR: i32 = -32603;

/// Dispatches one control method, returning its result payload or the JSON-RPC
/// error to frame back.
pub(crate) async fn dispatch(
    context: &ControlContext,
    principal: Option<&AuthenticatedPrincipal>,
    method: &str,
    params: Option<Value>,
) -> Result<Value, wire::ResponseError> {
    if let Some(outcome) = dispatch_config(&context.config, &context.config_path, method, params) {
        return outcome;
    }
    match method {
        "server.status" => Ok(result(status(context))),
        "token.list" => token_list(&context.pool, principal).await,
        other => Err(error(
            METHOD_NOT_FOUND,
            format!("no such control method: {other}"),
            None,
        )),
    }
}

// ---------------------------------------------------------------------------
// config.* — pure over the config surface, so it tests without an AppState
// ---------------------------------------------------------------------------

/// Dispatches a `config.*` method, or `None` when the method is not one.
fn dispatch_config(
    config: &TribalConfig,
    config_file: &Path,
    method: &str,
    params: Option<Value>,
) -> Option<Result<Value, wire::ResponseError>> {
    let outcome = match method {
        "config.schema" => Ok(result(config_schema())),
        "config.get" => parse_params(params).and_then(|request| config_get(config, request)),
        "config.getAll" => Ok(result(config_get_all(config))),
        "config.set" => {
            parse_params(params).and_then(|request| config_set(config, config_file, request))
        }
        "config.validate" => {
            parse_params(params).map(|request| result(config_validate(config, request)))
        }
        "config.path" => Ok(result(config_path(config_file))),
        _ => return None,
    };
    Some(outcome)
}

fn config_schema() -> wire::ConfigSchema {
    let assembled = tribal_config::config_schema();
    let fields = assembled
        .fields
        .into_iter()
        .map(|field| wire::ConfigFieldMeta {
            shadowed: tribal_config::shadowed_by(&field.path).is_some(),
            reload_class: reload_class(field.reload_class),
            secret: field.secret,
            path: field.path,
        })
        .collect();
    wire::ConfigSchema {
        schema: assembled.schema,
        fields,
    }
}

fn config_get(
    config: &TribalConfig,
    request: wire::ConfigGetRequest,
) -> Result<Value, wire::ResponseError> {
    match tribal_config::get(config, &request.key) {
        Ok(value) => Ok(result(wire::ConfigValue {
            key: request.key,
            value,
        })),
        Err(unknown) => Err(error(INVALID_PARAMS, unknown.to_string(), None)),
    }
}

fn config_get_all(config: &TribalConfig) -> wire::ConfigDocument {
    wire::ConfigDocument {
        values: tribal_config::get_all(config),
    }
}

fn config_set(
    config: &TribalConfig,
    config_file: &Path,
    request: wire::ConfigSetRequest,
) -> Result<Value, wire::ResponseError> {
    match tribal_config::set(config, config_file, &request.key, request.value) {
        Ok(effect) => Ok(result(write_outcome(effect))),
        Err(tribal_config::SetError::Rejected { violations }) => Err(error(
            INVALID_PARAMS,
            "the proposed configuration write is invalid".to_owned(),
            Some(violations_data(violations)),
        )),
        Err(other) => Err(error(INTERNAL_ERROR, other.to_string(), None)),
    }
}

fn config_validate(
    config: &TribalConfig,
    request: wire::ConfigValidateRequest,
) -> wire::ConfigValidation {
    let violations = tribal_config::validate_write(config, &request.key, request.value);
    wire::ConfigValidation {
        valid: violations.is_empty(),
        violations: violations.into_iter().map(violation).collect(),
    }
}

fn config_path(config_file: &Path) -> wire::ConfigPath {
    wire::ConfigPath {
        path: config_file.to_string_lossy().into_owned(),
    }
}

// ---------------------------------------------------------------------------
// server.status — live introspection over the running state
// ---------------------------------------------------------------------------

/// Composes the live status from the running context.
fn status(context: &ControlContext) -> wire::ServerStatus {
    server_status(
        &context.config,
        // The worker-death guard cancels the token when the worker exits, so an
        // un-cancelled token is a running worker; a cancelled one is stopping.
        !context.cancellation_token.is_cancelled(),
        context.project.clone(),
        context.started_at.elapsed().as_secs(),
        &context.binary_version,
        &context.instance_id,
    )
}

/// Builds the status DTO from its live inputs — pure, so it tests without an
/// `AppState`.
fn server_status(
    config: &TribalConfig,
    worker_alive: bool,
    project: Option<wire::ProjectSummary>,
    uptime_seconds: u64,
    binary_version: &str,
    instance_id: &str,
) -> wire::ServerStatus {
    let transport = config.server.transport;
    wire::ServerStatus {
        transport: transport.to_string(),
        // Only a listening transport binds an address; stdio has none.
        bind_address: (transport != TransportKind::Stdio)
            .then(|| config.server.bind_address.clone())
            .flatten(),
        uptime_seconds,
        worker: if worker_alive {
            wire::WorkerStatus::Running
        } else {
            wire::WorkerStatus::Stopped
        },
        // The worker exposes no cheap non-DB queue-depth source yet; the field
        // is honestly absent until one lands.
        queue_depth: None,
        project,
        binary_version: binary_version.to_owned(),
        protocol_version: CONTROL_CONTRACT_VERSION,
        instance_id: instance_id.to_owned(),
    }
}

// ---------------------------------------------------------------------------
// token.list — issued-token metadata for the local principal
// ---------------------------------------------------------------------------

/// Lists the local principal's issued tokens — their metadata only, never a
/// secret or a prefix, and with no mint or revoke.
async fn token_list(
    pool: &PgPool,
    principal: Option<&AuthenticatedPrincipal>,
) -> Result<Value, wire::ResponseError> {
    let principal = principal.ok_or_else(|| {
        error(
            INTERNAL_ERROR,
            "the local principal is unavailable; run `tribal setup`".to_owned(),
            None,
        )
    })?;
    let mut connection = pool.acquire().await.map_err(|source| {
        error(
            INTERNAL_ERROR,
            format!("control database unavailable: {source}"),
            None,
        )
    })?;
    let tokens = PgAuthTokenRepository
        .find_by_principal_id(&mut connection, principal.principal_id())
        .await
        .map_err(|source| {
            error(
                INTERNAL_ERROR,
                format!("could not list tokens: {source}"),
                None,
            )
        })?;
    let list = wire::TokenList {
        tokens: tokens
            .iter()
            .map(|token| token_info(principal.principal_key(), token))
            .collect(),
    };
    Ok(result(list))
}

/// Maps one stored token to its non-secret metadata.
fn token_info(principal_key: &str, token: &AuthToken) -> wire::TokenInfo {
    wire::TokenInfo {
        principal: principal_key.to_owned(),
        scopes: token
            .scopes()
            .iter()
            .map(|scope| scope.as_str().to_owned())
            .collect(),
        created_at: token.created_at(),
        expires_at: Some(token.expires_at()),
    }
}

// ---------------------------------------------------------------------------
// config-native → wire mapping
// ---------------------------------------------------------------------------

fn write_outcome(effect: tribal_config::WriteEffect) -> wire::ConfigWriteOutcome {
    match effect {
        tribal_config::WriteEffect::Live => wire::ConfigWriteOutcome {
            effect: wire::WriteEffect::Live,
            shadowed_by: None,
        },
        tribal_config::WriteEffect::NeedsRestart => wire::ConfigWriteOutcome {
            effect: wire::WriteEffect::NeedsRestart,
            shadowed_by: None,
        },
        tribal_config::WriteEffect::Shadowed { by } => wire::ConfigWriteOutcome {
            effect: wire::WriteEffect::Shadowed,
            shadowed_by: Some(by),
        },
    }
}

/// Maps the config-native class to the wire's two-value class. `Unclassified`
/// cannot reach here — `test_no_leaf_is_unclassified` forbids it for any real
/// leaf — but the total classifier maps it to the conservative `RequiresRestart`.
fn reload_class(class: tribal_config::ReloadClass) -> wire::ReloadClass {
    match class {
        tribal_config::ReloadClass::Hot => wire::ReloadClass::Hot,
        tribal_config::ReloadClass::RequiresRestart | tribal_config::ReloadClass::Unclassified => {
            wire::ReloadClass::RequiresRestart
        }
    }
}

fn violation(source: tribal_config::ConfigViolation) -> wire::ConfigViolation {
    wire::ConfigViolation {
        key: source.key,
        message: source.message,
    }
}

fn violations_data(violations: Vec<tribal_config::ConfigViolation>) -> Value {
    Value::Array(
        violations
            .into_iter()
            .map(|item| serde_json::json!({ "key": item.key, "message": item.message }))
            .collect(),
    )
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

/// Serialises a typed result payload to the opaque `Value` the response carries.
fn result<T: serde::Serialize>(value: T) -> Value {
    serde_json::to_value(value).expect("a control result serialises to JSON")
}

/// Parses the method's typed parameters, erroring when they are absent or the
/// wrong shape.
fn parse_params<T: DeserializeOwned>(params: Option<Value>) -> Result<T, wire::ResponseError> {
    let value = params.ok_or_else(|| {
        error(
            INVALID_PARAMS,
            "this method requires parameters".to_owned(),
            None,
        )
    })?;
    serde_json::from_value(value).map_err(|source| {
        error(
            INVALID_PARAMS,
            format!("invalid parameters: {source}"),
            None,
        )
    })
}

fn error(code: i32, message: String, data: Option<Value>) -> wire::ResponseError {
    wire::ResponseError {
        code,
        message,
        data,
    }
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, Utc};
    use serde_json::json;
    use tribal_domain::{AuthTokenId, PrincipalId, Scope};

    use super::*;

    fn base_config() -> TribalConfig {
        TribalConfig::minimum_valid("postgres://user:pass@localhost:5432/tribal")
    }

    fn dispatch_cfg(
        config: &TribalConfig,
        path: &Path,
        method: &str,
        params: Option<Value>,
    ) -> Result<Value, wire::ResponseError> {
        dispatch_config(config, path, method, params).expect("a config.* method")
    }

    #[test]
    fn test_config_get_returns_the_effective_value() {
        let value = dispatch_cfg(
            &base_config(),
            Path::new("/tmp/tribal.yaml"),
            "config.get",
            Some(json!({ "key": "server.transport" })),
        )
        .expect("config.get succeeds");
        let parsed: wire::ConfigValue = serde_json::from_value(value).unwrap();
        assert_eq!(parsed.key, "server.transport");
        assert_eq!(parsed.value, json!("stdio"));
    }

    #[test]
    fn test_config_get_redacts_a_secret() {
        let value = dispatch_cfg(
            &base_config(),
            Path::new("/tmp/tribal.yaml"),
            "config.get",
            Some(json!({ "key": "database.url" })),
        )
        .expect("config.get succeeds");
        let parsed: wire::ConfigValue = serde_json::from_value(value).unwrap();
        assert_eq!(
            parsed.value,
            json!("********"),
            "a secret never crosses in the clear"
        );
    }

    #[test]
    fn test_config_get_unknown_key_is_invalid_params() {
        let error = dispatch_cfg(
            &base_config(),
            Path::new("/tmp/tribal.yaml"),
            "config.get",
            Some(json!({ "key": "server.nope" })),
        )
        .expect_err("an unknown key errors");
        assert_eq!(error.code, INVALID_PARAMS);
    }

    #[test]
    fn test_config_path_reports_the_file() {
        let value = dispatch_cfg(
            &base_config(),
            Path::new("/home/op/.config/tribal/tribal.yaml"),
            "config.path",
            None,
        )
        .expect("config.path");
        let parsed: wire::ConfigPath = serde_json::from_value(value).unwrap();
        assert_eq!(parsed.path, "/home/op/.config/tribal/tribal.yaml");
    }

    #[test]
    fn test_config_validate_rejects_an_invalid_value() {
        let value = dispatch_cfg(
            &base_config(),
            Path::new("/tmp/tribal.yaml"),
            "config.validate",
            Some(json!({ "key": "server.transport", "value": "grpc" })),
        )
        .expect("config.validate answers");
        let parsed: wire::ConfigValidation = serde_json::from_value(value).unwrap();
        assert!(!parsed.valid, "an unknown transport is invalid");
        assert!(!parsed.violations.is_empty());
    }

    #[test]
    fn test_config_set_persists_and_reports_needs_restart() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("tribal.yaml");
        let value = dispatch_cfg(
            &base_config(),
            &path,
            "config.set",
            Some(json!({ "key": "logging.level", "value": "debug" })),
        )
        .expect("config.set succeeds");
        let parsed: wire::ConfigWriteOutcome = serde_json::from_value(value).unwrap();
        assert_eq!(parsed.effect, wire::WriteEffect::NeedsRestart);
        assert!(path.exists(), "the write persists to the file");
    }

    #[test]
    fn test_config_set_refuses_an_invalid_write_whole() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("tribal.yaml");
        let error = dispatch_cfg(
            &base_config(),
            &path,
            "config.set",
            Some(json!({ "key": "server.transport", "value": "grpc" })),
        )
        .expect_err("an invalid write errors");
        assert_eq!(error.code, INVALID_PARAMS);
        assert!(error.data.is_some(), "the violations ride the error data");
        assert!(!path.exists(), "a refused write never touches the file");
    }

    #[test]
    fn test_config_schema_covers_every_leaf_with_metadata() {
        let value = dispatch_cfg(
            &base_config(),
            Path::new("/tmp/tribal.yaml"),
            "config.schema",
            None,
        )
        .expect("config.schema");
        let parsed: wire::ConfigSchema = serde_json::from_value(value).unwrap();
        assert!(
            parsed.schema.is_object(),
            "the structural schema is an object"
        );
        let database_url = parsed
            .fields
            .iter()
            .find(|field| field.path == "database.url")
            .expect("database.url is a classified leaf");
        assert!(database_url.secret, "database.url is a secret leaf");
        assert_eq!(
            database_url.reload_class,
            wire::ReloadClass::RequiresRestart
        );
    }

    #[test]
    fn test_a_non_config_method_is_not_dispatched_here() {
        assert!(
            dispatch_config(&base_config(), Path::new("/tmp/x"), "server.status", None).is_none(),
            "server.status is not a config method",
        );
    }

    #[test]
    fn test_server_status_reports_a_running_worker_and_transport() {
        let mut config = base_config();
        config.server.transport = TransportKind::Http;
        config.server.bind_address = Some("127.0.0.1:8725".to_owned());
        let status = server_status(&config, true, None, 42, "1.2.3", "host~1~boot");
        assert_eq!(status.transport, "http");
        assert_eq!(status.bind_address.as_deref(), Some("127.0.0.1:8725"));
        assert_eq!(status.worker, wire::WorkerStatus::Running);
        assert_eq!(status.uptime_seconds, 42);
        assert_eq!(status.binary_version, "1.2.3");
        assert_eq!(status.protocol_version, CONTROL_CONTRACT_VERSION);
    }

    #[test]
    fn test_server_status_stdio_has_no_bind_address_and_a_stopped_worker() {
        let status = server_status(&base_config(), false, None, 0, "v", "id");
        assert_eq!(status.transport, "stdio");
        assert!(status.bind_address.is_none(), "stdio binds no address");
        assert_eq!(status.worker, wire::WorkerStatus::Stopped);
    }

    #[test]
    fn test_token_info_carries_metadata_and_no_secret() {
        let scope = Scope::parse("tribal:read").expect("a valid scope");
        let token = AuthToken::builder()
            .id(AuthTokenId::new())
            .token_hash("a".repeat(64))
            .principal_id(PrincipalId::new())
            .scopes(vec![scope.clone()])
            .audience(String::new())
            .expires_at(Utc::now() + Duration::hours(1))
            .created_at(Utc::now())
            .build();
        let info = token_info("principal:local", &token);
        assert_eq!(info.principal, "principal:local");
        assert_eq!(info.scopes, vec![scope.as_str().to_owned()]);
        assert!(info.expires_at.is_some(), "every token expires");
        let json = serde_json::to_value(&info).unwrap();
        for forbidden in ["value", "token", "hash", "prefix"] {
            assert!(
                json.get(forbidden).is_none(),
                "token metadata must not carry a {forbidden} field",
            );
        }
    }
}
