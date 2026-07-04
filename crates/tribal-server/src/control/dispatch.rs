//! Routing a control request to the surface that answers it, and back.
//!
//! Dispatch maps a JSON-RPC method name onto the config-native
//! [`tribal_config`] operations, the live status introspection, and the
//! token metadata in [`tribal_db`], then maps their answers onto the
//! [`tribal_wire::control`] DTOs the client speaks — the wire crate stays pure,
//! and this binding is the one place those vocabularies meet. An `Ok` carries
//! the result payload; an `Err` carries the JSON-RPC error the caller frames.

use std::{path::Path, sync::Arc};

use serde::de::DeserializeOwned;
use serde_json::Value;
use sqlx::PgPool;
use tribal_auth::AuthenticatedPrincipal;
use tribal_config::{CliShadow, TribalConfig, is_secret_key};
use tribal_db::{AuthTokenRepository, PgAuthTokenRepository};
use tribal_domain::{AuthToken, REDACTED};
use tribal_wire::control::{self as wire, CONTROL_CONTRACT_VERSION, ControlEvent, error_code};

use super::{ControlContext, listening_bind_address};

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
    match method {
        // Handled here, not in `dispatch_config`, because a successful write
        // publishes a `config.changed` event to the bus.
        "config.set" => config_set_and_publish(context, params).await,
        "server.status" => Ok(result(status(context))),
        "server.stop" => Ok(result(server_stop(context))),
        "server.restart" => Ok(result(server_restart(context))),
        "logs.tail" => logs_tail(context, params),
        "token.list" => token_list(&context.pool, principal).await,
        _ => match dispatch_config(
            &context.config,
            &context.config_path,
            &context.cli_shadow,
            method,
            params,
        ) {
            Some(outcome) => outcome,
            None => Err(error(
                METHOD_NOT_FOUND,
                format!("no such control method: {method}"),
                None,
            )),
        },
    }
}

/// Persists a `config.set` and announces it on the bus. A change every
/// subscriber learns of through `config.changed` — the binary is the only
/// writer, so the client awaits the event rather than assuming the write took.
///
/// The persist holds the write lock so concurrent sets serialise, and runs its
/// blocking file I/O on a blocking thread rather than the async worker.
async fn config_set_and_publish(
    context: &ControlContext,
    params: Option<Value>,
) -> Result<Value, wire::ResponseError> {
    let request: wire::ConfigSetRequest = parse_params(params)?;
    // A redacted read shows the mask, never the secret; writing the mask back
    // would overwrite the real value with `********`. Refuse it at the boundary.
    if is_secret_key(&request.key) && request.value.as_str() == Some(REDACTED) {
        return Err(error(
            error_code::SECRET_MASK_REJECTED,
            format!(
                "`{}` is a secret; its redacted mask cannot be written back as its value",
                request.key,
            ),
            None,
        ));
    }
    let key = request.key.clone();
    let config = Arc::clone(&context.config);
    let config_path = context.config_path.clone();
    let cli = context.cli_shadow.clone();

    let (outcome, document) = {
        let _guard = context.config_write_lock.lock().await;
        tokio::task::spawn_blocking(move || config_set(&config, &config_path, &cli, request))
            .await
            .map_err(|source| {
                error(INTERNAL_ERROR, format!("config.set did not complete: {source}"), None)
            })?
    }?;

    // Record the exact bytes written so the file watcher does not re-announce
    // this write as an external edit contradicting the per-key event below.
    context.self_write.record(document);
    // A send with no subscribers is not an error — no client is listening yet.
    let _ = context.events.send(ControlEvent::ConfigChanged {
        keys: vec![key],
        effect: outcome.effect,
    });
    Ok(result(outcome))
}

// ---------------------------------------------------------------------------
// config.* — pure over the config surface, so it tests without an AppState
// ---------------------------------------------------------------------------

/// Dispatches a `config.*` method, or `None` when the method is not one. It
/// reads only the config surface — never the pool — so it tests without an
/// `AppState`.
fn dispatch_config(
    config: &TribalConfig,
    config_file: &Path,
    cli_shadow: &CliShadow,
    method: &str,
    params: Option<Value>,
) -> Option<Result<Value, wire::ResponseError>> {
    let outcome = match method {
        "config.schema" => Ok(result(config_schema(cli_shadow))),
        "config.get" => parse_params(params).and_then(|request| config_get(config, request)),
        "config.getAll" => Ok(result(config_get_all(config))),
        "config.validate" => {
            parse_params(params).map(|request| result(config_validate(config, request)))
        }
        "config.path" => Ok(result(config_path(config_file))),
        _ => return None,
    };
    Some(outcome)
}

fn config_schema(cli_shadow: &CliShadow) -> wire::ConfigSchema {
    let assembled = tribal_config::config_schema();
    let fields = assembled
        .fields
        .into_iter()
        .map(|field| wire::ConfigFieldMeta {
            shadowed: tribal_config::shadowed_by(&field.path, cli_shadow).is_some(),
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

/// Persists the write and maps its result onto the wire outcome, returning the
/// exact bytes written alongside so the caller records its own write. Runs on a
/// blocking thread — the persist is synchronous file I/O.
fn config_set(
    config: &TribalConfig,
    config_file: &Path,
    cli_shadow: &CliShadow,
    request: wire::ConfigSetRequest,
) -> Result<(wire::ConfigWriteOutcome, Vec<u8>), wire::ResponseError> {
    match tribal_config::set(config, config_file, &request.key, request.value, cli_shadow) {
        Ok(persisted) => Ok((write_outcome(persisted.effect), persisted.document)),
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
    wire::ServerStatus {
        transport: config.server.transport.to_string(),
        bind_address: listening_bind_address(config),
        uptime_seconds,
        worker: if worker_alive {
            wire::WorkerStatus::Running
        } else {
            wire::WorkerStatus::Stopped
        },
        // No in-process queue-depth source exists that avoids a database
        // round-trip, so status reports the field absent rather than paying that
        // cost on every read.
        queue_depth: None,
        project,
        binary_version: binary_version.to_owned(),
        protocol_version: CONTROL_CONTRACT_VERSION,
        instance_id: instance_id.to_owned(),
    }
}

// ---------------------------------------------------------------------------
// server.stop / server.restart — the lifecycle contract
// ---------------------------------------------------------------------------

/// Initiates graceful shutdown by cancelling the serve token, and reports that
/// the binary is stopping.
fn server_stop(context: &ControlContext) -> wire::StopOutcome {
    context.cancellation_token.cancel();
    wire::StopOutcome { stopping: true }
}

/// Answers `server.restart` — never by re-exec'ing the binary itself. When a
/// supervisor owns the process it stops for the supervisor to relaunch; when
/// none does it refuses, leaving the process running for the operator to stop
/// and relaunch explicitly.
fn server_restart(context: &ControlContext) -> wire::RestartOutcome {
    if context.supervised {
        context.cancellation_token.cancel();
        wire::RestartOutcome::SupervisorMediated
    } else {
        wire::RestartOutcome::Unsupervised
    }
}

// ---------------------------------------------------------------------------
// logs.tail — a bounded window of recent lines from the capture ring
// ---------------------------------------------------------------------------

/// Returns the last `lines` captured log lines, oldest first, capped by the
/// ring's size. The same lines the live `logs.line` event streams.
fn logs_tail(
    context: &ControlContext,
    params: Option<Value>,
) -> Result<Value, wire::ResponseError> {
    let request: wire::LogsTailRequest = parse_params(params)?;
    let lines = context
        .log_ring
        .tail(usize::try_from(request.lines).unwrap_or(usize::MAX));
    Ok(result(wire::LogLines { lines }))
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
            error_code::PRINCIPAL_UNAVAILABLE,
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
    use std::{path::PathBuf, sync::Arc};

    use chrono::{Duration, Utc};
    use serde_json::json;
    use tokio::sync::broadcast;
    use tokio_util::sync::CancellationToken;
    use tribal_config::TransportKind;
    use tribal_domain::{AuthTokenId, PrincipalId, Scope};
    use tribal_telemetry::LogRing;

    use super::*;
    use crate::startup::SelfWriteSentinel;

    fn base_config() -> TribalConfig {
        TribalConfig::minimum_valid("postgres://user:pass@localhost:5432/tribal")
    }

    /// A control context over `config` and `config_path` with a fresh event bus,
    /// for the crossings that need no live pool. Callers that assert on published
    /// events keep the returned subscriber; the rest discard it.
    fn test_context(
        config: TribalConfig,
        config_path: PathBuf,
    ) -> (ControlContext, broadcast::Receiver<ControlEvent>) {
        let (events, subscriber) = broadcast::channel(16);
        let context = ControlContext {
            config: Arc::new(config),
            config_path,
            cli_shadow: CliShadow::default(),
            self_write: SelfWriteSentinel::default(),
            config_write_lock: tokio::sync::Mutex::new(()),
            pool: tribal_test_utils::lazy_pool(),
            events,
            log_ring: LogRing::new(16),
            project: None,
            cancellation_token: CancellationToken::new(),
            started_at: std::time::Instant::now(),
            binary_version: Arc::from("v"),
            instance_id: Arc::from("id"),
            supervised: false,
        };
        (context, subscriber)
    }

    /// A context for the lifecycle crossings, carrying the token they cancel and
    /// the supervision marker they read; the config surface is inert here.
    fn lifecycle_context(
        cancellation_token: CancellationToken,
        supervised: bool,
    ) -> ControlContext {
        let (mut context, _) = test_context(base_config(), PathBuf::from("/tmp/tribal.yaml"));
        context.cancellation_token = cancellation_token;
        context.supervised = supervised;
        context
    }

    fn dispatch_cfg(
        config: &TribalConfig,
        path: &Path,
        method: &str,
        params: Option<Value>,
    ) -> Result<Value, wire::ResponseError> {
        dispatch_config(config, path, &CliShadow::default(), method, params)
            .expect("a config.* method")
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

    fn set_request(key: &str, value: Value) -> wire::ConfigSetRequest {
        wire::ConfigSetRequest {
            key: key.to_owned(),
            value,
        }
    }

    #[test]
    fn test_config_set_persists_and_reports_needs_restart() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("tribal.yaml");
        let (outcome, _document) = config_set(
            &base_config(),
            &path,
            &CliShadow::default(),
            set_request("logging.level", json!("debug")),
        )
        .expect("config.set succeeds");
        assert_eq!(outcome.effect, wire::WriteEffect::NeedsRestart);
        assert!(path.exists(), "the write persists to the file");
    }

    #[test]
    fn test_config_set_refuses_an_invalid_write_whole() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("tribal.yaml");
        let error = config_set(
            &base_config(),
            &path,
            &CliShadow::default(),
            set_request("server.transport", json!("grpc")),
        )
        .expect_err("an invalid write errors");
        assert_eq!(error.code, INVALID_PARAMS);
        assert!(error.data.is_some(), "the violations ride the error data");
        assert!(!path.exists(), "a refused write never touches the file");
    }

    #[tokio::test]
    async fn test_config_set_publishes_config_changed() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (context, mut subscriber) = test_context(base_config(), dir.path().join("tribal.yaml"));

        let value = dispatch(
            &context,
            None,
            "config.set",
            Some(json!({ "key": "logging.level", "value": "debug" })),
        )
        .await
        .expect("config.set succeeds");
        let outcome: wire::ConfigWriteOutcome = serde_json::from_value(value).unwrap();
        assert_eq!(outcome.effect, wire::WriteEffect::NeedsRestart);

        match subscriber
            .try_recv()
            .expect("a config.changed was published")
        {
            ControlEvent::ConfigChanged { keys, effect } => {
                assert_eq!(keys, vec!["logging.level".to_owned()]);
                assert_eq!(effect, wire::WriteEffect::NeedsRestart);
            }
            other => panic!("expected ConfigChanged, got {other:?}"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_concurrent_config_sets_do_not_lose_an_update() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (context, _) = test_context(base_config(), dir.path().join("tribal.yaml"));
        let path = dir.path().join("tribal.yaml");
        let context = Arc::new(context);

        // Two concurrent read-modify-write sets of different keys: the write lock
        // serialises them, so neither drops the other's key from the document.
        let first = {
            let context = Arc::clone(&context);
            tokio::spawn(async move {
                dispatch(
                    &context,
                    None,
                    "config.set",
                    Some(json!({ "key": "logging.level", "value": "debug" })),
                )
                .await
            })
        };
        let second = {
            let context = Arc::clone(&context);
            tokio::spawn(async move {
                dispatch(
                    &context,
                    None,
                    "config.set",
                    Some(json!({ "key": "worker.poll_interval_ms", "value": 5000 })),
                )
                .await
            })
        };
        first.await.unwrap().expect("first set succeeds");
        second.await.unwrap().expect("second set succeeds");

        // Both keys survived in the one document — neither set clobbered the
        // other's write.
        let document = std::fs::read_to_string(&path).expect("the file was written");
        assert!(
            document.contains("level: debug"),
            "the first key survived: {document}",
        );
        assert!(
            document.contains("poll_interval_ms: 5000"),
            "the second key survived — no lost update: {document}",
        );
    }

    #[tokio::test]
    async fn test_config_set_refuses_the_redaction_mask_for_a_secret() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("tribal.yaml");
        let (context, _) = test_context(base_config(), path.clone());

        let error = dispatch(
            &context,
            None,
            "config.set",
            Some(json!({ "key": "database.url", "value": REDACTED })),
        )
        .await
        .expect_err("writing the mask to a secret is refused");
        assert_eq!(error.code, error_code::SECRET_MASK_REJECTED);
        assert!(
            !path.exists(),
            "a refused mask write never touches the file",
        );
    }

    #[tokio::test]
    async fn test_config_set_allows_the_mask_string_for_a_non_secret() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("tribal.yaml");
        let (context, _) = test_context(base_config(), path.clone());

        // `logging.level` is not a secret, so the mask guard does not apply: the
        // literal is an ordinary (if unusual) free-form string and it persists.
        dispatch(
            &context,
            None,
            "config.set",
            Some(json!({ "key": "logging.level", "value": REDACTED })),
        )
        .await
        .expect("a non-secret key is not subject to the mask refusal");
        assert!(path.exists(), "the non-secret write persisted");
    }

    #[tokio::test]
    async fn test_token_list_without_a_principal_refuses_with_a_typed_code() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (context, _) = test_context(base_config(), dir.path().join("tribal.yaml"));

        let error = token_list(&context.pool, None)
            .await
            .expect_err("no principal refuses");
        assert_eq!(error.code, error_code::PRINCIPAL_UNAVAILABLE);
        assert_ne!(
            error.code, INTERNAL_ERROR,
            "the refusal must not ride the reserved internal-error code",
        );
    }

    #[tokio::test]
    async fn test_logs_tail_routes_and_returns_a_log_lines_window() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (context, _) = test_context(base_config(), dir.path().join("tribal.yaml"));

        let value = dispatch(&context, None, "logs.tail", Some(json!({ "lines": 10 })))
            .await
            .expect("logs.tail is dispatched");
        let parsed: wire::LogLines = serde_json::from_value(value).unwrap();
        assert!(
            parsed.lines.is_empty(),
            "an unfilled ring tails to an empty window",
        );
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
            dispatch_config(
                &base_config(),
                Path::new("/tmp/x"),
                &CliShadow::default(),
                "server.status",
                None,
            )
            .is_none(),
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

    #[tokio::test]
    async fn test_server_stop_cancels_the_token_and_reports_stopping() {
        let token = CancellationToken::new();
        let context = lifecycle_context(token.clone(), false);
        let outcome = server_stop(&context);
        assert!(outcome.stopping, "stop reports the binary is shutting down");
        assert!(token.is_cancelled(), "stop initiates graceful shutdown");
    }

    #[tokio::test]
    async fn test_server_restart_unsupervised_refuses_without_stopping() {
        let token = CancellationToken::new();
        let context = lifecycle_context(token.clone(), false);
        let outcome = server_restart(&context);
        assert_eq!(outcome, wire::RestartOutcome::Unsupervised);
        assert!(
            !token.is_cancelled(),
            "an unsupervised restart never self-execs — the process keeps running",
        );
    }

    #[tokio::test]
    async fn test_server_restart_supervised_is_mediated_and_stops() {
        let token = CancellationToken::new();
        let context = lifecycle_context(token.clone(), true);
        let outcome = server_restart(&context);
        assert_eq!(outcome, wire::RestartOutcome::SupervisorMediated);
        assert!(
            token.is_cancelled(),
            "a supervised restart stops for the supervisor to relaunch",
        );
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
