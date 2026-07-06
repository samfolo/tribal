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
use tokio::sync::watch;
use tribal_auth::AuthenticatedPrincipal;
use tribal_config::{CliShadow, Persisted, TribalConfig, is_secret_key};
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
            &context.config_snapshot(),
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

/// Persists a `config.set`, adopts a live write in the running process, and
/// announces the change on the bus. A change every subscriber learns of
/// through `config.changed` — the binary is the only writer, so the client
/// awaits the event rather than assuming the write took.
///
/// The whole write — persist, side effect, snapshot swap, announcement — sits
/// inside one critical section, so concurrent writes serialise and each
/// writer's candidate builds on the snapshot the previous writer swapped in.
/// The blocking file I/O runs on a blocking thread rather than the async
/// worker.
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
    let config_path = context.config_path.clone();
    let cli = context.cli_shadow.clone();

    let _guard = context.config_write_lock.lock().await;
    let config = context.config_snapshot();
    let (outcome, persisted) =
        tokio::task::spawn_blocking(move || config_set(&config, &config_path, &cli, request))
            .await
            .map_err(|source| {
                error(
                    INTERNAL_ERROR,
                    format!("config.set did not complete: {source}"),
                    None,
                )
            })??;

    // Record the exact bytes written so the file watcher does not re-announce
    // this write as an external edit contradicting the per-key event below.
    context.self_write.record(persisted.document);

    if outcome.effect == wire::WriteEffect::Live {
        let updated = Arc::new(persisted.config);
        adopt_live_write(
            &context.config,
            Arc::clone(&updated),
            || apply_hot_key(context, &key, &updated),
            || publish_config_changed(context, key.clone(), wire::WriteEffect::Live),
        )
        .inspect_err(|_| {
            // The file changed but the process did not adopt the value: the
            // truthful effect of the persisted write is now needs-restart.
            publish_config_changed(context, key.clone(), wire::WriteEffect::NeedsRestart);
        })?;
    } else {
        publish_config_changed(context, key, outcome.effect);
    }
    Ok(result(outcome))
}

/// Adopts a validated live write in the running process: the side effect is
/// applied, the served snapshot swaps, then the change publishes — in that
/// order, so a read never returns a value whose behaviour is not yet in
/// force, and a published change is already visible to reads.
fn adopt_live_write<E>(
    snapshot: &watch::Sender<Arc<TribalConfig>>,
    updated: Arc<TribalConfig>,
    apply: impl FnOnce() -> Result<(), E>,
    publish: impl FnOnce(),
) -> Result<(), E> {
    apply()?;
    snapshot.send_replace(updated);
    publish();
    Ok(())
}

/// Applies a hot key's side effect to the running process — the substrate
/// whose existence the promotion rule demands of every `HOT_KEYS` entry.
fn apply_hot_key(
    context: &ControlContext,
    key: &str,
    updated: &TribalConfig,
) -> Result<(), wire::ResponseError> {
    match key {
        "logging.level" => context
            .log_filter
            .reload(&updated.logging.level)
            .map_err(|source| {
                error(
                    INTERNAL_ERROR,
                    format!("logging.level persisted but was not adopted: {source}"),
                    None,
                )
            }),
        other => Err(error(
            INTERNAL_ERROR,
            format!("hot key {other} has no live-apply substrate"),
            None,
        )),
    }
}

/// Announces a config change on the bus. A send with no subscribers is not an
/// error — no client is listening yet.
fn publish_config_changed(context: &ControlContext, key: String, effect: wire::WriteEffect) {
    let _ = context.events.send(ControlEvent::ConfigChanged {
        keys: vec![key],
        effect,
    });
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
            default_value: field.default,
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
/// persisted record alongside — the written bytes the caller records as its
/// own, and the applied configuration a live adoption swaps in. Runs on a
/// blocking thread — the persist is synchronous file I/O.
fn config_set(
    config: &TribalConfig,
    config_file: &Path,
    cli_shadow: &CliShadow,
    request: wire::ConfigSetRequest,
) -> Result<(wire::ConfigWriteOutcome, Persisted), wire::ResponseError> {
    match tribal_config::set(config, config_file, &request.key, request.value, cli_shadow) {
        Ok(persisted) => Ok((write_outcome(persisted.effect.clone()), persisted)),
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
        &context.config_snapshot(),
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
    use std::{
        cell::{Cell, RefCell},
        path::PathBuf,
        sync::Arc,
    };

    use chrono::{Duration, Utc};
    use serde_json::json;
    use tokio::sync::broadcast;
    use tokio_util::sync::CancellationToken;
    use tracing::instrument::WithSubscriber;
    use tracing_subscriber::layer::SubscriberExt;
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
    ///
    /// The filter layer is leaked so the handle's subscriber never reads as
    /// gone; a test that must observe the filter builds its own live stack.
    fn test_context(
        config: TribalConfig,
        config_path: PathBuf,
    ) -> (ControlContext, broadcast::Receiver<ControlEvent>) {
        let (filter_layer, log_filter) =
            tribal_telemetry::reloadable_env_filter("info").expect("the directive parses");
        std::mem::forget(filter_layer);
        let (context, subscriber) = test_context_with_filter(config, config_path, log_filter);
        (context, subscriber)
    }

    /// A control context whose hot writes reach `log_filter` — the seam the
    /// live-adoption tests observe a real subscriber through.
    fn test_context_with_filter(
        config: TribalConfig,
        config_path: PathBuf,
        log_filter: tribal_telemetry::LogFilterHandle,
    ) -> (ControlContext, broadcast::Receiver<ControlEvent>) {
        let (events, subscriber) = broadcast::channel(16);
        let context = ControlContext {
            config: watch::Sender::new(Arc::new(config)),
            config_path,
            cli_shadow: CliShadow::default(),
            self_write: SelfWriteSentinel::default(),
            config_write_lock: tokio::sync::Mutex::new(()),
            pool: tribal_test_utils::lazy_pool(),
            events,
            log_ring: LogRing::new(16),
            log_filter,
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
        let (outcome, _persisted) = config_set(
            &base_config(),
            &path,
            &CliShadow::default(),
            set_request("worker.poll_interval_ms", json!(5000)),
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
            Some(json!({ "key": "worker.poll_interval_ms", "value": 5000 })),
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
                assert_eq!(keys, vec!["worker.poll_interval_ms".to_owned()]);
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

    // -- live adoption -------------------------------------------------------

    /// A `MakeWriter` over a shared buffer, so a test reads back what the fmt
    /// layer wrote.
    #[derive(Clone, Default)]
    struct SharedWriter(Arc<std::sync::Mutex<Vec<u8>>>);

    impl SharedWriter {
        fn contents(&self) -> String {
            String::from_utf8_lossy(&self.0.lock().expect("writer lock")).into_owned()
        }
    }

    impl std::io::Write for SharedWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().expect("writer lock").extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for SharedWriter {
        type Writer = SharedWriter;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    /// A live subscriber stack filtered by `directive`: the subscriber to
    /// install for the test's scope, the handle a context adopts hot writes
    /// through, and the buffer its output lands in.
    fn live_filter_stack(
        directive: &str,
    ) -> (
        impl tracing::Subscriber + Send + Sync,
        tribal_telemetry::LogFilterHandle,
        SharedWriter,
    ) {
        let (filter_layer, handle) =
            tribal_telemetry::reloadable_env_filter(directive).expect("the directive parses");
        let writer = SharedWriter::default();
        let subscriber = tracing_subscriber::registry().with(filter_layer).with(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_writer(writer.clone()),
        );
        (subscriber, handle, writer)
    }

    #[test]
    fn test_a_live_write_is_applied_before_the_swap_and_published_after_it() {
        let snapshot = watch::Sender::new(Arc::new(base_config()));
        let mut updated = base_config();
        updated.logging.level = "debug".to_owned();
        let updated = Arc::new(updated);
        let steps: RefCell<Vec<(&str, String)>> = RefCell::new(Vec::new());
        let served_level = || snapshot.borrow().logging.level.clone();

        adopt_live_write(
            &snapshot,
            Arc::clone(&updated),
            || {
                steps.borrow_mut().push(("apply", served_level()));
                Ok::<(), wire::ResponseError>(())
            },
            || steps.borrow_mut().push(("publish", served_level())),
        )
        .expect("adoption succeeds");

        assert_eq!(
            *steps.borrow(),
            vec![
                ("apply", "info".to_owned()),
                ("publish", "debug".to_owned()),
            ],
            "the side effect lands while reads still serve the old value, and \
             the publish sees the already-swapped snapshot",
        );
    }

    #[test]
    fn test_a_failed_apply_leaves_the_snapshot_and_skips_the_publish() {
        let snapshot = watch::Sender::new(Arc::new(base_config()));
        let mut updated = base_config();
        updated.logging.level = "debug".to_owned();
        let published = Cell::new(false);

        let result = adopt_live_write(
            &snapshot,
            Arc::new(updated),
            || Err(error(INTERNAL_ERROR, "apply failed".to_owned(), None)),
            || published.set(true),
        );

        assert!(result.is_err());
        assert_eq!(
            snapshot.borrow().logging.level,
            "info",
            "a value not in force is never served",
        );
        assert!(
            !published.get(),
            "no event announces a value that was not adopted",
        );
    }

    #[tokio::test]
    async fn test_a_hot_write_reports_live_and_the_process_emits_at_the_new_level() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("tribal.yaml");
        let (subscriber, handle, writer) = live_filter_stack("error");
        let (context, mut events) = test_context_with_filter(base_config(), path.clone(), handle);

        async {
            tracing::debug!(target: "hot_write_proof", "below before");
            tracing::error!(target: "hot_write_proof", "above before");

            let value = dispatch(
                &context,
                None,
                "config.set",
                Some(json!({ "key": "logging.level", "value": "error,hot_write_proof=debug" })),
            )
            .await
            .expect("config.set succeeds");
            let outcome: wire::ConfigWriteOutcome = serde_json::from_value(value).unwrap();
            assert_eq!(
                outcome.effect,
                wire::WriteEffect::Live,
                "a hot key reports live",
            );

            tracing::debug!(target: "hot_write_proof", "below after");
        }
        .with_subscriber(subscriber)
        .await;

        let output = writer.contents();
        assert!(
            !output.contains("below before"),
            "debug is filtered before the write: {output}",
        );
        assert!(output.contains("above before"), "got: {output}");
        assert!(
            output.contains("below after"),
            "the process emits at the new level without restart: {output}",
        );

        // The swapped snapshot serves the new value to every read.
        let all = dispatch(&context, None, "config.getAll", None)
            .await
            .expect("config.getAll");
        let document: wire::ConfigDocument = serde_json::from_value(all).unwrap();
        assert_eq!(
            document.values["logging"]["level"],
            json!("error,hot_write_proof=debug"),
        );

        match events.try_recv().expect("a config.changed was published") {
            ControlEvent::ConfigChanged { keys, effect } => {
                assert_eq!(keys, vec!["logging.level".to_owned()]);
                assert_eq!(effect, wire::WriteEffect::Live);
            }
            other => panic!("expected ConfigChanged, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_an_unparseable_hot_directive_is_refused_before_anything_changes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("tribal.yaml");
        let (subscriber, handle, writer) = live_filter_stack("error,refusal_proof=info");
        let (context, mut events) = test_context_with_filter(base_config(), path.clone(), handle);

        let error = async {
            let error = dispatch(
                &context,
                None,
                "config.set",
                Some(json!({ "key": "logging.level", "value": "not valid [[" })),
            )
            .await
            .expect_err("an unparseable directive is refused");
            tracing::debug!(target: "refusal_proof", "still below");
            tracing::info!(target: "refusal_proof", "still above");
            error
        }
        .with_subscriber(subscriber)
        .await;

        assert_eq!(error.code, INVALID_PARAMS);
        assert!(error.data.is_some(), "the violations ride the error data");
        assert!(!path.exists(), "nothing persists");
        assert_eq!(
            context.config_snapshot().logging.level,
            base_config().logging.level,
            "nothing swaps",
        );
        assert!(
            matches!(
                events.try_recv(),
                Err(broadcast::error::TryRecvError::Empty)
            ),
            "no event publishes",
        );
        let output = writer.contents();
        assert!(
            !output.contains("still below"),
            "the old filter keeps filtering: {output}",
        );
        assert!(output.contains("still above"), "got: {output}");
    }

    #[tokio::test]
    async fn test_a_write_the_process_cannot_adopt_errors_and_downgrades_the_event() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("tribal.yaml");
        // The substrate is gone, so the apply must fail after the persist.
        let (filter_layer, handle) =
            tribal_telemetry::reloadable_env_filter("info").expect("the directive parses");
        drop(filter_layer);
        let (context, mut events) = test_context_with_filter(base_config(), path.clone(), handle);

        let error = dispatch(
            &context,
            None,
            "config.set",
            Some(json!({ "key": "logging.level", "value": "debug" })),
        )
        .await
        .expect_err("a write the process cannot adopt errors");

        assert_eq!(error.code, INTERNAL_ERROR);
        assert!(
            path.exists(),
            "the write persisted before the failed adoption"
        );
        assert_eq!(
            context.config_snapshot().logging.level,
            base_config().logging.level,
            "a value not in force is never served",
        );
        match events
            .try_recv()
            .expect("the persisted change is announced")
        {
            ControlEvent::ConfigChanged { keys, effect } => {
                assert_eq!(keys, vec!["logging.level".to_owned()]);
                assert_eq!(
                    effect,
                    wire::WriteEffect::NeedsRestart,
                    "the truthful effect of a persisted-but-unadopted write",
                );
            }
            other => panic!("expected ConfigChanged, got {other:?}"),
        }
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

        // `telemetry.service_name` is not a secret, so the mask guard does not
        // apply: the literal is an ordinary (if unusual) free-form string and
        // it persists.
        dispatch(
            &context,
            None,
            "config.set",
            Some(json!({ "key": "telemetry.service_name", "value": REDACTED })),
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
