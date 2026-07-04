//! Routing a control request to the config crossings and back.
//!
//! Dispatch maps a JSON-RPC method name onto the config-native
//! [`tribal_config`] operations, then maps their config-native answers onto the
//! [`tribal_wire::control`] DTOs the client speaks — the wire crate stays pure,
//! and this binding is the one place the two vocabularies meet. An `Ok` carries
//! the result payload; an `Err` carries the JSON-RPC error the caller frames.

use std::path::Path;

use serde::de::DeserializeOwned;
use serde_json::Value;
use tribal_config::TribalConfig;
use tribal_wire::control as wire;

/// JSON-RPC reserved code: the method name is not one this server dispatches.
const METHOD_NOT_FOUND: i32 = -32601;
/// JSON-RPC reserved code: the params were absent, ill-typed, or rejected.
const INVALID_PARAMS: i32 = -32602;
/// JSON-RPC reserved code: the server failed to complete a valid request.
const INTERNAL_ERROR: i32 = -32603;

/// The config-access context one dispatch reads: the running configuration
/// snapshot and the file a write persists to.
pub(crate) struct ConfigContext<'a> {
    /// The resolved configuration the server is running with.
    pub config: &'a TribalConfig,
    /// The YAML file `config.set` writes, the loader's layer four.
    pub config_file: &'a Path,
}

/// Dispatches one control method, returning its result payload or the JSON-RPC
/// error to frame back.
pub(crate) fn dispatch(
    context: &ConfigContext<'_>,
    method: &str,
    params: Option<Value>,
) -> Result<Value, wire::ResponseError> {
    match method {
        "config.schema" => Ok(result(config_schema())),
        "config.get" => {
            let request: wire::ConfigGetRequest = parse_params(params)?;
            config_get(context.config, request)
        }
        "config.getAll" => Ok(result(config_get_all(context.config))),
        "config.set" => {
            let request: wire::ConfigSetRequest = parse_params(params)?;
            config_set(context, request)
        }
        "config.validate" => {
            let request: wire::ConfigValidateRequest = parse_params(params)?;
            Ok(result(config_validate(context.config, request)))
        }
        "config.path" => Ok(result(config_path(context.config_file))),
        other => Err(error(
            METHOD_NOT_FOUND,
            format!("no such control method: {other}"),
            None,
        )),
    }
}

// ---------------------------------------------------------------------------
// config crossings
// ---------------------------------------------------------------------------

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
    context: &ConfigContext<'_>,
    request: wire::ConfigSetRequest,
) -> Result<Value, wire::ResponseError> {
    match tribal_config::set(
        context.config,
        context.config_file,
        &request.key,
        request.value,
    ) {
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
    use serde_json::json;

    use super::*;

    fn base_config() -> TribalConfig {
        TribalConfig::minimum_valid("postgres://user:pass@localhost:5432/tribal")
    }

    fn context<'a>(config: &'a TribalConfig, path: &'a Path) -> ConfigContext<'a> {
        ConfigContext {
            config,
            config_file: path,
        }
    }

    #[test]
    fn test_config_get_returns_the_effective_value() {
        let config = base_config();
        let path = Path::new("/tmp/tribal.yaml");
        let value = dispatch(
            &context(&config, path),
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
        let config = base_config();
        let path = Path::new("/tmp/tribal.yaml");
        let value = dispatch(
            &context(&config, path),
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
        let config = base_config();
        let path = Path::new("/tmp/tribal.yaml");
        let error = dispatch(
            &context(&config, path),
            "config.get",
            Some(json!({ "key": "server.nope" })),
        )
        .expect_err("an unknown key errors");
        assert_eq!(error.code, INVALID_PARAMS);
    }

    #[test]
    fn test_config_path_reports_the_file() {
        let config = base_config();
        let path = Path::new("/home/op/.config/tribal/tribal.yaml");
        let value = dispatch(&context(&config, path), "config.path", None).expect("config.path");
        let parsed: wire::ConfigPath = serde_json::from_value(value).unwrap();
        assert_eq!(parsed.path, "/home/op/.config/tribal/tribal.yaml");
    }

    #[test]
    fn test_config_validate_rejects_an_invalid_value() {
        let config = base_config();
        let path = Path::new("/tmp/tribal.yaml");
        let value = dispatch(
            &context(&config, path),
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
        let config = base_config();
        let value = dispatch(
            &context(&config, &path),
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
        let config = base_config();
        let error = dispatch(
            &context(&config, &path),
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
        let config = base_config();
        let path = Path::new("/tmp/tribal.yaml");
        let value =
            dispatch(&context(&config, path), "config.schema", None).expect("config.schema");
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
    fn test_an_unknown_method_is_method_not_found() {
        let config = base_config();
        let path = Path::new("/tmp/tribal.yaml");
        let error =
            dispatch(&context(&config, path), "config.teleport", None).expect_err("no such method");
        assert_eq!(error.code, METHOD_NOT_FOUND);
    }
}
