//! Lease-owning revisioned configuration CLI adapters.

use std::{io::Write as _, path::Path};

use tribal_wire::management::{
    ConfigDocument, ConfigFieldPath, ConfigGetRequest, ConfigSetRequest, ConfigValue,
    ConfigWriteOutcome,
};

use crate::{
    cli::{ConfigGetArgs, ConfigSetArgs, ConfigValidateArgs},
    error::AppError,
    management::{
        authority::{
            AuthorityAcquire, AuthorityConflict, AuthorityDescriptor, AuthorityError,
            AuthorityLease, AuthorityOwnerKind,
        },
        client::{ManagementClient, ManagementClientError},
        configuration::{ConfigAuthority, ConfigAuthorityError},
        worker::{self, OneShotConfigError},
    },
};

struct OneShotContext {
    lease: AuthorityLease,
    config: ConfigAuthority,
}

enum ConfigTarget {
    Manager(AuthorityDescriptor),
    OneShot(OneShotContext),
}

pub(crate) fn get(config_path: &str, args: &ConfigGetArgs) -> Result<(), AppError> {
    let key = parse_key(&args.key)?;
    let request = ConfigGetRequest { key };
    let value = match acquire(config_path)? {
        ConfigTarget::OneShot(context) => worker::run_one_shot(|| context.config.get(request))
            .map_err(one_shot_error)?
            .map_err(config_error)?,
        ConfigTarget::Manager(descriptor) => {
            manager_call::<ConfigValue>(&descriptor, "config.get", Some(to_params(request)?))?
        }
    };
    write_json(&value)
}

pub(crate) fn set(config_path: &str, args: &ConfigSetArgs) -> Result<(), AppError> {
    let key = parse_key(&args.key)?;
    let value = parse_value(&args.value);
    let outcome = match acquire(config_path)? {
        ConfigTarget::OneShot(context) => worker::run_one_shot(|| {
            let expected_revision = context.config.revision()?;
            context.config.set(ConfigSetRequest {
                key,
                value: tribal_wire::management::ConfigLiteral::new(value),
                expected_revision,
            })
        })
        .map_err(one_shot_error)?
        .map_err(config_error)?,
        ConfigTarget::Manager(descriptor) => {
            let document: ConfigDocument = manager_call(&descriptor, "config.getAll", None)?;
            let expected_revision = document_revision(document)?;
            manager_call::<ConfigWriteOutcome>(
                &descriptor,
                "config.set",
                Some(to_params(ConfigSetRequest {
                    key,
                    value: tribal_wire::management::ConfigLiteral::new(value),
                    expected_revision,
                })?),
            )?
        }
    };
    write_json(&outcome)
}

pub(crate) fn validate(config_path: &str, args: &ConfigValidateArgs) -> Result<(), AppError> {
    let key = parse_key(&args.key)?;
    let candidate = parse_value(&args.value);
    let value = match acquire(config_path)? {
        ConfigTarget::OneShot(context) => {
            let violations =
                worker::run_one_shot(|| context.config.validate_value(key.as_str(), candidate))
                    .map_err(one_shot_error)?
                    .map_err(config_error)?;
            validation_value(violations)
        }
        ConfigTarget::Manager(descriptor) => manager_call::<serde_json::Value>(
            &descriptor,
            "config.validate",
            Some(serde_json::json!({ "key": key, "value": candidate })),
        )?,
    };
    write_json(&value)
}

pub(crate) fn path(config_path: &str) -> Result<(), AppError> {
    let canonical_config_path = match acquire(config_path)? {
        ConfigTarget::OneShot(context) => context.lease.paths().canonical_config_path.clone(),
        ConfigTarget::Manager(descriptor) => descriptor.canonical_config_path,
    };
    write_json(&tribal_wire::management::ConfigFilePath {
        path: canonical_config_path.to_string_lossy().into_owned(),
    })
}

fn acquire(config_path: &str) -> Result<ConfigTarget, AppError> {
    let acquire = AuthorityLease::acquire(Path::new(config_path)).map_err(authority_error)?;
    let mut lease = match acquire {
        AuthorityAcquire::Acquired(lease) => lease,
        AuthorityAcquire::Occupied(AuthorityConflict::Manager(descriptor)) => {
            return Ok(ConfigTarget::Manager(descriptor));
        }
        AuthorityAcquire::Occupied(
            AuthorityConflict::StandaloneRuntime(_)
            | AuthorityConflict::OneShot(_)
            | AuthorityConflict::Recovering { .. },
        ) => {
            return Err(authority_error(AuthorityError::Occupied {
                path: Path::new(config_path).to_owned(),
            }));
        }
    };
    let descriptor = AuthorityDescriptor {
        kind: AuthorityOwnerKind::OneShot,
        instance_id: uuid::Uuid::new_v4().to_string(),
        pid: std::process::id(),
        binary_version: env!("CARGO_PKG_VERSION").to_owned(),
        canonical_config_path: lease.paths().canonical_config_path.clone(),
        socket_path: None,
        protocol_version: None,
    };
    lease.publish(&descriptor).map_err(authority_error)?;
    let config = ConfigAuthority::new(lease.paths().canonical_config_path.clone());
    Ok(ConfigTarget::OneShot(OneShotContext { lease, config }))
}

fn manager_call<T: serde::de::DeserializeOwned>(
    descriptor: &AuthorityDescriptor,
    method: &str,
    params: Option<serde_json::Value>,
) -> Result<T, AppError> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|source| AppError::Io {
            context: "creating configuration command executor".to_owned(),
            source,
        })?;
    runtime.block_on(async {
        let mut client = ManagementClient::connect(descriptor)
            .await
            .map_err(client_error)?;
        client.call(method, params).await.map_err(client_error)
    })
}

fn to_params(value: impl serde::Serialize) -> Result<serde_json::Value, AppError> {
    serde_json::to_value(value).map_err(|source| AppError::Io {
        context: "encoding configuration request".to_owned(),
        source: std::io::Error::new(std::io::ErrorKind::InvalidData, source),
    })
}

fn document_revision(
    document: ConfigDocument,
) -> Result<tribal_wire::management::ConfigRevision, AppError> {
    match document {
        ConfigDocument::DurableValid { revision, .. }
        | ConfigDocument::DurableInvalid { revision } => Ok(revision),
        ConfigDocument::UncertainValid { .. }
        | ConfigDocument::UncertainInvalid { .. }
        | ConfigDocument::Unreadable { .. } => {
            Err(config_error(ConfigAuthorityError::StableWinnerUnavailable))
        }
    }
}

fn validation_value(violations: Vec<tribal_config::ConfigViolation>) -> serde_json::Value {
    serde_json::json!({
        "valid": violations.is_empty(),
        "violations": violations
            .into_iter()
            .map(|violation| serde_json::json!({
                "key": violation.key,
                "message": violation.message,
            }))
            .collect::<Vec<_>>(),
    })
}

fn parse_key(raw: &str) -> Result<ConfigFieldPath, AppError> {
    ConfigFieldPath::parse(raw).map_err(|error| {
        config_error(ConfigAuthorityError::Invalid {
            message: error.to_string(),
        })
    })
}

fn parse_value(raw: &str) -> serde_json::Value {
    match serde_json::from_str(raw) {
        Ok(value) => value,
        Err(_) => serde_json::Value::String(raw.to_owned()),
    }
}

fn write_json(value: &impl serde::Serialize) -> Result<(), AppError> {
    let stdout = std::io::stdout();
    let mut writer = stdout.lock();
    serde_json::to_writer(&mut writer, value).map_err(|source| AppError::Io {
        context: "writing configuration result".to_owned(),
        source: std::io::Error::new(std::io::ErrorKind::InvalidData, source),
    })?;
    writer.write_all(b"\n").map_err(|source| AppError::Io {
        context: "writing configuration result".to_owned(),
        source,
    })
}

fn authority_error(source: AuthorityError) -> AppError {
    AppError::Management {
        source: Box::new(source),
    }
}

fn config_error(source: ConfigAuthorityError) -> AppError {
    AppError::Management {
        source: Box::new(source),
    }
}

fn client_error(source: ManagementClientError) -> AppError {
    AppError::Management {
        source: Box::new(source),
    }
}

fn one_shot_error(source: OneShotConfigError) -> AppError {
    AppError::Management {
        source: Box::new(source),
    }
}
