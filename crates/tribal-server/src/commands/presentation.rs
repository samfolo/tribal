//! Terminal rendering shared by management-backed command projections.

use std::io::Write as _;

use serde::Serialize;

use crate::error::AppError;

pub(crate) fn write_json(value: &impl Serialize, context: &str) -> Result<(), AppError> {
    let stdout = std::io::stdout();
    let mut writer = stdout.lock();
    serde_json::to_writer(&mut writer, value).map_err(|source| AppError::Io {
        context: context.to_owned(),
        source: std::io::Error::new(std::io::ErrorKind::InvalidData, source),
    })?;
    writer.write_all(b"\n").map_err(|source| AppError::Io {
        context: context.to_owned(),
        source,
    })
}

pub(crate) fn write_human(
    heading: &str,
    value: &impl Serialize,
    context: &str,
) -> Result<(), AppError> {
    let rendered = serde_yaml::to_string(value).map_err(|source| AppError::Io {
        context: context.to_owned(),
        source: std::io::Error::new(std::io::ErrorKind::InvalidData, source),
    })?;
    let stdout = std::io::stdout();
    let mut writer = stdout.lock();
    writeln!(writer, "{heading}").map_err(|source| AppError::Io {
        context: context.to_owned(),
        source,
    })?;
    writer
        .write_all(rendered.as_bytes())
        .map_err(|source| AppError::Io {
            context: context.to_owned(),
            source,
        })
}

pub(crate) fn write(
    json: bool,
    heading: &str,
    value: &impl Serialize,
    context: &str,
) -> Result<(), AppError> {
    if json {
        write_json(value, context)
    } else {
        write_human(heading, value, context)
    }
}
