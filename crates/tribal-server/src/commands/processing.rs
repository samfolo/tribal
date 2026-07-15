//! Typed processing-profile command projections.

use tribal_domain::ProviderConnectionName;
use tribal_wire::management::{
    ConfigGetAllCall, PresetModelSettings, ProcessingProfile, ProcessingProfileCall,
    ProcessingProfileSetCall, ProcessingProfileSetRequest,
};

use super::{config, presentation};
use crate::{
    cli::{OutputArgs, ProcessingPresetArg},
    error::AppError,
};

pub(crate) async fn show(config_path: &str, output: OutputArgs) -> Result<(), AppError> {
    let mut connection = config::connect(config_path).await?;
    let profile = connection
        .call::<ProcessingProfileCall>(&())
        .await
        .map_err(config::client_error)?;
    presentation::write(
        output.json,
        "Processing profile",
        &profile,
        "writing processing profile",
    )
}

pub(crate) async fn set(
    config_path: &str,
    profile: ProcessingPresetArg,
    connection_name: ProviderConnectionName,
    model: String,
    output: OutputArgs,
) -> Result<(), AppError> {
    let model = PresetModelSettings {
        connection: connection_name,
        model,
    };
    let profile = match profile {
        ProcessingPresetArg::Efficient => ProcessingProfile::Efficient { model },
        ProcessingPresetArg::HigherQuality => ProcessingProfile::HigherQuality { model },
    };
    let mut connection = config::connect(config_path).await?;
    let expected_revision = config::stable_revision(
        connection
            .call::<ConfigGetAllCall>(&())
            .await
            .map_err(config::client_error)?,
    )?;
    let outcome = connection
        .call::<ProcessingProfileSetCall>(&ProcessingProfileSetRequest {
            profile,
            expected_revision,
        })
        .await
        .map_err(config::client_error)?;
    presentation::write(
        output.json,
        "Processing profile updated",
        &outcome,
        "writing processing update",
    )
}
