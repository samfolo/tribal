//! Typed projections of product and credential discovery calls.

use tribal_wire::management::{
    ConfigGetAllCall, CredentialSourcesCall, CredentialSourcesRequest, CredentialUse,
    EndpointSelection, GenesisEmbeddingInput, GraphGenesisOptionsCall, ModelsCatalogueCall,
};

use super::{config, presentation};
use crate::{
    cli::{GenesisCredentialSourceArgs, ModelCredentialSourceArgs, OutputArgs},
    error::AppError,
};

/// Failure validating a discovery request assembled from command arguments.
#[derive(Debug, thiserror::Error)]
enum DiscoveryCommandError {
    #[error("--provider-default and --endpoint cannot be used together")]
    ConflictingEndpoint,
}

pub(crate) async fn models(config_path: &str, output: OutputArgs) -> Result<(), AppError> {
    let mut connection = config::connect(config_path).await?;
    let catalogue = connection
        .call::<ModelsCatalogueCall>(&())
        .await
        .map_err(config::client_error)?;
    presentation::write(output.json, "Models", &catalogue, "writing model catalogue")
}

pub(crate) async fn model_credentials(
    config_path: &str,
    args: ModelCredentialSourceArgs,
) -> Result<(), AppError> {
    let mut connection = config::connect(config_path).await?;
    let expected_revision = config::stable_revision(
        connection
            .call::<ConfigGetAllCall>(&())
            .await
            .map_err(config::client_error)?,
    )?;
    let endpoint = match (args.provider_default, args.endpoint) {
        (true, None) => EndpointSelection::ProviderDefault,
        (false, Some(value)) => EndpointSelection::Custom { value },
        (false, None) => EndpointSelection::Preserve,
        (true, Some(_)) => return Err(invalid_discovery_input()),
    };
    let request = CredentialSourcesRequest {
        use_case: CredentialUse::ModelSelection {
            model: args.model,
            stages: args.stage.into_iter().map(Into::into).collect(),
            endpoint,
        },
        expected_revision,
    };
    let sources = connection
        .call::<CredentialSourcesCall>(&request)
        .await
        .map_err(config::client_error)?;
    presentation::write(
        args.output.json,
        "Credential sources",
        &sources,
        "writing credential sources",
    )
}

pub(crate) async fn genesis_credentials(
    config_path: &str,
    args: GenesisCredentialSourceArgs,
) -> Result<(), AppError> {
    let mut connection = config::connect(config_path).await?;
    let expected_revision = config::stable_revision(
        connection
            .call::<ConfigGetAllCall>(&())
            .await
            .map_err(config::client_error)?,
    )?;
    let request = CredentialSourcesRequest {
        use_case: CredentialUse::Genesis {
            embedding: GenesisEmbeddingInput {
                provider: args.provider,
                model: args.model,
                dimensions: args.dimensions,
                base_url: args.base_url,
            },
        },
        expected_revision,
    };
    let sources = connection
        .call::<CredentialSourcesCall>(&request)
        .await
        .map_err(config::client_error)?;
    presentation::write(
        args.output.json,
        "Credential sources",
        &sources,
        "writing credential sources",
    )
}

pub(crate) async fn genesis_options(config_path: &str, output: OutputArgs) -> Result<(), AppError> {
    let mut connection = config::connect(config_path).await?;
    let options = connection
        .call::<GraphGenesisOptionsCall>(&())
        .await
        .map_err(config::client_error)?;
    presentation::write(
        output.json,
        "Graph genesis options",
        &options,
        "writing graph genesis options",
    )
}

fn invalid_discovery_input() -> AppError {
    AppError::Management {
        source: Box::new(DiscoveryCommandError::ConflictingEndpoint),
    }
}
