//! Typed projections of reindex administration.

use tribal_wire::management::{
    ConfigGetAllCall, GenesisEmbeddingInput, GraphGenesisOptionsCall, MutationMode,
    ReindexCancelCall, ReindexCancelRequest, ReindexPruneCall, ReindexPruneRequest, ReindexRunCall,
    ReindexRunRequest,
};

use super::{config, presentation};
use crate::{
    cli::{OutputArgs, ReindexPruneArgs, ReindexRunArgs},
    error::AppError,
};

pub(crate) async fn run(config_path: &str, args: ReindexRunArgs) -> Result<(), AppError> {
    let mut connection = config::connect(config_path).await?;
    let expected_revision = revision(&mut connection).await?;
    let options = connection
        .call::<GraphGenesisOptionsCall>(&())
        .await
        .map_err(config::client_error)?;
    let provider = options
        .connections
        .iter()
        .find(|candidate| candidate.connection == args.connection)
        .map(|candidate| candidate.provider)
        .ok_or(AppError::CheckFailed)?;
    let result = connection
        .call::<ReindexRunCall>(&ReindexRunRequest {
            expected_revision,
            target: GenesisEmbeddingInput {
                connection: args.connection,
                provider,
                model: args.model,
                dimensions: args.dimensions,
            },
            mode: mutation_mode(args.apply),
        })
        .await
        .map_err(config::client_error)?;
    presentation::write(
        args.output.json,
        "Reindex",
        &result,
        "writing reindex result",
    )
}

pub(crate) async fn cancel(config_path: &str, output: OutputArgs) -> Result<(), AppError> {
    let mut connection = config::connect(config_path).await?;
    let expected_revision = revision(&mut connection).await?;
    let result = connection
        .call::<ReindexCancelCall>(&ReindexCancelRequest { expected_revision })
        .await
        .map_err(config::client_error)?;
    presentation::write(
        output.json,
        "Reindex cancellation",
        &result,
        "writing reindex cancellation",
    )
}

pub(crate) async fn prune(config_path: &str, args: ReindexPruneArgs) -> Result<(), AppError> {
    let mut connection = config::connect(config_path).await?;
    let expected_revision = revision(&mut connection).await?;
    let result = connection
        .call::<ReindexPruneCall>(&ReindexPruneRequest {
            expected_revision,
            mode: mutation_mode(args.apply),
        })
        .await
        .map_err(config::client_error)?;
    presentation::write(
        args.output.json,
        "Reindex pruning",
        &result,
        "writing reindex pruning",
    )
}

async fn revision(
    connection: &mut crate::management::connector::ManagerConnection,
) -> Result<tribal_wire::management::ConfigRevision, AppError> {
    config::stable_revision(
        connection
            .call::<ConfigGetAllCall>(&())
            .await
            .map_err(config::client_error)?,
    )
}

const fn mutation_mode(apply: bool) -> MutationMode {
    if apply {
        MutationMode::Apply
    } else {
        MutationMode::Preview
    }
}
