//! Typed projections of product and credential discovery calls.

use tribal_wire::management::{GraphGenesisOptionsCall, ModelsCatalogueCall};

use super::{config, presentation};
use crate::{cli::OutputArgs, error::AppError};

pub(crate) async fn models(config_path: &str, output: OutputArgs) -> Result<(), AppError> {
    let mut connection = config::connect(config_path).await?;
    let catalogue = connection
        .call::<ModelsCatalogueCall>(&())
        .await
        .map_err(config::client_error)?;
    presentation::write(output.json, "Models", &catalogue, "writing model catalogue")
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
