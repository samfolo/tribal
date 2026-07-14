//! Typed projection of database administration.

use tribal_wire::management::{
    ConfigGetAllCall, DatabaseInitialiseCall, DatabaseInitialiseRequest,
};

use super::{config, presentation};
use crate::{cli::OutputArgs, error::AppError};

pub(crate) async fn initialise(config_path: &str, output: OutputArgs) -> Result<(), AppError> {
    let mut connection = config::connect(config_path).await?;
    let expected_revision = config::stable_revision(
        connection
            .call::<ConfigGetAllCall>(&())
            .await
            .map_err(config::client_error)?,
    )?;
    let result = connection
        .call::<DatabaseInitialiseCall>(&DatabaseInitialiseRequest { expected_revision })
        .await
        .map_err(config::client_error)?;
    presentation::write(
        output.json,
        "Database initialisation",
        &result,
        "writing database initialisation result",
    )
}
