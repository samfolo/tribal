//! Typed projections of token administration.

use tribal_wire::management::{
    ConfigGetAllCall, PageCursor, PageCursorError, PageRequest, PageSize, PageSizeError,
    TokenCreateCall, TokenCreateRequest, TokenListCall, TokenListRequest, TokenRevokeAllCall,
    TokenRevokeAllRequest, TokenRevokeCall, TokenRevokeRequest,
};

use super::{config, presentation};
use crate::{
    cli::{TokenCreateArgs, TokenListArgs, TokenRevokeAllArgs, TokenRevokeArgs},
    error::AppError,
};

/// Failure validating a token inventory request.
#[derive(Debug, thiserror::Error)]
enum TokenCommandError {
    #[error("invalid page size: {source}")]
    PageSize {
        #[source]
        source: PageSizeError,
    },
    #[error("invalid page cursor: {source}")]
    Cursor {
        #[source]
        source: PageCursorError,
    },
}

pub(crate) async fn create(config_path: &str, args: TokenCreateArgs) -> Result<(), AppError> {
    let mut connection = config::connect(config_path).await?;
    let expected_revision = stable_revision(&mut connection).await?;
    let result = connection
        .call::<TokenCreateCall>(&TokenCreateRequest {
            expected_revision,
            principal: args.principal,
            ttl_hours: args.ttl,
            scopes: args.scope,
            persist_as_default: args.persist_as_default,
        })
        .await
        .map_err(config::client_error)?;
    presentation::write(
        args.output.json,
        "Token created",
        &result,
        "writing token creation",
    )
}

pub(crate) async fn list(config_path: &str, args: TokenListArgs) -> Result<(), AppError> {
    let page = PageRequest {
        size: PageSize::try_from(args.page_size)
            .map_err(|source| command_error(TokenCommandError::PageSize { source }))?,
        after: args
            .after
            .map(PageCursor::try_from)
            .transpose()
            .map_err(|source| command_error(TokenCommandError::Cursor { source }))?,
    };
    let mut connection = config::connect(config_path).await?;
    let result = connection
        .call::<TokenListCall>(&TokenListRequest { page })
        .await
        .map_err(config::client_error)?;
    presentation::write(
        args.output.json,
        "Tokens",
        &result,
        "writing token inventory",
    )
}

pub(crate) async fn revoke(config_path: &str, args: TokenRevokeArgs) -> Result<(), AppError> {
    let mut connection = config::connect(config_path).await?;
    let expected_revision = stable_revision(&mut connection).await?;
    let result = connection
        .call::<TokenRevokeCall>(&TokenRevokeRequest {
            expected_revision,
            id: args.id,
        })
        .await
        .map_err(config::client_error)?;
    presentation::write(
        args.output.json,
        "Token revocation",
        &result,
        "writing token revocation",
    )
}

pub(crate) async fn revoke_all(
    config_path: &str,
    args: TokenRevokeAllArgs,
) -> Result<(), AppError> {
    let mut connection = config::connect(config_path).await?;
    let expected_revision = stable_revision(&mut connection).await?;
    let result = connection
        .call::<TokenRevokeAllCall>(&TokenRevokeAllRequest {
            expected_revision,
            principal: args.principal,
        })
        .await
        .map_err(config::client_error)?;
    presentation::write(
        args.output.json,
        "Token revocation",
        &result,
        "writing bulk token revocation",
    )
}

async fn stable_revision(
    connection: &mut crate::management::connector::ManagerConnection,
) -> Result<tribal_wire::management::ConfigRevision, AppError> {
    config::stable_revision(
        connection
            .call::<ConfigGetAllCall>(&())
            .await
            .map_err(config::client_error)?,
    )
}

fn command_error(source: TokenCommandError) -> AppError {
    AppError::Management {
        source: Box::new(source),
    }
}
