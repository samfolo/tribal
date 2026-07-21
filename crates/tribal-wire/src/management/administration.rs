//! Database, project, and token administration DTOs.

use std::{fmt, path::Path};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tribal_domain::{AuthTokenId, GitRemote, ProjectId, ProjectOrigin, Scope};

use super::{ConfigRevision, RuntimeIdentity};

/// A database result tied to the configuration revision it observed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Revisioned<T> {
    pub config_revision: ConfigRevision,
    pub value: T,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct DatabaseInitialiseRequest {
    pub expected_revision: ConfigRevision,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "outcome", content = "data", rename_all = "snake_case")]
pub enum DatabaseInitialiseOutcome {
    Initialised,
    AlreadyInitialised,
}

/// Concrete schema identity for a revisioned database-initialisation receipt.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DatabaseInitialiseResult(Revisioned<DatabaseInitialiseOutcome>);

#[cfg(feature = "schema")]
impl schemars::JsonSchema for DatabaseInitialiseResult {
    fn schema_name() -> String {
        "DatabaseInitialiseResult".to_owned()
    }

    fn json_schema(generator: &mut schemars::r#gen::SchemaGenerator) -> schemars::schema::Schema {
        <Revisioned<DatabaseInitialiseOutcome> as schemars::JsonSchema>::json_schema(generator)
    }
}

impl From<Revisioned<DatabaseInitialiseOutcome>> for DatabaseInitialiseResult {
    fn from(value: Revisioned<DatabaseInitialiseOutcome>) -> Self {
        Self(value)
    }
}

impl std::ops::Deref for DatabaseInitialiseResult {
    type Target = Revisioned<DatabaseInitialiseOutcome>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "source", content = "data", rename_all = "snake_case")]
pub enum ProjectRegistrationSource {
    WorkingTree {
        directory: AbsoluteDirectoryPath,
        default_branch: Option<String>,
    },
    GitRemote {
        remote: GitRemote,
        default_branch: Option<String>,
    },
}

/// An absolute path whose filesystem meaning remains manager-owned.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct AbsoluteDirectoryPath(String);

#[cfg(feature = "schema")]
impl schemars::JsonSchema for AbsoluteDirectoryPath {
    fn schema_name() -> String {
        "AbsoluteDirectoryPath".to_owned()
    }

    fn json_schema(_generator: &mut schemars::r#gen::SchemaGenerator) -> schemars::schema::Schema {
        super::wire_id::marked_string_schema(Some(r"^/.*$"), "validated-string", None)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AbsoluteDirectoryPathError {
    #[error("directory path is empty")]
    Empty,
    #[error("directory path is not absolute")]
    Relative,
}

impl TryFrom<String> for AbsoluteDirectoryPath {
    type Error = AbsoluteDirectoryPathError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.is_empty() {
            return Err(AbsoluteDirectoryPathError::Empty);
        }
        if !Path::new(&value).is_absolute() {
            return Err(AbsoluteDirectoryPathError::Relative);
        }
        Ok(Self(value))
    }
}

impl From<AbsoluteDirectoryPath> for String {
    fn from(value: AbsoluteDirectoryPath) -> Self {
        value.0
    }
}

impl AbsoluteDirectoryPath {
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ProjectRegisterInput {
    pub source: ProjectRegistrationSource,
    pub name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ProjectRegisterRequest {
    pub expected_revision: ConfigRevision,
    pub project: ProjectRegisterInput,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "outcome", content = "data", rename_all = "snake_case")]
pub enum ProjectRegisterOutcome {
    Registered { project: ProjectSummary },
    AlreadyRegistered { project: ProjectSummary },
}

pub type ProjectRegisterResult = Revisioned<ProjectRegisterOutcome>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ProjectSummary {
    pub id: ProjectId,
    pub origin: ProjectOrigin,
    pub name: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Requested inventory page size.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "u16", into = "u16")]
pub struct PageSize(u16);

#[cfg(feature = "schema")]
impl schemars::JsonSchema for PageSize {
    fn schema_name() -> String {
        "PageSize".to_owned()
    }

    fn json_schema(generator: &mut schemars::r#gen::SchemaGenerator) -> schemars::schema::Schema {
        let mut schema = generator.subschema_for::<u16>();
        if let schemars::schema::Schema::Object(object) = &mut schema {
            let number = object.number.get_or_insert_with(Default::default);
            number.minimum = Some(1.0);
            number.maximum = Some(f64::from(Self::MAX));
        }
        schema
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("page size must be between 1 and {max}", max = PageSize::MAX)]
pub struct PageSizeError;

impl PageSize {
    pub const MAX: u16 = 50;

    #[must_use]
    pub const fn get(self) -> u16 {
        self.0
    }
}

impl TryFrom<u16> for PageSize {
    type Error = PageSizeError;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        if (1..=Self::MAX).contains(&value) {
            Ok(Self(value))
        } else {
            Err(PageSizeError)
        }
    }
}

impl From<PageSize> for u16 {
    fn from(value: PageSize) -> Self {
        value.0
    }
}

/// Opaque continuation token issued by an inventory method.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct PageCursor(String);

#[cfg(feature = "schema")]
impl schemars::JsonSchema for PageCursor {
    fn schema_name() -> String {
        "PageCursor".to_owned()
    }

    fn json_schema(_generator: &mut schemars::r#gen::SchemaGenerator) -> schemars::schema::Schema {
        super::wire_id::marked_string_schema(Some(r"^.+$"), "validated-string", None)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("page cursor is empty")]
pub struct PageCursorError;

impl TryFrom<String> for PageCursor {
    type Error = PageCursorError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.is_empty() {
            Err(PageCursorError)
        } else {
            Ok(Self(value))
        }
    }
}

impl From<PageCursor> for String {
    fn from(value: PageCursor) -> Self {
        value.0
    }
}

impl PageCursor {
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct PageRequest {
    pub size: PageSize,
    pub after: Option<PageCursor>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ProjectListRequest {
    pub page: PageRequest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ProjectPage {
    pub items: Vec<ProjectSummary>,
    pub next: Option<PageCursor>,
}

pub type ProjectList = Revisioned<ProjectPage>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "state", content = "data", rename_all = "snake_case")]
pub enum TokenState {
    Active,
    Expired,
    Revoked { revoked_at: DateTime<Utc> },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct TokenSummary {
    pub id: AuthTokenId,
    pub principal: String,
    pub scopes: Vec<Scope>,
    pub audience: String,
    pub state: TokenState,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct TokenListRequest {
    pub page: PageRequest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct TokenPage {
    pub items: Vec<TokenSummary>,
    pub next: Option<PageCursor>,
}

pub type TokenInventory = Revisioned<TokenPage>;

/// A token issuance request. With `expected_runtime` set, issuance
/// targets that exact attached runtime and its audience; omitted, the
/// call retains audience-from-configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct TokenCreateRequest {
    pub expected_revision: ConfigRevision,
    pub principal: Option<String>,
    pub ttl_hours: Option<u64>,
    pub scopes: Vec<Scope>,
    pub persist_as_default: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_runtime: Option<RuntimeIdentity>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "outcome", content = "data", rename_all = "snake_case")]
pub enum CredentialPersistenceResult {
    NotRequested,
    Persisted,
}

/// Bearer material returned once by an explicit issuance action.
#[derive(PartialEq, zeroize::Zeroize, zeroize::ZeroizeOnDrop, Serialize, Deserialize)]
#[serde(transparent)]
pub struct IssuedBearerToken(String);

impl IssuedBearerToken {
    #[must_use]
    pub fn new(value: String) -> Self {
        Self(value)
    }

    #[must_use]
    pub fn expose_secret(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for IssuedBearerToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("<redacted issued bearer token>")
    }
}

impl fmt::Display for IssuedBearerToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("<redacted>")
    }
}

#[cfg(feature = "schema")]
impl schemars::JsonSchema for IssuedBearerToken {
    fn schema_name() -> String {
        "IssuedBearerToken".to_owned()
    }

    fn json_schema(_generator: &mut schemars::r#gen::SchemaGenerator) -> schemars::schema::Schema {
        super::wire_id::marked_string_schema(None, "scoped-redacted-string", None)
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct TokenCreateOutcome {
    pub token: IssuedBearerToken,
    pub summary: TokenSummary,
    pub credential: CredentialPersistenceResult,
}

pub type TokenCreateResult = Revisioned<TokenCreateOutcome>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct TokenRevokeRequest {
    pub expected_revision: ConfigRevision,
    pub id: AuthTokenId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "outcome", content = "data", rename_all = "snake_case")]
pub enum TokenRevokeOutcome {
    Revoked { token: TokenSummary },
    AlreadyRevoked { token: TokenSummary },
    NotFound { id: AuthTokenId },
}

pub type TokenRevokeResult = Revisioned<TokenRevokeOutcome>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct TokenRevokeAllRequest {
    pub expected_revision: ConfigRevision,
    pub principal: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct TokenRevokeAllOutcome {
    pub revoked: u64,
}

pub type TokenRevokeAllResult = Revisioned<TokenRevokeAllOutcome>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_invalid_boundary_values_are_rejected_during_deserialisation() {
        assert!(serde_json::from_str::<AbsoluteDirectoryPath>(r#""relative""#).is_err());
        assert!(serde_json::from_str::<PageSize>("0").is_err());
        assert!(serde_json::from_str::<PageSize>("51").is_err());
        assert!(serde_json::from_str::<PageCursor>(r#""""#).is_err());
    }

    #[test]
    fn test_issued_bearer_projections_are_redacted() {
        let token = IssuedBearerToken::new("sentinel-secret".to_owned());
        assert!(!format!("{token:?}").contains("sentinel"));
        assert!(!token.to_string().contains("sentinel"));
    }
}
