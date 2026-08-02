//! Database, project, and token administration DTOs.

use std::{fmt, path::Path};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tribal_domain::{AuthTokenId, GitRemote, ProjectId, ProjectOrigin, Scope, StorageTransitionId};

use super::{ConfigRevision, RuntimeIdentity, SecretLiteral};

/// A database result tied to the configuration revision it observed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Revisioned<T> {
    pub config_revision: ConfigRevision,
    pub value: T,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct DatabaseInitialiseRequest {
    pub expected_revision: ConfigRevision,
    /// Which database to initialise; the configured target by default.
    #[serde(default)]
    pub target: DatabaseAdministrationTarget,
}

/// The database an administration request applies to.
#[derive(Default, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "target", content = "data", rename_all = "snake_case")]
pub enum DatabaseAdministrationTarget {
    /// The database the configuration names.
    #[default]
    Configured,
    /// A candidate supplied for this request alone; the secret is neither
    /// persisted nor echoed.
    Candidate { url: SecretLiteral },
}

impl fmt::Debug for DatabaseAdministrationTarget {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Configured => formatter.write_str("Configured"),
            Self::Candidate { .. } => formatter.write_str("Candidate { url: <redacted> }"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "outcome", content = "data", rename_all = "snake_case")]
pub enum DatabaseInitialiseOutcome {
    Initialised,
    AlreadyInitialised,
    /// A storage transition holds the database; nothing was begun. The id is
    /// present when this manager's own pending transition refused the work,
    /// and absent when the refusal came from another process's admission
    /// lock, which exposes no transition identity.
    GraphTransitionInProgress {
        transition_id: Option<StorageTransitionId>,
    },
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

/// Creates one named database on an explicitly supplied administration
/// target. Management-only — never an MCP tool — and independent of the
/// configured database: the connection opens from the request's URL alone.
/// The secret lives only for the request.
#[derive(PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct DatabaseProvisionRequest {
    pub expected_revision: ConfigRevision,
    pub administrative_url: SecretLiteral,
    pub database: DatabaseName,
}

impl fmt::Debug for DatabaseProvisionRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DatabaseProvisionRequest")
            .field("expected_revision", &self.expected_revision)
            .field("administrative_url", &"<redacted>")
            .field("database", &self.database)
            .finish()
    }
}

/// What provisioning found. An earlier run and a lost creation race are
/// deliberately indistinguishable: either way the database exists, and a
/// fresh inspection remains the authority for its state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "outcome", content = "data", rename_all = "snake_case")]
pub enum DatabaseProvisionOutcome {
    Created,
    AlreadyPresent,
}

/// Concrete schema identity for a revisioned database-provision receipt.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DatabaseProvisionResult(Revisioned<DatabaseProvisionOutcome>);

#[cfg(feature = "schema")]
impl schemars::JsonSchema for DatabaseProvisionResult {
    fn schema_name() -> String {
        "DatabaseProvisionResult".to_owned()
    }

    fn json_schema(generator: &mut schemars::r#gen::SchemaGenerator) -> schemars::schema::Schema {
        <Revisioned<DatabaseProvisionOutcome> as schemars::JsonSchema>::json_schema(generator)
    }
}

impl From<Revisioned<DatabaseProvisionOutcome>> for DatabaseProvisionResult {
    fn from(value: Revisioned<DatabaseProvisionOutcome>) -> Self {
        Self(value)
    }
}

impl std::ops::Deref for DatabaseProvisionResult {
    type Target = Revisioned<DatabaseProvisionOutcome>;

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

/// Maintenance and template databases provisioning must never target.
const RESERVED_DATABASE_NAMES: [&str; 3] = ["postgres", "template0", "template1"];

/// A database name the management plane may create: a lowercase ASCII
/// identifier of at most 63 bytes, never a reserved maintenance or template
/// database. The 63-byte ceiling is the server's own identifier limit, so a
/// valid name is never silently truncated server-side.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct DatabaseName(String);

#[cfg(feature = "schema")]
impl schemars::JsonSchema for DatabaseName {
    fn schema_name() -> String {
        "DatabaseName".to_owned()
    }

    fn json_schema(_generator: &mut schemars::r#gen::SchemaGenerator) -> schemars::schema::Schema {
        super::wire_id::marked_string_schema(
            Some(r"^[a-z][a-z0-9_]{0,62}$"),
            "validated-string",
            None,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DatabaseNameError {
    #[error("database name is empty")]
    Empty,
    #[error("database name exceeds 63 bytes")]
    TooLong,
    #[error("database name must be lowercase ASCII, starting with a letter")]
    InvalidCharacters,
    #[error("database name is reserved")]
    Reserved,
}

impl TryFrom<String> for DatabaseName {
    type Error = DatabaseNameError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.is_empty() {
            return Err(DatabaseNameError::Empty);
        }
        if value.len() > 63 {
            return Err(DatabaseNameError::TooLong);
        }
        let mut characters = value.chars();
        let leads_with_letter = characters.next().is_some_and(|c| c.is_ascii_lowercase());
        let rest_is_identifier =
            characters.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
        if !leads_with_letter || !rest_is_identifier {
            return Err(DatabaseNameError::InvalidCharacters);
        }
        if RESERVED_DATABASE_NAMES.contains(&value.as_str()) {
            return Err(DatabaseNameError::Reserved);
        }
        Ok(Self(value))
    }
}

impl From<DatabaseName> for String {
    fn from(value: DatabaseName) -> Self {
        value.0
    }
}

impl fmt::Display for DatabaseName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl DatabaseName {
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

    #[test]
    fn test_administration_target_debug_is_redacted() {
        let target = DatabaseAdministrationTarget::Candidate {
            url: SecretLiteral::try_from(
                "postgres://user:hunter2@db.internal:5432/tribal".to_owned(),
            )
            .expect("a valid secret"),
        };

        let rendered = format!("{target:?}");
        assert!(
            !rendered.contains("hunter2"),
            "no credential in Debug: {rendered}"
        );
        assert!(
            !rendered.contains("db.internal"),
            "no host in Debug: {rendered}"
        );
        assert!(rendered.contains("<redacted>"));
    }

    #[test]
    fn test_database_name_admits_a_plain_identifier() {
        let name = DatabaseName::try_from("tribal".to_owned()).expect("valid name");
        assert_eq!(name.as_str(), "tribal");
    }

    #[test]
    fn test_database_name_admits_the_63_byte_boundary_and_refuses_64() {
        let at_limit = format!("a{}", "b".repeat(62));
        assert!(DatabaseName::try_from(at_limit).is_ok());
        let over = format!("a{}", "b".repeat(63));
        assert_eq!(
            DatabaseName::try_from(over),
            Err(DatabaseNameError::TooLong)
        );
    }

    #[test]
    fn test_database_name_refuses_bad_shapes() {
        for (candidate, expected) in [
            ("", DatabaseNameError::Empty),
            ("1tribal", DatabaseNameError::InvalidCharacters),
            ("_tribal", DatabaseNameError::InvalidCharacters),
            ("Tribal", DatabaseNameError::InvalidCharacters),
            ("tri-bal", DatabaseNameError::InvalidCharacters),
            ("tri bal", DatabaseNameError::InvalidCharacters),
            ("tribal;drop", DatabaseNameError::InvalidCharacters),
            ("tribál", DatabaseNameError::InvalidCharacters),
        ] {
            assert_eq!(
                DatabaseName::try_from(candidate.to_owned()),
                Err(expected.clone()),
                "candidate {candidate:?}"
            );
        }
    }

    #[test]
    fn test_database_name_refuses_every_reserved_database() {
        for reserved in ["postgres", "template0", "template1"] {
            assert_eq!(
                DatabaseName::try_from(reserved.to_owned()),
                Err(DatabaseNameError::Reserved),
                "reserved {reserved:?}"
            );
        }
    }

    #[test]
    fn test_provision_request_debug_redacts_the_administrative_url() {
        let request = DatabaseProvisionRequest {
            expected_revision: ConfigRevision::from_digest(
                &super::super::ConfigDigest::from_bytes(b"config"),
            ),
            administrative_url: SecretLiteral::try_from(
                "postgresql://owner:secret@%2Ftmp%2Fsock/postgres".to_owned(),
            )
            .expect("valid secret"),
            database: DatabaseName::try_from("tribal".to_owned()).expect("valid name"),
        };
        let rendered = format!("{request:?}");
        assert!(
            !rendered.contains("secret"),
            "no secret in Debug: {rendered}"
        );
        assert!(!rendered.contains("sock"), "no route in Debug: {rendered}");
        assert!(rendered.contains("<redacted>"));
        assert!(rendered.contains("tribal"));
    }
}
