//! Implementation of the `tribal setup` subcommand.
//!
//! Bootstraps a fresh Tribal installation: creates the config directory,
//! writes default prompt files, connects to the database, runs migrations,
//! creates the `principal:local` identity, generates an initial bearer
//! token, and writes a minimal config file.

use std::path::Path;

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{TimeDelta, Utc};
use rand::RngExt;
use tribal_common::sha256_hex;
use tribal_config::{ConfigError, TribalConfig, load_config};
use tribal_db::{
    AuthTokenRepository, DbError, NewAuthToken, NewPrincipal, PgAuthTokenRepository,
    PgPrincipalRepository, PrincipalRepository,
};

use crate::{
    cli::SetupArgs,
    error::AppError,
    startup::{ensure_prompt_files, run_migrations},
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Principal key for the local stdio transport identity.
const LOCAL_PRINCIPAL_KEY: &str = "principal:local";

/// Default database URL used when no other source provides one.
///
/// Injected as a command-defaults-layer value so that the figment cascade
/// still respects YAML, env vars, and CLI overrides.
const DEFAULT_DATABASE_URL: &str = "postgresql://tribal@localhost:5432/tribal";

/// Number of cryptographically random bytes for token generation.
const TOKEN_BYTE_LENGTH: usize = 32;

/// Pool name for the single setup connection.
const POOL_NAME_SETUP: &str = "setup";

/// Maximum connections for the setup pool.
const SETUP_POOL_MAX_CONNECTIONS: u32 = 1;

/// Statement timeout for setup operations (migrations + inserts).
const SETUP_STATEMENT_TIMEOUT_MS: u64 = 60_000;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Runs the `tribal setup` bootstrap sequence and exits.
///
/// # Errors
///
/// Returns an [`AppError`] if any phase of the setup fails.
pub(crate) fn run(config_path: &str, args: SetupArgs) -> Result<(), AppError> {
    let cli_overrides = args.into_cli_overrides();
    let command_defaults = [("database.url", DEFAULT_DATABASE_URL)];

    let config = load_config(config_path, Some(cli_overrides), Some(&command_defaults))?;

    if config.auth.token_ttl_hours == 0 {
        return Err(AppError::Config {
            source: ConfigError::ValidationFailed {
                errors: vec!["auth.token_ttl_hours must be greater than zero".into()],
            },
        });
    }

    let expanded_config_path = shellexpand::tilde(config_path).into_owned();

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|source| AppError::Runtime { source })?;

    rt.block_on(run_async(&config, &expanded_config_path))
}

// ---------------------------------------------------------------------------
// Async flow
// ---------------------------------------------------------------------------

/// Executes the setup steps asynchronously.
///
/// # Panics
///
/// Panics if `auth.token_ttl_hours` exceeds `i64::MAX` or overflows
/// `TimeDelta::try_hours`.
async fn run_async(config: &TribalConfig, config_path: &str) -> Result<(), AppError> {
    // 1. Create config directory
    let config_dir = Path::new(config_path)
        .parent()
        .unwrap_or_else(|| Path::new("."));
    tokio::fs::create_dir_all(config_dir)
        .await
        .map_err(|source| AppError::SetupIo {
            context: format!("create config directory {}", config_dir.display()),
            source,
        })?;
    eprintln!("  config directory: {}", config_dir.display());

    // 2. Write default prompt files
    let prompts_dir = Path::new(&config.prompts.directory);
    ensure_prompt_files(prompts_dir).await?;
    eprintln!("  prompt files: {}", prompts_dir.display());

    // 3. Connect to database (single attempt, no retry)
    let pool = tribal_db::create_pool(
        &config.database,
        POOL_NAME_SETUP,
        SETUP_POOL_MAX_CONNECTIONS,
        SETUP_STATEMENT_TIMEOUT_MS,
    )
    .await
    .map_err(|source| AppError::Database { source })?;
    eprintln!("  database: connected");

    // 4. Run migrations
    run_migrations(&pool).await?;
    eprintln!("  migrations: complete");

    // 5. Acquire a connection for repository calls
    let mut conn = pool
        .acquire()
        .await
        .map_err(|err| AppError::pool_acquire(POOL_NAME_SETUP, "acquiring setup connection", err))?;

    // 6. Find or create principal:local
    let principal = find_or_create_principal(&mut conn).await?;
    eprintln!("  principal: {}", principal.principal_key());

    // 7. Generate token
    let raw_token = generate_raw_token();

    // 8. Hash and store
    let token_hash = sha256_hex(&raw_token);
    let ttl_hours =
        i64::try_from(config.auth.token_ttl_hours).expect("auth.token_ttl_hours exceeds i64");
    let expires_at =
        Utc::now() + TimeDelta::try_hours(ttl_hours).expect("token TTL overflows TimeDelta");

    let new_token = NewAuthToken::builder()
        .token_hash(token_hash)
        .principal_id(principal.id())
        .expires_at(expires_at)
        .build();

    PgAuthTokenRepository
        .insert(&mut *conn, &new_token)
        .await
        .map_err(|source| AppError::Database { source })?;
    eprintln!(
        "  token: created (expires {})",
        expires_at.format("%Y-%m-%d")
    );

    drop(conn);

    // 9. Write config file (skip if already exists)
    write_config_file_if_absent(config_path, config).await?;

    // 10. Display token and next-step instructions
    display_instructions(&raw_token);

    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Finds the `principal:local` principal or creates it if absent.
///
/// Handles the TOCTOU race from concurrent setup processes by catching
/// `UniqueViolation` on insert and falling back to a second lookup.
async fn find_or_create_principal(
    conn: &mut sqlx::pool::PoolConnection<sqlx::Postgres>,
) -> Result<tribal_domain::Principal, AppError> {
    if let Some(existing) = PgPrincipalRepository
        .find_by_key(&mut **conn, LOCAL_PRINCIPAL_KEY)
        .await
        .map_err(|source| AppError::Database { source })?
    {
        return Ok(existing);
    }

    let new = NewPrincipal::builder()
        .principal_key(LOCAL_PRINCIPAL_KEY.to_owned())
        .build();

    match PgPrincipalRepository.insert(&mut **conn, &new).await {
        Ok(principal) => Ok(principal),
        Err(DbError::UniqueViolation { .. }) => {
            // Concurrent setup race — the other process won; fetch their row.
            PgPrincipalRepository
                .find_by_key(&mut **conn, LOCAL_PRINCIPAL_KEY)
                .await
                .map_err(|source| AppError::Database { source })?
                .ok_or_else(|| AppError::Database {
                    source: DbError::NotFound {
                        entity: "principal".into(),
                        id: LOCAL_PRINCIPAL_KEY.into(),
                    },
                })
        }
        Err(source) => Err(AppError::Database { source }),
    }
}

/// Generates a cryptographically random bearer token.
///
/// Returns a base64url-encoded string (no padding) of 32 random bytes,
/// producing exactly 43 characters.
fn generate_raw_token() -> String {
    let mut bytes = [0u8; TOKEN_BYTE_LENGTH];
    rand::rng().fill(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
}

/// Renders the minimal config file content containing only the database URL.
fn render_minimal_config(database_url: &str) -> String {
    format!(
        "# Tribal configuration\n\
         # See documentation for full options.\n\
         \n\
         database:\n\
         {}\n",
        format_database_url_line(database_url),
    )
}

/// Formats a single YAML line for the database URL.
fn format_database_url_line(url: &str) -> String {
    format!("  url: \"{url}\"")
}

/// Writes the minimal config file if it does not already exist.
///
/// When the file exists, checks whether the resolved database URL diverges
/// from the file's `database.url` value and emits a warning if so.
async fn write_config_file_if_absent(
    config_path: &str,
    config: &TribalConfig,
) -> Result<(), AppError> {
    let path = Path::new(config_path);

    if tokio::fs::try_exists(path).await.unwrap_or(false) {
        check_config_divergence(config_path, config).await;
        eprintln!("  config file: already exists, skipping");
        return Ok(());
    }

    let content = render_minimal_config(&config.database.url);

    tokio::fs::write(path, content)
        .await
        .map_err(|source| AppError::SetupIo {
            context: format!("write config file {}", path.display()),
            source,
        })?;

    eprintln!("  config file: written to {config_path}");
    Ok(())
}

/// Compares the resolved database URL against the existing config file's
/// `database.url` value and warns if they differ.
///
/// Never prints either URL — database URLs may contain credentials.
async fn check_config_divergence(config_path: &str, config: &TribalConfig) {
    let Ok(content) = tokio::fs::read_to_string(config_path).await else {
        return;
    };

    let existing: serde_yaml::Value = match serde_yaml::from_str(&content) {
        Ok(value) => value,
        Err(_) => {
            eprintln!("  warning: existing config file could not be parsed, skipping URL comparison");
            return;
        }
    };

    let Some(file_url) = existing
        .get("database")
        .and_then(|d| d.get("url"))
        .and_then(|u| u.as_str())
    else {
        return;
    };

    if file_url != config.database.url {
        eprintln!(
            "  warning: resolved database URL differs from the config file's database.url; \
             the config file was not updated"
        );
    }
}

/// Prints the bearer token and next-step instructions.
fn display_instructions(raw_token: &str) {
    eprintln!();
    eprintln!("Setup complete. Your bearer token:");
    eprintln!();
    eprintln!("  {raw_token}");
    eprintln!();
    eprintln!("Save this token — it will not be shown again.");
    eprintln!();
    eprintln!("Next steps:");
    eprintln!("  1. Export the token:  export TRIBAL_AUTH_TOKEN=\"{raw_token}\"");
    eprintln!("  2. Register a project:  tribal project register");
    eprintln!();
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- Token generation ----------------------------------------------------

    #[test]
    fn test_generate_raw_token_length() {
        let token = generate_raw_token();
        assert_eq!(
            token.len(),
            43,
            "32 bytes base64url-encoded (no padding) should be 43 chars"
        );
    }

    #[test]
    fn test_generate_raw_token_uniqueness() {
        let a = generate_raw_token();
        let b = generate_raw_token();
        assert_ne!(a, b, "two generated tokens should differ");
    }

    #[test]
    fn test_generate_raw_token_is_valid_base64url() {
        let token = generate_raw_token();
        let decoded = URL_SAFE_NO_PAD
            .decode(&token)
            .expect("token should be valid base64url");
        assert_eq!(decoded.len(), TOKEN_BYTE_LENGTH);
    }

    // -- Token hashing -------------------------------------------------------

    #[test]
    fn test_token_hash_is_deterministic() {
        let token = "test-token-value";
        let hash_a = sha256_hex(token);
        let hash_b = sha256_hex(token);
        assert_eq!(hash_a, hash_b);
    }

    #[test]
    fn test_token_hash_is_lowercase_hex_64_chars() {
        let token = generate_raw_token();
        let hash = sha256_hex(&token);
        assert_eq!(hash.len(), 64, "SHA-256 hex digest should be 64 characters");
        assert!(
            hash.chars().all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()),
            "hash should be lowercase hex: {hash}",
        );
    }

    // -- Expiry computation --------------------------------------------------

    #[test]
    fn test_compute_expiry_is_in_future() {
        let before = Utc::now();
        let ttl_hours: i64 = 8760;
        let expires_at =
            Utc::now() + TimeDelta::try_hours(ttl_hours).expect("hours should not overflow");
        let expected_min = before + TimeDelta::try_hours(ttl_hours).unwrap();
        assert!(expires_at >= expected_min);
    }

    // -- Config file rendering -----------------------------------------------

    #[test]
    fn test_minimal_config_content() {
        let content = render_minimal_config("postgres://host/db");
        assert!(content.contains("database:"), "should contain database section");
        assert!(
            content.contains("url: \"postgres://host/db\""),
            "should contain the URL: {content}",
        );
    }

    #[test]
    fn test_format_database_url_line() {
        let line = format_database_url_line("postgres://h/db");
        assert_eq!(line, "  url: \"postgres://h/db\"");
    }

    // -- Config file write ---------------------------------------------------

    #[tokio::test]
    async fn test_write_config_file_creates_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tribal.yaml");
        let path_str = path.to_str().unwrap();

        let config = TribalConfig::default();
        write_config_file_if_absent(path_str, &config)
            .await
            .unwrap();

        let content = tokio::fs::read_to_string(&path).await.unwrap();
        assert!(content.contains("database:"));
    }

    #[tokio::test]
    async fn test_write_config_file_skips_existing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tribal.yaml");
        let path_str = path.to_str().unwrap();

        let original = "# my custom config\n";
        tokio::fs::write(&path, original).await.unwrap();

        let config = TribalConfig::default();
        write_config_file_if_absent(path_str, &config)
            .await
            .unwrap();

        let content = tokio::fs::read_to_string(&path).await.unwrap();
        assert_eq!(content, original, "existing file should not be overwritten");
    }

    // -- Config divergence ---------------------------------------------------

    #[tokio::test]
    async fn test_check_config_divergence_handles_malformed_yaml() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tribal.yaml");
        let path_str = path.to_str().unwrap();

        tokio::fs::write(&path, "{{{{not valid yaml")
            .await
            .unwrap();

        let config = TribalConfig::default();
        // Should not panic or error — just silently skip comparison
        check_config_divergence(path_str, &config).await;
    }
}
