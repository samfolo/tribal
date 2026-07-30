//! Postgres advisory-lock identifiers that coordinate cross-process operations.
//!
//! Each id is a fixed 8-byte ASCII tag (`"tribal"` plus a two-character suffix)
//! so the keys are stable and recognisable in `pg_locks`. They are co-located
//! here and checked distinct at compile time so a collision cannot slip in
//! across the crates that take them.

/// Serialises migration runs (`"tribalmi"`).
pub const MIGRATION: i64 = 0x7472_6962_616C_6D69;

/// Serialises first-boot provisioning (`"tribalpv"`).
pub const PROVISIONING: i64 = 0x7472_6962_616C_7076;

/// Serialises reindex run creation, enforcing single-flight per table
/// (`"tribalrx"`).
pub const REINDEX_SINGLE_FLIGHT: i64 = 0x7472_6962_616C_7278;

/// Guards the reindex cutover: ingest commits hold the shared form, and the
/// cutover takes it exclusively to drain them (`"tribalco"`).
pub const CUTOVER: i64 = 0x7472_6962_616C_636F;

/// Serialises mutations and reads of the active embedding profile (`"tribalep"`).
pub const EMBEDDING_PROFILE_AUTHORITY: i64 = 0x7472_6962_616C_6570;

/// Domain-separates namespace-scoped default-credential replacement locks.
pub const CREDENTIAL_REPLACEMENT: i64 = 0x7472_6962_616C_6372;

/// Serialises one storage transition against graph-mutating admission: held
/// exclusively on the source by the manager driving a switch, in shared form
/// by every operation class that creates durable live graph work
/// (`"tribalgt"`).
pub const GRAPH_TRANSITION: i64 = 0x7472_6962_616C_6774;

/// Reserves a switch's candidate database: held exclusively on the candidate
/// from final assessment through source quiescence or abort, in shared form by
/// the migration runner for its schema work (`"tribaltr"`).
pub const TARGET_MIGRATION_RESERVATION: i64 = 0x7472_6962_616C_7472;

/// Every advisory-lock id, the input to the compile-time uniqueness check.
pub const ALL: [i64; 8] = [
    MIGRATION,
    PROVISIONING,
    REINDEX_SINGLE_FLIGHT,
    CUTOVER,
    EMBEDDING_PROFILE_AUTHORITY,
    CREDENTIAL_REPLACEMENT,
    GRAPH_TRANSITION,
    TARGET_MIGRATION_RESERVATION,
];

const fn all_distinct(ids: &[i64]) -> bool {
    let mut i = 0;
    while i < ids.len() {
        let mut j = i + 1;
        while j < ids.len() {
            if ids[i] == ids[j] {
                return false;
            }
            j += 1;
        }
        i += 1;
    }
    true
}

const _: () = assert!(all_distinct(&ALL), "advisory lock ids must be distinct");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_all_advisory_lock_ids_are_declared_once() {
        assert_eq!(ALL.len(), 8);
        assert!(ALL.contains(&EMBEDDING_PROFILE_AUTHORITY));
        assert!(ALL.contains(&CREDENTIAL_REPLACEMENT));
        assert!(ALL.contains(&GRAPH_TRANSITION));
        assert!(ALL.contains(&TARGET_MIGRATION_RESERVATION));
    }
}
