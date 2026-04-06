//! Mock implementation of [`SystemFingerprintRepository`].

use tribal_db::{NewSystemFingerprint, SystemFingerprintRepository};
use tribal_domain::SystemFingerprint;

use super::mock_repository;

mock_repository! {
    MockSystemFingerprintRepository for SystemFingerprintRepository, tribal_db::DbError {
        upsert(NewSystemFingerprint => SystemFingerprint)
            (new: &NewSystemFingerprint) { new.clone() };
        find_by_hash(String => Option<SystemFingerprint>)
            (content_hash: &str) { content_hash.to_owned() }
    }
}
