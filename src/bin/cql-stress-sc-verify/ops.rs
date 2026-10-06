//! Operations on the checked path: driver settings, cell encoders and outcome classes.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use scylla::client::execution_profile::ExecutionProfile;
use scylla::client::session::Session;
use scylla::policies::retry::FallthroughRetryPolicy;
use scylla::statement::prepared::PreparedStatement;
use scylla::statement::Consistency;

use crate::cli::CheckedConsistency;

/// The execution profile of the checked session (spec §8.3).
///
/// A strongly consistent write carries no deduplication token, so a write sent twice is
/// applied twice (spec F8), and one recorded write would become two applied ones. Nothing on
/// this path may resend a request: the retry policy never retries and there is no
/// speculative execution. The driver itself only moves to the next node without asking the
/// policy when it cannot get a connection, before anything was sent.
pub fn checked_profile(
    consistency: CheckedConsistency,
    request_timeout: Duration,
) -> ExecutionProfile {
    ExecutionProfile::builder()
        .consistency(match consistency {
            CheckedConsistency::Quorum => Consistency::Quorum,
            CheckedConsistency::LocalQuorum => Consistency::LocalQuorum,
        })
        .request_timeout(Some(request_timeout))
        .retry_policy(Arc::new(FallthroughRetryPolicy::new()))
        .speculative_execution_policy(None)
        .build()
}

/// Prepares a checked statement. It is marked not idempotent, so no policy may ever treat it
/// as safe to resend.
pub async fn prepare_checked(session: &Session, query: &str) -> Result<PreparedStatement> {
    let mut statement = session
        .prepare(query)
        .await
        .with_context(|| format!("Failed to prepare {query}"))?;
    statement.set_is_idempotent(false);
    Ok(statement)
}

#[cfg(test)]
mod tests {
    use super::*;
    use scylla::client::session_builder::SessionBuilder;
    use scylla::policies::retry::FallthroughRetryPolicy;
    use std::time::Duration;

    fn assert_never_retries(profile: &ExecutionProfile) {
        let retry = profile.get_retry_policy();
        assert!(
            retry
                .as_any()
                .and_then(|any| any.downcast_ref::<FallthroughRetryPolicy>())
                .is_some(),
            "the checked path must never retry: {retry:?}"
        );
        assert!(profile.get_speculative_execution_policy().is_none());
    }

    #[test]
    fn checked_profile_never_retries_test() {
        let profile = checked_profile(CheckedConsistency::Quorum, Duration::from_secs(5));
        assert_never_retries(&profile);
        assert_eq!(profile.get_consistency(), Consistency::Quorum);
        assert_eq!(profile.get_request_timeout(), Some(Duration::from_secs(5)));

        let profile = checked_profile(CheckedConsistency::LocalQuorum, Duration::from_millis(250));
        assert_eq!(profile.get_consistency(), Consistency::LocalQuorum);
        assert_eq!(
            profile.get_request_timeout(),
            Some(Duration::from_millis(250))
        );
    }

    /// The session's statements really get the profile, and are not idempotent.
    #[tokio::test]
    async fn checked_session_and_statements_test() {
        let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_owned());
        let profile = checked_profile(CheckedConsistency::Quorum, Duration::from_secs(5));
        let session = SessionBuilder::new()
            .known_node(uri)
            .default_execution_profile_handle(profile.into_handle())
            .build()
            .await
            .unwrap();
        assert_never_retries(&session.get_default_execution_profile_handle().to_profile());

        let statement = prepare_checked(&session, "SELECT key FROM system.local")
            .await
            .unwrap();
        assert!(!statement.get_is_idempotent());
        assert!(
            statement.get_execution_profile_handle().is_none(),
            "uses the session's profile"
        );
    }
}
