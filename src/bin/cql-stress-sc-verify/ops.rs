//! Operations on the checked path: driver settings, cell encoders and outcome classes.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use scylla::client::execution_profile::ExecutionProfile;
use scylla::client::session::Session;
use scylla::policies::retry::FallthroughRetryPolicy;
use scylla::statement::prepared::PreparedStatement;
use scylla::statement::Consistency;
use scylla::value::CqlValue;

use crate::cli::CheckedConsistency;
use crate::profile::CellType;

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

/// Encodes write id `wid` into a cell of type `cell` (spec §7.2). `size` is the profile's
/// `text_size` or `blob_size` and is ignored for the integer types.
pub fn encode(cell: CellType, wid: u64, size: usize) -> Result<CqlValue> {
    Ok(match cell {
        CellType::Bigint => CqlValue::BigInt(
            i64::try_from(wid).with_context(|| format!("wid {wid} does not fit a bigint cell"))?,
        ),
        CellType::Int => CqlValue::Int(
            i32::try_from(wid).with_context(|| format!("wid {wid} does not fit an int cell"))?,
        ),
        CellType::Text => {
            let mut text = format!("{wid}:");
            text.extend(std::iter::repeat_n('.', size.saturating_sub(text.len())));
            CqlValue::Text(text)
        }
        CellType::Blob => {
            let mut blob = wid.to_le_bytes().to_vec();
            blob.resize(size.max(blob.len()), 0);
            CqlValue::Blob(blob)
        }
    })
}

/// The write id a cell holds. Strict: the value must be exactly what [`encode`] makes of that
/// wid, so a truncated or corrupted value is an error, not a wid (INV-0, spec §10.2).
pub fn decode(cell: CellType, size: usize, value: &CqlValue) -> Result<u64> {
    let wid = match (cell, value) {
        (CellType::Bigint, CqlValue::BigInt(v)) => u64::try_from(*v).unwrap_or(0),
        (CellType::Int, CqlValue::Int(v)) => u64::try_from(*v).unwrap_or(0),
        (CellType::Text, CqlValue::Text(text)) => text
            .split_once(':')
            .and_then(|(digits, _)| digits.parse().ok())
            .unwrap_or(0),
        (CellType::Blob, CqlValue::Blob(blob)) => blob
            .get(..8)
            .map(|bytes| u64::from_le_bytes(bytes.try_into().unwrap()))
            .unwrap_or(0),
        _ => anyhow::bail!("a {} cell holds {value:?}", cell.cql_name()),
    };
    // Write ids start at 1, so 0 is never a real one.
    anyhow::ensure!(
        wid != 0 && encode(cell, wid, size)? == *value,
        "{value:?} is not a {} cell written by this tool",
        cell.cql_name()
    );
    Ok(wid)
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

    #[test]
    fn cells_round_trip_test() {
        for cell in [
            CellType::Int,
            CellType::Bigint,
            CellType::Text,
            CellType::Blob,
        ] {
            for wid in [1, 2, 9, 10, 4096, i32::MAX as u64] {
                let value = encode(cell, wid, 32).unwrap();
                assert_eq!(decode(cell, 32, &value).unwrap(), wid, "{cell:?} {wid}");
            }
        }
        // Write ids count from 1 within a row; i64::MAX is the largest any cell type can hold.
        let big = i64::MAX as u64;
        for cell in [CellType::Bigint, CellType::Text, CellType::Blob] {
            assert_eq!(
                decode(cell, 32, &encode(cell, big, 32).unwrap()).unwrap(),
                big
            );
        }
    }

    #[test]
    fn cell_encodings_test() {
        assert_eq!(
            encode(CellType::Bigint, 7, 32).unwrap(),
            CqlValue::BigInt(7)
        );
        assert_eq!(encode(CellType::Int, 7, 32).unwrap(), CqlValue::Int(7));
        let CqlValue::Text(text) = encode(CellType::Text, 42, 32).unwrap() else {
            panic!()
        };
        assert_eq!(text.len(), 32);
        assert!(text.starts_with("42:"), "{text}");
        let CqlValue::Blob(blob) = encode(CellType::Blob, 0x0102, 64).unwrap() else {
            panic!()
        };
        assert_eq!(blob.len(), 64);
        assert_eq!(blob[..8], [0x02, 0x01, 0, 0, 0, 0, 0, 0]);
        assert!(
            encode(CellType::Int, 1 << 31, 32).is_err(),
            "int holds wid < 2^31 only"
        );
    }

    #[test]
    fn values_that_do_not_decode_test() {
        let good_text = encode(CellType::Text, 42, 32).unwrap();
        let CqlValue::Text(text) = good_text else {
            panic!()
        };
        let good_blob = encode(CellType::Blob, 42, 32).unwrap();
        let CqlValue::Blob(blob) = good_blob else {
            panic!()
        };
        for (cell, value) in [
            (CellType::Bigint, CqlValue::BigInt(0)),
            (CellType::Bigint, CqlValue::BigInt(-5)),
            (CellType::Int, CqlValue::Int(0)),
            (CellType::Int, CqlValue::BigInt(5)),
            (CellType::Text, CqlValue::Text(text[..31].to_owned())),
            (CellType::Text, CqlValue::Text(text.replace("42:", "42;"))),
            (CellType::Text, CqlValue::Text(format!("x{}", &text[1..]))),
            (CellType::Text, CqlValue::Text(format!("{}y", &text[..31]))),
            (CellType::Blob, CqlValue::Blob(blob[..31].to_vec())),
            (
                CellType::Blob,
                CqlValue::Blob([&blob[..31], &[0xff]].concat()),
            ),
            (CellType::Blob, CqlValue::Blob(vec![0; 32])),
        ] {
            assert!(
                decode(cell, 32, &value).is_err(),
                "{cell:?} {value:?} must not decode"
            );
        }
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
