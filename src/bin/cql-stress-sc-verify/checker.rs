//! The full check: porcupine_checker run as a child process on each check file (spec §11.3).

use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::process::Command;

/// How long the start-up probe waits for the checker.
pub const PROBE_TIMEOUT: Duration = Duration::from_secs(10);

/// Start-up: the checker must run, and refuse empty input with exit 2 ("no input"), as
/// porcupine_checker does. Anything else, a hang included, means it cannot check the run.
pub async fn probe(bin: &Path, timeout: Duration) -> Result<()> {
    let mut child = Command::new(bin)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .with_context(|| format!("Cannot run the checker {}", bin.display()))?;
    let status = tokio::time::timeout(timeout, child.wait())
        .await
        .with_context(|| {
            format!(
                "The checker {} did not answer in {timeout:?}",
                bin.display()
            )
        })?
        .with_context(|| format!("The checker {} failed", bin.display()))?;
    anyhow::ensure!(
        status.code() == Some(2),
        "The checker {} answered empty input with {status}, not exit 2: is it porcupine_checker?",
        bin.display()
    );
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    use std::path::PathBuf;

    /// A fake checker: a shell script with `body`, in a fresh temp dir. Like the real one, it
    /// exits 2 on empty input unless the body says otherwise.
    pub(crate) fn fake_checker(name: &str, body: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("sc-verify-fake-{}-{name}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("porcupine_checker");
        std::fs::write(&path, format!("#!/bin/sh\n{body}\n")).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        path
    }

    #[tokio::test]
    async fn probe_test() {
        let timeout = Duration::from_millis(500);
        let good = fake_checker(
            "probe-good",
            "cat >/dev/null; echo 'error: no input data' >&2; exit 2",
        );
        assert!(probe(&good, timeout).await.is_ok());

        let wrong = fake_checker("probe-wrong", "exit 0");
        assert!(
            probe(&wrong, timeout).await.is_err(),
            "empty input must be refused"
        );

        let hangs = fake_checker("probe-hangs", "sleep 30");
        let start = std::time::Instant::now();
        assert!(probe(&hangs, timeout).await.is_err());
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "the probe has its own timeout"
        );

        assert!(probe(Path::new("/nonexistent/porcupine_checker"), timeout)
            .await
            .is_err());
    }
}
