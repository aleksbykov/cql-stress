//! The full check: porcupine_checker run as a child process on each check file (spec §11.3).

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context, Result};
use serde::Deserialize;
use tokio::io::{AsyncBufReadExt, BufReader};
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

/// How each check file is checked.
#[derive(Debug, Clone)]
pub struct CheckerConfig {
    pub bin: PathBuf,
    /// The child is killed after this long.
    pub timeout: Duration,
    /// Memory for the child, in bytes.
    pub mem: u64,
    pub key_timeout: Duration,
    pub max_viz: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowResult {
    Ok,
    Illegal,
    Unknown,
}

/// The verdicts of one check file, `per_key[key]` for each row in it.
#[derive(Debug)]
pub struct FileVerdicts {
    pub per_key: Vec<RowResult>,
    /// The child ran out of time and was killed.
    pub killed: bool,
}

#[derive(Deserialize)]
struct Line {
    key: Option<usize>,
    result: Option<String>,
}

/// Checks the `rows` rows of `file` in a porcupine_checker child, which writes its artifacts
/// into `archive_dir`.
///
/// The child runs at `nice 10`, with `GOMEMLIMIT` at `mem` and `RLIMIT_DATA` a quarter above
/// it, so the Go GC can act before the hard limit; never `RLIMIT_AS`, which the Go runtime's
/// up-front address-space reservation would hit. It writes a line per row as soon as the row
/// is decided, so when it is killed (`timeout`) or dies, the rows it finished keep their
/// verdict and the others are `unknown`.
pub async fn check_file(
    cfg: &CheckerConfig,
    file: &Path,
    rows: usize,
    archive_dir: &Path,
) -> Result<FileVerdicts> {
    std::fs::create_dir_all(archive_dir)
        .with_context(|| format!("Failed to create {}", archive_dir.display()))?;
    let stdin =
        std::fs::File::open(file).with_context(|| format!("Failed to open {}", file.display()))?;
    let stderr = std::fs::File::create(archive_dir.join("checker.stderr"))?;
    let data_limit = cfg.mem.saturating_mul(5) / 4;
    let mut command = Command::new(&cfg.bin);
    command
        .arg("--output-dir")
        .arg(archive_dir)
        .arg("--key-timeout")
        .arg(format!("{}ms", cfg.key_timeout.as_millis()))
        .arg("--max-viz")
        .arg(cfg.max_viz.to_string())
        .env("GOMEMLIMIT", cfg.mem.to_string())
        .stdin(stdin)
        .stdout(Stdio::piped())
        .stderr(stderr)
        .kill_on_drop(true);
    // SAFETY: only async-signal-safe calls (nice, setrlimit) between fork and exec.
    unsafe {
        command.pre_exec(move || {
            libc::nice(10);
            let limit = libc::rlimit {
                rlim_cur: data_limit,
                rlim_max: data_limit,
            };
            if libc::setrlimit(libc::RLIMIT_DATA, &limit) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut child = command
        .spawn()
        .with_context(|| format!("Cannot run the checker {}", cfg.bin.display()))?;

    let mut per_key = vec![RowResult::Unknown; rows];
    let mut lines = BufReader::new(child.stdout.take().expect("stdout is piped")).lines();
    let read_all = async {
        while let Ok(Some(text)) = lines.next_line().await {
            let Ok(Line {
                key: Some(key),
                result: Some(result),
            }) = serde_json::from_str::<Line>(&text)
            else {
                continue; // the summary, or a line this tool does not read
            };
            if let Some(slot) = per_key.get_mut(key) {
                *slot = match result.as_str() {
                    "ok" => RowResult::Ok,
                    "illegal" => RowResult::Illegal,
                    _ => RowResult::Unknown,
                };
            }
        }
    };
    let killed = tokio::time::timeout(cfg.timeout, read_all).await.is_err();
    if killed {
        let _ = child.kill().await;
    }
    let _ = child.wait().await;
    Ok(FileVerdicts { per_key, killed })
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

    fn config(bin: PathBuf, timeout: Duration) -> CheckerConfig {
        CheckerConfig {
            bin,
            timeout,
            mem: 512 << 20,
            key_timeout: Duration::from_secs(7),
            max_viz: 2,
        }
    }

    /// A check file of `rows` one-read rows, and an empty archive directory for it.
    fn check_file_fixture(name: &str, rows: usize) -> (PathBuf, PathBuf) {
        let dir =
            std::env::temp_dir().join(format!("sc-verify-check-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("0.jsonl");
        let mut content = String::new();
        for key in 0..rows {
            content.push_str(&format!(
                "{{\"id\":{key},\"client_id\":0,\"kind\":\"call\",\"op\":\"read\",\"key\":{key},\"time_ns\":1}}\n"
            ));
        }
        std::fs::write(&file, content).unwrap();
        (file, dir.join("archive"))
    }

    const OK_0: &str = r#"echo '{"key":0,"result":"ok","ops":1,"ms":0}'"#;
    const OK_1: &str = r#"echo '{"key":1,"result":"ok","ops":1,"ms":0}'"#;
    const ILLEGAL_1: &str = r#"echo '{"key":1,"result":"illegal","ops":1,"ms":0}'"#;
    const SUMMARY: &str = r#"echo '{"summary":true}'"#;

    #[tokio::test]
    async fn check_file_reads_one_verdict_per_row_test() {
        let (file, archive) = check_file_fixture("ok", 2);
        let all_ok = fake_checker(
            "all-ok",
            &format!("cat >/dev/null\n{OK_0}\n{OK_1}\n{SUMMARY}\nexit 0"),
        );
        let verdicts = check_file(&config(all_ok, Duration::from_secs(5)), &file, 2, &archive)
            .await
            .unwrap();
        assert_eq!(verdicts.per_key, [RowResult::Ok, RowResult::Ok]);
        assert!(!verdicts.killed);

        let illegal = fake_checker(
            "illegal",
            &format!("cat >/dev/null\n{OK_0}\n{ILLEGAL_1}\n{SUMMARY}\nexit 1"),
        );
        let verdicts = check_file(&config(illegal, Duration::from_secs(5)), &file, 2, &archive)
            .await
            .unwrap();
        assert_eq!(verdicts.per_key, [RowResult::Ok, RowResult::Illegal]);
    }

    /// A killed or crashed checker keeps the rows it finished; the others are unknown.
    #[tokio::test]
    async fn rows_without_a_line_are_unknown_test() {
        let (file, archive) = check_file_fixture("hang", 2);
        let hangs = fake_checker("hangs", &format!("cat >/dev/null\n{OK_0}\nsleep 30"));
        let start = std::time::Instant::now();
        let verdicts = check_file(
            &config(hangs, Duration::from_millis(500)),
            &file,
            2,
            &archive,
        )
        .await
        .unwrap();
        assert_eq!(verdicts.per_key, [RowResult::Ok, RowResult::Unknown]);
        assert!(verdicts.killed);
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "killed at --checker-timeout"
        );

        let crashes = fake_checker("crashes", &format!("cat >/dev/null\n{OK_0}\nkill -9 $$"));
        let verdicts = check_file(&config(crashes, Duration::from_secs(5)), &file, 2, &archive)
            .await
            .unwrap();
        assert_eq!(verdicts.per_key, [RowResult::Ok, RowResult::Unknown]);
    }

    /// The child runs at lower priority, with its memory limited, and gets the right flags.
    #[tokio::test]
    async fn the_child_is_limited_test() {
        let (file, archive) = check_file_fixture("env", 1);
        // $2 is the --output-dir.
        let env = fake_checker(
            "env",
            "cat >/dev/null\necho \"$(nice) $GOMEMLIMIT $(ulimit -d) $*\" > \"$2/env\"",
        );
        check_file(&config(env, Duration::from_secs(5)), &file, 1, &archive)
            .await
            .unwrap();
        let seen = std::fs::read_to_string(archive.join("env")).unwrap();
        let fields: Vec<&str> = seen.split_whitespace().collect();
        assert!(fields[0].parse::<i32>().unwrap() >= 10, "nice 10: {seen}");
        assert_eq!(
            fields[1],
            (512u64 << 20).to_string(),
            "GOMEMLIMIT in bytes: {seen}"
        );
        assert_eq!(
            fields[2],
            ((512u64 << 20) * 5 / 4 / 1024).to_string(),
            "RLIMIT_DATA, kB: {seen}"
        );
        assert_eq!(
            fields[3..].join(" "),
            format!(
                "--output-dir {} --key-timeout 7000ms --max-viz 2",
                archive.display()
            )
        );
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
