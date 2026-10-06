//! The command line of `cql-stress-sc-verify`. Defaults follow the sc-verify spec, §15.2.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use cql_stress::java_generate::distribution::parse_population;
use openssl::ssl::{SslContext, SslContextBuilder, SslFiletype, SslMethod, SslVerifyMode};

#[derive(Parser, Debug)]
#[command(
    name = "cql-stress-sc-verify",
    version,
    about = "A verified load for strongly consistent tables"
)]
pub struct Cli {
    /// The YAML profile: the only definition of the keyspace, the table and its cells.
    #[arg(long)]
    pub profile: PathBuf,
    #[arg(long, value_enum, default_value_t = Mode::Both)]
    pub mode: Mode,

    // Connection.
    /// Contact points, comma separated: `host` or `host:port`.
    #[arg(long, value_delimiter = ',', default_value = "127.0.0.1")]
    pub nodes: Vec<String>,
    #[arg(long, requires = "password")]
    pub user: Option<String>,
    #[arg(long, requires = "user")]
    pub password: Option<String>,
    /// Connect over TLS.
    #[arg(long)]
    pub ssl: bool,
    /// PEM file of the CA that signed the server certificates; without it they are not verified.
    #[arg(long, requires = "ssl")]
    pub ssl_ca: Option<PathBuf>,
    /// PEM client certificate, for servers that require one.
    #[arg(long, requires_all = ["ssl", "ssl_key"])]
    pub ssl_cert: Option<PathBuf>,
    /// PEM private key of `--ssl-cert`.
    #[arg(long, requires = "ssl_cert")]
    pub ssl_key: Option<PathBuf>,
    /// Consistency level of the checked stream.
    #[arg(long, value_enum, default_value_t = CheckedConsistency::Quorum)]
    pub consistency: CheckedConsistency,

    /// How long to run, e.g. `30s`, `5m`, `4h`.
    #[arg(long, value_parser = parse_duration)]
    pub duration: Option<Duration>,
    /// Number of bulk operations; `--mode bulk` only.
    #[arg(short = 'n')]
    pub ops: Option<u64>,

    // Checked stream (modes verify and both).
    /// Partition keys of checked rows, e.g. `seq=0..1023`.
    #[arg(long, default_value = "seq=0..1023", value_parser = population)]
    pub pop: String,
    #[arg(long, default_value_t = 32)]
    pub slots: usize,
    #[arg(long, default_value_t = 8)]
    pub clients_per_row: usize,
    #[arg(long, default_value_t = 200)]
    pub ops_per_gen: u64,
    #[arg(long, default_value = "30s", value_parser = parse_duration)]
    pub max_gen_duration: Duration,
    #[arg(long, default_value_t = 16)]
    pub burst_ops: u64,
    #[arg(long, default_value = "1s", value_parser = parse_duration)]
    pub burst_interval: Duration,
    #[arg(long, default_value_t = 0.5, value_parser = ratio)]
    pub read_ratio: f64,
    #[arg(long, default_value_t = 0.5, value_parser = ratio)]
    pub insert_ratio: f64,
    #[arg(long, default_value = "5s", value_parser = parse_duration)]
    pub request_timeout: Duration,
    #[arg(long, default_value_t = 10)]
    pub sweep_retries: u32,
    #[arg(long, default_value = "1s", value_parser = parse_duration)]
    pub sweep_backoff: Duration,
    /// Seal a row early once this many of its writes are indeterminate.
    #[arg(long, default_value_t = 8)]
    pub max_indeterminate: u64,
    /// Record `Unavailable` as a failed write instead of an indeterminate one.
    #[arg(long)]
    pub unavailable_is_fail: bool,
    #[arg(long, default_value_t = 10)]
    pub max_workload_errors: u64,
    /// Row TTL in seconds; 0 means rows never expire.
    #[arg(long, default_value_t = 0)]
    pub ttl: u32,
    #[arg(long, default_value = "/history")]
    pub history_dir: PathBuf,
    #[arg(long, default_value_t = 50)]
    pub check_rows: usize,
    #[arg(long, default_value = "5m", value_parser = parse_duration)]
    pub check_age: Duration,
    /// Milestone 1 only writes check files; `on` arrives with the checker queue.
    #[arg(long, value_enum, default_value_t = Checker::Off)]
    pub checker: Checker,

    // Bulk (modes bulk and both).
    #[arg(long, value_enum, default_value_t = BulkOp::Mixed)]
    pub bulk_op: BulkOp,
    #[arg(long, default_value_t = 0.5, value_parser = ratio)]
    pub bulk_read_ratio: f64,
    #[arg(long, default_value = "seq=1099511627776..1099522113535", value_parser = population)]
    pub bulk_pop: String,
    #[arg(long, default_value_t = 64)]
    pub bulk_threads: u64,
    /// Bulk operations per second; 0 means unthrottled.
    #[arg(long, default_value_t = 0)]
    pub bulk_rate: u64,
    #[arg(long, value_enum, default_value_t = BulkReadConsistency::Quorum)]
    pub bulk_read_consistency: BulkReadConsistency,
    #[arg(long, default_value_t = 9)]
    pub bulk_retries: usize,

    #[arg(long, default_value = "10s", value_parser = parse_duration)]
    pub report_interval: Duration,

    /// Test only: with this probability a read returns the cells of the row's first read
    /// instead, a stale read the invariants must catch (milestone 1 "can fail").
    #[arg(long, hide = true, default_value_t = 0.0, value_parser = ratio)]
    pub fault_stale_reads: f64,
}

#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    Verify,
    Bulk,
    Both,
}

#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum CheckedConsistency {
    Quorum,
    LocalQuorum,
}

#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum Checker {
    Off,
}

#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum BulkOp {
    Write,
    Read,
    Mixed,
}

#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum BulkReadConsistency {
    Quorum,
    One,
}

impl Cli {
    /// Parses the arguments and checks the rules clap cannot express.
    pub fn parse_checked<I, T>(args: I) -> Result<Self>
    where
        I: IntoIterator<Item = T>,
        T: Into<std::ffi::OsString> + Clone,
    {
        let cli = Self::try_parse_from(args)?;
        anyhow::ensure!(
            cli.ops.is_none() || cli.mode == Mode::Bulk,
            "-n is only allowed with --mode bulk; checked rows run for --duration"
        );
        anyhow::ensure!(
            cli.duration.is_some() || cli.ops.is_some(),
            "give --duration, or -n with --mode bulk"
        );
        // A cell expiring mid-round would look exactly like a lost write (spec §7.4).
        let longest_round = cli.max_gen_duration
            + cli.request_timeout
            + (cli.sweep_backoff + cli.request_timeout) * cli.sweep_retries;
        anyhow::ensure!(
            cli.ttl == 0 || Duration::from_secs(cli.ttl.into()) >= longest_round * 10,
            "--ttl {} is shorter than 10 rounds of the longest possible length ({}s)",
            cli.ttl,
            (longest_round * 10).as_secs()
        );
        Ok(cli)
    }

    /// The TLS context for the sessions, `None` without `--ssl`.
    pub fn tls_context(&self) -> Result<Option<SslContext>> {
        if !self.ssl {
            return Ok(None);
        }
        let mut builder = SslContextBuilder::new(SslMethod::tls())?;
        builder.set_verify(SslVerifyMode::NONE);
        if let Some(ca) = &self.ssl_ca {
            builder
                .set_ca_file(ca)
                .with_context(|| format!("Failed to load --ssl-ca {}", ca.display()))?;
            builder.set_verify(SslVerifyMode::PEER);
        }
        if let (Some(cert), Some(key)) = (&self.ssl_cert, &self.ssl_key) {
            builder
                .set_certificate_file(cert, SslFiletype::PEM)
                .with_context(|| format!("Failed to load --ssl-cert {}", cert.display()))?;
            builder
                .set_private_key_file(key, SslFiletype::PEM)
                .with_context(|| format!("Failed to load --ssl-key {}", key.display()))?;
        }
        Ok(Some(builder.build()))
    }
}

/// `<number><unit>` with the unit one of `ms`, `s`, `m`, `h`.
fn parse_duration(s: &str) -> Result<Duration> {
    let split = s
        .find(|c: char| !c.is_ascii_digit())
        .with_context(|| format!("Duration {s:?} has no unit (ms, s, m, h)"))?;
    let (number, unit) = s.split_at(split);
    let number: u64 = number
        .parse()
        .with_context(|| format!("Invalid duration {s:?}"))?;
    let seconds = match unit {
        "ms" => return Ok(Duration::from_millis(number)),
        "s" => 1,
        "m" => 60,
        "h" => 3600,
        _ => anyhow::bail!("Duration {s:?} has an unknown unit; use ms, s, m or h"),
    };
    Ok(Duration::from_secs(number * seconds))
}

fn ratio(s: &str) -> Result<f64> {
    let value: f64 = s.parse().with_context(|| format!("Invalid ratio {s:?}"))?;
    anyhow::ensure!((0.0..=1.0).contains(&value), "Ratio {s} is not in 0..1");
    Ok(value)
}

/// Checks the population at parse time; the slots build their generators from the text.
fn population(s: &str) -> Result<String> {
    parse_population(s)?;
    Ok(s.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &str) -> Result<Cli> {
        Cli::parse_checked(
            ["cql-stress-sc-verify", "--profile", "p.yaml"]
                .into_iter()
                .chain(args.split_ascii_whitespace()),
        )
    }

    #[test]
    fn defaults_follow_the_spec_test() {
        let cli = parse("--duration 4h").unwrap();
        assert_eq!(cli.mode, Mode::Both);
        assert_eq!(cli.nodes, ["127.0.0.1"]);
        assert_eq!(cli.consistency, CheckedConsistency::Quorum);
        assert_eq!(cli.duration, Some(Duration::from_secs(4 * 3600)));
        assert_eq!(cli.pop, "seq=0..1023");
        assert_eq!((cli.slots, cli.clients_per_row), (32, 8));
        assert_eq!(cli.ops_per_gen, 200);
        assert_eq!(cli.max_gen_duration, Duration::from_secs(30));
        assert_eq!(
            (cli.burst_ops, cli.burst_interval),
            (16, Duration::from_secs(1))
        );
        assert_eq!((cli.read_ratio, cli.insert_ratio), (0.5, 0.5));
        assert_eq!(cli.request_timeout, Duration::from_secs(5));
        assert_eq!(
            (cli.sweep_retries, cli.sweep_backoff),
            (10, Duration::from_secs(1))
        );
        assert_eq!(cli.max_indeterminate, 8);
        assert!(!cli.unavailable_is_fail);
        assert_eq!((cli.max_workload_errors, cli.ttl), (10, 0));
        assert_eq!(cli.history_dir, PathBuf::from("/history"));
        assert_eq!(
            (cli.check_rows, cli.check_age),
            (50, Duration::from_secs(300))
        );
        assert_eq!(cli.checker, Checker::Off);
        assert_eq!((cli.bulk_op, cli.bulk_read_ratio), (BulkOp::Mixed, 0.5));
        assert_eq!(cli.bulk_pop, "seq=1099511627776..1099522113535");
        assert_eq!(
            (cli.bulk_threads, cli.bulk_rate, cli.bulk_retries),
            (64, 0, 9)
        );
        assert_eq!(cli.bulk_read_consistency, BulkReadConsistency::Quorum);
        assert_eq!(cli.report_interval, Duration::from_secs(10));
        assert!(!cli.ssl);
        assert_eq!(
            cli.fault_stale_reads, 0.0,
            "the fault is off unless asked for"
        );
    }

    #[test]
    fn rejected_combinations_test() {
        for bad in [
            "--mode verify -n 10",
            "--mode both -n 10",
            "--mode verify",
            "--duration 1m --checker on",
            "--duration 1m --pop seq=5..1",
            "--duration 1m --read-ratio 1.5",
            "--duration 1x",
            "--duration 1m --user u",
            "--duration 1m --ssl-ca ca.pem",
            "--duration 1m --ssl --ssl-cert c.pem",
            "--duration 1m --ssl --ssl-key k.pem",
            "--duration 1m --ttl 300",
        ] {
            assert!(parse(bad).is_err(), "{bad:?} must be rejected");
        }
        assert!(parse("--mode bulk -n 10").is_ok());
        // 10 × (30 s + 5 s + 10 × (1 s + 5 s)) = 950 s.
        assert!(parse("--duration 1m --ttl 950").is_ok());
        assert!(parse("--duration 1m --ttl 949").is_err());
        assert!(parse("--duration 250ms --nodes a,b:9043 --consistency local-quorum").is_ok());
    }

    #[test]
    fn tls_context_test() {
        let cli = parse("--duration 1m").unwrap();
        assert!(cli.tls_context().unwrap().is_none());

        let cli = parse("--duration 1m --ssl").unwrap();
        assert!(cli.tls_context().unwrap().is_some());

        let cli = parse("--duration 1m --ssl --ssl-ca /nonexistent/ca.pem").unwrap();
        assert!(cli.tls_context().is_err(), "a missing CA file is an error");
    }
}
