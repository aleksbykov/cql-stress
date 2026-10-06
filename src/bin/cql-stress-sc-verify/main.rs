//! `cql-stress-sc-verify`: a verified load for strongly consistent tables.
//! Design: tasks/SCYLLADB-4519/spec.md.

mod cli;

use anyhow::Result;
use tracing_subscriber::EnvFilter;

use cli::Cli;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_ansi(false)
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or(EnvFilter::new("warn")))
        .init();

    let cli = Cli::parse_checked(std::env::args_os()).unwrap_or_else(|err| {
        // clap prints --help and --version itself and exits 0; anything else is a usage error.
        match err.downcast::<clap::Error>() {
            Ok(clap_err) => clap_err.exit(),
            Err(err) => {
                eprintln!("error: {err:#}");
                std::process::exit(2)
            }
        }
    });
    // Built up front, so a bad certificate path fails before anything connects.
    let tls = cli.tls_context()?;
    println!(
        "{cli:#?}\nTLS: {}",
        if tls.is_some() { "on" } else { "off" }
    );
    Ok(())
}
