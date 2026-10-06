//! `cql-stress-sc-verify`: a verified load for strongly consistent tables.
//! Design: tasks/SCYLLADB-4519/spec.md.

mod cli;
// These modules get their caller with the slot loop (plan task 13), which removes the
// allows.
#[allow(dead_code)]
mod history;
#[allow(dead_code)]
mod invariants;
#[allow(dead_code)]
mod keys;
#[allow(dead_code)]
mod ops;
mod profile;
mod startup;

use anyhow::Result;
use tracing_subscriber::EnvFilter;

use cli::Cli;
use profile::Profile;

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
            Err(err) => exit_setup_failure(err),
        }
    });
    let profile = Profile::load(&cli.profile).unwrap_or_else(|err| exit_setup_failure(err));
    // Built up front, so a bad certificate path fails before anything connects.
    let tls = cli
        .tls_context()
        .unwrap_or_else(|err| exit_setup_failure(err));
    let session = startup::connect(&cli, tls)
        .await
        .unwrap_or_else(|err| exit_setup_failure(err));
    startup::startup(&session, &profile, &cli)
        .await
        .unwrap_or_else(|err| exit_setup_failure(err));
    // The slots' read; preparing it here also proves the checked statements are accepted.
    ops::prepare_checked(&session, &profile.read_query())
        .await
        .unwrap_or_else(|err| exit_setup_failure(err));
    println!(
        "Start-up checks passed: {}.{} is strongly consistent and matches the profile",
        profile.keyspace, profile.table
    );
    Ok(())
}

/// Exit code 2: the run cannot start as configured (spec §14.5).
fn exit_setup_failure(err: anyhow::Error) -> ! {
    eprintln!("error: {err:#}");
    std::process::exit(2)
}
