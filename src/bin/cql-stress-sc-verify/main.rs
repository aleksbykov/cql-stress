//! `cql-stress-sc-verify`: a verified load for strongly consistent tables.
//! Design: tasks/SCYLLADB-4519/spec.md.

mod cli;
// `CheckFile` gets its caller with the check files on disk (plan task 14), which removes
// this allow.
#[allow(dead_code)]
mod history;
mod invariants;
mod keys;
mod ops;
mod profile;
mod slot;
mod startup;

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use cql_stress::java_generate::distribution::parse_population;
use tokio::sync::mpsc;
use tracing_subscriber::EnvFilter;

use cli::{Cli, Mode};
use keys::GenMinter;
use profile::Profile;
use slot::Checked;

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
    // Every checked statement, prepared once; this also proves the cluster accepts them.
    let statements = ops::Statements::prepare(&session, &profile, cli.ttl)
        .await
        .unwrap_or_else(|err| exit_setup_failure(err));
    println!(
        "Start-up checks passed: {}.{} is strongly consistent and matches the profile",
        profile.keyspace, profile.table
    );
    if cli.mode != Mode::Verify {
        exit_setup_failure(anyhow::anyhow!(
            "--mode bulk and --mode both are not implemented yet; use --mode verify"
        ));
    }

    let pks = parse_population(&cli.pop)
        .unwrap_or_else(|err| exit_setup_failure(err))
        .create();
    let now_ms = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64;
    let gens = GenMinter::new(now_ms).unwrap_or_else(|err| exit_setup_failure(err));
    println!(
        "Checked stream: {} slots of {} clients, pk {}, gen base {}",
        cli.slots,
        cli.clients_per_row,
        cli.pop,
        gens.base()
    );
    let duration = cli.duration.expect("--mode verify requires --duration");
    let checked = Arc::new(Checked::new(
        Arc::new(session),
        statements,
        cli,
        profile.cells.len(),
        pks,
        gens,
    ));

    let (sealed_tx, mut sealed) = mpsc::unbounded_channel();
    let slots: Vec<_> = (0..checked.cli.slots)
        .map(|slot| tokio::spawn(slot::run_slot(checked.clone(), slot, sealed_tx.clone())))
        .collect();
    drop(sealed_tx);
    let stopper = {
        let checked = checked.clone();
        tokio::spawn(async move {
            tokio::time::sleep(duration).await;
            checked.stop();
        })
    };

    let (mut rows, mut ops, mut violations) = (0u64, 0usize, 0usize);
    while let Some(row) = sealed.recv().await {
        tracing::debug!(
            "sealed pk {} gen {} slot {} wall {}..{} ms: {} ops, {} reads, {} writes ok, \
             {} indeterminate, {} errors, max gap {} ms, stop {}, sweep {}, {} violations",
            row.key.pk,
            row.key.gen,
            row.slot,
            row.wall_start_ms,
            row.wall_end_ms,
            row.ops.len(),
            row.reads,
            row.writes_ok,
            row.writes_indet,
            row.errors,
            row.max_gap_ms,
            row.stop_reason.as_str(),
            if row.sweep_ok { "ok" } else { "incomplete" },
            row.violations.len()
        );
        rows += 1;
        ops += row.ops.len();
        violations += row.violations.len();
    }
    for slot in slots {
        if slot.await.is_err() {
            // A panicked slot: its rows cannot be vouched for.
            checked.exit.raise(3);
        }
    }
    stopper.abort();
    if violations > 0 {
        checked.exit.raise(1);
    }
    println!("Sealed {rows} rows, {ops} recorded operations, {violations} violations");
    std::process::exit(checked.exit.get().into())
}

/// Exit code 2: the run cannot start as configured (spec §14.5).
fn exit_setup_failure(err: anyhow::Error) -> ! {
    eprintln!("error: {err:#}");
    std::process::exit(2)
}
