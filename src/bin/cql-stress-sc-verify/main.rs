//! `cql-stress-sc-verify`: a verified load for strongly consistent tables.
//! Design: tasks/SCYLLADB-4519/spec.md.

mod cli;
mod history;
mod invariants;
mod keys;
mod ops;
mod profile;
mod report;
mod slot;
mod startup;

use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use cql_stress::java_generate::distribution::parse_population;
use tokio::sync::mpsc;
use tracing_subscriber::EnvFilter;

use cli::{Cli, Mode};
use history::Recorder;
use keys::GenMinter;
use profile::Profile;
use report::{Report, Scv};
use slot::{Checked, SealedRow};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_ansi(false)
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or(EnvFilter::new("warn")))
        .init();
    // Release builds abort on panic, which would skip the exit code and the last SCV line: a
    // crashed tool must still say so, with exit 3 (spec §14.5).
    std::panic::set_hook(Box::new(|panic| {
        eprintln!("error: cql-stress-sc-verify crashed: {panic}");
        report::print(&Scv::End { exit: 3 });
        std::process::exit(3);
    }));

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
    report::print(&Scv::Start {
        mode: "verify",
        pop: &cli.pop,
        slots: cli.slots,
        gen_base: gens.base(),
    });
    let duration = cli.duration.expect("--mode verify requires --duration");
    let mut recorder = Recorder::new(
        &cli.history_dir,
        profile.cells.len(),
        cli.check_rows,
        cli.check_age,
    )
    .unwrap_or_else(|err| exit_setup_failure(err));
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

    let history_dir = checked.cli.history_dir.clone();
    let mut totals = Report::default();
    let mut reports = tokio::time::interval(checked.cli.report_interval);
    reports.tick().await; // the first tick is immediate
    let mut last_report = Instant::now();
    loop {
        tokio::select! {
            row = sealed.recv() => {
                let Some(row) = row else { break };
                record(&checked, &mut recorder, &mut totals, row);
            }
            _ = reports.tick() => {
                let interval = checked.stats.take_interval(last_report.elapsed());
                last_report = Instant::now();
                report::print(&Scv::Stats(interval));
                write_report(&checked, &mut totals, &history_dir, None);
            }
        }
    }
    if let Err(err) = recorder.finish() {
        eprintln!("error: failed to close the history files: {err:#}");
        checked.exit.raise(3);
    }
    for slot in slots {
        // A panic exits 3 through the hook; this is only a second line of defence.
        if slot.await.is_err() {
            checked.exit.raise(3);
        }
    }
    stopper.abort();
    if totals.violations > 0 {
        checked.exit.raise(1);
    }
    let exit = checked.exit.get();
    write_report(&checked, &mut totals, &history_dir, Some(exit));
    println!(
        "Sealed {} rows, {} recorded operations, {} violations",
        totals.rows, totals.ops, totals.violations
    );
    report::print(&Scv::End { exit });
    std::process::exit(exit.into())
}

/// Records a sealed row, adds it to the totals, and prints the SCV lines of its violations
/// once its evidence is archived.
fn record(checked: &Checked, recorder: &mut Recorder, totals: &mut Report, row: SealedRow) {
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
    totals.rows += 1;
    totals.ops += row.ops.len() as u64;
    totals.reads += row.reads;
    totals.writes_ok += row.writes_ok;
    totals.errors += row.errors;
    totals.violations += row.violations.len() as u64;
    match recorder.record(&row) {
        Ok(Some(archive)) => {
            for detected in &row.violations {
                report::print(&Scv::Violation {
                    kind: &detected.violation.kind.to_string(),
                    pk: row.key.pk,
                    gen: row.key.gen,
                    cell: detected.violation.cell,
                    wall_ms: detected.wall_ms,
                    archive: &archive,
                });
            }
        }
        Ok(None) => {}
        Err(err) => {
            // Evidence that cannot be written makes every verdict unverifiable.
            eprintln!("error: failed to record a sealed row: {err:#}");
            checked.exit.raise(3);
            checked.stop();
        }
    }
}

fn write_report(checked: &Checked, totals: &mut Report, dir: &std::path::Path, exit: Option<u8>) {
    let latencies = checked.stats.totals();
    totals.read_p99_ms = latencies.read_p99_ms;
    totals.write_p99_ms = latencies.write_p99_ms;
    totals.sched_delay_p99_ms = latencies.sched_delay_p99_ms;
    totals.writes_indet = latencies.writes_indet;
    totals.exit = exit;
    if let Err(err) = totals.write(dir) {
        tracing::warn!("failed to write report.json: {err:#}");
    }
}

/// Exit code 2: the run cannot start as configured (spec §14.5).
fn exit_setup_failure(err: anyhow::Error) -> ! {
    eprintln!("error: {err:#}");
    std::process::exit(2)
}
