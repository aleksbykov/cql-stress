# SCYLLADB-4519 — implementation plan

**Spec:** `tasks/SCYLLADB-4519/spec.md`

A decision during the build that changes what the spec states updates the
spec in the same commit. A line in its `Decisions` section records the
decision when the Design section does not state the reason. This paragraph
stays in every plan built from a spec.

## Rules

- Each task below is one commit on `feat/sc-verify-m1`, with its boxes checked.
  The `(Tn)` tag maps it to the task list in the tracking repo.
- Verify = the `## Commands` section of `CLAUDE.md`, with
  `export RUSTFLAGS="-D warnings --cfg scylla_unstable"`: the Lint block, then
  `cargo test --features "user-profile,strong-consistency" -- --test-threads=1`
  against `docker compose -f docker/scylla-test/compose.yml --profile strong-consistency up -d --wait scylla-sc-test`.
  A task that touches cassandra-stress also runs
  `pytest -s ./tools/cql-stress-cassandra-stress-ci.py -k strong_consistency`.
- Baseline before task 1, on f990890: 135 Rust tests pass, 5 SC integration tests pass.
- A move is a move: `git mv` where a whole file moves, no edits beyond paths and
  `pub`. The cassandra-stress golden files `cs_args_*_test.in` are never edited.
- New binary code lives in `src/bin/cql-stress-sc-verify/`. Unit tests sit at the
  end of the file under test. Integration logic goes in `tools/test_cs_sc_verify.py`
  as `run_<case>()`, called from `test_strong_consistency_sc_verify_<case>()` in
  `tools/cql-stress-cassandra-stress-ci.py`, so the CI job `-k strong_consistency`
  runs it.
- Errors: `anyhow` with `.context()`. Diagnostics go through `tracing`; `println!`
  is for the user and for `SCV` lines.

## Task 1 — move the protocol-extension probe into the library (T1)

**Files:**
- Move: `src/bin/cql-stress-cassandra-stress/settings/protocol_extensions.rs` → `src/strong_consistency/protocol_extensions.rs`
- Create: `src/strong_consistency.rs`
- Modify: `src/lib.rs`, `src/bin/cql-stress-cassandra-stress/settings/mod.rs:9-10,45-46`

**Internals:** `pub mod strong_consistency` in `lib.rs`, gated on the feature;
`pub use protocol_extensions::fetch_protocol_features`.

- [x] Move the file; point the binary's `use` at `cql_stress::strong_consistency::fetch_protocol_features`.
- [x] Run verify; the SC pytest still passes 5/5.
- [x] Commit `refactor: move the protocol extension probe into the library [SCYLLADB-4519]`.

## Task 2 — move the SC keyspace check into the library (T1)

**Files:**
- Modify: `src/strong_consistency.rs`, `src/bin/cql-stress-cassandra-stress/settings/mod.rs:175-235,380-555`, `.../settings/test.rs:165-235`

**Internals:** moved from `settings/mod.rs`: `STRONG_CONSISTENCY_UNAVAILABLE_CODE`,
`strong_consistency_failure_message`, `summarise_v2_probe`, the probe loop of
`diagnose_missing_strong_consistency` as `diagnose_v2(nodes: &[String], tls: bool) -> String`;
new `keyspace_consistency_mode(session, keyspace)` (refresh plus lookup) and
`unavailable_error(keyspace, reported_mode, ddl, nodes, tls) -> anyhow::Error`.
`verify_consistency_mode` stays in the binary and calls them. The `tls` flag keeps
today's "not asked, TLS" message, so the spec's `unavailable_error` gains `tls: bool`.
Update the spec's Module API in this commit.

- [x] Move `summarise_v2_probe_test` and `strong_consistency_failure_carries_its_diagnostic_code_test` to the end of `src/strong_consistency.rs`; they fail to compile until the code moves.
- [x] Move the code; the binary keeps its CL check and datacenter warning.
- [x] Run verify and the SC pytest; the failure text is unchanged (the pytest matches the code).
- [x] Commit `refactor: move the strong consistency keyspace check into the library [SCYLLADB-4519]`.

## Task 3 — move the java distributions into the library (T2)

**Files:**
- Move: `src/bin/cql-stress-cassandra-stress/java_generate/{mod.rs,faster_random.rs,distribution/}` → `src/java_generate/`
- Keep: `src/bin/cql-stress-cassandra-stress/java_generate/distribution/enumerated.rs`
- Create: `src/bin/cql-stress-cassandra-stress/java_generate/mod.rs` (re-exports)
- Modify: `src/lib.rs`

**Internals:** the `Random` wrapper, `faster_random` and the distributions move
together, because both of the latter use `Random`'s private methods.
`EnumeratedDistribution` stays in the binary, because `-mixed ratio(...)` adds inherent
parsers to it through the `OperationRatio` alias, which only compiles in the crate
that defines the type. The binary's `java_generate::distribution` is an inline module
that re-exports the library's and declares `enumerated`, so every `crate::java_generate`
path in the binary stays unchanged. The tests in the moved files move with them.

- [x] Move the files; fix the `crate::`/`super::` paths only.
- [x] Run verify; the total test count is unchanged and the library gains the moved tests.
- [x] Commit `refactor: move the java distributions into the library [SCYLLADB-4519]`.

## Task 4 — distribution and population parsers in the library (T2)

**Files:**
- Modify: `src/java_generate/distribution/mod.rs`, `src/bin/cql-stress-cassandra-stress/settings/param/types.rs:265-290`

**Internals:** `pub fn parse_distribution(s: &str) -> Result<Box<dyn DistributionFactory>>`
(the body of today's `impl Parsable for Box<dyn DistributionFactory>`, which now calls it);
`pub fn parse_population(s: &str) -> Result<Box<dyn DistributionFactory>>` for
`seq=a..b` (inclusive, i64, with the k/m/b suffixes of the library's `parse_long`) or `dist=<distribution>`.

- [x] Write tests: `seq=0..1023` yields 0, 1, …, 1023, 0; `dist=uniform(1..10)` parses; `seq=5..1`, `foo=1` and `dist=bogus(1)` fail.
- [x] Run them and confirm the failure.
- [x] Add both functions; route the `Parsable` impl through `parse_distribution`.
- [x] Run verify; `cs_args_good_test.in` and `cs_args_bad_test.in` pass unchanged.
- [x] Commit `feat: parse populations in the library [SCYLLADB-4519]`.

**Checkpoint A** (human review): Rust tests and the SC pytest are green; cassandra-stress output is unchanged.

## Task 5 — binary skeleton and CLI (T3)

**Files:**
- Modify: `Cargo.toml` (the `[[bin]]` entry with `required-features = ["strong-consistency"]`; the feature adds `dep:clap`, optional `clap` 4 with `derive`; `serde`/`serde_yaml` join in task 6, which uses them)
- Create: `src/bin/cql-stress-sc-verify/main.rs`, `.../cli.rs`

**Internals:** `struct Cli` (clap derive) holding every flag of the spec's
Command block, with spec defaults; `enum Mode { Verify, Bulk, Both }`;
`enum Checker { Off }`. `-n` is accepted only with `--mode bulk`. The connection
flags are `--nodes`, `--user`, `--password` and TLS: `--ssl` turns it on,
`--ssl-ca <pem>` verifies the server, and `--ssl-cert <pem>` with `--ssl-key <pem>`
gives a client certificate. `fn tls_context(&Cli) -> Result<Option<SslContext>>`
uses `openssl`, which is already a dependency. The spec's Command block gets these
exact flags in this commit (user decision: TLS is in M1). `main` parses the flags,
builds the TLS context and prints the settings; usage errors exit 2. `parse_duration`
(`ms`/`s`/`m`/`h`) is ten lines in `cli.rs`, not a new crate. One of `--duration` or
`-n` is required.

- [x] Write a `tls_context` test: no `--ssl` → `None`; `--ssl-cert` without `--ssl-key` → error.
- [x] Write `cli.rs` tests: the defaults; `-n` with `--mode verify` is an error; `--checker on` is an error.
- [x] Run them and confirm the failure. (The code came first here; two deliberate breaks, a changed default and a dropped `-n` rule, each failed a test.)
- [x] Write the skeleton.
- [x] Run verify; also `cargo build` with no features and confirm the binary is skipped.
- [x] Commit `feat: add the cql-stress-sc-verify binary skeleton [SCYLLADB-4519]`.

## Task 6 — profile and DDL (T3)

**Files:** Create: `src/bin/cql-stress-sc-verify/profile.rs`

**Internals:** `struct Profile` (serde), `enum CellType { Int, Bigint, Text, Blob }`;
`Profile::load(path)`, `validate()` (1..8 cells, sizes ≥ 9 for `text`/`blob`
so a wid fits), `keyspace_ddl()`, `table_ddl(table)`.

- [ ] Write tests: the default profile gives the spec's DDL text exactly; 0 or 9 cells fail; an unknown type fails.
- [ ] Run them and confirm the failure.
- [ ] Write `profile.rs`.
- [ ] Run verify.
- [ ] Commit `feat: load the sc-verify profile and build its DDL [SCYLLADB-4519]`.

## Task 7 — schema create, diff and start-up checks (T3)

**Files:** Create: `src/bin/cql-stress-sc-verify/startup.rs`; Modify: `main.rs`; Create: `tools/test_cs_sc_verify.py`; Modify: `tools/cql-stress-cassandra-stress-ci.py`

**Internals:** `fn diff(profile_cols, live_cols) -> Vec<String>` (pure, `"c1: table has text, profile says int"`);
`async fn startup(session, profile) -> Result<(), Exit2>`: run `IF NOT EXISTS` DDL,
call `keyspace_consistency_mode`/`unavailable_error`, read `system_schema.columns`, diff.
`main` maps a start-up failure to exit 2.

- [ ] Write tests: `diff` on equal, type-changed, missing and extra columns; pytest `run_schema`: an empty keyspace gets created; an altered table exits 2 with the diff.
- [ ] Run them and confirm the failure.
- [ ] Write the code.
- [ ] Run verify and the new pytest.
- [ ] Commit `feat: create and check the sc-verify schema at start-up [SCYLLADB-4519]`.

## Task 8 — checked session settings (T4, high risk first)

**Files:** Create: `src/bin/cql-stress-sc-verify/ops.rs`

**Internals:** `fn checked_profile(cl, request_timeout) -> ExecutionProfile` with
`FallthroughRetryPolicy`, `speculative_execution_policy(None)`, the given CL and timeout;
`fn prepare_checked(session, query)` marks each statement not idempotent.

- [ ] Write a test that reads `get_retry_policy`, `get_speculative_execution_policy` and the consistency back from the profile, and checks `is_idempotent()` on a prepared statement.
- [ ] Run it and confirm the failure.
- [ ] Write the code. If the driver cannot express any of it, stop and ask.
- [ ] Run verify.
- [ ] Commit `feat: build the non-retrying checked execution profile [SCYLLADB-4519]`.

## Task 9 — cell encoders (T4)

**Files:** Modify: `src/bin/cql-stress-sc-verify/ops.rs`

**Internals:** `fn encode(cell: CellType, wid: u64, size: usize) -> CqlValue`;
`fn decode(cell, &CqlValue) -> Result<u64>`; `int` refuses wid ≥ 2^31.

- [ ] Write tests: round trip for each type; text length = `text_size`; blob is 8 bytes little-endian plus filler; garbage does not decode.
- [ ] Run them and confirm the failure.
- [ ] Write the code.
- [ ] Run verify.
- [ ] Commit `feat: encode write ids into cells [SCYLLADB-4519]`.

## Task 10 — outcome classes (T4)

**Files:** Modify: `src/bin/cql-stress-sc-verify/ops.rs`

**Internals:** `enum Outcome { Ok, Indeterminate, Fail, WorkloadError, ReadFailed }`;
`fn classify(op: OpKind, err: &ExecutionError, unavailable_is_fail: bool) -> Outcome`
per the spec's outcome table.

- [ ] Write table tests: write timeout, unknown-outcome server error, client timeout, broken connection, `Unavailable` with and without the flag, `InvalidRequest`, and a failed read.
- [ ] Run them and confirm the failure.
- [ ] Write the code.
- [ ] Run verify.
- [ ] Commit `feat: classify checked operation outcomes [SCYLLADB-4519]`.

## Task 11 — invariants engine (T5)

**Files:** Create: `src/bin/cql-stress-sc-verify/invariants.rs`

**Internals:** `struct RowState` (per wid: cells set, start, end or `None`, status;
per cell: `ack_floor`, `ack_seen`, `obs_floor`, `obs_seen`);
`fn snapshot(&self) -> Floors`, taken before the read's start time;
`fn on_write_end(..)`; `fn check_read(&Floors, start, cells) -> Vec<Violation>`;
`fn on_read_end(..)`, which skips INV-0 values. `Violation { kind, cell }`.

- [ ] Write tests: for each of INV-0..3, one timeline that fires and one legal one; an indeterminate write is never older; an op ending between `snapshot` and the read's start does not count; the F12 shape with non-overlapping writes fires INV-3.
- [ ] Run them and confirm the failure.
- [ ] Write the code.
- [ ] Run verify.
- [ ] Commit `feat: check reads with streaming invariants [SCYLLADB-4519]`.

## Task 12 — keys and history file (T6)

**Files:** Create: `src/bin/cql-stress-sc-verify/history.rs`, `.../keys.rs`; Create: `tests/data/sc_verify/history_v2.jsonl`

**Internals:** `struct GenMinter` (`start_ms × 2^20 + n`, re-base at 2^20 and
when the clock is behind); `struct PkPool` (wraps around the population);
`struct CheckFile` writes the `# key …` line, then call and return lines;
`gen` is a string in JSON.

- [ ] Write tests: gen ≥ 2^60 and strictly increasing across a re-base; pk wraps; one known row written equals the golden file byte for byte.
- [ ] Run them and confirm the failure.
- [ ] Write the code.
- [ ] Run verify.
- [ ] Commit `feat: mint row keys and write v2 history files [SCYLLADB-4519]`.

## Task 13 — the slot loop (T6)

**Files:** Create: `src/bin/cql-stress-sc-verify/slot.rs`; Modify: `main.rs`

**Internals:** `async fn run_slot(..)`: mint, bursts (a budget of `--burst-ops`
starts per `--burst-interval`, offset `i × interval / slots`), stop on ops, time or
indeterminate, drain, sweep (`--sweep-retries`, the only retry), seal. Each op
takes floors, then its start time, then sends; at the end it takes its end
time, checks, then updates the state (spec timing rule). `fn should_stop(..)` is
pure. Fresh-row check (start-up item 4): the first read of each slot's first row
must find the row absent.

- [ ] Write tests: `should_stop` for each `stop_reason`; the burst budget gives 16 starts per tick.
- [ ] Run them and confirm the failure.
- [ ] Write the code; `--mode verify` runs slots until `--duration`.
- [ ] Run verify; a 10 s compose run exits 0.
- [ ] Commit `feat: run checked slots in bursts [SCYLLADB-4519]`.

## Task 14 — rows.jsonl and check files on disk (T6)

**Files:** Modify: `slot.rs`, `history.rs`, `main.rs`; Modify: `tools/test_cs_sc_verify.py`, `tools/cql-stress-cassandra-stress-ci.py`

**Internals:** `struct RowRecord` (spec `rows.jsonl` shape); one shared open
`CheckFile` per process, rotated on `--check-rows` or `--check-age` into
`sealed/<seq>.jsonl`; a row with a violation is copied to `archive/<seq>/`.

- [ ] Write pytest `run_verify_quiet`: 60 s, `--checker off`; every row has ≥ 150 ops, `verdict` `unchecked`, 0 violations.
- [ ] Run it and confirm the failure.
- [ ] Write the code.
- [ ] Run verify and the pytest.
- [ ] Commit `feat: record sealed rows and check files [SCYLLADB-4519]`.

## Task 15 — report and SCV lines (T7)

**Files:** Create: `src/bin/cql-stress-sc-verify/report.rs`; Modify: `main.rs`, `slot.rs`

**Internals:** `fn scv(line: &impl Serialize)` prints `SCV {json}`; `start`,
`violation`, `stats` (per `--report-interval`), `end`; `report.json` rewritten
atomically (write a temp file, then rename); hdrhistogram p99 per op kind;
`sched_delay_p99_ms` from a ticker measuring lateness; exit 1 when any
violation was seen.

- [ ] Write tests: each line's JSON shape; `gen_base` is a string.
- [ ] Run them and confirm the failure.
- [ ] Write the code.
- [ ] Run verify; the compose run prints `start`, `stats` and `end`.
- [ ] Commit `feat: report sc-verify progress as SCV lines [SCYLLADB-4519]`.

## Task 16 — stale-read fault flag (T8)

**Files:** Modify: `ops.rs`, `cli.rs`; Modify: `tools/test_cs_sc_verify.py`, `tools/cql-stress-cassandra-stress-ci.py`

**Internals:** hidden `--fault-stale-reads <p>`: with probability p, a read
returns the cells of an earlier read of the same row.

- [ ] Write pytest `run_stale_reads`: with the flag, `SCV violation` of INV-1 or INV-2 within one row, exit 1.
- [ ] Run it and confirm the failure.
- [ ] Write the code.
- [ ] Run verify and the pytest.
- [ ] Commit `test: inject stale reads to prove the invariants fire [SCYLLADB-4519]`.

**Checkpoint B** (human review): the 60 s compose run is clean; the fault flag gives exit 1.

## Task 17 — bulk mode (T9)

**Files:** Create: `src/bin/cql-stress-sc-verify/bulk.rs`; Modify: `main.rs`; Modify: `tools/test_cs_sc_verify.py`, `tools/cql-stress-cassandra-stress-ci.py`

**Internals:** `fn bulk_gen(pk) -> i64` (fixed hash into `[0, 2^40)`); a bulk
`OperationFactory`/`Operation` on `run.rs` with its own session: `write`,
`read` (misses counted), `mixed` by `--bulk-read-ratio`; `-n` stops after n
ops; `--bulk-retries` goes to `max_retries_per_op`; `--bulk-read-consistency`.

- [ ] Write tests: `bulk_gen < 2^40` over many pks; pytest `run_bulk`: write `-n` then read the same range → 0 misses.
- [ ] Run them and confirm the failure.
- [ ] Write the code.
- [ ] Run verify and the pytest.
- [ ] Commit `feat: add sc-verify bulk mode [SCYLLADB-4519]`.

## Task 18 — `--mode both` (T9)

**Files:** Modify: `main.rs`; Modify: `tools/test_cs_sc_verify.py`, `tools/cql-stress-cassandra-stress-ci.py`

- [ ] Write pytest `run_both`: 60 s; every checked row is clean and bulk ops are > 0.
- [ ] Run it and confirm the failure.
- [ ] Run slots and bulk together in one process, each on its own session.
- [ ] Run verify and the pytest.
- [ ] Commit `feat: run checked and bulk load together [SCYLLADB-4519]`.

## Task 19 — CI and image (T10)

**Files:** Modify: `.github/workflows/rust.yml` (the strong-consistency build uploads `cql-stress-sc-verify` too, and the SC job puts it on PATH); `Dockerfile` only if the feature build skips the new binary

- [ ] Build the image locally with `--build-arg CARGO_BUILD_FEATURES=strong-consistency`; `docker run --rm <image> -c "cql-stress-sc-verify --help"` works.
- [ ] Make the CI change.
- [ ] Run verify.
- [ ] Commit `ci: build and test cql-stress-sc-verify in the strong consistency job [SCYLLADB-4519]`.
- [ ] Ask the user, then `buildx` for amd64 and arm64 and push `aleksbykov/cql-stress:sc-verify-m1-<short-sha>`; `docker manifest inspect` shows both.

**Checkpoint C** (human review): the PR is ready to open, linking the spec.
