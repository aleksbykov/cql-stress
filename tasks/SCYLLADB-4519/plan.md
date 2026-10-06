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
`Profile::load(path)`, `validate()` (1..8 cells; `text_size` ≥ 21 and `blob_size` ≥ 8
so any u64 wid fits; keyspace and table names must be lower-case identifiers because
they go into DDL verbatim; unknown keys rejected), `keyspace_ddl()`, `table_ddl(table)`,
`bulk_table_name()`. `Cargo.toml`: the feature adds `dep:serde`, `dep:serde_yaml`.
`main` loads the profile (failure → exit 2) and prints the DDL.

- [x] Write tests: the default profile gives the spec's DDL text exactly; 0 or 9 cells fail; an unknown type fails.
- [x] Run them and confirm the failure.
- [x] Write `profile.rs`.
- [x] Run verify.
- [x] Commit `feat: load the sc-verify profile and build its DDL [SCYLLADB-4519]`.

## Task 7 — schema create, diff and start-up checks (T3)

**Files:** Create: `src/bin/cql-stress-sc-verify/startup.rs`; Modify: `main.rs`; Create: `tools/test_cs_sc_verify.py`; Modify: `tools/cql-stress-cassandra-stress-ci.py`

**Internals:** `fn diff(profile_cols, live_cols) -> Vec<String>` (pure, `"c1: table has text, profile says int"`);
`async fn startup(session, profile) -> Result<(), Exit2>`: run `IF NOT EXISTS` DDL,
call `keyspace_consistency_mode`/`unavailable_error`, read `system_schema.columns`, diff.
`main` maps a start-up failure to exit 2. Order: keyspace DDL, then the SC check, and only
then the table DDL and the diff, so no table goes into a keyspace that is not strongly
consistent. `connect(cli, tls)` builds the session from `--nodes`, the credentials and
TLS. The None/Eventual wording of the mode (four lines) is repeated from cassandra-stress
rather than moved into the library. The pytest also covers an eventually consistent
keyspace (exit 2, the diagnostic code, no table created) and a second start over an
existing schema.

- [x] Write tests: `diff` on equal, type-changed, missing and extra columns; pytest `run_schema`: an empty keyspace gets created; an altered table exits 2 with the diff.
- [x] Run them and confirm the failure. (The unit tests failed to compile first; the pytest was written after the code and asserts the exact diff line and code.)
- [x] Write the code.
- [x] Run verify and the new pytest.
- [x] Commit `feat: create and check the sc-verify schema at start-up [SCYLLADB-4519]`.

## Task 8 — checked session settings (T4, high risk first)

**Files:** Create: `src/bin/cql-stress-sc-verify/ops.rs`

**Internals:** `fn checked_profile(cl, request_timeout) -> ExecutionProfile` with
`FallthroughRetryPolicy`, `speculative_execution_policy(None)`, the given CL and timeout;
`fn prepare_checked(session, query)` marks each statement not idempotent. `connect` sets
the profile as the checked session's default; `main` prepares the checked read
(`Profile::read_query`) at start-up. Driver check (scylla 1.9 `client/execution.rs`): the
only path that moves to another node without the retry policy is a failure to get a
connection, before anything is sent.

- [x] Write a test that reads `get_retry_policy`, `get_speculative_execution_policy` and the consistency back from the profile, and checks `is_idempotent()` on a prepared statement.
- [x] Run it and confirm the failure. (It failed to compile; after the code, turning on speculative execution and marking statements idempotent each failed it.)
- [x] Write the code. If the driver cannot express any of it, stop and ask.
- [x] Run verify.
- [x] Commit `feat: build the non-retrying checked execution profile [SCYLLADB-4519]`.

## Task 9 — cell encoders (T4)

**Files:** Modify: `src/bin/cql-stress-sc-verify/ops.rs`

**Internals:** `fn encode(cell: CellType, wid: u64, size: usize) -> CqlValue`;
`fn decode(cell, size, &CqlValue) -> Result<u64>` is strict: the value must equal
`encode` of the wid it claims, and 0 is never a wid. `int` refuses wid ≥ 2^31, `bigint`
wid ≥ 2^63. Until task 13 gives them a caller, `mod ops` carries `#[allow(dead_code)]`.

- [x] Write tests: round trip for each type; text length = `text_size`; blob is 8 bytes little-endian plus filler; garbage does not decode.
- [x] Run them and confirm the failure.
- [x] Write the code.
- [x] Run verify.
- [x] Commit `feat: encode write ids into cells [SCYLLADB-4519]`.

## Task 10 — outcome classes (T4)

**Files:** Modify: `src/bin/cql-stress-sc-verify/ops.rs`

**Internals:** `enum Failure { Indeterminate, Fail, WorkloadError, ReadFailed }` (a success
is not an error, so it is not a class); `enum OpKind { Read, Write }`;
`fn classify(op: OpKind, err: &ExecutionError, unavailable_is_fail: bool) -> Failure` per the
spec's outcome table. Workload errors: `SyntaxError`, `Invalid`, `Unauthorized`,
`AuthenticationError`, `ConfigError`, `AlreadyExists`, `BadQuery`, request serialization.
Any other write failure is indeterminate (`Unavailable` is `Fail` with the flag); any other
read failure is not recorded.

- [x] Write table tests: write timeout, unknown-outcome server error, client timeout, broken connection, `Unavailable` with and without the flag, `InvalidRequest`, and a failed read.
- [x] Run them and confirm the failure.
- [x] Write the code.
- [x] Run verify.
- [x] Commit `feat: classify checked operation outcomes [SCYLLADB-4519]`.

## Task 11 — invariants engine (T5)

**Files:** Create: `src/bin/cql-stress-sc-verify/invariants.rs`

**Internals:** `struct RowState` (per wid: cell mask, start, `WriteStatus { InFlight,
Ok(end), Indeterminate, Fail(end) }`; per cell: `ack` and `obs` floors as `Option<u64>`,
where `Some` means seen); `begin_write(mask, start) -> wid` (wids from 1; an empty mask
panics); `end_write(wid, WriteEnd)`; `snapshot() -> Floors`, taken before the read's start
time; `check_read(&Floors, seen) -> Vec<Violation>` (no start-time parameter: the floors carry
the timing); `end_read(seen)`, which skips INV-0 values; `status(wid)`. `Seen { Null,
Wid, Undecodable }`, `Violation { kind: Inv, cell }`. The module doc states the locking
contract for task 13.

- [x] Write tests: for each of INV-0..3, one timeline that fires and one legal one; an indeterminate write is never older; an op ending between `snapshot` and the read's start does not count; the F12 shape with non-overlapping writes fires INV-3. Added from the advisor's review: a wid issued after the snapshot; F12 with overlapping writes fires nothing (the known gap); INV-3 against an in-flight or indeterminate `u`; INV-0 values do not move the floors; equal times are not "before"; an empty write panics; write status.
- [x] Run them and confirm the failure. (They failed to compile; after the code, ten guarded breaks, one per rule, each failed a test.)
- [x] Write the code.
- [x] Run verify.
- [x] Commit `feat: check reads with streaming invariants [SCYLLADB-4519]`.

## Task 12 — keys and history file (T6)

**Files:** Create: `src/bin/cql-stress-sc-verify/history.rs`, `.../keys.rs`, `.../history_test.jsonl` (the golden file sits next to the code, like the repo's other fixtures); Modify: `Cargo.toml` (optional `serde_json` in the feature; it was a build dependency only)

**Internals:** `struct RowKey { pk, gen, ck }`; `struct GenMinter` (`start_ms × 2^20 + n`;
`next(now_ms) -> Option<i64>` returns `None` when a millisecond's 2^20 are used up and
the clock has not moved on; `new` refuses a clock before 2004, where gens could approach the
bulk range). No pk pool type: the `seq` population already wraps around and is thread-safe.
`enum OpRecord { Write { client, wid, mask, start_ns, status }, Read { client, start_ns,
end_ns, seen } }`; `struct CheckFile` with `create`, `append_row(key, cells, ops) -> key`,
`rows`, `finish`. It writes the `# key …` line, then call and return lines, with no return
for an indeterminate write; `gen` is a string; an undecodable cell is `-1`.

- [x] Write tests: gen ≥ 2^60 and strictly increasing across a re-base; pk wraps; one known row written equals the golden file byte for byte.
- [x] Run them and confirm the failure. (They failed to compile; after the code, five guarded breaks, covering an indeterminate write's return line, undecodable as null, `gen` unquoted, a non-strict re-base and no 2004 guard, each failed a test.)
- [x] Write the code.
- [x] Run verify.
- [x] Commit `feat: mint row keys and write v2 history files [SCYLLADB-4519]`.

## Task 13a — one checked operation (T6)

**Files:** Modify: `src/bin/cql-stress-sc-verify/ops.rs`, `.../main.rs`

**Internals:** `fn write_query(profile, mask, ttl)`: a full-row `INSERT` for all cells, a
partial `UPDATE` otherwise, with `USING TTL n` when `ttl > 0`; `struct Statements` prepared
once at start-up (the read and one write per non-empty mask, up to 255 with 8 cells);
`write(session, key, mask, wid, unavailable_is_fail)` and `read(session, key) -> Vec<Seen>`
(no row → all `Null`; strict `decode`), both returning `OpError { class: Failure, message }`.
`main` prepares the `Statements` at start-up instead of only the read.

- [x] Write tests: the four query shapes; against an ordinary keyspace (CI runs cargo tests
  on the plain node): absent → all null, INSERT then partial UPDATE read back as the
  expected wids, a foreign value → `Undecodable`.
- [x] Run them and confirm the failure. (They failed to compile; after the code, binding the
  UPDATE's key values first failed the DB test.)
- [x] Write the code.
- [x] Run verify.
- [x] Commit `feat: execute checked reads and writes [SCYLLADB-4519]`.

## Task 13 — the slot loop (T6; 13b in the log)

**Files:** Create: `src/bin/cql-stress-sc-verify/slot.rs`; Modify: `main.rs`

**Internals:** a shared `Checked` (session, `Statements`, settings, pk distribution,
`Mutex<GenMinter>`, process clock, stop flag, `ExitCode` where the highest code wins,
workload-error counter). `run_slot(i)`: a ticker at phase `i × interval / slots`
(`MissedTickBehavior::Skip`) advances a `watch` tick counter; the first row is checked
absent (start-up check 4, else exit 2); then rounds until stopped. A round: `Mutex<Round>`
(`RowState`, `OpRecord`s, a `Budget` that refills lazily on a new tick and starts empty, so
a new row waits for the next tick, counters, max gap) and `clients_per_row` client futures
(`join_all` = drain). A client marks the tick seen, then under the lock checks
`should_stop` and takes a start; it waits on `changed()` when the budget is empty. A write
takes its start time, `begin_write`, sends, takes its end time, then locks and records. A
read snapshots, unlocks, takes its start time, sends, takes its end time, then locks,
checks and records. The sweep is client `clients_per_row`, retried with backoff. Sealed rows
go over an mpsc channel. `main` (`--mode verify`) stops at `--duration`, awaits the slots
(a panic → exit 3), and exits with the highest code (a violation → 1). `--ttl` must cover 10
of the longest rounds. `history` keeps its dead-code allow until task 14.

- [x] Write tests: `should_stop` for each `stop_reason`; the burst budget gives 16 starts per tick.
- [x] Run them and confirm the failure. (They failed to compile; after the code, five guarded breaks each failed a test: an accumulating budget, a new row bursting at once, `end` before the row's own limits, a full mask as a partial update, exit codes overwritten.)
- [x] Write the code; `--mode verify` runs slots until `--duration`.
- [x] Remove the temporary `#[allow(dead_code)]` on the modules in `main.rs` (all except `history`, whose `CheckFile` is used from task 14).
- [x] Run verify; a 10 s compose run exits 0. (128 rows of 41 operations with `--ops-per-gen 40`, 0 violations, every sweep ok; the schema pytest now runs real slots.)
- [x] Commit `feat: run checked slots in bursts [SCYLLADB-4519]`.

## Task 14 — rows.jsonl and check files on disk (T6)

**Files:** Modify: `slot.rs`, `history.rs`, `main.rs`; Modify: `tools/test_cs_sc_verify.py`, `tools/cql-stress-cassandra-stress-ci.py`

**Internals:** `struct Recorder` in `history.rs` (`new(dir, cells, check_rows,
check_age)`, `record(&SealedRow) -> Option<archive dir>`, `finish`): creates `sealed/` and
`archive/`, continues the `sealed/<seq>` numbering after a restart, appends to
`rows.jsonl` (flushed per row; `gen` and `file` as strings; `verdict` is `violation` or
`unchecked`), rotates on `--check-rows`/`--check-age`. A row with a violation closes its
check file at once and copies it to `archive/<seq>/<seq>.jsonl`. `main` records every
sealed row; a failure to record is exit 3. The pytest helper passes `--history-dir`.

- [x] Write pytest `run_verify_quiet`: 60 s, `--checker off`; every row has ≥ 150 ops, `verdict` `unchecked`, 0 violations.
- [x] Run it and confirm the failure. (The Recorder unit tests failed to compile first; the pytest was written after the code.)
- [x] Write the code.
- [x] Run verify and the pytest. (60 s on the SC node: 160 rows, 128 full at 201 operations, 12.9 s each, read share 0.503, 0 violations.)
- [x] Commit `feat: record sealed rows and check files [SCYLLADB-4519]`.

## Task 15 — report and SCV lines (T7)

**Files:** Create: `src/bin/cql-stress-sc-verify/report.rs`; Modify: `main.rs`, `slot.rs`

**Internals:** `enum Scv` (serde, tagged `t`): `Start`, `Violation` (printed at seal with its
archive and the detection time `wall_ms`), `Stats(IntervalStats)`, `End`; `gen` and
`gen_base` as strings, `cell` as `c<n>`. `struct Stats` shared by the slots: hdrhistograms
in microseconds (read, write, scheduling delay; per interval and total; successful operations
only), interval counters for ops, writes, indeterminate writes. The slot ticker records how
late each tick ran. `struct Report` (totals) is written to `report.json` atomically (a temp
file, then rename) every `--report-interval` and at exit with `exit`. `main`'s consumer is a
`select!` over sealed rows and the report interval.

- [x] Write tests: each line's JSON shape; `gen_base` is a string.
- [x] Run them and confirm the failure. (They failed to compile.)
- [x] Write the code.
- [x] Run verify; the compose run prints `start`, `stats` and `end`. (12 s: 512 checked ops/s = 32 slots × 16, p99 about 5 ms, scheduling delay p99 1.9 ms, `report.json` with `exit: 0`.)
- [x] Commit `feat: report sc-verify progress as SCV lines [SCYLLADB-4519]`.

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
