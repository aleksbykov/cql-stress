//! Check files: the histories of sealed rows in the v2 format porcupine_checker reads
//! (spec §14.1).
//!
//! One JSON object per event: an operation's start (`call`) and its end (`return`), paired
//! by `id`. An indeterminate write has no return line, which the checker reads as "may have
//! been applied at any time after it started". Each row begins with a comment line naming
//! its key, so a check file on its own says which row every `key` is.

use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use serde::Serialize;

use crate::invariants::{Seen, WriteStatus};
use crate::keys::RowKey;
use crate::slot::SealedRow;

/// A cell that does not decode is written as this wid, which is never issued, so the
/// checker cannot explain the read either.
const UNDECODABLE: i64 = -1;

/// One recorded operation of a row. A failed read is never recorded (spec §9).
#[derive(Debug, Clone)]
pub enum OpRecord {
    Write {
        client: usize,
        wid: u64,
        mask: u8,
        start_ns: u64,
        status: WriteStatus,
    },
    Read {
        client: usize,
        start_ns: u64,
        end_ns: u64,
        seen: Vec<Seen>,
    },
}

#[derive(Serialize)]
struct Line {
    id: u64,
    client_id: usize,
    kind: &'static str,
    op: &'static str,
    key: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    cells: Option<Vec<Option<i64>>>,
    time_ns: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<&'static str>,
}

/// An open check file. `id`s are unique within the file, `key`s count the rows in it.
pub struct CheckFile {
    path: PathBuf,
    out: BufWriter<File>,
    rows: usize,
    next_id: u64,
}

impl CheckFile {
    pub fn create(path: &Path) -> Result<Self> {
        let file = File::create(path)
            .with_context(|| format!("Failed to create check file {}", path.display()))?;
        Ok(Self {
            path: path.to_owned(),
            out: BufWriter::new(file),
            rows: 0,
            next_id: 1,
        })
    }

    pub fn rows(&self) -> usize {
        self.rows
    }

    /// Appends a sealed row with `cells` cells, and returns its `key` in this file.
    pub fn append_row(&mut self, row: &RowKey, cells: usize, ops: &[OpRecord]) -> Result<usize> {
        let key = self.rows;
        // `gen` is above 2^53, so it is a string for JSON readers that use doubles.
        writeln!(
            self.out,
            "# key {key} pk {} gen \"{}\" ck {}",
            row.pk, row.gen, row.ck
        )
        .with_context(|| format!("Failed to write check file {}", self.path.display()))?;
        for op in ops {
            let id = self.next_id;
            self.next_id += 1;
            let line = |client_id, kind, op, cells, time_ns, status| Line {
                id,
                client_id,
                kind,
                op,
                key,
                cells,
                time_ns,
                status,
            };
            match op {
                OpRecord::Write {
                    client,
                    wid,
                    mask,
                    start_ns,
                    status,
                } => {
                    let written = (0..cells)
                        .map(|cell| (mask & (1 << cell) != 0).then_some(*wid as i64))
                        .collect();
                    self.write(&line(
                        *client,
                        "call",
                        "write",
                        Some(written),
                        *start_ns,
                        None,
                    ))?;
                    let end = match status {
                        WriteStatus::Ok(end) => Some((*end, "ok")),
                        WriteStatus::Fail(end) => Some((*end, "fail")),
                        WriteStatus::InFlight | WriteStatus::Indeterminate => None,
                    };
                    if let Some((end, status)) = end {
                        self.write(&line(*client, "return", "write", None, end, Some(status)))?;
                    }
                }
                OpRecord::Read {
                    client,
                    start_ns,
                    end_ns,
                    seen,
                } => {
                    let cells = seen
                        .iter()
                        .map(|seen| match seen {
                            Seen::Null => None,
                            Seen::Wid(wid) => Some(*wid as i64),
                            Seen::Undecodable => Some(UNDECODABLE),
                        })
                        .collect();
                    self.write(&line(*client, "call", "read", None, *start_ns, None))?;
                    self.write(&line(
                        *client,
                        "return",
                        "read",
                        Some(cells),
                        *end_ns,
                        Some("ok"),
                    ))?;
                }
            }
        }
        self.rows += 1;
        // Flushed per row: rows.jsonl, written next, must never name a row a kill could lose.
        self.out
            .flush()
            .with_context(|| format!("Failed to write check file {}", self.path.display()))?;
        Ok(key)
    }

    /// Flushes and closes the file, and returns its path.
    pub fn finish(mut self) -> Result<PathBuf> {
        self.out
            .flush()
            .with_context(|| format!("Failed to write check file {}", self.path.display()))?;
        Ok(self.path)
    }

    fn write(&mut self, line: &Line) -> Result<()> {
        let result = (|| -> Result<()> {
            serde_json::to_writer(&mut self.out, line)?;
            self.out.write_all(b"\n")?;
            Ok(())
        })();
        result.with_context(|| format!("Failed to write check file {}", self.path.display()))
    }
}

/// A canary (spec §11.4): the first row of `check_file` that has a read, alone as key 0, with
/// the first cell of its earliest read set to a wid no write produced (the row's highest + 1).
/// No order can explain that read, and the search fails right there, so the check costs
/// almost nothing. `None` when no row has a read.
pub fn make_canary(check_file: &str) -> Option<String> {
    let mut blocks: Vec<Vec<&str>> = Vec::new();
    for line in check_file.lines() {
        if line.starts_with("# key ") || blocks.is_empty() {
            blocks.push(Vec::new());
        }
        blocks.last_mut().unwrap().push(line);
    }
    blocks.into_iter().find_map(|block| canary_of(&block))
}

fn canary_of(block: &[&str]) -> Option<String> {
    let header = block.first()?.strip_prefix("# key ")?;
    let (key, rest) = header.split_once(' ')?;
    let events: Vec<serde_json::Value> = block[1..]
        .iter()
        .map(|line| serde_json::from_str(line).ok())
        .collect::<Option<_>>()?;
    let max_wid = events
        .iter()
        .filter(|ev| ev["kind"] == "call" && ev["op"] == "write")
        .flat_map(|ev| ev["cells"].as_array().into_iter().flatten())
        .filter_map(|cell| cell.as_i64())
        .max()
        .unwrap_or(0);
    let returned: Vec<&serde_json::Value> = events
        .iter()
        .filter(|ev| ev["kind"] == "return" && ev["op"] == "read")
        .map(|ev| &ev["id"])
        .collect();
    let earliest = events
        .iter()
        .filter(|ev| ev["kind"] == "call" && ev["op"] == "read" && returned.contains(&&ev["id"]))
        .min_by_key(|ev| ev["time_ns"].as_u64())?;
    let corrupted_id = &earliest["id"];

    let key_field = format!("\"key\":{key},");
    let mut out = format!("# key 0 {rest}\n");
    for (line, ev) in block[1..].iter().zip(&events) {
        let mut line = line.replacen(&key_field, "\"key\":0,", 1);
        if ev["kind"] == "return" && &ev["id"] == corrupted_id {
            let start = line.find("\"cells\":[")? + "\"cells\":[".len();
            let end = start + line[start..].find([',', ']'])?;
            line.replace_range(start..end, &(max_wid + 1).to_string());
        }
        out.push_str(&line);
        out.push('\n');
    }
    Some(out)
}

/// One line of `rows.jsonl` (spec §14.3).
#[derive(Serialize, Debug, Clone)]
pub struct RowLine {
    pk: i64,
    gen: String,
    ck: i32,
    slot: usize,
    file: String,
    key: usize,
    wall_start_ms: u64,
    wall_end_ms: u64,
    ops: usize,
    reads: u64,
    writes_ok: u64,
    writes_indet: u64,
    errors: u64,
    max_gap_ms: u64,
    stop_reason: &'static str,
    invariants: &'static str,
    sweep: &'static str,
    pub verdict: &'static str,
}

/// One line of `expected.jsonl`.
#[derive(Serialize)]
struct ExpectedLine<'a> {
    pk: i64,
    gen: String,
    ck: i32,
    cells: &'a [Vec<Option<u64>>],
    max_wid: u64,
    burned: &'a [u64],
    indeterminate: &'a [u64],
}

/// A row of a closed check file whose verdict waits for the checker.
#[derive(Debug, Clone)]
pub struct PendingRow {
    pub line: RowLine,
    /// An invariant fired: the verdict is `violation`, whatever the checker says.
    pub violated: bool,
}

/// A closed check file handed to the checker, with its rows in key order.
#[derive(Debug)]
pub struct ClosedFile {
    pub seq: u64,
    pub path: PathBuf,
    pub rows: Vec<PendingRow>,
}

/// What recording one sealed row did.
#[derive(Debug, Default)]
pub struct Recorded {
    /// The archive directory, relative to `--history-dir`, when the row had a violation: its
    /// check file is closed and copied there at once.
    pub archive: Option<String>,
    /// With `--checker on`, the check file the row closed.
    pub closed: Option<ClosedFile>,
}

/// Writes sealed rows under `--history-dir`: their histories into check files
/// (`sealed/<seq>.jsonl`, rotated by `--check-rows` and `--check-age`), a line per row into
/// `rows.jsonl`, and a copy of every check file that holds a violation into `archive/<seq>/`.
///
/// With `--checker off`, a row's line is written when it seals. With `--checker on`, the
/// lines wait in the closed file for the checker's verdicts, and the caller writes them.
pub struct Recorder {
    dir: PathBuf,
    cells: usize,
    check_rows: usize,
    check_age: Duration,
    defer: bool,
    rows: BufWriter<File>,
    expected: BufWriter<File>,
    open: Option<(CheckFile, u64, Instant)>,
    pending: Vec<PendingRow>,
    next_seq: u64,
}

impl Recorder {
    pub fn new(
        dir: &Path,
        cells: usize,
        check_rows: usize,
        check_age: Duration,
        defer: bool,
    ) -> Result<Self> {
        let sealed = dir.join("sealed");
        std::fs::create_dir_all(&sealed)
            .with_context(|| format!("Failed to create {}", sealed.display()))?;
        std::fs::create_dir_all(dir.join("archive"))
            .with_context(|| format!("Failed to create {}", dir.join("archive").display()))?;
        // A restarted process continues the numbering, so it never overwrites the evidence
        // of the previous one.
        let next_seq = std::fs::read_dir(&sealed)
            .with_context(|| format!("Failed to list {}", sealed.display()))?
            .filter_map(|entry| {
                entry
                    .ok()?
                    .path()
                    .file_stem()?
                    .to_str()?
                    .parse::<u64>()
                    .ok()
            })
            .max()
            .map_or(0, |seq| seq + 1);
        let append = |name: &str| {
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(dir.join(name))
                .with_context(|| format!("Failed to open {}", dir.join(name).display()))
        };
        let rows = append("rows.jsonl")?;
        let expected = append("expected.jsonl")?;
        Ok(Self {
            dir: dir.to_owned(),
            cells,
            check_rows,
            check_age,
            defer,
            rows: BufWriter::new(rows),
            expected: BufWriter::new(expected),
            open: None,
            pending: Vec::new(),
            next_seq,
        })
    }

    /// Records a sealed row.
    pub fn record(&mut self, row: &SealedRow) -> Result<Recorded> {
        if self.open.is_none() {
            let seq = self.next_seq;
            self.next_seq += 1;
            let file = CheckFile::create(&self.sealed_path(seq))?;
            self.open = Some((file, seq, Instant::now()));
        }
        let (file, seq, opened) = self.open.as_mut().unwrap();
        let (seq, opened) = (*seq, *opened);
        let key = file.append_row(&row.key, self.cells, &row.ops)?;
        let full = file.rows() >= self.check_rows || opened.elapsed() >= self.check_age;

        let violated = !row.violations.is_empty();
        let line = RowLine {
            pk: row.key.pk,
            gen: row.key.gen.to_string(),
            ck: row.key.ck,
            slot: row.slot,
            file: seq.to_string(),
            key,
            wall_start_ms: row.wall_start_ms,
            wall_end_ms: row.wall_end_ms,
            ops: row.ops.len(),
            reads: row.reads,
            writes_ok: row.writes_ok,
            writes_indet: row.writes_indet,
            errors: row.errors,
            max_gap_ms: row.max_gap_ms,
            stop_reason: row.stop_reason.as_str(),
            invariants: if violated { "violation" } else { "ok" },
            sweep: if row.sweep_ok { "ok" } else { "incomplete" },
            // Without the checker a row is a violation or unchecked (spec §4.3).
            verdict: if violated { "violation" } else { "unchecked" },
        };
        self.write_expected(row)?;
        if self.defer {
            self.pending.push(PendingRow { line, violated });
        } else {
            self.write_line(&line)?;
        }

        if violated {
            return self.close(true);
        }
        if full {
            return self.close(false);
        }
        Ok(Recorded::default())
    }

    /// Appends the row's expected final state to `expected.jsonl` (spec §12.1).
    fn write_expected(&mut self, row: &SealedRow) -> Result<()> {
        let line = ExpectedLine {
            pk: row.key.pk,
            gen: row.key.gen.to_string(),
            ck: row.key.ck,
            cells: &row.expected.cells,
            max_wid: row.expected.max_wid,
            burned: &row.expected.burned,
            indeterminate: &row.expected.indeterminate,
        };
        let out = &mut self.expected;
        let result = (|| -> Result<()> {
            serde_json::to_writer(&mut *out, &line)?;
            out.write_all(b"\n")?;
            out.flush()?;
            Ok(())
        })();
        result.context("Failed to write expected.jsonl")
    }

    /// Writes a row's final line to `rows.jsonl`.
    pub fn write_line(&mut self, line: &RowLine) -> Result<()> {
        let rows = &mut self.rows;
        let result = (|| -> Result<()> {
            serde_json::to_writer(&mut *rows, line)?;
            rows.write_all(b"\n")?;
            rows.flush()?;
            Ok(())
        })();
        result.context("Failed to write rows.jsonl")
    }

    /// Closes the open check file and returns it, with `--checker on`.
    pub fn finish(&mut self) -> Result<Option<ClosedFile>> {
        let closed = self.close(false)?.closed;
        self.rows.flush().context("Failed to write rows.jsonl")?;
        Ok(closed)
    }

    /// Moves a check file into `archive/<seq>/` and returns that directory, relative to
    /// `--history-dir`. A file already copied there (a violation) only leaves `sealed/`.
    pub fn archive(&self, seq: u64, path: &Path) -> Result<String> {
        let relative = format!("archive/{seq}");
        let dir = self.dir.join(&relative);
        std::fs::create_dir_all(&dir)
            .with_context(|| format!("Failed to create {}", dir.display()))?;
        let target = dir.join(format!("{seq}.jsonl"));
        if target.exists() {
            std::fs::remove_file(path)
        } else {
            std::fs::rename(path, &target)
        }
        .with_context(|| format!("Failed to archive {}", path.display()))?;
        Ok(relative)
    }

    /// Deletes a check file whose rows were all ok, with the directory the checker made.
    pub fn discard(&self, seq: u64, path: &Path) -> Result<()> {
        std::fs::remove_file(path)
            .with_context(|| format!("Failed to delete {}", path.display()))?;
        let dir = self.dir.join(format!("archive/{seq}"));
        if dir.exists() {
            std::fs::remove_dir_all(&dir)
                .with_context(|| format!("Failed to delete {}", dir.display()))?;
        }
        Ok(())
    }

    fn close(&mut self, archive: bool) -> Result<Recorded> {
        let Some((file, seq, _)) = self.open.take() else {
            return Ok(Recorded::default());
        };
        let path = file.finish()?;
        let mut recorded = Recorded::default();
        if archive {
            let relative = format!("archive/{seq}");
            let dir = self.dir.join(&relative);
            std::fs::create_dir_all(&dir)
                .with_context(|| format!("Failed to create {}", dir.display()))?;
            std::fs::copy(&path, dir.join(format!("{seq}.jsonl")))
                .with_context(|| format!("Failed to archive {}", path.display()))?;
            recorded.archive = Some(relative);
        }
        if self.defer {
            recorded.closed = Some(ClosedFile {
                seq,
                path,
                rows: std::mem::take(&mut self.pending),
            });
        }
        Ok(recorded)
    }

    fn sealed_path(&self, seq: u64) -> PathBuf {
        self.dir.join("sealed").join(format!("{seq}.jsonl"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::invariants::{Expected, Inv, Seen, Violation, WriteStatus};
    use crate::slot::{Detected, StopReason};

    /// The v2 contract with porcupine_validator, which keeps a byte-for-byte copy as
    /// `testdata/v2_cql_stress_golden.jsonl` and checks it: change both together.
    const GOLDEN: &str = include_str!("history_test.jsonl");

    fn sealed(pk: i64, violations: usize) -> SealedRow {
        let read = OpRecord::Read {
            client: 0,
            start_ns: 10,
            end_ns: 20,
            seen: vec![Seen::Null],
        };
        SealedRow {
            key: RowKey {
                pk,
                gen: (1 << 60) + pk,
                ck: 0,
            },
            slot: 3,
            wall_start_ms: 100,
            wall_end_ms: 200,
            ops: vec![read.clone(), read],
            reads: 2,
            writes_ok: 0,
            writes_indet: 0,
            errors: 1,
            max_gap_ms: 7,
            stop_reason: StopReason::Ops,
            expected: Expected {
                cells: vec![vec![None, Some(3)]],
                max_wid: 4,
                burned: vec![2],
                indeterminate: vec![3],
            },
            violations: vec![
                Detected {
                    violation: Violation {
                        kind: Inv::Inv1,
                        cell: 0
                    },
                    wall_ms: 150,
                };
                violations
            ],
            sweep_ok: true,
        }
    }

    fn lines(path: &Path) -> Vec<String> {
        std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect()
    }

    #[test]
    fn recorder_rotates_check_files_and_writes_rows_test() {
        let dir = std::env::temp_dir().join(format!("sc-verify-recorder-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let mut recorder = Recorder::new(&dir, 1, 2, Duration::from_secs(300), false).unwrap();
        recorder.record(&sealed(0, 0)).unwrap();
        // On disk before the file closes: rows.jsonl never names a row that a kill could lose.
        assert_eq!(
            lines(&dir.join("sealed/0.jsonl")).len(),
            5,
            "the key line and 2 reads"
        );
        for pk in 1..3 {
            recorder.record(&sealed(pk, 0)).unwrap();
        }
        recorder.finish().unwrap();

        // 2 rows per file: 0.jsonl is full, 1.jsonl was closed at the end.
        assert_eq!(
            lines(&dir.join("sealed/0.jsonl"))
                .iter()
                .filter(|l| l.starts_with("# key"))
                .count(),
            2
        );
        assert_eq!(
            lines(&dir.join("sealed/1.jsonl"))[0],
            format!("# key 0 pk 2 gen \"{}\" ck 0", (1i64 << 60) + 2)
        );
        assert!(
            !dir.join("archive").join("0").exists(),
            "nothing to archive"
        );

        // A restarted process continues the numbering.
        let mut again = Recorder::new(&dir, 1, 2, Duration::from_secs(300), false).unwrap();
        again.record(&sealed(9, 0)).unwrap();
        again.finish().unwrap();
        assert!(dir.join("sealed/2.jsonl").exists());

        let rows = lines(&dir.join("rows.jsonl"));
        assert_eq!(rows.len(), 4, "rows.jsonl is appended to, never truncated");
        let expected = lines(&dir.join("expected.jsonl"));
        assert_eq!(expected.len(), 4);
        assert_eq!(
            expected[2],
            format!(
                "{{\"pk\":2,\"gen\":\"{}\",\"ck\":0,\"cells\":[[null,3]],\"max_wid\":4,\"burned\":[2],\"indeterminate\":[3]}}",
                (1i64 << 60) + 2
            )
        );
        assert_eq!(
            rows[2],
            format!(
                "{{\"pk\":2,\"gen\":\"{}\",\"ck\":0,\"slot\":3,\"file\":\"1\",\"key\":0,\
                 \"wall_start_ms\":100,\"wall_end_ms\":200,\"ops\":2,\"reads\":2,\"writes_ok\":0,\
                 \"writes_indet\":0,\"errors\":1,\"max_gap_ms\":7,\"stop_reason\":\"ops\",\
                 \"invariants\":\"ok\",\"sweep\":\"ok\",\"verdict\":\"unchecked\"}}",
                (1i64 << 60) + 2
            )
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_violating_row_is_archived_when_it_seals_test() {
        let dir = std::env::temp_dir().join(format!("sc-verify-archive-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let mut recorder = Recorder::new(&dir, 1, 50, Duration::from_secs(300), false).unwrap();
        recorder.record(&sealed(0, 0)).unwrap();
        let recorded = recorder.record(&sealed(1, 2)).unwrap();
        assert_eq!(recorded.archive.as_deref(), Some("archive/0"));
        assert!(
            recorded.closed.is_none(),
            "--checker off hands nothing over"
        );
        // Closed at once, so the evidence is on disk when the row seals.
        let archived = lines(&dir.join("archive/0/0.jsonl"));
        assert_eq!(archived, lines(&dir.join("sealed/0.jsonl")));
        assert_eq!(
            archived.iter().filter(|l| l.starts_with("# key")).count(),
            2
        );
        // The next row opens the next file.
        recorder.record(&sealed(2, 0)).unwrap();
        recorder.finish().unwrap();
        assert!(dir.join("sealed/1.jsonl").exists());
        let rows = lines(&dir.join("rows.jsonl"));
        assert!(
            rows[1].contains("\"invariants\":\"violation\""),
            "{}",
            rows[1]
        );
        assert!(rows[1].contains("\"verdict\":\"violation\""), "{}", rows[1]);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_canary_corrupts_the_first_read_test() {
        let file = r#"# key 0 pk 1 gen "10" ck 0
{"id":1,"client_id":0,"kind":"call","op":"write","key":0,"cells":[1,1],"time_ns":100}
{"id":1,"client_id":0,"kind":"return","op":"write","key":0,"time_ns":200,"status":"ok"}
# key 1 pk 2 gen "11" ck 0
{"id":2,"client_id":0,"kind":"call","op":"write","key":1,"cells":[1,1],"time_ns":100}
{"id":3,"client_id":1,"kind":"call","op":"read","key":1,"time_ns":600}
{"id":3,"client_id":1,"kind":"return","op":"read","key":1,"cells":[1,1],"time_ns":700,"status":"ok"}
{"id":4,"client_id":0,"kind":"call","op":"write","key":1,"cells":[null,2],"time_ns":300}
{"id":4,"client_id":0,"kind":"return","op":"write","key":1,"time_ns":400,"status":"ok"}
{"id":5,"client_id":2,"kind":"call","op":"read","key":1,"time_ns":500}
{"id":5,"client_id":2,"kind":"return","op":"read","key":1,"cells":[1,2],"time_ns":550,"status":"ok"}
{"id":2,"client_id":0,"kind":"return","op":"write","key":1,"time_ns":200,"status":"ok"}
"#;
        // Row 0 has no read, so row 1 is copied as key 0. Its earliest read is id 5 (started
        // at 500); that read's first cell gets wid 3, which no write produced.
        let canary = make_canary(file).unwrap();
        let want = file
            .lines()
            .skip(3)
            .map(|line| {
                line.replace("# key 1 ", "# key 0 ")
                    .replace("\"key\":1", "\"key\":0")
                    .replace(
                        "\"cells\":[1,2],\"time_ns\":550",
                        "\"cells\":[3,2],\"time_ns\":550",
                    )
            })
            .collect::<Vec<_>>()
            .join("\n")
            + "\n";
        assert_eq!(canary, want);

        assert!(
            make_canary(&file.lines().take(3).collect::<Vec<_>>().join("\n")).is_none(),
            "no read"
        );
    }

    /// With `--checker on`, the lines wait for the checker; a closed file carries them.
    #[test]
    fn recorder_hands_closed_files_over_test() {
        let dir = std::env::temp_dir().join(format!("sc-verify-defer-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let mut recorder = Recorder::new(&dir, 1, 2, Duration::from_secs(300), true).unwrap();
        assert!(recorder.record(&sealed(0, 0)).unwrap().closed.is_none());
        let closed = recorder.record(&sealed(1, 0)).unwrap().closed.unwrap();
        assert_eq!((closed.seq, closed.rows.len()), (0, 2));
        assert_eq!(closed.path, dir.join("sealed/0.jsonl"));
        assert_eq!(
            std::fs::read_to_string(dir.join("rows.jsonl")).unwrap(),
            "",
            "lines wait"
        );

        let mut line = closed.rows[1].line.clone();
        line.verdict = "ok";
        recorder.write_line(&line).unwrap();
        assert!(lines(&dir.join("rows.jsonl"))[0].contains("\"pk\":1"));

        // A violation still closes and copies the file at once, and hands it over too.
        let recorded = recorder.record(&sealed(2, 1)).unwrap();
        assert_eq!(recorded.archive.as_deref(), Some("archive/1"));
        let closed = recorded.closed.unwrap();
        assert!(closed.rows[0].violated);
        // Archiving it again only removes it from sealed/; the evidence stays.
        assert_eq!(
            recorder.archive(closed.seq, &closed.path).unwrap(),
            "archive/1"
        );
        assert!(!closed.path.exists() && dir.join("archive/1/1.jsonl").exists());

        recorder.record(&sealed(3, 0)).unwrap();
        let last = recorder.finish().unwrap().unwrap();
        recorder.discard(last.seq, &last.path).unwrap();
        assert!(!last.path.exists());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn rows_are_written_in_the_v2_format_test() {
        let dir = std::env::temp_dir().join(format!("sc-verify-history-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("0.jsonl");
        let mut file = CheckFile::create(&path).unwrap();

        let gen = 1_878_232_883_200_000_000;
        let ops = [
            OpRecord::Write {
                client: 0,
                wid: 1,
                mask: 0b111,
                start_ns: 1000,
                status: WriteStatus::Ok(1900),
            },
            OpRecord::Write {
                client: 1,
                wid: 2,
                mask: 0b101,
                start_ns: 1500,
                status: WriteStatus::Indeterminate,
            },
            OpRecord::Read {
                client: 2,
                start_ns: 1600,
                end_ns: 2100,
                seen: vec![Seen::Wid(1); 3],
            },
            OpRecord::Write {
                client: 3,
                wid: 3,
                mask: 0b010,
                start_ns: 2200,
                status: WriteStatus::Fail(2300),
            },
            OpRecord::Read {
                client: 8,
                start_ns: 3000,
                end_ns: 3100,
                seen: vec![Seen::Wid(2), Seen::Undecodable, Seen::Null],
            },
        ];
        let key = RowKey { pk: 7, gen, ck: 0 };
        assert_eq!(file.append_row(&key, 3, &ops).unwrap(), 0);
        let empty = [OpRecord::Read {
            client: 0,
            start_ns: 4000,
            end_ns: 4100,
            seen: vec![Seen::Null; 3],
        }];
        let key = RowKey {
            pk: 8,
            gen: gen + 1,
            ck: 0,
        };
        assert_eq!(file.append_row(&key, 3, &empty).unwrap(), 1);
        assert_eq!(file.rows(), 2);
        file.finish().unwrap();

        assert_eq!(std::fs::read_to_string(&path).unwrap(), GOLDEN);
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
