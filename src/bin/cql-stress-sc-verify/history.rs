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
        )?;
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
        serde_json::to_writer(&mut self.out, line)?;
        self.out.write_all(b"\n")?;
        Ok(())
    }
}

/// One line of `rows.jsonl` (spec §14.3).
#[derive(Serialize)]
struct RowLine<'a> {
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
    stop_reason: &'a str,
    invariants: &'a str,
    sweep: &'a str,
    verdict: &'a str,
}

/// Writes sealed rows under `--history-dir`: their histories into check files
/// (`sealed/<seq>.jsonl`, rotated by `--check-rows` and `--check-age`), a line per row into
/// `rows.jsonl`, and a copy of every check file that holds a violation into `archive/<seq>/`.
pub struct Recorder {
    dir: PathBuf,
    cells: usize,
    check_rows: usize,
    check_age: Duration,
    rows: BufWriter<File>,
    open: Option<(CheckFile, u64, Instant)>,
    next_seq: u64,
}

impl Recorder {
    pub fn new(dir: &Path, cells: usize, check_rows: usize, check_age: Duration) -> Result<Self> {
        let sealed = dir.join("sealed");
        std::fs::create_dir_all(&sealed)
            .with_context(|| format!("Failed to create {}", sealed.display()))?;
        std::fs::create_dir_all(dir.join("archive"))?;
        // A restarted process continues the numbering, so it never overwrites the evidence
        // of the previous one.
        let next_seq = std::fs::read_dir(&sealed)?
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
        let rows = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join("rows.jsonl"))
            .with_context(|| format!("Failed to open {}", dir.join("rows.jsonl").display()))?;
        Ok(Self {
            dir: dir.to_owned(),
            cells,
            check_rows,
            check_age,
            rows: BufWriter::new(rows),
            open: None,
            next_seq,
        })
    }

    /// Records a sealed row. Returns the archive directory, relative to `--history-dir`, when
    /// the row had a violation: its check file is closed and archived at once.
    pub fn record(&mut self, row: &SealedRow) -> Result<Option<String>> {
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
            // Milestone 1 runs no checker: a row is a violation or unchecked (spec §4.3).
            verdict: if violated { "violation" } else { "unchecked" },
        };
        serde_json::to_writer(&mut self.rows, &line)?;
        self.rows.write_all(b"\n")?;
        self.rows.flush()?;

        if violated {
            return self.close(true);
        }
        if full {
            self.close(false)?;
        }
        Ok(None)
    }

    /// Closes the open check file and flushes `rows.jsonl`.
    pub fn finish(mut self) -> Result<()> {
        self.close(false)?;
        self.rows.flush()?;
        Ok(())
    }

    fn close(&mut self, archive: bool) -> Result<Option<String>> {
        let Some((file, seq, _)) = self.open.take() else {
            return Ok(None);
        };
        let path = file.finish()?;
        if !archive {
            return Ok(None);
        }
        let relative = format!("archive/{seq}");
        let dir = self.dir.join(&relative);
        std::fs::create_dir_all(&dir)?;
        std::fs::copy(&path, dir.join(format!("{seq}.jsonl")))
            .with_context(|| format!("Failed to archive {}", path.display()))?;
        Ok(Some(relative))
    }

    fn sealed_path(&self, seq: u64) -> PathBuf {
        self.dir.join("sealed").join(format!("{seq}.jsonl"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::invariants::{Inv, Seen, Violation, WriteStatus};
    use crate::slot::StopReason;

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
            violations: vec![
                Violation {
                    kind: Inv::Inv1,
                    cell: 0
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
        let mut recorder = Recorder::new(&dir, 1, 2, Duration::from_secs(300)).unwrap();
        for pk in 0..3 {
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
        let mut again = Recorder::new(&dir, 1, 2, Duration::from_secs(300)).unwrap();
        again.record(&sealed(9, 0)).unwrap();
        again.finish().unwrap();
        assert!(dir.join("sealed/2.jsonl").exists());

        let rows = lines(&dir.join("rows.jsonl"));
        assert_eq!(rows.len(), 4, "rows.jsonl is appended to, never truncated");
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
        let mut recorder = Recorder::new(&dir, 1, 50, Duration::from_secs(300)).unwrap();
        recorder.record(&sealed(0, 0)).unwrap();
        let archive = recorder.record(&sealed(1, 2)).unwrap();
        assert_eq!(archive.as_deref(), Some("archive/0"));
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
