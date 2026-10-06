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

use anyhow::{Context, Result};
use serde::Serialize;

use crate::invariants::{Seen, WriteStatus};
use crate::keys::RowKey;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::invariants::{Seen, WriteStatus};

    const GOLDEN: &str = include_str!("history_test.jsonl");

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
