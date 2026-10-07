#! /usr/bin/env python3

"""Integration tests for cql-stress-sc-verify, the verified load for strongly consistent tables.

They need the `strong-consistency` compose node and `cql-stress-sc-verify` on PATH; the
`test_strong_consistency_sc_verify_*` functions in cql-stress-cassandra-stress-ci.py call
them with the shared fixtures, which skip when the node cannot do strong consistency.
"""

import json
import os
import shutil
import subprocess
import time
from pathlib import Path

from test_cs_strong_consistency import UNAVAILABLE_CODE, keyspace_consistency

BINARY = "cql-stress-sc-verify"
# The strongly consistent node of docker/scylla-test/compose.yml.
SC_CONTAINER = os.getenv("SCYLLA_SC_CONTAINER", "scylla_sc_test")


def write_profile(path, keyspace: str, cells="[bigint, text, blob]") -> str:
    path.write_text(
        f"keyspace: {keyspace}\n"
        "replication_factor: 1\n"
        "tablets_initial: 4\n"
        "table: reg\n"
        f"cells: {cells}\n"
        "text_size: 32\n"
        "blob_size: 64\n"
    )
    return str(path)


def sc_verify(node, profile: str, *args: str, duration="1s") -> subprocess.CompletedProcess:
    """Runs a checked stream; its history goes next to the profile."""
    history = Path(profile).parent / "history"
    cmd = [BINARY, "--profile", profile, "--nodes", f"{node.ip}:{node.port}",
           "--mode", "verify", "--duration", duration, "--history-dir", str(history),
           "--checker", "off", *args]
    print(" ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    print(result.stdout, result.stderr, sep="\n")
    return result


def columns(session, keyspace: str, table: str) -> dict:
    rows = session.execute(
        "SELECT column_name, type, kind FROM system_schema.columns "
        "WHERE keyspace_name = %s AND table_name = %s", (keyspace, table))
    return {row.column_name: (row.type, row.kind) for row in rows}


def run_schema(node, session, keyspace: str, tmp_path):
    """Every start creates the schema it needs, and refuses a schema it cannot vouch for."""
    print("\n=== An empty cluster gets the profile's schema ===\n")
    profile = write_profile(tmp_path / "profile.yaml", keyspace)
    result = sc_verify(node, profile)
    assert result.returncode == 0, "start-up failed on an empty cluster"
    assert "Start-up checks passed" in result.stdout
    assert keyspace_consistency(session, keyspace) == "global"
    assert columns(session, keyspace, "reg") == {
        "pk": ("bigint", "partition_key"),
        "gen": ("bigint", "clustering"),
        "ck": ("int", "clustering"),
        "c0": ("bigint", "regular"),
        "c1": ("text", "regular"),
        "c2": ("blob", "regular"),
    }

    print("\n=== A second start over the same schema is fine ===\n")
    assert sc_verify(node, profile).returncode == 0

    print("\n=== A table that differs from the profile exits 2 with the diff ===\n")
    mismatched = write_profile(tmp_path / "mismatched.yaml", keyspace, cells="[bigint, int, blob]")
    result = sc_verify(node, mismatched)
    assert result.returncode == 2, f"expected exit 2, got {result.returncode}"
    assert "c1: table has text (regular), profile says int (regular)" in result.stderr

    print("\n=== An eventually consistent keyspace exits 2 with the diagnostic code ===\n")
    eventual = f"{keyspace}_ec"
    session.execute(f"DROP KEYSPACE IF EXISTS {eventual}")
    session.execute(
        f"CREATE KEYSPACE {eventual} WITH replication = "
        "{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}")
    try:
        result = sc_verify(node, write_profile(tmp_path / "eventual.yaml", eventual))
        assert result.returncode == 2, f"expected exit 2, got {result.returncode}"
        assert UNAVAILABLE_CODE in result.stderr
        assert not columns(session, eventual, "reg"), "no table may go into a non-SC keyspace"
    finally:
        session.execute(f"DROP KEYSPACE IF EXISTS {eventual}")


def run_verify_quiet(node, session, keyspace: str, tmp_path):
    """A quiet 60 s checked run: every row is recorded, unchecked and free of violations."""
    profile = write_profile(tmp_path / "profile.yaml", keyspace)
    result = sc_verify(node, profile, duration="60s")
    assert result.returncode == 0, f"expected exit 0, got {result.returncode}"

    history = tmp_path / "history"
    rows = [json.loads(line) for line in (history / "rows.jsonl").read_text().splitlines()]
    assert rows, "no row was sealed"
    for row in rows:
        assert row["invariants"] == "ok" and row["verdict"] == "unchecked", row
        assert row["sweep"] == "ok", row
        assert isinstance(row["gen"], str) and int(row["gen"]) >= 1 << 60, row

    # A row cut short by the end of the run has fewer; every row that ran to --ops-per-gen
    # (200 started, plus the sweep) is long enough to be worth checking.
    full = [row for row in rows if row["stop_reason"] == "ops"]
    assert full, "no row reached --ops-per-gen in 60 s"
    short = [row for row in full if row["ops"] < 150]
    assert not short, f"rows with fewer than 150 recorded operations: {short}"

    # The read-back finds every row as it was left.
    readback = scv_lines(result.stdout, "readback")
    assert readback and readback[0]["ok"] == readback[0]["rows"] == len(rows), readback

    # Every row can be found in its check file by its key.
    for row in rows:
        check_file = (history / "sealed" / f"{row['file']}.jsonl").read_text()
        assert f'# key {row["key"]} pk {row["pk"]} gen "{row["gen"]}" ck 0' in check_file, row


def run_stale_reads(node, session, keyspace: str, tmp_path):
    """The test-only fault serves stale reads: the invariants must catch them, with evidence."""
    profile = write_profile(tmp_path / "profile.yaml", keyspace)
    result = sc_verify(node, profile, "--fault-stale-reads", "0.2", duration="15s")
    assert result.returncode == 1, f"expected exit 1, got {result.returncode}"

    violations = [json.loads(line[len("SCV "):]) for line in result.stdout.splitlines()
                  if line.startswith('SCV {"t":"violation"')]
    assert violations, "no SCV violation line"
    assert {v["kind"] for v in violations} & {"INV-1", "INV-2"}, violations
    end = [line for line in result.stdout.splitlines() if line.startswith('SCV {"t":"end"')]
    assert end == ['SCV {"t":"end","exit":1}'], end

    # The evidence is on disk: the archived check file holds the violating row.
    history = tmp_path / "history"
    first = violations[0]
    archived = (history / first["archive"] / f"{first['archive'].split('/')[1]}.jsonl").read_text()
    assert f'pk {first["pk"]} gen "{first["gen"]}"' in archived
    rows = [json.loads(line) for line in (history / "rows.jsonl").read_text().splitlines()]
    violated = [row for row in rows if row["verdict"] == "violation"]
    assert violated and all(row["invariants"] == "violation" for row in violated)

    # The fault is in the tool's reads, not in the database: the sweep must read the truth,
    # so the expected state is right and the read-back finds nothing lost.
    readback = scv_lines(result.stdout, "readback")
    assert readback and readback[0]["lost"] == readback[0]["phantom"] == 0, readback


def bulk(node, profile: str, history, *args: str) -> dict:
    """Runs bulk mode and returns its report.json."""
    cmd = [BINARY, "--profile", profile, "--nodes", f"{node.ip}:{node.port}",
           "--mode", "bulk", "--history-dir", str(history), *args]
    print(" ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    print(result.stdout, result.stderr, sep="\n")
    assert result.returncode == 0, f"bulk exited {result.returncode}"
    return json.loads((history / "report.json").read_text())


def run_bulk(node, session, keyspace: str, tmp_path):
    """A preload written with -n reads back with no misses; a range never written misses."""
    profile = write_profile(tmp_path / "profile.yaml", keyspace)
    preload = "seq=1099511627776..1099511629775"   # 2000 pks from 2^40
    written = bulk(node, profile, tmp_path / "write", "--bulk-op", "write", "-n", "2000",
                   "--bulk-pop", preload)
    assert (written["bulk_ops"], written["bulk_errors"]) == (2000, 0), written

    read = bulk(node, profile, tmp_path / "read", "--bulk-op", "read", "-n", "2000",
                "--bulk-pop", preload)
    assert (read["bulk_ops"], read["bulk_misses"]) == (2000, 0), read

    never = bulk(node, profile, tmp_path / "never", "--bulk-op", "read", "-n", "100",
                 "--bulk-pop", "seq=1099600000000..1099600000099")
    assert never["bulk_misses"] == 100, never


def run_both(node, session, keyspace: str, tmp_path):
    """Checked slots and bulk load together: every checked row is checked ok."""
    profile = write_profile(tmp_path / "profile.yaml", keyspace)
    history = tmp_path / "history"
    cmd = [BINARY, "--profile", profile, "--nodes", f"{node.ip}:{node.port}",
           "--mode", "both", "--duration", "20s", "--bulk-threads", "8",
           "--report-interval", "5s", "--history-dir", str(history),
           "--checker-bin", checker_bin(), "--check-age", "5s"]
    print(" ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    print(result.stdout, result.stderr, sep="\n")
    assert result.returncode == 0, f"expected exit 0, got {result.returncode}"
    assert '"mode":"both"' in result.stdout and '"bulk_pop":' in result.stdout

    stats = [json.loads(line[len("SCV "):]) for line in result.stdout.splitlines()
             if line.startswith('SCV {"t":"stats"')]
    assert any(s["verified_ops_s"] > 0 and s["bulk_ops_s"] > 0 for s in stats), stats

    report = json.loads((history / "report.json").read_text())
    assert report["bulk_ops"] > 0 and report["rows"] > 0 and report["violations"] == 0, report
    rows = [json.loads(line) for line in (history / "rows.jsonl").read_text().splitlines()]
    assert rows and all(row["verdict"] == "ok" for row in rows), \
        {row["verdict"] for row in rows}


def checker_bin() -> str:
    """porcupine_checker (porcupine_validator checker-v2) on PATH. A missing checker fails the
    test rather than skipping it, so a CI job without it cannot pass by checking nothing."""
    path = shutil.which("porcupine_checker")
    assert path, "porcupine_checker (porcupine_validator checker-v2) is not on PATH"
    return path


def hanging_checker(tmp_path) -> str:
    """A checker that answers the start-up probe, then hangs on every real file."""
    path = tmp_path / "hanging_checker"
    path.write_text('#!/bin/sh\nif [ -z "$(head -c1)" ]; then exit 2; fi\nsleep 600\n')
    path.chmod(0o755)
    return str(path)


def scv_lines(stdout: str, kind: str) -> list:
    prefix = 'SCV {"t":"' + kind + '"'
    return [json.loads(line[len("SCV "):]) for line in stdout.splitlines()
            if line.startswith(prefix)]


def checked_run(node, profile: str, history, duration: str, *args: str):
    cmd = [BINARY, "--profile", profile, "--nodes", f"{node.ip}:{node.port}",
           "--mode", "verify", "--duration", duration, "--history-dir", str(history), *args]
    print(" ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=900)
    print(result.stdout[-3000:], result.stderr[-3000:], sep="\n")
    return result


def rows_of(history) -> list:
    return [json.loads(line) for line in (history / "rows.jsonl").read_text().splitlines()]


def run_checker(node, session, keyspace: str, tmp_path):
    """--checker on: every quiet row is checked ok, and an ok file is deleted."""
    profile = write_profile(tmp_path / "profile.yaml", keyspace)
    history = tmp_path / "history"
    result = checked_run(node, profile, history, "30s", "--checker-bin", checker_bin(),
                         "--check-age", "5s", "--readback", "off")
    assert result.returncode == 0, f"expected exit 0, got {result.returncode}"
    rows = rows_of(history)
    assert rows and all(row["verdict"] == "ok" for row in rows), \
        {row["verdict"] for row in rows}
    checked = scv_lines(result.stdout, "checked")
    assert sum(c["ok"] for c in checked) == len(rows), checked
    assert not list((history / "sealed").iterdir()), "an all-ok check file is deleted"
    assert not list((history / "archive").iterdir()), "nothing to keep"


def run_hanging_checker(node, session, keyspace: str, tmp_path):
    """A checker that hangs: rows are unknown, and the checked stream does not slow down."""
    profile = write_profile(tmp_path / "profile.yaml", keyspace)

    def ops_per_s(result):
        stats = scv_lines(result.stdout, "stats")[1:-1]  # whole intervals only
        return sum(s["verified_ops_s"] for s in stats) / len(stats)

    base = checked_run(node, profile, tmp_path / "off", "20s", "--checker", "off",
                       "--report-interval", "2s")
    assert base.returncode == 0
    hung = checked_run(node, profile, tmp_path / "hung", "20s", "--checker-bin",
                       hanging_checker(tmp_path), "--check-age", "3s", "--checker-timeout", "4s",
                       "--checker-deadline", "5s", "--report-interval", "2s", "--readback", "off")
    assert hung.returncode == 0, f"expected exit 0, got {hung.returncode}"
    rows = rows_of(tmp_path / "hung")
    assert rows and {row["verdict"] for row in rows} <= {"unknown", "skipped"}, \
        {row["verdict"] for row in rows}
    assert any(row["verdict"] == "unknown" for row in rows)
    assert abs(ops_per_s(hung) - ops_per_s(base)) <= 0.05 * ops_per_s(base), \
        (ops_per_s(hung), ops_per_s(base))


def run_queue_full(node, session, keyspace: str, tmp_path):
    """A full queue never waits: the file is archived unchecked and its rows are skipped."""
    profile = write_profile(tmp_path / "profile.yaml", keyspace)
    history = tmp_path / "history"
    result = checked_run(node, profile, history, "15s", "--checker-bin", hanging_checker(tmp_path),
                         "--checker-workers", "1", "--queue-max", "1", "--check-rows", "2",
                         "--ops-per-gen", "20", "--checker-timeout", "30s",
                         "--checker-deadline", "2s", "--readback", "off")
    assert result.returncode == 0, f"expected exit 0, got {result.returncode}"
    skipped = scv_lines(result.stdout, "skipped")
    assert skipped, "no SCV skipped line"
    rows = rows_of(history)
    assert sum(row["verdict"] == "skipped" for row in rows) >= sum(s["rows"] for s in skipped)
    archived = list((history / "archive").glob("*/*.jsonl"))
    assert archived, "a skipped file is kept in archive/"


def lenient_checker(tmp_path) -> str:
    """A broken checker: it answers the probe, then says ok to every row of every file."""
    path = tmp_path / "lenient_checker"
    path.write_text(
        '#!/bin/sh\ninput=$(cat)\n[ -z "$input" ] && exit 2\n'
        'printf "%s\\n" "$input" | '
        'sed -n \'s/^# key \\([0-9]*\\) .*/{"key":\\1,"result":"ok","ops":1,"ms":0}/p\'\n'
        'exit 0\n')
    path.chmod(0o755)
    return str(path)


def run_canaries(node, session, keyspace: str, tmp_path):
    """Every canary is rejected by the real checker, and no real row is touched by them."""
    profile = write_profile(tmp_path / "profile.yaml", keyspace)
    history = tmp_path / "history"
    result = checked_run(node, profile, history, "60s", "--checker-bin", checker_bin(),
                         "--canary-every", "1", "--check-age", "10s")
    assert result.returncode == 0, f"expected exit 0, got {result.returncode}"
    canaries = scv_lines(result.stdout, "canary")
    assert canaries, "no canary was checked"
    assert all(c["result"] == "illegal" for c in canaries), canaries
    report = json.loads((history / "report.json").read_text())
    assert report["canaries_ok"] == len(canaries) and report["canaries_failed"] == 0, report
    rows = rows_of(history)
    assert rows and all(row["verdict"] == "ok" for row in rows), \
        {row["verdict"] for row in rows}
    assert not list((history / "sealed").iterdir()), "checked canaries are deleted"
    readback = scv_lines(result.stdout, "readback")
    assert readback and readback[0]["ok"] == readback[0]["rows"] == len(rows), readback


def run_broken_checker(node, session, keyspace: str, tmp_path):
    """A checker that accepts everything accepts a canary too: verifier-broken, exit 1."""
    profile = write_profile(tmp_path / "profile.yaml", keyspace)
    history = tmp_path / "history"
    result = checked_run(node, profile, history, "20s", "--checker-bin", lenient_checker(tmp_path),
                         "--canary-every", "1", "--check-age", "5s", "--readback", "off")
    assert result.returncode == 1, f"expected exit 1, got {result.returncode}"
    canaries = scv_lines(result.stdout, "canary")
    assert canaries and all(c["result"] == "ok" for c in canaries), canaries
    assert "verifier-broken" in result.stderr
    assert list((history / "archive").glob("canary-*/canary-*.jsonl")), "the canary is kept"


def run_deleted_row(node, session, keyspace: str, tmp_path):
    """A row deleted behind the tool's back is found lost by the read-back: exit 1.

    It deletes through `cqlsh` inside the compose node's container (SCYLLA_SC_CONTAINER), so it
    needs `docker`. Without it the test fails rather than skips: a skip would hide that the
    read-back's `lost` path is not exercised.
    """
    profile = write_profile(tmp_path / "profile.yaml", keyspace)
    history = tmp_path / "history"
    cmd = [BINARY, "--profile", profile, "--nodes", f"{node.ip}:{node.port}",
           "--mode", "verify", "--duration", "20s", "--history-dir", str(history),
           "--checker", "off", "--ops-per-gen", "20"]
    print(" ".join(cmd))
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    rows_file = history / "rows.jsonl"
    deadline = time.time() + 60
    while time.time() < deadline and not (rows_file.exists() and rows_file.read_text().strip()):
        time.sleep(0.2)
    row = json.loads(rows_file.read_text().splitlines()[0])
    # Through cqlsh in the node's container: with the tablets-routing-v2 extension
    # negotiated, the server rejects writes from the Python driver ("requires that every
    # EXECUTE request carry a tablet_version_block"). SC tables take QUORUM writes only.
    delete = (f"CONSISTENCY QUORUM; DELETE FROM {keyspace}.reg WHERE pk = {int(row['pk'])} "
              f"AND gen = {int(row['gen'])} AND ck = 0;")
    subprocess.run(["docker", "exec", SC_CONTAINER, "cqlsh", "-e", delete], check=True)
    out, err = proc.communicate(timeout=600)
    print(out[-3000:], err[-3000:], sep="\n")
    assert proc.returncode == 1, f"expected exit 1, got {proc.returncode}"
    readback = scv_lines(out, "readback")
    assert readback and readback[0]["lost"] >= 1, readback
    lines = [json.loads(line) for line in (history / "readback.jsonl").read_text().splitlines()]
    assert any(line["pk"] == row["pk"] and line["gen"] == row["gen"] and line["result"] == "lost"
               for line in lines), lines


def run_tiny_checker_mem(node, session, keyspace: str, tmp_path):
    """A checker without enough memory decides nothing: rows are unknown, never ok."""
    profile = write_profile(tmp_path / "profile.yaml", keyspace)
    history = tmp_path / "history"
    result = checked_run(node, profile, history, "15s", "--checker-bin", checker_bin(),
                         "--checker-mem", "1M", "--check-age", "5s", "--readback", "off")
    assert result.returncode == 0, f"expected exit 0, got {result.returncode}"
    rows = rows_of(history)
    assert rows and all(row["verdict"] == "unknown" for row in rows), \
        {row["verdict"] for row in rows}
