#! /usr/bin/env python3

"""Integration tests for cql-stress-sc-verify, the verified load for strongly consistent tables.

They need the `strong-consistency` compose node and `cql-stress-sc-verify` on PATH; the
`test_strong_consistency_sc_verify_*` functions in cql-stress-cassandra-stress-ci.py call
them with the shared fixtures, which skip when the node cannot do strong consistency.
"""

import subprocess

from test_cs_strong_consistency import UNAVAILABLE_CODE, keyspace_consistency

BINARY = "cql-stress-sc-verify"


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


def sc_verify(node, profile: str, *args: str) -> subprocess.CompletedProcess:
    cmd = [BINARY, "--profile", profile, "--nodes", f"{node.ip}:{node.port}",
           "--mode", "verify", "--duration", "1s", *args]
    print(" ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
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
