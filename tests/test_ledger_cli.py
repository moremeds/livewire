"""Tests for ``livewire_ops.py ledger emit|query``."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from clients import ledger
from clients.ledger import ledger_root
from livewire_scripts import ledger_cli


@pytest.fixture(autouse=True)
def root(tmp_path: Path, monkeypatch) -> Path:
    monkeypatch.setenv("LW_LEDGER_ROOT", str(tmp_path / "ledger"))
    return tmp_path / "ledger"


def _evidence(**over) -> dict:
    return {
        "evidence_hash": "h1",
        "kind": "delisting",
        "subject": "ZZZZ",
        "payload_json": "{}",
        "source_url": "https://example.invalid/x",
        "fetched_at": "2026-09-02T06:00:00+00:00",
        "proposer": "human",
        "run_id": "manual-1",
    } | over


def test_emit_writes_one_row() -> None:
    assert ledger_cli.main(["emit", "--table", "evidence", "--json", json.dumps(_evidence())]) == 0
    assert ledger.query("select subject from evidence") == [{"subject": "ZZZZ"}]


def test_emit_accepts_a_list_of_rows() -> None:
    payload = json.dumps([_evidence(evidence_hash="h1"), _evidence(evidence_hash="h2")])
    assert ledger_cli.main(["emit", "--table", "evidence", "--json", payload]) == 0
    assert len(ledger.query("select evidence_hash from evidence")) == 2


def test_emit_uses_lw_run_id_when_set(monkeypatch) -> None:
    monkeypatch.setenv("LW_RUN_ID", "daily-update-20260902T060000Z-7")
    ledger_cli.main(["emit", "--table", "evidence", "--json", json.dumps(_evidence())])
    assert [path.name for path in ledger_root().glob("evidence/*/*.parquet")] == [
        "daily-update-20260902T060000Z-7.parquet"
    ]


def test_emit_without_lw_run_id_mints_a_manual_one(monkeypatch) -> None:
    monkeypatch.delenv("LW_RUN_ID", raising=False)
    ledger_cli.main(["emit", "--table", "evidence", "--json", json.dumps(_evidence())])
    assert next(ledger_root().glob("evidence/*/*.parquet")).name.startswith("manual-")


def test_a_bad_column_exits_nonzero_and_says_which(capsys) -> None:
    assert ledger_cli.main(["emit", "--table", "evidence", "--json", json.dumps(_evidence(nonsense=1))]) == 1
    assert "unexpected column" in capsys.readouterr().err


def test_query_prints_one_json_object_per_line(capsys) -> None:
    ledger_cli.main(["emit", "--table", "evidence", "--json", json.dumps(_evidence())])
    capsys.readouterr()
    assert ledger_cli.main(["query", "select kind, subject from evidence"]) == 0
    lines = capsys.readouterr().out.strip().splitlines()
    assert [json.loads(line) for line in lines] == [{"kind": "delisting", "subject": "ZZZZ"}]


def test_the_ops_entrypoint_dispatches_ledger() -> None:
    """The real signature, not a mock: ``livewire_ops.py ledger`` reaches main."""
    import scripts.livewire_ops as ops

    assert ops.COMMANDS["ledger"] == "livewire_scripts.ledger_cli"
    assert ops.main(["ledger", "query", "select 1 as one"]) == 0


def test_a_malformed_query_exits_2_and_prints_the_duckdb_message_with_no_traceback(capsys) -> None:
    assert ledger_cli.main(["query", "select * frm evidence"]) == 2
    err = capsys.readouterr().err
    assert "ledger query failed:" in err
    assert "Parser Error" in err
    assert "Traceback (most recent call last)" not in err


def test_a_reserved_word_as_a_bare_column_alias_exits_2_and_names_the_word(capsys) -> None:
    ledger_cli.main(["emit", "--table", "evidence", "--json", json.dumps(_evidence())])
    capsys.readouterr()

    assert ledger_cli.main(["query", "select subject first from evidence"]) == 2

    err = capsys.readouterr().err
    assert "`first` is a DuckDB reserved word" in err
    assert 'double-quoted (as "first")' in err


def test_a_query_naming_a_nonexistent_column_exits_2_with_the_candidate_bindings(capsys) -> None:
    ledger_cli.main(["emit", "--table", "evidence", "--json", json.dumps(_evidence())])
    capsys.readouterr()

    assert ledger_cli.main(["query", "select nonexistent_col from evidence"]) == 2

    err = capsys.readouterr().err
    assert "Referenced column" in err
    assert "candidate bindings printed above are the table's real columns" in err


def test_a_valid_query_still_returns_0_and_prints_one_json_object_per_line(capsys) -> None:
    """No regression on the happy path once errors are caught."""
    ledger_cli.main(["emit", "--table", "evidence", "--json", json.dumps(_evidence(evidence_hash="h1"))])
    ledger_cli.main(["emit", "--table", "evidence", "--json", json.dumps(_evidence(evidence_hash="h2"))])
    capsys.readouterr()

    assert ledger_cli.main(["query", "select evidence_hash from evidence order by evidence_hash"]) == 0

    lines = capsys.readouterr().out.strip().splitlines()
    assert [json.loads(line) for line in lines] == [{"evidence_hash": "h1"}, {"evidence_hash": "h2"}]


def test_query_help_contains_the_quoting_and_reserved_word_guidance(capsys) -> None:
    with pytest.raises(SystemExit) as excinfo:
        ledger_cli.main(["query", "--help"])

    assert excinfo.value.code == 0
    out = capsys.readouterr().out
    assert "reserves more words than you expect" in out
    assert "double-quote the alias or rename it" in out
