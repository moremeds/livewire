from __future__ import annotations

import pytest


class FakeCursor:
    def __init__(self):
        self.last_table = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        pass

    def execute(self, stmt):
        text = str(stmt)
        if "COUNT(*)" in text:
            self.last_table = text.split("FROM ", 1)[1]

    def fetchone(self):
        if self.last_table:
            return (len(self.last_table),)
        return (1,)


class FakeConn:
    def __init__(self):
        self.cursor_obj = FakeCursor()

    def cursor(self):
        return self.cursor_obj


class FakeSmokeClient:
    def __init__(self, dsn=None, schema=None):
        self.dsn = dsn
        self.schema = schema or "md"
        self.conn = FakeConn()
        self.ensured = False

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        pass

    def ensure_schema(self):
        self.ensured = True


def test_missing_dsn_exits_with_clear_message(monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    import scripts.smoke_postgres_analytical as script

    monkeypatch.delenv("MDW_POSTGRES_DSN", raising=False)

    assert script.main([]) == 2
    assert "MDW_POSTGRES_DSN" in capsys.readouterr().err


def test_fake_client_prints_expected_counts(monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    import scripts.smoke_postgres_analytical as script

    monkeypatch.setenv("MDW_POSTGRES_DSN", "postgresql://example/livewire")
    monkeypatch.setattr(script, "PostgresClient", FakeSmokeClient)

    assert script.main(["--ensure-schema"]) == 0

    out = capsys.readouterr().out
    assert "SELECT 1 ok" in out
    assert "symbols" in out
    assert "quality_flags" in out
