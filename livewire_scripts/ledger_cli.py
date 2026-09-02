#!/usr/bin/env python3
"""``livewire_ops.py ledger emit|query`` — the one ledger command surface.

Agents, humans, and cron all use this command; only ``LW_LEDGER_ROOT`` differs
per caller. There is no second format and no second validator.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Sequence
from datetime import datetime

import pyarrow as pa

from clients import ledger


def _coerce(table: str, row: dict) -> dict:
    """Turn JSON timestamp strings into the declared schema's types."""
    out = dict(row)
    for field in ledger.LEDGER_TABLES[table]:
        value = out.get(field.name)
        if isinstance(value, str) and pa.types.is_timestamp(field.type):
            out[field.name] = datetime.fromisoformat(value)
    return out


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="livewire_ops.py ledger",
        description="Read and write the run ledger",
    )
    sub = parser.add_subparsers(dest="action", required=True)
    emit_parser = sub.add_parser("emit", help="Append rows to one ledger table")
    emit_parser.add_argument("--table", required=True, choices=sorted(ledger.LEDGER_TABLES))
    emit_parser.add_argument("--json", required=True, help="One row object, or a JSON array of rows")
    query_parser = sub.add_parser("query", help="Run SQL; print one JSON object per line")
    query_parser.add_argument("sql")
    args = parser.parse_args(list(argv) if argv is not None else None)

    if args.action == "emit":
        payload = json.loads(args.json)
        rows = payload if isinstance(payload, list) else [payload]
        run_id = os.environ.get("LW_RUN_ID") or ledger.new_run_id("manual")
        try:
            path = ledger.emit(args.table, [_coerce(args.table, row) for row in rows], run_id=run_id)
        except ValueError as exc:
            print(str(exc), file=sys.stderr)
            return 1
        print(str(path))
        return 0

    for row in ledger.query(args.sql):
        print(json.dumps(row, default=str))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
