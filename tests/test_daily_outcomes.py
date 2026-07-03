"""Tests for livewire_scripts.daily_outcomes."""
import json

from livewire_scripts.daily_outcomes import (
    SUMMARY_PREFIX,
    build_summary_line,
    parse_all_summary_json,
    parse_last_summary_json,
    resolve_exit_code,
)


def _line(**kw):
    base = dict(
        job="daily_update", asset_class="equity", source="massive",
        target_date="2026-07-02", updated=9091, no_trade=277, partial=95,
        errors=0, bars_inserted=9186, validation_issues=0, top_errors=[],
    )
    base.update(kw)
    return build_summary_line(**base)


def test_build_summary_line_round_trips():
    line = _line(top_errors=[("HTTP 500 from Massive", 12)])
    assert line.startswith(SUMMARY_PREFIX)
    payload = json.loads(line[len(SUMMARY_PREFIX):])
    assert payload["updated"] == 9091
    assert payload["no_trade"] == 277
    assert payload["top_errors"] == [["HTTP 500 from Massive", 12]]


def test_parse_last_summary_json_returns_last_line():
    text = "\n".join(["noise", _line(updated=1), "more", _line(updated=2)])
    assert parse_last_summary_json(text)["updated"] == 2


def test_parse_last_summary_json_none_when_absent_or_corrupt():
    assert parse_last_summary_json("no summary here") is None
    assert parse_last_summary_json(SUMMARY_PREFIX + "{not json") is None


def test_parse_all_summary_json_returns_every_line_in_order():
    text = "\n".join([_line(asset_class="equity"), "noise", _line(asset_class="futures")])
    payloads = parse_all_summary_json(text)
    assert [p["asset_class"] for p in payloads] == ["equity", "futures"]


def test_parse_all_summary_json_skips_corrupt_and_empty():
    assert parse_all_summary_json("nothing here") == []
    text = "\n".join([_line(updated=1), SUMMARY_PREFIX + "{bad json"])
    payloads = parse_all_summary_json(text)
    assert len(payloads) == 1 and payloads[0]["updated"] == 1


def test_no_trade_and_partial_never_fail():
    assert resolve_exit_code(updated=0, no_trade=277, partial=95, errors=0) == 0


def test_small_error_count_tolerated():
    assert resolve_exit_code(updated=9091, no_trade=277, partial=0, errors=50) == 0


def test_error_rate_over_threshold_fails():
    # errors=600 of 10000 processed > max(50, 500) -> fail
    assert resolve_exit_code(updated=9000, no_trade=400, partial=0, errors=600) == 1


def test_zero_updates_with_errors_fails():
    assert resolve_exit_code(updated=0, no_trade=0, partial=0, errors=3) == 1


def test_all_updated_ok():
    assert resolve_exit_code(updated=10, no_trade=0, partial=0, errors=0) == 0
