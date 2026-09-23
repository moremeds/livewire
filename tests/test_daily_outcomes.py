"""Tests for livewire_scripts.daily_outcomes."""

import json

from livewire_scripts import daily_outcomes
from livewire_scripts.daily_outcomes import (
    SUMMARY_PREFIX,
    build_summary_line,
    parse_all_summary_json,
    parse_last_summary_json,
    resolve_exit_code,
)


def _line(**kw):
    base = dict(
        job="daily_update",
        asset_class="equity",
        source="massive",
        target_date="2026-07-02",
        updated=9091,
        no_trade=277,
        partial=95,
        errors=0,
        bars_inserted=9186,
        validation_issues=0,
        top_errors=[],
    )
    base.update(kw)
    return build_summary_line(**base)


def test_build_summary_line_round_trips():
    line = _line(top_errors=[("HTTP 500 from Massive", 12)])
    assert line.startswith(SUMMARY_PREFIX)
    payload = json.loads(line[len(SUMMARY_PREFIX) :])
    assert payload["updated"] == 9091
    assert payload["no_trade"] == 277
    assert payload["top_errors"] == [["HTTP 500 from Massive", 12]]


def test_the_denominator_is_carried_when_the_run_knows_it():
    """The four counters cover only symbols that HAD a gap. Without `scanned`,
    `no_trade=974` cannot be told from "we only looked at 974 of 13,385"."""
    payload = json.loads(_line(scanned=13385, up_to_date=12411)[len(SUMMARY_PREFIX) :])
    assert payload["scanned"] == 13385
    assert payload["up_to_date"] == 12411


def test_the_denominator_keys_are_absent_rather_than_null_when_unknown():
    """Optional because they were added late: consumers parse old log lines for
    baselines, and a null would make every reader special-case the type."""
    payload = json.loads(_line()[len(SUMMARY_PREFIX) :])
    assert "scanned" not in payload
    assert "up_to_date" not in payload


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


class TestTheSharedErrorExtractor:
    """One extractor for both job runners and the status surface.

    Fixtures are verbatim lines from the mini's 2026-09-08 logs.
    → pm:2026-09-08-failure-email-named-the-lane-not-the-error
    """

    REAL_TRACEBACK = (
        "bronze_equity_1d: 0 symbols\n"
        "published -> /Users/moremeds/market-warehouse/analytics.duckdb\n"
        "Traceback (most recent call last):\n"
        '  File "/Users/moremeds/market-warehouse/releases/59f18e38/scripts/livewire_store.py", line 59, in <module>\n'
        "    raise SystemExit(main())\n"
        '  File "/Users/moremeds/market-warehouse/releases/59f18e38/clients/duckdb_catalog.py",'
        " line 599, in _build_coverage_locked\n"
        "    con.execute(_coverage_insert(view_name, date_column))\n"
        "_duckdb.InvalidInputException: Invalid Input Error: No magic bytes found at end of file"
        " '/Users/moremeds/market-warehouse/data-lake/bronze/asset_class=equity/symbol=RJF/1d.parquet'\n"
    )

    def test_a_traceback_yields_the_innermost_frame_and_the_exception(self):
        lines = daily_outcomes.extract_error_lines(self.REAL_TRACEBACK)

        assert len(lines) == 2
        assert lines[0].startswith('File "') and "line 599, in _build_coverage_locked" in lines[0]
        assert lines[1].startswith("_duckdb.InvalidInputException")
        assert "symbol=RJF/1d.parquet" in lines[1]
        # Nine identical dispatch frames every night crowd out the one line
        # that differs.
        assert not any("livewire_store.py" in line for line in lines)

    def test_top_errors_speak_when_there_is_no_traceback(self):
        text = (
            "  XOMA: no trade (no bars returned)\n"
            + daily_outcomes.SUMMARY_PREFIX
            + '{"job":"daily_update","updated":0,"errors":1,'
            '"top_errors":[["ArrowInvalid: Parquet magic bytes not found in footer.",1]]}\n'
        )

        assert daily_outcomes.extract_error_lines(text) == [
            'dominant error (1x): "ArrowInvalid: Parquet magic bytes not found in footer."'
        ]

    def test_error_lines_are_the_last_resort_and_are_bounded(self):
        text = "\n".join(f"2026-09-08 ERROR failure number {index}" for index in range(50))

        lines = daily_outcomes.extract_error_lines(text)

        assert len(lines) == daily_outcomes.MAX_ERROR_LINES
        assert lines[-1].endswith("failure number 49")

    def test_clamp_truncates_a_runaway_line(self):
        (clamped,) = daily_outcomes.clamp_lines(["x" * 5000])

        assert len(clamped) == daily_outcomes.MAX_ERROR_LINE_CHARS
        assert clamped.endswith("…")

    def test_a_section_with_nothing_to_say_says_nothing(self):
        assert daily_outcomes.extract_error_lines("  AAPL: 1 bar published from Massive\n") == []

    def test_the_failing_section_is_the_lane_that_failed(self):
        text = (
            "=== Silver Rebuild 2026-09-08T07:10:00Z ===\n"
            'SUMMARY_JSON {"failed": 269}\n'
            "=== Done silver 2026-09-08T07:25:34Z ===\n"
            "=== DuckDB Catalog Build 2026-09-08T07:25:34Z ===\n"
            + self.REAL_TRACEBACK
            + "=== DuckDB Catalog Build Failed 2026-09-08T07:28:34Z (exit_code=1) ===\n"
        )

        unit, exit_code, section = daily_outcomes.last_failed_section(text)

        assert unit == "DuckDB Catalog Build"
        assert exit_code == 1
        assert "_duckdb.InvalidInputException" in section
        assert '"failed": 269' not in section

    def test_no_failure_marker_leaves_the_text_whole(self):
        assert daily_outcomes.last_failed_section("just a line\n") == (None, None, "just a line\n")

    def test_last_meaningful_line_skips_section_markers(self):
        assert daily_outcomes.last_meaningful_line("tail\n=== Failed ===\n") == "tail"
        assert daily_outcomes.last_meaningful_line("=== Failed ===\n") is None

    def test_read_log_tail_reads_only_the_end_of_a_huge_log(self, tmp_path):
        log = tmp_path / "daily_backfill_equity_union.log"
        log.write_text("old noise\n" * 10_000 + "the last error\n", encoding="utf-8")

        tail = daily_outcomes.read_log_tail(log, max_bytes=64)

        assert tail.endswith("the last error\n")
        assert len(tail) <= 64
