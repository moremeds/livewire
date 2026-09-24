import json
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest

from clients.ingestion_common import resolve_rolling_futures_preset


def _detail(root, month, last_trade, *, trading_class=None, multiplier=""):
    return SimpleNamespace(
        contractMonth=month,
        contract=SimpleNamespace(
            symbol=root,
            tradingClass=trading_class or root,
            multiplier=multiplier,
            lastTradeDateOrContractMonth=last_trade,
        ),
    )


def test_rolling_preset_uses_live_ib_months_and_standard_si_only(tmp_path: Path):
    preset = tmp_path / "rolling.json"
    preset.write_text(
        json.dumps(
            {
                "name": "rolling-test",
                "rolling_contracts": [
                    {"roots": ["CL"], "count": 2},
                    {"roots": ["SI"], "count": 2},
                ],
            }
        )
    )
    details_by_root = {
        "CL": [
            _detail("CL", "202610", "20260922"),
            _detail("CL", "202611", "20261120"),
            _detail("CL", "202612", "20261220"),
        ],
        "SI": [
            _detail("SI", "202609", "20260922", trading_class="SI", multiplier="5000"),
            _detail("SI", "202610", "20261028", trading_class="SI", multiplier="5000"),
            _detail("SI", "202610", "20261028", trading_class="SIL", multiplier="1000"),
            _detail("SI", "202612", "20261229", trading_class="SI", multiplier="5000"),
        ],
    }

    class FakeIB:
        def get_contract_details(self, request):
            assert request.lastTradeDateOrContractMonth == ""
            return details_by_root[request.symbol]

    name, tickers, exchanges = resolve_rolling_futures_preset(preset, FakeIB(), date(2026, 9, 23))

    assert name == "rolling-test"
    assert tickers == ["CL_202611", "CL_202612", "SI_202610", "SI_202612"]
    assert exchanges == {ticker: "NYMEX" for ticker in tickers[:2]} | {ticker: "COMEX" for ticker in tickers[2:]}


def test_rolling_preset_fails_closed_if_ib_has_too_few_live_months(tmp_path: Path):
    preset = tmp_path / "rolling.json"
    preset.write_text(json.dumps({"name": "rolling-test", "rolling_contracts": [{"roots": ["CL"], "count": 2}]}))

    class FakeIB:
        def get_contract_details(self, _request):
            return [_detail("CL", "202610", "20261020")]

    with pytest.raises(ValueError, match="listed only 1 live contracts for CL"):
        resolve_rolling_futures_preset(preset, FakeIB(), date(2026, 9, 23))


def test_shipped_rolling_preset_uses_relative_contract_counts():
    repo = Path(__file__).resolve().parents[1]
    payload = json.loads((repo / "presets/futures-rolling.json").read_text())

    assert payload["rolling_contracts"] == [
        {"roots": ["CL", "NG", "COIL", "RB", "HO"], "months_ahead": 15},
        {"roots": ["GC", "SI", "HG"], "count": 2},
        {"roots": ["SB", "KC", "CC", "CT", "OJ", "ZS", "ZM", "ZL", "ZC", "ZW", "LE", "HE"], "count": 2},
        {"roots": ["ES", "NQ", "RTY", "YM", "ZN", "ZB", "ZF"], "count": 2},
    ]


def test_energy_horizon_rolls_from_as_of_month_instead_of_fixed_expiry(tmp_path: Path):
    repo = Path(__file__).resolve().parents[1]
    payload = json.loads((repo / "presets/futures-rolling.json").read_text())
    payload["rolling_contracts"] = [{"roots": ["CL"], "months_ahead": 15}]
    preset = tmp_path / "futures-rolling.json"
    preset.write_text(json.dumps(payload))

    class FakeIB:
        def get_contract_details(self, _request):
            return [
                _detail("CL", "202610", "20260922"),
                _detail("CL", "202611", "20261120"),
                _detail("CL", "202712", "20271220"),
                _detail("CL", "202801", "20280120"),
            ]

    _, tickers, _ = resolve_rolling_futures_preset(preset, FakeIB(), date(2026, 9, 23))

    assert tickers == ["CL_202611", "CL_202712"]
