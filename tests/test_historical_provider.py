"""Tests for HistoricalProvider implementations."""

from clients.historical_provider import (
    BarRecord,
    ib_contract_to_spec,
    spec_to_ib_contract,
)


class TestBarRecord:
    def test_iso_date_format(self):
        bar = BarRecord(date="2025-01-02", open=150.0, high=152.0, low=149.5, close=151.0, volume=1000000)
        assert bar.date == "2025-01-02"
        assert bar.open == 150.0


class TestContractSpecHelpers:
    def test_stock_to_spec(self):
        from ib_async import Stock

        contract = Stock("AAPL", "SMART", "USD")
        spec = ib_contract_to_spec(contract)
        assert spec["sec_type"] == "STK"
        assert spec["symbol"] == "AAPL"
        assert spec["exchange"] == "SMART"

    def test_spec_to_stock(self):
        spec = {"sec_type": "STK", "symbol": "AAPL", "exchange": "SMART", "currency": "USD"}
        contract = spec_to_ib_contract(spec)
        assert contract.symbol == "AAPL"
        assert contract.secType == "STK"

    def test_future_roundtrip(self):
        from ib_async import Future

        contract = Future("ES", "202506", "CME", "USD")
        spec = ib_contract_to_spec(contract)
        assert spec["sec_type"] == "FUT"
        assert spec["last_trade_date"] == "202506"
        rebuilt = spec_to_ib_contract(spec)
        assert rebuilt.symbol == "ES"

    def test_index_roundtrip(self):
        from ib_async import Index

        contract = Index("VIX", "CBOE", "USD")
        spec = ib_contract_to_spec(contract)
        assert spec["sec_type"] == "IND"
