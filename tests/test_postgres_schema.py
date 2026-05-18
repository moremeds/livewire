import pytest

from clients.postgres_schema import POSTGRES_TABLES, iter_schema_statements, validate_schema_name


def render_schema(schema: str = "md") -> str:
    return "\n".join(iter_schema_statements(schema))


def test_schema_sql_contains_expected_tables() -> None:
    schema_sql = render_schema()

    assert POSTGRES_TABLES == (
        "symbols",
        "equities_daily",
        "futures_daily",
        "equities_1h",
        "equities_5m",
        "telemetry_events",
        "quality_flags",
    )
    for table in POSTGRES_TABLES:
        assert f"CREATE TABLE IF NOT EXISTS md.{table}" in schema_sql


def test_schema_name_is_quoted() -> None:
    schema_sql = render_schema("market_data")

    assert "CREATE SCHEMA IF NOT EXISTS market_data" in schema_sql
    assert "market_data.symbols" in schema_sql
    assert "md.symbols" not in schema_sql


@pytest.mark.parametrize("schema", ["1md", "md-schema", "md.schema", "md schema", ""])
def test_invalid_schema_name_rejected(schema: str) -> None:
    with pytest.raises(ValueError, match="Invalid Postgres schema name"):
        validate_schema_name(schema)


def test_schema_sql_contains_quality_timeframe_and_detail_columns() -> None:
    schema_sql = render_schema()

    assert "timeframe text NOT NULL DEFAULT '1d'" in schema_sql
    assert "parquet_path text" in schema_sql
    assert "detail jsonb NOT NULL DEFAULT '{}'::jsonb" in schema_sql


def test_schema_sql_contains_telemetry_code_req_id_and_message_columns() -> None:
    schema_sql = render_schema()

    assert "code integer" in schema_sql
    assert "req_id integer" in schema_sql
    assert "message text" in schema_sql
