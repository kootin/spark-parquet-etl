from decimal import Decimal

import pytest
from pyspark.sql import Row

from spark_etl import pipeline
from spark_etl.generate import generate
from spark_etl.session import build_session


@pytest.fixture(scope="session")
def spark():
    s = build_session("tests", shuffle_partitions=2)
    yield s
    s.stop()


@pytest.fixture
def instruments(spark):
    return spark.createDataFrame([Row(symbol="AAPL", name="Apple", sector="Technology", currency="USD")])


def raw_rows(spark, rows):
    return spark.createDataFrame(rows, schema=pipeline.RAW_SCHEMA)


def test_validation_routes_bad_rows_to_quarantine(spark, instruments):
    raw = raw_rows(spark, [
        ("T1", "AAPL", "BUY", "190.5", "10", "2026-10-05T14:00:00", "XNAS"),
        ("T2", "AAPL", "BUY", "N/A", "10", "2026-10-05T14:00:00", "XNAS"),
        ("T3", "AAPL", "SELL", "190", "-5", "2026-10-05T14:00:00", "XNAS"),
        ("T4", "ZZZZ", "SELL", "10", "5", "2026-10-05T14:00:00", "XNAS"),
        ("T5", "AAPL", "HOLD", "10", "5", "2026-10-05T14:00:00", "XNAS"),
    ])
    valid, rejected = pipeline.validate(pipeline.to_typed(raw), instruments)
    assert [r.trade_id for r in valid.collect()] == ["T1"]
    reasons = {r.trade_id: r.reject_reason for r in rejected.collect()}
    assert reasons == {"T2": "invalid_price", "T3": "invalid_quantity", "T4": "unknown_symbol", "T5": "invalid_side"}


def test_duplicates_removed(spark):
    raw = raw_rows(spark, [
        ("T1", "AAPL", "BUY", "1", "1", "2026-10-05T14:00:00", "XNAS"),
        ("T1", "AAPL", "BUY", "1", "1", "2026-10-05T14:00:00", "XNAS"),
    ])
    assert pipeline.deduplicate(pipeline.to_typed(raw)).count() == 1


def test_gold_ohlc_vwap(spark, instruments):
    raw = raw_rows(spark, [
        ("T1", "AAPL", "BUY", "100", "10", "2026-10-05T14:00:00", "XNAS"),
        ("T2", "AAPL", "SELL", "110", "30", "2026-10-05T15:00:00", "XNAS"),
        ("T3", "AAPL", "BUY", "105", "10", "2026-10-05T16:00:00", "XNAS"),
    ])
    valid, _ = pipeline.validate(pipeline.to_typed(raw), instruments)
    g = pipeline.build_gold(pipeline.build_silver(valid, instruments)).first()
    assert (g.open, g.high, g.low, g.close) == (Decimal("100"), Decimal("110"), Decimal("100"), Decimal("105"))
    assert g.volume == 50 and g.trade_count == 3
    assert g.vwap == Decimal("107.0000")          # (1000 + 3300 + 1050) / 50
    assert g.net_buy_qty == -10 and g.imbalance_pct == -20.0


def test_end_to_end_writes_partitioned_parquet(spark, tmp_path):
    raw_dir = tmp_path / "raw"
    generate(raw_dir, n_trades=2000, days=3)
    stats = pipeline.run(spark, str(raw_dir), str(tmp_path / "lake"))
    assert stats.silver_rows + stats.rejected_rows + stats.duplicates_removed == stats.raw_rows
    assert stats.rejected_rows > 0 and stats.duplicates_removed > 0
    partitions = sorted(p.name for p in (tmp_path / "lake/silver/trades").iterdir() if p.is_dir())
    assert len(partitions) == 3 and all(p.startswith("trade_date=") for p in partitions)
