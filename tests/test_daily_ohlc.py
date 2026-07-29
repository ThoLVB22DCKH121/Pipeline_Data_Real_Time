"""Test cho logic tính OHLC trong Spark job daily_ohlc.

Yêu cầu: pip install pyspark pytest
Chạy: pytest tests/test_daily_ohlc.py -v
"""

import os
import sys
from datetime import date

pyspark = __import__("pytest").importorskip("pyspark")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "services", "spark"))

# pyrefly: ignore [missing-import]
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# pyrefly: ignore [missing-import]
from jobs.daily_ohlc import compute_ohlc

TRADES_SCHEMA = StructType([
    StructField("symbol", StringType()),
    StructField("price", DoubleType()),
    StructField("quantity", DoubleType()),
    StructField("notional_value", DoubleType()),
    StructField("trade_time_ms", LongType()),
])

TARGET_DATE = date(2026, 7, 25)
# Timestamp tương ứng: 2026-07-25 trong milliseconds
TS_BASE = int(TARGET_DATE.strftime("%s")) * 1000


@pytest.fixture(scope="module")
def spark():
    """Tạo SparkSession local cho testing (không cần cluster)."""
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("test_daily_ohlc")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


class TestComputeOHLC:
    def test_single_symbol(self, spark):
        """Kiểm tra tính OHLC cho một symbol duy nhất."""
        data = [
            ("BTCUSDT", 50000.0, 1.0, 50000.0, TS_BASE + 1000),   # Trade 1: open
            ("BTCUSDT", 52000.0, 0.5, 26000.0, TS_BASE + 2000),   # Trade 2: high
            ("BTCUSDT", 49000.0, 2.0, 98000.0, TS_BASE + 3000),   # Trade 3: low
            ("BTCUSDT", 51000.0, 1.5, 76500.0, TS_BASE + 4000),   # Trade 4: close
        ]
        trades_df = spark.createDataFrame(data, TRADES_SCHEMA)
        result = compute_ohlc(trades_df, TARGET_DATE).collect()

        assert len(result) == 1
        row = result[0]
        assert row["symbol"] == "BTCUSDT"
        assert row["open_price"] == 50000.0     # Trade đầu tiên
        assert row["close_price"] == 51000.0    # Trade cuối cùng
        assert row["high_price"] == 52000.0
        assert row["low_price"] == 49000.0
        assert row["trade_count"] == 4

    def test_multiple_symbols(self, spark):
        """Kiểm tra tính OHLC cho nhiều symbol cùng lúc."""
        data = [
            ("BTCUSDT", 50000.0, 1.0, 50000.0, TS_BASE + 1000),
            ("ETHUSDT", 3000.0,  2.0, 6000.0,  TS_BASE + 1000),
            ("BTCUSDT", 51000.0, 0.5, 25500.0, TS_BASE + 2000),
            ("ETHUSDT", 3100.0,  1.0, 3100.0,  TS_BASE + 2000),
        ]
        trades_df = spark.createDataFrame(data, TRADES_SCHEMA)
        result = compute_ohlc(trades_df, TARGET_DATE)

        assert result.count() == 2
        symbols = [r["symbol"] for r in result.collect()]
        assert "BTCUSDT" in symbols
        assert "ETHUSDT" in symbols

    def test_no_data_for_date(self, spark):
        """Kiểm tra trả về DataFrame rỗng khi không có dữ liệu cho ngày đó."""
        data = [
            ("BTCUSDT", 50000.0, 1.0, 50000.0, TS_BASE + 1000),
        ]
        trades_df = spark.createDataFrame(data, TRADES_SCHEMA)
        wrong_date = date(2020, 1, 1)
        result = compute_ohlc(trades_df, wrong_date)

        assert result.count() == 0

    def test_volume_and_notional_sum(self, spark):
        """Kiểm tra tổng volume và notional value được cộng đúng."""
        data = [
            ("BTCUSDT", 50000.0, 1.0,  50000.0,  TS_BASE + 1000),
            ("BTCUSDT", 51000.0, 2.0,  102000.0, TS_BASE + 2000),
            ("BTCUSDT", 49000.0, 0.5,  24500.0,  TS_BASE + 3000),
        ]
        trades_df = spark.createDataFrame(data, TRADES_SCHEMA)
        row = compute_ohlc(trades_df, TARGET_DATE).collect()[0]

        assert row["total_volume"] == pytest.approx(3.5)        # 1.0 + 2.0 + 0.5
        assert row["total_notional"] == pytest.approx(176500.0)  # 50000 + 102000 + 24500
