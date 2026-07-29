"""Cấu hình tập trung cho Sink service."""

import os

# ── Kafka ────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC_ENRICHED = "trades.enriched"
KAFKA_TOPIC_DLQ = "trades.dlq"
KAFKA_CONSUMER_GROUP = "sink"

# ── ClickHouse ───────────────────────────────────────────
CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = "default"
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "")

# ── Batching ─────────────────────────────────────────────
BATCH_SIZE = 500
FLUSH_INTERVAL_SEC = 2
MAX_BUFFER_SIZE = 10000

# ── Metrics ──────────────────────────────────────────────
METRICS_PORT = 8002

# ── Schema ───────────────────────────────────────────────
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS trades (
    symbol          String,
    price           Float64,
    quantity        Float64,
    notional_value  Float64,
    trade_time_ms   UInt64,
    trade_time      DateTime DEFAULT toDateTime(intDiv(trade_time_ms, 1000))
) ENGINE = MergeTree()
ORDER BY (symbol, trade_time_ms)
TTL trade_time + INTERVAL 30 DAY
"""

INSERT_COLUMNS = ["symbol", "price", "quantity", "notional_value", "trade_time_ms"]
