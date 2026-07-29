"""Cấu hình tập trung cho Processor service."""

import os

# ── Kafka ────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC_RAW = "trades.raw"
KAFKA_TOPIC_ENRICHED = "trades.enriched"
KAFKA_TOPIC_DLQ = "trades.dlq"
KAFKA_CONSUMER_GROUP = "processor-group"

# ── Metrics ──────────────────────────────────────────────
METRICS_PORT = 8000
