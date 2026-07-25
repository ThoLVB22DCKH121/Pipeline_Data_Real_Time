import json
import logging
import os
import signal
import time

import clickhouse_connect

# pyrefly: ignore [missing-import]
from batch import build_rows
from confluent_kafka import Consumer, Producer
from prometheus_client import Counter, start_http_server

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("sink")

password = os.environ.get("CLICKHOUSE_PASSWORD")

records_written = Counter(
    "sink_records_written_total",
    "Tổng số records đã ghi vào ClickHouse"
)
batches_written = Counter(
    "sink_batches_written_total",
    "Tổng số batch đã ghi thành công"
)
write_errors = Counter(
    "sink_write_errors_total",
    "Tổng số lần ghi vào ClickHouse bị lỗi"
)

start_http_server(8002)

consumer = Consumer({
    "bootstrap.servers": "kafka:9092",
    "group.id": "sink",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
})
consumer.subscribe(["trades.enriched"])

dlq_producer = Producer({"bootstrap.servers": "kafka:9092"})

client = clickhouse_connect.get_client(host="clickhouse", port=8123, username="default", password=password)

client.command("""
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
""")

BATCH_SIZE = 500
FLUSH_INTERVAL_SEC = 2
MAX_BUFFER_SIZE = 10000

buffer = []
last_flush = time.monotonic()

running = True


def handle_sigterm(signum, frame):
    global running
    logger.info("Nhận SIGTERM, đang shutdown gracefully...")
    running = False


signal.signal(signal.SIGTERM, handle_sigterm)


def send_to_dlq(raw_value, error_reason):
    """Gửi message lỗi vào Dead Letter Queue để điều tra sau."""
    try:
        dlq_message = json.dumps({
            "original_value": raw_value.decode("utf-8") if isinstance(raw_value, bytes) else str(raw_value),
            "error": str(error_reason),
            "source_topic": "trades.enriched",
            "failed_at": time.time(),
        }).encode("utf-8")
        dlq_producer.produce(topic="trades.dlq", value=dlq_message)
        dlq_producer.poll(0)
    except Exception as e:
        logger.error("Không thể gửi message vào DLQ: %s", e)


def flush_buffer():
    """Ghi toàn bộ buffer vào ClickHouse. Giữ lại buffer nếu ghi thất bại."""
    global buffer, last_flush
    if not buffer:
        return

    rows = build_rows(buffer)
    try:
        client.insert("trades", rows, column_names=["symbol", "price", "quantity", "notional_value", "trade_time_ms"])
        logger.info("Đã ghi %d dòng vào ClickHouse", len(rows))
        records_written.inc(len(rows))
        batches_written.inc()
        consumer.commit(asynchronous=False)
        buffer = []
        last_flush = time.monotonic()
    except Exception as e:
        write_errors.inc()
        logger.error("Lỗi ghi vào ClickHouse: %s. Giữ %d records, thử lại sau...", e, len(buffer))
        time.sleep(3)
        last_flush = time.monotonic()


logger.info("Sink đã khởi động, đang consume từ trades.enriched...")

try:
    while running:
        if len(buffer) >= MAX_BUFFER_SIZE:
            logger.warning("Buffer đạt giới hạn %d records, tạm dừng consume để flush...", MAX_BUFFER_SIZE)
            flush_buffer()
            continue

        msg = consumer.poll(timeout=1.0)

        if msg is not None and not msg.error():
            try:
                data = json.loads(msg.value())
                buffer.append(data)
            except json.JSONDecodeError as e:
                logger.warning("Bỏ qua message lỗi: %s", msg.value())
                send_to_dlq(msg.value(), f"JSONDecodeError: {e}")

        should_flush = len(buffer) >= BATCH_SIZE or time.monotonic() - last_flush >= FLUSH_INTERVAL_SEC

        if should_flush and buffer:
            flush_buffer()

except KeyboardInterrupt:
    logger.info("Đã dừng sink.")

finally:
    if buffer:
        logger.info("Đang flush %d records còn lại trong buffer...", len(buffer))
        flush_buffer()
    dlq_producer.flush()
    consumer.close()
    logger.info("Sink shutdown hoàn tất.")
