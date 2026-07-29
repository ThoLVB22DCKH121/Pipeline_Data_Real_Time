import json
import logging
import time

# pyrefly: ignore [missing-import]
from batch import build_rows

# pyrefly: ignore [missing-import]
from config import (
    BATCH_SIZE,
    FLUSH_INTERVAL_SEC,
    INSERT_COLUMNS,
    KAFKA_TOPIC_DLQ,
    MAX_BUFFER_SIZE,
)
from confluent_kafka import Consumer, Producer
from prometheus_client import Counter

logger = logging.getLogger("sink")

# ── Prometheus Metrics ───────────────────────────────────
records_written = Counter(
    "sink_records_written_total",
    "Tổng số records đã ghi vào ClickHouse",
)
batches_written = Counter(
    "sink_batches_written_total",
    "Tổng số batch đã ghi thành công",
)
write_errors = Counter(
    "sink_write_errors_total",
    "Tổng số lần ghi vào ClickHouse bị lỗi",
)


def send_to_dlq(dlq_producer: Producer, raw_value, error_reason: str):
    """Gửi message lỗi vào Dead Letter Queue."""
    try:
        dlq_message = json.dumps({
            "original_value": raw_value.decode("utf-8") if isinstance(raw_value, bytes) else str(raw_value),
            "error": str(error_reason),
            "source_topic": "trades.enriched",
            "failed_at": time.time(),
        }).encode("utf-8")
        dlq_producer.produce(topic=KAFKA_TOPIC_DLQ, value=dlq_message)
        dlq_producer.poll(0)
    except Exception as e:
        logger.error("Không thể gửi message vào DLQ: %s", e)


def flush_buffer(buffer, ch_client, consumer):
    """Ghi toàn bộ buffer vào ClickHouse. Trả về buffer mới (rỗng nếu thành công, giữ nguyên nếu thất bại).

    Args:
        buffer: List các trade dict đang chờ ghi.
        ch_client: ClickHouse client instance.
        consumer: Kafka Consumer để commit offset sau khi ghi thành công.

    Returns:
        Tuple (buffer_mới, last_flush_time).
    """
    if not buffer:
        return buffer, time.monotonic()

    rows = build_rows(buffer)
    try:
        ch_client.insert("trades", rows, column_names=INSERT_COLUMNS)
        logger.info("Đã ghi %d dòng vào ClickHouse", len(rows))
        records_written.inc(len(rows))
        batches_written.inc()
        consumer.commit(asynchronous=False)
        return [], time.monotonic()
    except Exception as e:
        write_errors.inc()
        logger.error("Lỗi ghi vào ClickHouse: %s. Giữ %d records, thử lại sau...", e, len(buffer))
        time.sleep(3)
        return buffer, time.monotonic()


def consume_loop(consumer: Consumer, dlq_producer: Producer, ch_client, running_flag):
    """Vòng lặp chính: consume → buffer → micro-batch write.

    Args:
        consumer: Kafka Consumer đã subscribe sẵn.
        dlq_producer: Kafka Producer dùng gửi message lỗi vào DLQ.
        ch_client: ClickHouse client instance.
        running_flag: Callable trả về bool, False khi cần shutdown.
    """
    buffer = []
    last_flush = time.monotonic()

    logger.info("Sink đã khởi động, đang consume từ trades.enriched...")

    while running_flag():
        # Backpressure: nếu buffer đầy, dừng consume, chỉ flush
        if len(buffer) >= MAX_BUFFER_SIZE:
            logger.warning("Buffer đạt giới hạn %d records, tạm dừng consume để flush...", MAX_BUFFER_SIZE)
            buffer, last_flush = flush_buffer(buffer, ch_client, consumer)
            continue

        msg = consumer.poll(timeout=1.0)

        if msg is not None and not msg.error():
            try:
                data = json.loads(msg.value())
                buffer.append(data)
            except json.JSONDecodeError as e:
                logger.warning("Bỏ qua message lỗi: %s", msg.value())
                send_to_dlq(dlq_producer, msg.value(), f"JSONDecodeError: {e}")

        should_flush = len(buffer) >= BATCH_SIZE or time.monotonic() - last_flush >= FLUSH_INTERVAL_SEC

        if should_flush and buffer:
            buffer, last_flush = flush_buffer(buffer, ch_client, consumer)

    # Flush buffer còn lại khi shutdown
    if buffer:
        logger.info("Đang flush %d records còn lại trong buffer...", len(buffer))
        flush_buffer(buffer, ch_client, consumer)
