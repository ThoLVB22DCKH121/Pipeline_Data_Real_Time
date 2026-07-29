import json
import logging
import time

# pyrefly: ignore [missing-import]
from config import KAFKA_TOPIC_DLQ, KAFKA_TOPIC_ENRICHED
from confluent_kafka import Consumer, Producer
from prometheus_client import Counter

# pyrefly: ignore [missing-import]
from transform import enrich_trade

logger = logging.getLogger("processor")

# ── Prometheus Metrics ───────────────────────────────────
messages_processed = Counter(
    "processor_messages_processed_total",
    "Tổng số message processor đã xử lý thành công",
)
messages_failed = Counter(
    "processor_messages_failed_total",
    "Tổng số message processor gặp lỗi",
)
messages_dlq = Counter(
    "processor_messages_dlq_total",
    "Tổng số message đã gửi vào Dead Letter Queue",
)


def send_to_dlq(kafka_producer: Producer, raw_value, error_reason: str, source_topic: str):
    """Gửi message lỗi vào Dead Letter Queue để điều tra sau."""
    try:
        dlq_message = json.dumps({
            "original_value": raw_value.decode("utf-8") if isinstance(raw_value, bytes) else str(raw_value),
            "error": str(error_reason),
            "source_topic": source_topic,
            "failed_at": time.time(),
        }).encode("utf-8")
        kafka_producer.produce(topic=KAFKA_TOPIC_DLQ, value=dlq_message)
        kafka_producer.poll(0)
        messages_dlq.inc()
        logger.info("Đã gửi message lỗi vào %s", KAFKA_TOPIC_DLQ)
    except Exception as e:
        logger.error("Không thể gửi message vào DLQ: %s", e)


def process_loop(consumer: Consumer, kafka_producer: Producer, running_flag):
    """Vòng lặp chính: consume → enrich → produce."""
    logger.info("Processor đã khởi động, đang consume từ trades.raw...")

    while running_flag():
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            logger.error("Lỗi Kafka: %s", msg.error())
            continue

        raw_value = msg.value()

        try:
            data = json.loads(raw_value)
        except json.JSONDecodeError as e:
            messages_failed.inc()
            logger.warning("Message không hợp lệ JSON: %s", raw_value)
            send_to_dlq(kafka_producer, raw_value, f"JSONDecodeError: {e}", "trades.raw")
            consumer.commit(asynchronous=False)
            continue

        try:
            enriched = enrich_trade(data)
        except (KeyError, ValueError, TypeError) as e:
            messages_failed.inc()
            logger.warning("Message thiếu/sai field: %s | data=%s", e, data)
            send_to_dlq(kafka_producer, raw_value, f"EnrichmentError: {e}", "trades.raw")
            consumer.commit(asynchronous=False)
            continue

        kafka_producer.produce(
            topic=KAFKA_TOPIC_ENRICHED,
            key=data["s"],
            value=json.dumps(enriched).encode("utf-8"),
        )
        kafka_producer.poll(0)
        messages_processed.inc()
        consumer.commit(asynchronous=False)
