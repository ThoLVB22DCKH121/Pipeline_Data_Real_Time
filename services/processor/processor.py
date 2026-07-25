import json
import logging
import signal
import time

from confluent_kafka import Consumer, Producer
from prometheus_client import Counter, start_http_server
from transform import enrich_trade

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("processor")

messages_processed = Counter(
    "processor_messages_processed_total",
    "Tổng số message processor đã xử lý thành công"
)
messages_failed = Counter(
    "processor_messages_failed_total",
    "Tổng số message processor gặp lỗi"
)
messages_dlq = Counter(
    "processor_messages_dlq_total",
    "Tổng số message đã gửi vào Dead Letter Queue"
)

start_http_server(8000)

consumer = Consumer({
    "bootstrap.servers": "kafka:9092",
    "group.id": "processor-group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
})
consumer.subscribe(["trades.raw"])

kafka_producer = Producer({"bootstrap.servers": "kafka:9092"})

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
            "source_topic": "trades.raw",
            "failed_at": time.time(),
        }).encode("utf-8")
        kafka_producer.produce(topic="trades.dlq", value=dlq_message)
        kafka_producer.poll(0)
        messages_dlq.inc()
        logger.info("Đã gửi message lỗi vào trades.dlq")
    except Exception as e:
        logger.error("Không thể gửi message vào DLQ: %s", e)


logger.info("Processor đã khởi động, đang consume từ trades.raw...")

try:
    while running:
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
            send_to_dlq(raw_value, f"JSONDecodeError: {e}")
            consumer.commit(asynchronous=False)
            continue

        try:
            enriched = enrich_trade(data)
        except (KeyError, ValueError, TypeError) as e:
            messages_failed.inc()
            logger.warning("Message thiếu/sai field: %s | data=%s", e, data)
            send_to_dlq(raw_value, f"EnrichmentError: {e}")
            consumer.commit(asynchronous=False)
            continue

        kafka_producer.produce(
            topic="trades.enriched",
            key=data["s"],
            value=json.dumps(enriched).encode("utf-8"),
        )
        kafka_producer.poll(0)
        messages_processed.inc()
        consumer.commit(asynchronous=False)

except KeyboardInterrupt:
    logger.info("Đã dừng processor.")

finally:
    logger.info("Đang flush producer và đóng consumer...")
    kafka_producer.flush()
    consumer.close()
    logger.info("Processor shutdown hoàn tất.")
