"""Entry point cho Sink service.

Khởi tạo Kafka Consumer/Producer, ClickHouse client,
Prometheus metrics server, và chạy consume loop.
"""

import logging
import signal

import clickhouse_connect
# pyrefly: ignore [missing-import]
from config import (
    CLICKHOUSE_HOST,
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_PORT,
    CLICKHOUSE_USER,
    CREATE_TABLE_SQL,
    KAFKA_BOOTSTRAP,
    KAFKA_CONSUMER_GROUP,
    KAFKA_TOPIC_ENRICHED,
    METRICS_PORT,
)
from confluent_kafka import Consumer, Producer
from prometheus_client import start_http_server
# pyrefly: ignore [missing-import]
from sink import consume_loop

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("sink")

running = True


def handle_sigterm(signum, frame):
    """Xử lý SIGTERM: đặt cờ dừng vòng lặp."""
    global running
    logger.info("Nhận SIGTERM, đang shutdown gracefully...")
    running = False


def main():
    """Khởi tạo resources và chạy consume loop."""
    signal.signal(signal.SIGTERM, handle_sigterm)

    start_http_server(METRICS_PORT)

    # ── ClickHouse ───────────────────────────────────────
    ch_client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
    )
    ch_client.command(CREATE_TABLE_SQL)

    # ── Kafka ────────────────────────────────────────────
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": KAFKA_CONSUMER_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([KAFKA_TOPIC_ENRICHED])

    dlq_producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

    try:
        consume_loop(consumer, dlq_producer, ch_client, lambda: running)
    except KeyboardInterrupt:
        logger.info("Đã dừng sink.")
    finally:
        dlq_producer.flush()
        consumer.close()
        logger.info("Sink shutdown hoàn tất.")


if __name__ == "__main__":
    main()
