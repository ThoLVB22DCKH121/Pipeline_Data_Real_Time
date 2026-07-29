"""Entry point cho Processor service.

Khởi tạo Kafka Consumer/Producer, Prometheus metrics server,
đăng ký signal handler, và chạy process loop.
"""

import logging
import signal

# pyrefly: ignore [missing-import]
from config import KAFKA_BOOTSTRAP, KAFKA_CONSUMER_GROUP, KAFKA_TOPIC_RAW, METRICS_PORT
from confluent_kafka import Consumer, Producer
# pyrefly: ignore [missing-import]
from processor import process_loop
from prometheus_client import start_http_server

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("processor")

running = True


def handle_sigterm(signum, frame):
    """Xử lý SIGTERM: đặt cờ dừng vòng lặp."""
    global running
    logger.info("Nhận SIGTERM, đang shutdown gracefully...")
    running = False


def main():
    """Khởi tạo resources và chạy process loop."""
    signal.signal(signal.SIGTERM, handle_sigterm)

    start_http_server(METRICS_PORT)

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": KAFKA_CONSUMER_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([KAFKA_TOPIC_RAW])

    kafka_producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

    try:
        process_loop(consumer, kafka_producer, lambda: running)
    except KeyboardInterrupt:
        logger.info("Đã dừng processor.")
    finally:
        logger.info("Đang flush producer và đóng consumer...")
        kafka_producer.flush()
        consumer.close()
        logger.info("Processor shutdown hoàn tất.")


if __name__ == "__main__":
    main()
