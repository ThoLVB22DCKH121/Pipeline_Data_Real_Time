"""Entry point cho Producer service.

Khởi tạo Kafka Producer, Prometheus metrics server,
và chạy vòng lặp WebSocket stream.
"""

import asyncio
import logging
import signal

# pyrefly: ignore [missing-import]
from config import KAFKA_BOOTSTRAP, METRICS_PORT
from confluent_kafka import Producer

# pyrefly: ignore [missing-import]
from producer import stream_trades
from prometheus_client import start_http_server

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("producer")


async def run():
    kafka_producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    start_http_server(METRICS_PORT)

    loop = asyncio.get_running_loop()
    stop = asyncio.Event()

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass

    try:
        await stream_trades(kafka_producer, stop)
    finally:
        logger.info("Đang flush các message còn lại...")
        kafka_producer.flush()
        logger.info("Producer shutdown hoàn tất.")


if __name__ == "__main__":
    asyncio.run(run())
