import asyncio
import json
import logging
import os
import signal

import websockets
from confluent_kafka import Producer
from prometheus_client import Counter, start_http_server

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("producer")

raw_symbols = os.environ.get("BINANCE_SYMBOL")
if not raw_symbols:
    raise SystemExit("Biến môi trường BINANCE_SYMBOL chưa được cấu hình.")

symbols = raw_symbols.split(",")
streams = "/".join(f"{s.strip()}@trade" for s in symbols)
URL = f"wss://stream.binance.com:9443/stream?streams={streams}"

producer = Producer({"bootstrap.servers": "kafka:9092"})

messages_produced = Counter(
    "producer_messages_produced_total",
    "Tổng số message producer đã gửi vào Kafka"
)
websocket_reconnects = Counter(
    "producer_websocket_reconnects_total",
    "Số lần producer phải reconnect WebSocket"
)

start_http_server(8001)

MAX_RETRIES = 10
BASE_DELAY = 1


async def main():
    loop = asyncio.get_running_loop()
    stop = asyncio.Event()

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass

    retries = 0
    while not stop.is_set():
        try:
            async with websockets.connect(URL) as ws:
                logger.info("Đã kết nối WebSocket tới Binance (%d symbols)", len(symbols))
                retries = 0

                while not stop.is_set():
                    try:
                        raw_message = await asyncio.wait_for(ws.recv(), timeout=1.0)
                    except asyncio.TimeoutError:
                        continue

                    payload = json.loads(raw_message)
                    data = payload.get("data")
                    if data is not None and "s" in data and "p" in data and "q" in data and "T" in data:
                        producer.produce(
                            topic="trades.raw",
                            key=data.get("s"),
                            value=json.dumps(data).encode("utf-8"),
                        )
                        messages_produced.inc()
                    producer.poll(0)

        except (websockets.exceptions.ConnectionClosed, ConnectionError, OSError) as e:
            retries += 1
            websocket_reconnects.inc()
            if retries > MAX_RETRIES:
                logger.error("Đã vượt quá %d lần retry. Dừng producer.", MAX_RETRIES)
                raise

            delay = min(BASE_DELAY * (2 ** (retries - 1)), 60)
            logger.warning("WebSocket bị đóng: %s. Retry %d/%d sau %ds...", e, retries, MAX_RETRIES, delay)
            await asyncio.sleep(delay)

    logger.info("Nhận shutdown signal, đang dừng...")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Đã dừng producer.")
    finally:
        logger.info("Đang flush các message còn lại...")
        producer.flush()
        logger.info("Producer shutdown hoàn tất.")
