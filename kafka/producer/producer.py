import asyncio
import json
import logging

import websockets
# pyrefly: ignore [missing-import]
from config import (
    BASE_DELAY,
    KAFKA_TOPIC_RAW,
    MAX_DELAY,
    MAX_RETRIES,
    SYMBOLS,
    WS_URL,
)
from confluent_kafka import Producer
from prometheus_client import Counter

logger = logging.getLogger("producer")

# ── Prometheus Metrics ───────────────────────────────────
messages_produced = Counter(
    "producer_messages_produced_total",
    "Tổng số message producer đã gửi vào Kafka",
)
websocket_reconnects = Counter(
    "producer_websocket_reconnects_total",
    "Số lần producer phải reconnect WebSocket",
)


async def stream_trades(kafka_producer: Producer, stop: asyncio.Event):
    retries = 0
    while not stop.is_set():
        try:
            async with websockets.connect(WS_URL) as ws:
                logger.info("Đã kết nối WebSocket tới Binance (%d symbols)", len(SYMBOLS))
                retries = 0

                while not stop.is_set():
                    try:
                        raw_message = await asyncio.wait_for(ws.recv(), timeout=1.0)
                    except asyncio.TimeoutError:
                        continue

                    payload = json.loads(raw_message)
                    data = payload.get("data")
                    if data and all(k in data for k in ("s", "p", "q", "T")):
                        kafka_producer.produce(
                            topic=KAFKA_TOPIC_RAW,
                            key=data["s"],
                            value=json.dumps(data).encode("utf-8"),
                        )
                        messages_produced.inc()
                    kafka_producer.poll(0)

        except (websockets.exceptions.ConnectionClosed, ConnectionError, OSError) as e:
            retries += 1
            websocket_reconnects.inc()
            if retries > MAX_RETRIES:
                logger.error("Đã vượt quá %d lần retry. Dừng producer.", MAX_RETRIES)
                raise

            delay = min(BASE_DELAY * (2 ** (retries - 1)), MAX_DELAY)
            logger.warning("WebSocket bị đóng: %s. Retry %d/%d sau %ds...", e, retries, MAX_RETRIES, delay)
            await asyncio.sleep(delay)

    logger.info("Nhận shutdown signal, đang dừng...")
