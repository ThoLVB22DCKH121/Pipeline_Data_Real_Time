# Prototype script - kết nối trực tiếp tới Binance WebSocket để test
# File này chỉ dùng cho mục đích học tập/debug, KHÔNG phải một phần của pipeline chính.
# Pipeline chính sử dụng: services/producer/producer.py

import asyncio
import json

import websockets

URL = "wss://stream.binance.com:9443/ws/btcusdt@trade"

async def main():
    async with websockets.connect(URL) as ws:
        while True:
            raw_message = await ws.recv()
            data = json.loads(raw_message)

            symbol = data.get("s")
            price = data.get("p")
            quantity = data.get("q")

            print(f"{symbol} | price={price} | qty={quantity}")

asyncio.run(main())
