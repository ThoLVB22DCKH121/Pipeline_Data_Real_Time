import os

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC_RAW = "trades.raw"

raw_symbols = os.environ.get("BINANCE_SYMBOL")
if not raw_symbols:
    raise SystemExit("Biến môi trường BINANCE_SYMBOL chưa được cấu hình.")

SYMBOLS = [s.strip() for s in raw_symbols.split(",")]
STREAMS = "/".join(f"{s}@trade" for s in SYMBOLS)
WS_URL = f"wss://stream.binance.com:9443/stream?streams={STREAMS}"

MAX_RETRIES = 10
BASE_DELAY = 1
MAX_DELAY = 60
METRICS_PORT = 8001
