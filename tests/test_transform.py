import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'kafka', 'processor')))

# pyrefly: ignore [missing-import]
from transform import enrich_trade


def test_enrich_trade():
    """Test tính toán Notional Value từ Giá (p) x Khối lượng (q)"""
    mock_data = {
        "s": "BTCUSDT",
        "p": "50000.00",
        "q": "2.5",
        "T": 1678888888888,
        "m": True
    }

    result = enrich_trade(mock_data)

    assert result["s"] == "BTCUSDT"
    assert result["notional_value"] == 125000.00  # 50000 * 2.5
    assert isinstance(result["notional_value"], float)
