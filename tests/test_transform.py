import os
import sys

# pyrefly: ignore [missing-import]
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "services", "processor"))

# pyrefly: ignore [missing-import]
from transform import enrich_trade


class TestEnrichTrade:
    def test_basic_enrichment(self):
        """Kiểm tra tính toán chính xác trường notional_value (price * quantity) cho dữ liệu chuẩn."""
        data = {"s": "BTCUSDT", "p": "50000.00", "q": "0.5", "T": 1700000000000}
        result = enrich_trade(data)

        assert result["notional_value"] == 25000.0
        assert result["s"] == "BTCUSDT"
        assert result["p"] == "50000.00"
        assert result["q"] == "0.5"
        assert result["T"] == 1700000000000

    def test_preserves_original_fields(self):
        """Kiểm tra đảm bảo các trường dữ liệu gốc (không tham gia tính toán) được giữ nguyên."""
        data = {"s": "ETHUSDT", "p": "3000", "q": "2.0", "T": 123, "extra": "field"}
        result = enrich_trade(data)

        assert result["extra"] == "field"
        assert result["notional_value"] == 6000.0

    def test_small_quantities(self):
        """Kiểm tra độ chính xác khi tính toán với số lượng cực nhỏ (float precision)."""
        data = {"s": "BTCUSDT", "p": "50000", "q": "0.00001", "T": 123}
        result = enrich_trade(data)

        assert result["notional_value"] == pytest.approx(0.5)

    def test_missing_price_raises_key_error(self):
        """Kiểm tra ném ngoại lệ KeyError nếu dữ liệu đầu vào thiếu trường giá ('p')."""
        with pytest.raises(KeyError):
            enrich_trade({"s": "BTCUSDT", "q": "0.5", "T": 123})

    def test_missing_quantity_raises_key_error(self):
        """Kiểm tra ném ngoại lệ KeyError nếu dữ liệu đầu vào thiếu trường số lượng ('q')."""
        with pytest.raises(KeyError):
            enrich_trade({"s": "BTCUSDT", "p": "50000", "T": 123})

    def test_invalid_price_raises_value_error(self):
        """Kiểm tra ném ngoại lệ ValueError nếu trường giá ('p') không thể ép kiểu sang float."""
        with pytest.raises(ValueError):
            enrich_trade({"s": "BTCUSDT", "p": "not_a_number", "q": "0.5", "T": 123})

    def test_invalid_quantity_raises_value_error(self):
        """Kiểm tra ném ngoại lệ ValueError nếu trường số lượng ('q') không thể ép kiểu sang float."""
        with pytest.raises(ValueError):
            enrich_trade({"s": "BTCUSDT", "p": "50000", "q": "abc", "T": 123})

    def test_empty_dict_raises_key_error(self):
        """Kiểm tra ném ngoại lệ KeyError khi hàm nhận được một dictionary rỗng."""
        with pytest.raises(KeyError):
            enrich_trade({})

    def test_zero_values(self):
        """Kiểm tra xử lý không bị lỗi và tính toán đúng khi các giá trị đầu vào bằng không (0)."""
        data = {"s": "BTCUSDT", "p": "0", "q": "100", "T": 123}
        result = enrich_trade(data)

        assert result["notional_value"] == 0.0
