import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "services", "sink"))

# pyrefly: ignore [missing-import]
from batch import build_rows


class TestBuildRows:
    def test_single_record(self):
        """Kiểm tra chuyển đổi một bản ghi duy nhất thành một dòng (row) hợp lệ."""
        buffer = [
            {"s": "BTCUSDT", "p": "50000", "q": "0.5", "notional_value": 25000.0, "T": 1700000000000}
        ]
        rows = build_rows(buffer)

        assert len(rows) == 1
        assert rows[0] == ["BTCUSDT", 50000.0, 0.5, 25000.0, 1700000000000]

    def test_multiple_records(self):
        """Kiểm tra chuyển đổi danh sách nhiều bản ghi thành nhiều dòng tương ứng."""
        buffer = [
            {"s": "BTCUSDT", "p": "50000", "q": "0.5", "notional_value": 25000.0, "T": 100},
            {"s": "ETHUSDT", "p": "3000", "q": "2.0", "notional_value": 6000.0, "T": 200},
            {"s": "BNBUSDT", "p": "300", "q": "10", "notional_value": 3000.0, "T": 300},
        ]
        rows = build_rows(buffer)

        assert len(rows) == 3
        assert rows[0][0] == "BTCUSDT"
        assert rows[1][0] == "ETHUSDT"
        assert rows[2][0] == "BNBUSDT"

    def test_empty_buffer(self):
        """Kiểm tra xử lý danh sách rỗng, phải trả về một mảng rỗng mà không gây lỗi."""
        assert build_rows([]) == []

    def test_string_price_converted_to_float(self):
        """Kiểm tra tự động ép kiểu các giá trị chuỗi (string) thành số thực (float)."""
        buffer = [
            {"s": "BTCUSDT", "p": "50000.123", "q": "0.5", "notional_value": 25000.0, "T": 100}
        ]
        rows = build_rows(buffer)

        assert isinstance(rows[0][1], float)
        assert rows[0][1] == 50000.123

    def test_column_order(self):
        """Kiểm tra thứ tự các cột được tạo ra phải khớp với schema bảng trades trong ClickHouse."""
        buffer = [
            {"s": "BTCUSDT", "p": "50000", "q": "0.5", "notional_value": 25000.0, "T": 999}
        ]
        row = build_rows(buffer)[0]

        assert row[0] == "BTCUSDT"          # symbol
        assert row[1] == 50000.0             # price
        assert row[2] == 0.5                 # quantity
        assert row[3] == 25000.0             # notional_value
        assert row[4] == 999                 # trade_time_ms
