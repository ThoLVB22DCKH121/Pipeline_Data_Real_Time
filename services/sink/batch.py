def build_rows(buffer):
    """Convert buffer of trade dicts to row tuples for ClickHouse insert.

    Args:
        buffer: List of enriched trade dicts, mỗi dict chứa 's', 'p', 'q',
                'notional_value', 'T'.

    Returns:
        List of lists, mỗi list là một row [symbol, price, quantity,
        notional_value, trade_time_ms].
    """
    return [
        [b["s"], float(b["p"]), float(b["q"]), b["notional_value"], b["T"]]
        for b in buffer
    ]
