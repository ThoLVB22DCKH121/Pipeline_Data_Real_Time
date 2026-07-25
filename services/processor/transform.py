def enrich_trade(data):
    """Enrich raw trade data by computing notional_value (price * quantity).

    Args:
        data: Dict chứa ít nhất các field 'p' (price) và 'q' (quantity).

    Returns:
        Dict gốc kèm thêm field 'notional_value'.

    Raises:
        KeyError: Nếu thiếu field 'p' hoặc 'q'.
        ValueError: Nếu 'p' hoặc 'q' không parse được thành float.
    """
    return {
        **data,
        "notional_value": float(data["p"]) * float(data["q"]),
    }
