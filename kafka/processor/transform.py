def enrich_trade(data):
    return {
        **data,
        "notional_value": float(data["p"]) * float(data["q"]),
    }
