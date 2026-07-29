def build_rows(buffer):
    return [
        [b["s"], float(b["p"]), float(b["q"]), b["notional_value"], b["T"]]
        for b in buffer
    ]
