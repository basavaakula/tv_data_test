from typing import Any, Dict, List


def chartify_records(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Convert bridge payload to chart-ready records.

    - Filters out records missing `time` or OHLC values
    - Sorts by `time`
    - Returns a payload with `records` replaced by cleaned list and `nrows` updated
    """
    records = payload.get("records") or []
    cleaned: List[Dict[str, Any]] = []
    for r in records:
        t = r.get("time")
        o = r.get("open")
        h = r.get("high")
        l = r.get("low")
        c = r.get("close")
        if t is None:
            continue
        if any(v is None for v in (o, h, l, c)):
            continue
        cleaned.append({"time": int(t), "open": float(o), "high": float(h), "low": float(l), "close": float(c)})
    cleaned.sort(key=lambda x: x["time"]) if cleaned else None
    out = dict(payload)
    out["records"] = cleaned
    out["nrows"] = len(cleaned)
    return out
