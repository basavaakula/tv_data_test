"""Bridge wrapper to expose helper entrypoints as a simple class.

This file is intended to be imported into Pyodide runtime and provide a
small, testable class that the JS orchestrator can call after loading
`helpers.py`.

Example usage in Pyodide:

    from scripts.py.bridge import DuckDBBridge
    bridge = DuckDBBridge()
    result = bridge.from_arrow(buf)

Methods simply delegate to the corresponding functions in `helpers.py`.
"""
from typing import Any, Dict, Optional, List, Tuple
import pandas as pd


def _detect_date_column(df: pd.DataFrame) -> Optional[str]:
    for c in df.columns:
        if any(kw in c.lower() for kw in ("date", "time", "timestamp")):
            return c
    for c in df.columns:
        try:
            if pd.api.types.is_datetime64_any_dtype(df[c]):
                return c
        except Exception:
            continue
    return None


def _to_epoch_ms(series: pd.Series) -> Optional[pd.Series]:
    try:
        dt = pd.to_datetime(series, errors="coerce", utc=True)
        return dt.astype("datetime64[ms]").astype("int64")
    except Exception:
        try:
            dt = pd.to_datetime(series, errors="coerce")
            return dt.astype("datetime64[ms]").astype("int64")
        except Exception:
            return None


def _detect_ohlc_columns(cols: List[str]) -> Dict[str, Optional[str]]:
    def find(keywords: Tuple[str, ...]) -> Optional[str]:
        for c in cols:
            if c.lower() in keywords:
                return c
        for kw in keywords:
            for c in cols:
                if kw in c.lower():
                    return c
        return None

    return {
        "open": find(("open", "o")),
        "high": find(("high", "h")),
        "low": find(("low", "l")),
        "close": find(("close", "c", "adj close", "adj_close")),
    }


def _build_records(df: pd.DataFrame) -> List[Dict[str, Any]]:
    date_col = _detect_date_column(df)
    ohlc = _detect_ohlc_columns(list(df.columns))
    epoch_ms = _to_epoch_ms(df[date_col]) if date_col else None

    def val(col: Optional[str]):
        return df[col].to_numpy() if col and col in df.columns else None

    o = val(ohlc.get("open"))
    h = val(ohlc.get("high"))
    l = val(ohlc.get("low"))
    c = val(ohlc.get("close"))

    records: List[Dict[str, Any]] = []
    for i in range(len(df)):
        try:
            t = int(epoch_ms.iloc[i]) if epoch_ms is not None else None
            records.append({
                "time": t,
                "open": float(o[i]) if o is not None else None,
                "high": float(h[i]) if h is not None else None,
                "low": float(l[i]) if l is not None else None,
                "close": float(c[i]) if c is not None else None,
            })
        except Exception:
            continue
    return records


def _table_to_df_from_arrow(buf: Any) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    try:
        import pyarrow as pa
        reader = pa.ipc.open_stream(buf)
        table = reader.read_all()
        return table.to_pandas(), None
    except Exception:
        try:
            import arro3.core as ac
            table = ac.Table.from_ipc(buf)
            return table.to_pandas(), None
        except Exception as e:
            return None, f"Arrow parse failed: {e}"


class DuckDBBridge:
    """Bridge that implements parsing methods directly.

    Methods return dict payloads compatible with the frontend: head, columns,
    nrows, source, records, debug OR error.
    """

    def from_arrow(self, buf: Any) -> Dict[str, Any]:
        df, err = _table_to_df_from_arrow(buf)
        if err:
            return {"error": err}
        if df is None:
            return {"error": "failed to parse arrow buffer"}
        if df.empty:
            return {"head": [], "columns": list(df.columns), "nrows": 0, "source": "arrow", "records": []}
        records = _build_records(df)
        head = df.head(10).to_dict(orient="records")
        return {"head": head, "columns": list(df.columns), "nrows": int(len(df)), "source": "arrow", "records": records, "debug": {"date_col": _detect_date_column(df)}}

    def from_rows(self, rows: Any) -> Dict[str, Any]:
        try:
            df = pd.DataFrame(rows)
        except Exception as e:
            return {"error": f"bad rows: {e}"}
        if df.empty:
            return {"head": [], "columns": list(df.columns), "nrows": 0, "source": "rows", "records": []}
        records = _build_records(df)
        head = df.head(10).to_dict(orient="records")
        return {"head": head, "columns": list(df.columns), "nrows": int(len(df)), "source": "rows", "records": records, "debug": {"date_col": _detect_date_column(df)}}

    def from_columns(self, cols: Dict[str, Any], dtypes: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        dtypes = dtypes or {}
        import numpy as np
        try:
            cols = dict(cols)
        except Exception:
            pass
        rebuilt: Dict[str, Any] = {}
        for name, val in cols.items():
            dt = dtypes.get(name, "object")
            try:
                is_mem = isinstance(val, (memoryview, bytes, bytearray)) or hasattr(val, "tobytes")
            except Exception:
                is_mem = False
            if is_mem and dt != "object":
                try:
                    buf = val if isinstance(val, memoryview) else memoryview(val)
                    if dt == "float64":
                        arr = np.frombuffer(buf, dtype=np.float64)
                    elif dt == "int64":
                        arr = np.frombuffer(buf, dtype=np.int64)
                    elif dt == "bool":
                        arr = np.frombuffer(buf, dtype=np.bool_)
                    else:
                        arr = np.frombuffer(buf, dtype=np.float64)
                    rebuilt[name] = arr
                except Exception:
                    try:
                        rebuilt[name] = list(val)
                    except Exception:
                        rebuilt[name] = None
            else:
                rebuilt[name] = list(val) if val is not None else []

        try:
            import pyarrow as pa
            table = pa.table(rebuilt)
            df = table.to_pandas()
        except Exception:
            df = pd.DataFrame(rebuilt)

        if df is None or df.empty:
            return {"head": [], "columns": list(df.columns) if df is not None else [], "nrows": 0, "source": "cols", "records": []}
        records = _build_records(df)
        head = df.head(10).to_dict(orient="records")
        return {"head": head, "columns": list(df.columns), "nrows": int(len(df)), "source": "cols", "records": records, "debug": {"date_col": _detect_date_column(df)}}


# convenience instance
bridge = DuckDBBridge()
