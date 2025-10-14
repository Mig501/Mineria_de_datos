# ---------------- ohlc_aggregator.py ----------------
import pandas as pd
import numpy as np

def aggregate_ticks_to_ohlc(ticks_iter, window="1D"):
    rows = []
    for t in ticks_iter:
        if not all(k in t for k in ("ticker","price","high","low","volume","_ingest_ts")):
            continue
        rows.append({
            "Ticker": t["ticker"],
            "price": pd.to_numeric(t["price"], errors="coerce"),
            "high": pd.to_numeric(t["high"], errors="coerce"),
            "low": pd.to_numeric(t["low"], errors="coerce"),
            "volume": pd.to_numeric(t["volume"], errors="coerce"),
            "_ingest_ts": pd.to_datetime(t["_ingest_ts"], utc=True),
        })
    if not rows:
        return pd.DataFrame(columns=["Date","Open","High","Low","Close","Volume","Ticker"])

    df = pd.DataFrame(rows).set_index("_ingest_ts")
    out = []
    for ticker, g in df.groupby("Ticker"):
        ohlc = g["price"].resample(window).agg(["first","max","min","last"])
        ohlc.columns = ["Open","High","Low","Close"]
        vol = g["volume"].resample(window).sum(min_count=1)
        tmp = ohlc.join(vol.rename("Volume")).dropna(subset=["Open","Close"])
        tmp["Ticker"] = ticker
        tmp["Date"] = tmp.index.tz_convert(None).date
        out.append(tmp.reset_index(drop=True))
    return pd.concat(out, ignore_index=True)
