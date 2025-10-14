# ---------------- yahoo_source.py ----------------
import pandas as pd
import yfinance as yf

class YahooFinanceSource:
    def fetch_history(self, ticker, start, end, interval="1d") -> pd.DataFrame:
        df = yf.download(ticker, start=start, end=end, interval=interval, progress=False)
        if df.empty:
            return pd.DataFrame(columns=["Date","Open","High","Low","Close","Volume","Ticker"])
        df.reset_index(inplace=True)
        df["Ticker"] = ticker
        df = df[["Date","Open","High","Low","Close","Volume","Ticker"]]
        df["Date"] = pd.to_datetime(df["Date"]).dt.date
        return df
