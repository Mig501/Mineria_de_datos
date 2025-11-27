import yfinance as yf
import pandas as pd
class DataLoader:
    
    def load_bbva_data(self):
        df = yf.download("BBVA.MC", group_by='column', auto_adjust=False)

        df = df[["Open", "Close"]].dropna()

        df = df.reset_index()

        df["Open"] = df["Open"].astype(float)
        df["Close"] = df["Close"].astype(float)

        return df

    def load_ticker(self, ticker):
        df = yf.download(
            ticker,
            start="2024-12-01",
            end="2025-01-01",
            progress=False
        ).reset_index()

        df["Ticker"] = ticker
        return df

    def load_multiple_tickers(self, tickers):
        dfs = []
        for t in tickers:
            df_t = self.load_ticker(t)
            dfs.append(df_t)

        df_final = pd.concat(dfs, ignore_index=True)
        return df_final