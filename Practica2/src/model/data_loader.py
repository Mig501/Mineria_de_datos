import yfinance as yf

class DataLoader:
    
    def load_bbva_data(self):
        df = yf.download("BBVA.MC", group_by='column', auto_adjust=False)

        df = df[["Open", "Close"]].dropna()

        df = df.reset_index()

        df["Open"] = df["Open"].astype(float)
        df["Close"] = df["Close"].astype(float)

        return df
