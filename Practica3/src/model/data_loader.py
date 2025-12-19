import yfinance as yf

def download_data(tickers, start, end):
    df = yf.download(
        tickers,
        start=start,
        end=end,
        group_by="ticker",
        auto_adjust=True
    )

    df = df.reset_index()

    df.columns = [
        col if isinstance(col, str) else "_".join(col).strip("_")
        for col in df.columns
    ]

    return df
