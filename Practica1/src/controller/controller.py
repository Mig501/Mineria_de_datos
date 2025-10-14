# ---------------- controller.py ----------------
from model.connection.spark_session import SparkSessionManager
from model.datasources.yahoo_source import YahooFinanceSource
from model.datasources.socket_source import SocketTickSource
from model.transformers.market_transformer import MarketTransformer
from model.transformers.ohlc_aggregator import aggregate_ticks_to_ohlc
from model.io.parquet_io import ParquetIO

class Controller:
    def __init__(self):
        self.spark = SparkSessionManager.get_session()
        self.source = YahooFinanceSource()

    # === Ej1a: YFinance ===
    def ej1a_download_history_to_spark(
        self, ticker, start, end, interval="1d", parquet_path=None, save_mode="overwrite"
    ):
        pdf = self.source.fetch_history(ticker, start, end, interval)
        sdf = MarketTransformer.pandas_to_spark_market_df(self.spark, pdf)
        if parquet_path:
            ParquetIO.write_parquet(sdf, parquet_path, mode=save_mode)
        return sdf

    # === Ej1a: Socket ===
    def ej1a_from_socket(
        self, host="localhost", port=8080, max_rows=200, window="1D",
        parquet_path=None, save_mode="overwrite"
    ):
        src = SocketTickSource(host, port)
        ticks_iter = src.iter_ticks(max_rows=max_rows)
        pdf = aggregate_ticks_to_ohlc(ticks_iter, window=window)
        sdf = MarketTransformer.pandas_to_spark_market_df(self.spark, pdf)
        if parquet_path:
            ParquetIO.write_parquet(sdf, parquet_path, mode=save_mode)
        return sdf
