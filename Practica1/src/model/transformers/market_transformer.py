# ---------------- market_transformer.py ----------------
from pyspark.sql import types as T
import pandas as pd

MARKET_SCHEMA = T.StructType([
    T.StructField("Date", T.DateType(), False),
    T.StructField("Open", T.DoubleType(), False),
    T.StructField("High", T.DoubleType(), False),
    T.StructField("Low", T.DoubleType(), False),
    T.StructField("Close", T.DoubleType(), False),
    T.StructField("Volume", T.LongType(), True),
    T.StructField("Ticker", T.StringType(), False),
])

class MarketTransformer:
    @staticmethod
    def pandas_to_spark_market_df(spark, pdf):
        if pdf.empty:
            return spark.createDataFrame(pdf, schema=MARKET_SCHEMA)
        pdf = pdf.copy()
        pdf["Date"] = pd.to_datetime(pdf["Date"]).dt.date
        return spark.createDataFrame(pdf, schema=MARKET_SCHEMA)
