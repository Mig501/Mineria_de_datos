from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, DateType, DoubleType, LongType, StringType
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType, LongType
import pandas as pd

class Logic:
    EJ1A_SCHEMA = StructType([
        StructField("Date",   DateType(),   nullable=False),
        StructField("Open",   DoubleType(), nullable=False),
        StructField("High",   DoubleType(), nullable=False),
        StructField("Low",    DoubleType(), nullable=False),
        StructField("Close",  DoubleType(), nullable=False),
        StructField("Volume", DoubleType(),   nullable=True),
        StructField("Ticker", StringType(), nullable=False),
    ])

    def _to_float_or_none(self, x):
        if x is None: return None
        if isinstance(x, str) and x.strip().lower() == "null": return None
        try: return float(x)
        except Exception: return None

    def _to_int_or_none(self, x):
        if x is None: return None
        if isinstance(x, str) and x.strip().lower() == "null": return None
        try: return int(round(float(x)))
        except Exception: return None

    def pandas_to_spark(self, spark: SparkSession, pdf) -> DataFrame:
        if pdf is None or len(pdf) == 0:
            return spark.createDataFrame([], schema=self.EJ1A_SCHEMA)

        pdf = pdf.copy()

        pdf["Open"]  = pdf["Open"].apply(self._to_float_or_none)
        pdf["High"]  = pdf["High"].apply(self._to_float_or_none)
        pdf["Low"]   = pdf["Low"].apply(self._to_float_or_none)
        pdf["Close"] = pdf["Close"].apply(self._to_float_or_none)
        if "Volume" not in pdf.columns:
            pdf["Volume"] = None
        pdf["Volume"] = pdf["Volume"].apply(self._to_int_or_none)

        pdf["Date"] = pd.to_datetime(pdf["Date"], errors="coerce").dt.date

        pdf = pdf.dropna(subset=["Date", "Open", "High", "Low", "Close", "Ticker"])

        records = pdf[["Date","Open","High","Low","Close","Volume","Ticker"]].to_dict(orient="records")
        sdf = spark.createDataFrame(records, schema=self.EJ1A_SCHEMA)
        return sdf

    def clean_and_validate_data(self, sdf: DataFrame) -> DataFrame:
        sdf = sdf.na.drop(subset=["Date","Open","High","Low","Close","Ticker"])

        sdf = (sdf
            .withColumn("Open", col("Open").cast("double"))
            .withColumn("High", col("High").cast("double"))
            .withColumn("Low", col("Low").cast("double"))
            .withColumn("Close", col("Close").cast("double"))
            .withColumn("Volume", col("Volume").cast("long")))

        sdf = sdf.filter(
            (col("Open") >= 0) & (col("High") >= 0) & (col("Low") >= 0) &
            (col("Close") >= 0) & ((col("Volume") >= 0) | col("Volume").isNull())
        )

        sdf = sdf.filter(
            (col("High") >= col("Low")) &
            (col("Close") >= col("Low")) &
            (col("Close") <= col("High"))
        )

        return sdf

    def filter_new_tickers_for_storage(self,
                                       df_current: DataFrame,
                                       df_stored: DataFrame) -> DataFrame:
        tickers_guardados = [row["Ticker"] for row in df_stored.select("Ticker").distinct().collect()]

        df_nuevos = df_current.filter(~col("Ticker").isin(tickers_guardados))
        
        return df_nuevos
    