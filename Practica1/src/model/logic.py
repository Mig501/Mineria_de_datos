from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, DateType, DoubleType, LongType, StringType
import datetime
from pyspark.sql.functions import col

class Logic:
    SCHEMA = StructType([
        StructField("Date",   DateType(),   nullable=False),
        StructField("Open",   DoubleType(), nullable=False),
        StructField("High",   DoubleType(), nullable=False),
        StructField("Low",    DoubleType(), nullable=False),
        StructField("Close",  DoubleType(), nullable=False),
        StructField("Volume", LongType(),   nullable=True),
        StructField("Ticker", StringType(), nullable=False),
    ])

    def _to_float_or_none(self, x):
        if x is None: return None
        if isinstance(x, str) and x.strip().lower() == "null": return None
        try:
            return float(x)
        except Exception:
            return None

    def _to_int_or_none(self, x):
        if x is None: return None
        if isinstance(x, str) and x.strip().lower() == "null": return None
        try:
            return int(round(float(x)))
        except Exception:
            return None

    def pandas_to_spark(self, spark: SparkSession, pdf, ticker: str) -> DataFrame:
        if pdf is None or len(pdf) == 0:
            return spark.createDataFrame([], schema=self.SCHEMA)

        pdf = pdf.copy()
        if "Ticker" not in pdf.columns:
            pdf["Ticker"] = ticker if ticker else "DESCONOCIDO"

        # Normalizaciones de tipos y nulos
        pdf["Open"]  = pdf["Open"].apply(self._to_float_or_none)
        pdf["High"]  = pdf["High"].apply(self._to_float_or_none)
        pdf["Low"]   = pdf["Low"].apply(self._to_float_or_none)
        pdf["Close"] = pdf["Close"].apply(self._to_float_or_none)
        if "Volume" not in pdf.columns:
            pdf["Volume"] = None
        pdf["Volume"] = pdf["Volume"].apply(self._to_int_or_none)

        pdf["Date"] = pdf["Date"].apply(
            lambda d: d if isinstance(d, datetime.date) else
            (d.date() if hasattr(d, "date") else None)
        )

        pdf = pdf.dropna(subset=["Date", "Open", "High", "Low", "Close", "Ticker"])

        records = pdf[["Date", "Open", "High", "Low", "Close", "Volume", "Ticker"]].to_dict(orient="records")
        sdf = spark.createDataFrame(records, schema=self.SCHEMA)
        return sdf

    def clean_and_validate_data(self, sdf: DataFrame) -> DataFrame:
        sdf = sdf.na.drop(subset=["Date", "Open", "High", "Low", "Close", "Ticker"])

        sdf = sdf.withColumn("Open",  col("Open").cast(DoubleType()))
        sdf = sdf.withColumn("High",  col("High").cast(DoubleType()))
        sdf = sdf.withColumn("Low",   col("Low").cast(DoubleType()))
        sdf = sdf.withColumn("Close", col("Close").cast(DoubleType()))
        sdf = sdf.withColumn("Volume",col("Volume").cast(LongType()))

        sdf = sdf.filter(
            (col("Open")  >= 0) &
            (col("High")  >= 0) &
            (col("Low")   >= 0) &
            (col("Close") >= 0) &
            ((col("Volume") >= 0) | col("Volume").isNull())
        )

        sdf = sdf.filter(
            (col("High") >= col("Low")) &
            (col("Close") >= col("Low")) &
            (col("Close") <= col("High"))
        )

        sdf_cleaned = sdf

        return sdf_cleaned