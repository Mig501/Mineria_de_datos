from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, DateType, DoubleType, StringType, TimestampType, IntegerType
)
from pyspark.sql.functions import col, date_format
import pandas as pd
from pyspark.sql import functions as F

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

    EJ5_SCHEMA = StructType([
        StructField("EventTS",   TimestampType(), False),  # instante de recepción
        StructField("Date",      DateType(),      False),
        StructField("Hour",      IntegerType(),   False),
        StructField("Minute",    IntegerType(),   False),
        StructField("DayOfWeek", StringType(),    False),
        StructField("Ticker",    StringType(),    False),
        StructField("Open",      DoubleType(),    False),
        StructField("High",      DoubleType(),    False),
        StructField("Low",       DoubleType(),    False),
        StructField("Close",     DoubleType(),    False),
        StructField("Volume",    DoubleType(),    True),
        StructField("EURUSD",    DoubleType(),    False),
        StructField("RawJSON",   StringType(),    True),   # JSON crudo (opcional)
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

    def _normalize_pdf_to_schema(self, pdf: pd.DataFrame) -> pd.DataFrame:
        pdf = pdf.copy()

        if isinstance(pdf.columns, pd.MultiIndex):
            pdf.columns = ['_'.join([c for c in tup if c]) for tup in pdf.columns]

        rename_map = {
            "Datetime": "Date",
            "Adj Close": "Close",
            "AdjClose": "Close",
        }
        pdf = pdf.rename(columns=rename_map)

        needed = ["Date","Open","High","Low","Close","Volume","Ticker"]
        for c in needed:
            if c not in pdf.columns:
                pdf[c] = pd.NA

        pdf["Date"] = pd.to_datetime(pdf["Date"], errors="coerce").dt.date
        for c in ["Open","High","Low","Close"]:
            pdf[c] = pd.to_numeric(pdf[c], errors="coerce")
        pdf["Volume"] = pd.to_numeric(pdf["Volume"], errors="coerce").astype("Int64")
        pdf["Ticker"] = pdf["Ticker"].astype("string")

        pdf = pdf[needed]
        return pdf

    def pandas_to_spark(self, spark: SparkSession, pdf) -> DataFrame:
        if pdf is None or len(pdf) == 0:
            return spark.createDataFrame([], schema=self.EJ1A_SCHEMA)

        if hasattr(pdf, "columns") and isinstance(pdf.columns, pd.MultiIndex):
            pdf.columns = [a if a else b for (a, b) in pdf.columns]

        pdf = pdf.rename(columns={"Adj Close": "Close", "AdjClose": "Close", "Datetime": "Date"})

        for c in ["Date","Open","High","Low","Close","Volume","Ticker"]:
            if c not in pdf.columns:
                if c == "Close" and "Open" in pdf.columns:
                    pdf["Close"] = pdf["Open"]
                elif c == "Volume":
                    pdf["Volume"] = None
                else:
                    pdf[c] = pd.NA

        pdf["Date"] = pd.to_datetime(pdf["Date"], errors="coerce").dt.date
        for c in ["Open","High","Low","Close"]:
            pdf[c] = pd.to_numeric(pdf[c], errors="coerce")
        pdf["Volume"] = pd.to_numeric(pdf["Volume"], errors="coerce")
        pdf["Ticker"] = pdf["Ticker"].astype("string")

        pdf = pdf.copy()
        pdf["Open"]  = pdf["Open"].apply(self._to_float_or_none)
        pdf["High"]  = pdf["High"].apply(self._to_float_or_none)
        pdf["Low"]   = pdf["Low"].apply(self._to_float_or_none)
        pdf["Close"] = pdf["Close"].apply(self._to_float_or_none)
        if "Volume" not in pdf.columns:
            pdf["Volume"] = None
        pdf["Volume"] = pdf["Volume"].apply(self._to_float_or_none)

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
            .withColumn("Volume", col("Volume").cast("long"))
        )

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

        return sdf

    def filter_new_tickers_for_storage(self,
                                       df_current: DataFrame,
                                       df_stored: DataFrame) -> DataFrame:
        if df_stored is None or df_stored.rdd.isEmpty():
            return df_current

        tickers_guardados = [r["Ticker"] for r in df_stored.select("Ticker").distinct().collect()]
        df_nuevos = df_current.filter(~col("Ticker").isin(tickers_guardados))
        return df_nuevos

    def add_day_of_week(self, df: DataFrame) -> DataFrame:
        return df.withColumn("DayOfWeek", date_format("Date", "EEEE"))

    def map_stream_ej5(self, df_json: DataFrame, eurusd_value: float):
        now = F.current_timestamp()
        return (
            df_json
            .withColumn("RawJSON", F.to_json(F.struct("*")))
            .withColumnRenamed("ticker", "Ticker")
            .withColumnRenamed("price",  "Open")
            .withColumnRenamed("high",   "High")
            .withColumnRenamed("low",    "Low")
            .withColumnRenamed("volume", "Volume")
            .withColumn("Close",  F.col("Open"))
            .withColumn("Open",   F.col("Open").cast("double"))
            .withColumn("High",   F.col("High").cast("double"))
            .withColumn("Low",    F.col("Low").cast("double"))
            .withColumn("Close",  F.col("Close").cast("double"))
            .withColumn("Volume", F.col("Volume").cast("double"))
            .withColumn("EventTS",   now)
            .withColumn("Date",      F.to_date(now))
            .withColumn("Hour",      F.hour(now))
            .withColumn("Minute",    F.minute(now))
            .withColumn("DayOfWeek", F.date_format(now, "EEEE"))
            .withColumn("EURUSD", F.lit(float(eurusd_value)))
            .select("EventTS","Date","Hour","Minute","DayOfWeek",
                    "Ticker","Open","High","Low","Close","Volume","EURUSD","RawJSON")
        )