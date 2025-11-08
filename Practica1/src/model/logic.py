from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.types import (
    StructType, StructField, DateType, DoubleType, StringType, TimestampType, IntegerType
)
import pandas as pd
from pyspark.sql import functions as F
import numpy as np

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
            .withColumn("Open", F.col("Open").cast("double"))
            .withColumn("High", F.col("High").cast("double"))
            .withColumn("Low", F.col("Low").cast("double"))
            .withColumn("Close", F.col("Close").cast("double"))
            .withColumn("Volume", F.col("Volume").cast("long"))
        )

        sdf = sdf.filter(
            (F.col("Open")  >= 0) &
            (F.col("High")  >= 0) &
            (F.col("Low")   >= 0) &
            (F.col("Close") >= 0) &
            ((F.col("Volume") >= 0) | F.col("Volume").isNull())
        )

        sdf = sdf.filter(
            (F.col("High") >= F.col("Low")) &
            (F.col("Close") >= F.col("Low")) &
            (F.col("Close") <= F.col("High"))
        )

        return sdf

    def filter_new_tickers_for_storage(self,
                                       df_current: DataFrame,
                                       df_stored: DataFrame) -> DataFrame:
        if df_stored is None or df_stored.rdd.isEmpty():
            return df_current

        tickers_guardados = [r["Ticker"] for r in df_stored.select("Ticker").distinct().collect()]
        df_nuevos = df_current.filter(~F.col("Ticker").isin(tickers_guardados))
        return df_nuevos

    def add_day_of_week(self, df: DataFrame) -> DataFrame:
        return df.withColumn("DayOfWeek", F.date_format("Date", "EEEE"))

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
    
    def add_is_friday(self, df: DataFrame) -> DataFrame:
        return df.withColumn(
            "IsFriday",
            F.when(F.dayofweek(F.col("Date")) == 6, "Friday").otherwise("Weekday")
        )

    def add_daily_returns(self, df: DataFrame) -> DataFrame:
        w = Window.partitionBy("Ticker").orderBy("Date")
        prev_close = F.lag(F.col("Close")).over(w)
        df_ret = df.withColumn("Return", ((F.col("Close") - prev_close) / prev_close) * 100.0)
        return df_ret.filter(F.col("Return").isNotNull())

    def prepare_friday_effect(self, df: DataFrame) -> DataFrame:
        df1 = self.add_day_of_week(df)
        df2 = self.add_is_friday(df1)
        df3 = self.add_daily_returns(df2)
        return df3.select("Date", "Ticker", "Close", "Return", "DayOfWeek", "IsFriday")
    
    def add_simple_return(self, df) -> DataFrame:
        w = Window.partitionBy("Ticker").orderBy("Date")
        return df.withColumn(
            "Return",
            (F.col("Close") / F.lag("Close", 1).over(w) - 1.0)
        )
    
    def corr_price_volume_by_ticker(self, df: DataFrame) -> dict[str, float]:
        if df.rdd.isEmpty():
            return {}
        corr_df = (
            df.groupBy("Ticker")
              .agg(F.corr(F.col("Close"), F.col("Volume")).alias("corr"))
        )
        rows = corr_df.collect()
        return {r["Ticker"]: (float(r["corr"]) if r["corr"] is not None else float("nan"))
                for r in rows}

    def price_volume_pdf(self, df: DataFrame, ticker: str, max_points: int = 5000) -> pd.DataFrame:
        sdf = df.filter(F.col("Ticker") == ticker).select("Date", "Close", "Volume")
        if sdf.rdd.isEmpty():
            return pd.DataFrame(columns=["Date", "Close", "Volume"])

        pdf = sdf.toPandas()
        pdf = pdf.dropna(subset=["Close", "Volume"])
        if len(pdf) > max_points:
            pdf = pdf.sample(n=max_points, random_state=42)
        return pdf
    
    def normalize_parquet_any(self, pdf: pd.DataFrame) -> pd.DataFrame:
        df = pdf.copy()

        rename_map = {}
        if "price" in df.columns and "Close" not in df.columns: rename_map["price"] = "Close"
        if "high"  in df.columns and "High"  not in df.columns: rename_map["high"]  = "High"
        if "low"   in df.columns and "Low"   not in df.columns: rename_map["low"]   = "Low"
        if "volume" in df.columns and "Volume" not in df.columns: rename_map["volume"] = "Volume"
        if "ticker" in df.columns and "Ticker" not in df.columns: rename_map["ticker"] = "Ticker"
        if rename_map:
            df = df.rename(columns=rename_map)

        if "EventTS" not in df.columns:
            if "Datetime" in df.columns:
                df = df.rename(columns={"Datetime": "EventTS"})
            elif "Date" in df.columns:
                df["EventTS"] = pd.to_datetime(df["Date"], errors="coerce")
        if "EventTS" in df.columns:
            df["EventTS"] = pd.to_datetime(df["EventTS"], errors="coerce")

        if "EventTS" not in df.columns or df["EventTS"].isna().all():
            df = df.reset_index(drop=True)
            df["EventTS"] = pd.to_datetime(pd.Series(range(len(df))), unit="s", origin="unix")

        if "Close" not in df.columns and "Open" in df.columns:
            df["Close"] = df["Open"]
        for c in ["Close", "High", "Low", "Volume", "Ticker"]:
            if c not in df.columns:
                df[c] = np.nan if c != "Ticker" else "UNKNOWN"

        for c in ["Close", "High", "Low"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        df["Volume"] = pd.to_numeric(df["Volume"], errors="coerce")

        df = df.dropna(subset=["Ticker", "Close"])
        df = df.sort_values(["Ticker", "EventTS"]).reset_index(drop=True)

        return df[["EventTS", "Ticker", "Close", "High", "Low", "Volume"]]

    def add_simple_features(self, df: pd.DataFrame, sma_window:int=10) -> pd.DataFrame:
        out = []
        for tk, g in df.groupby("Ticker"):
            g = g.sort_values("EventTS").copy()
            g["SMA"] = g["Close"].rolling(sma_window, min_periods=1).mean()
            out.append(g)
        return pd.concat(out, ignore_index=True)
