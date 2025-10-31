import socket
import json
import pandas as pd
from datetime import datetime
import yfinance as yf
import importlib.util, os
from typing import List, Dict, Any
from pyspark.sql import SparkSession, DataFrame
from model.logic import Logic

class Model:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def _records_to_standard_pdf(self, registros: List[Dict[str, Any]]) -> pd.DataFrame:
        if not registros:
            return pd.DataFrame(columns=["Date","Open","High","Low","Close","Volume","Ticker"])

        df = pd.DataFrame(registros)

        rename_map = {}
        if "price" in df.columns and "Open" not in df.columns:
            rename_map["price"] = "Open"
        if "high" in df.columns and "High" not in df.columns:
            rename_map["high"] = "High"
        if "low" in df.columns and "Low" not in df.columns:
            rename_map["low"] = "Low"
        if "volume" in df.columns and "Volume" not in df.columns:
            rename_map["volume"] = "Volume"
        if "ticker" in df.columns and "Ticker" not in df.columns:
            rename_map["ticker"] = "Ticker"
        if "Datetime" in df.columns and "Date" not in df.columns:
            rename_map["Datetime"] = "Date"
        if "Date" not in df.columns and "date" in df.columns:
            rename_map["date"] = "Date"

        df.rename(columns=rename_map, inplace=True)

        if "Close" not in df.columns and "Open" in df.columns:
            df["Close"] = df["Open"]

        if "Date" not in df.columns:
            df["Date"] = datetime.now().date()

        if "Volume" not in df.columns:
            df["Volume"] = None

        if "Ticker" not in df.columns:
            df["Ticker"] = "DESCONOCIDO"

        df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.date

        df = df[["Date","Open","High","Low","Close","Volume","Ticker"]]
        return df

    def fetch_from_socket_or_local(self,
                                   host: str = "localhost",
                                   port: int = 8080,
                                   fallback_local: bool = True) -> pd.DataFrame:

        registros: List[Dict[str, Any]] = []

        try:
            with socket.create_connection((host, port)) as s:
                f = s.makefile("r", encoding="utf-8", newline="\n")
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        msg = json.loads(line)
                        registros.append(msg)
                    except json.JSONDecodeError:
                        continue
        except Exception as e:
            if fallback_local:
                try:
                    base_dir = os.path.dirname(os.path.dirname(__file__)) 
                    path = os.path.join(base_dir, "data", "datos.py")
                    spec = importlib.util.spec_from_file_location("datos_local", path)
                    mod = importlib.util.module_from_spec(spec)
                    assert spec and spec.loader
                    spec.loader.exec_module(mod)
                    DATA = mod.DATA

                    for ticker, rows in DATA.items():
                        for r in rows:
                            registros.append({
                                "ticker": ticker,
                                "price":  r.get("price"),
                                "high":   r.get("high"),
                                "low":    r.get("low"),
                                "volume": r.get("volume"),
                            })
                except Exception:
                    pass  

        return self._records_to_standard_pdf(registros)

    def fetch_from_yfinance(self,
                            ticker: str,
                            start: str,
                            end: str,
                            interval: str = "1d") -> pd.DataFrame:
        df_yf = yf.download(
            ticker,
            start=start,
            end=end,
            interval=interval,
            auto_adjust=False,
            progress=False
        )

        if df_yf is None or df_yf.empty:
            return pd.DataFrame(columns=["Date","Open","High","Low","Close","Volume","Ticker"])

        df_yf = df_yf.reset_index()

        registros = []
        for _, row in df_yf.iterrows():
            registros.append({
                "Date":   row.get("Date",   row.get("Datetime")),
                "Open":   row.get("Open"),
                "High":   row.get("High"),
                "Low":    row.get("Low"),
                "Close":  row.get("Close"),
                "Volume": row.get("Volume"),
                "Ticker": ticker
            })

        return self._records_to_standard_pdf(registros)

    def get_raw_data(self,
                     source: str,
                     **kwargs) -> pd.DataFrame:
        if source == "socket":
            return self.fetch_from_socket_or_local(
                host=kwargs.get("host", "localhost"),
                port=kwargs.get("port", 8080),
                fallback_local=kwargs.get("fallback_local", True)
            )
        elif source == "yfinance":
            return self.fetch_from_yfinance(
                ticker=kwargs["ticker"],
                start=kwargs["start"],
                end=kwargs["end"],
                interval=kwargs.get("interval", "1d")
            )
        else:
            return pd.DataFrame(columns=["Date","Open","High","Low","Close","Volume","Ticker"])
        
    def read_parquet_via_spark(self, spark, path: str, cols: list[str] | None = None) -> DataFrame:
        if not os.path.exists(path):
            logic_tmp = Logic()
            return spark.createDataFrame([], schema=logic_tmp.EJ1A_SCHEMA)

        pdf = pd.read_parquet(os.path.join(path, "data.parquet"))
        if cols:
            pdf = pdf[cols]
        setattr(pd.DataFrame, "iteritems", pd.DataFrame.items)

        return spark.createDataFrame(pdf)

    def save_parquet(self, df_spark: DataFrame, path: str) -> None:
        os.makedirs(path, exist_ok=True)

        df_pandas = df_spark.toPandas()

        parquet_file = os.path.join(path, "data.parquet")
        df_pandas.to_parquet(parquet_file, index=False)

        df_spark.write.mode("append").parquet(path)
