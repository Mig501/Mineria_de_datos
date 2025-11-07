import socket
import json
import pandas as pd
from datetime import datetime
import yfinance as yf
import importlib.util, os
from typing import List, Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

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

    def fetch_from_yfinance(self, **params) -> pd.DataFrame:
        ticker   = params.pop("ticker", "IBE.MC")
        start    = params.pop("start",  "2023-01-01")
        end      = params.pop("end",    "2023-12-31")
        interval = params.pop("interval", "1d")

        intraday_limits = {
            "1m": "7d", "2m": "60d", "5m": "60d", "15m": "60d",
            "30m": "60d", "90m": "60d", "60m": "730d", "1h": "730d",
        }

        dl_kwargs = dict(
            tickers=ticker,
            auto_adjust=False, prepost=False, progress=False, threads=False
        )

        if interval in intraday_limits:
            df = yf.download(
                interval=interval,
                period=intraday_limits[interval],
                group_by="column",           
                **dl_kwargs)
        else:
            df = yf.download(
                interval=interval,
                start=start,
                end=end,
                group_by="column",        
                **dl_kwargs)
            
        if df is None or df.empty:
            raise ValueError(f"yfinance devolvió vacío para {ticker} (interval={interval}, {start}→{end}).")

        df = df.reset_index()
        df["Ticker"] = ticker
        df = df.rename(columns={
            "Date":"Date","Open":"Open","High":"High","Low":"Low","Close":"Close","Volume":"Volume"
        })
        return df[["Date","Open","High","Low","Close","Volume","Ticker"]]

    def get_raw_data(self, source: str, **kwargs) -> pd.DataFrame:
        if source == "socket":
            return self.fetch_from_socket_or_local(
                host=kwargs.get("host", "localhost"),
                port=kwargs.get("port", 8080),
                fallback_local=kwargs.get("fallback_local", True)
            )

        if source == "yfinance":
            params = {
                "ticker":   kwargs.get("ticker", "IBE.MC"),
                "start":    kwargs.get("start",  "2023-01-01"),
                "end":      kwargs.get("end",    "2023-12-31"),
                "interval": kwargs.get("interval", "1d"),
            }
            return self.fetch_from_yfinance(**params)

        return pd.DataFrame(columns=["Date","Open","High","Low","Close","Volume","Ticker"])
        
    def read_parquet_via_spark(self, spark, path: str, cols: list[str] | None = None) -> DataFrame:
        from model.logic import Logic as L
        schema = L().EJ1A_SCHEMA

        parquet_file = os.path.join(path, "data.parquet")

        if not os.path.exists(parquet_file):
            return spark.createDataFrame([], schema=schema)

        try:
            pdf = pd.read_parquet(parquet_file)
        except Exception:
            return spark.createDataFrame([], schema=schema)

        if pdf is None or pdf.empty:
            return spark.createDataFrame([], schema=schema)

        if cols:
            cols = [c for c in cols if c in pdf.columns]
            if cols:
                pdf = pdf[cols]

        pdf = pdf.copy()
        if "Date" in pdf.columns:
            pdf["Date"] = pd.to_datetime(pdf["Date"], errors="coerce").dt.date
        for c in ["Open","High","Low","Close","Volume"]:
            if c in pdf.columns:
                pdf[c] = pd.to_numeric(pdf[c], errors="coerce")

        if not hasattr(pd.DataFrame, "iteritems"):
            setattr(pd.DataFrame, "iteritems", pd.DataFrame.items)

        if "Volume" in pdf.columns:
            pdf["Volume"] = pd.to_numeric(pdf["Volume"], errors="coerce").astype(float)

        return spark.createDataFrame(pdf, schema=schema)

    def save_parquet(self, df_spark: DataFrame, path: str,  is_streaming: bool = False) -> None:
        os.makedirs(path, exist_ok=True)

        if is_streaming:
            for name, dtype in df_spark.dtypes:
                if dtype == "date":
                    df_spark = df_spark.withColumn(name, F.date_format(F.col(name), "yyyy-MM-dd"))
                elif dtype == "timestamp":
                    df_spark = df_spark.withColumn(name, F.date_format(F.col(name), "yyyy-MM-dd HH:mm:ss"))

        parquet_file = os.path.join(path, "data.parquet")
        df_pandas = df_spark.toPandas()
        df_pandas.to_parquet(parquet_file, index=False)

    def get_current_eurusd(self) -> float:
        try:
            df = yf.download("EURUSD=X", interval="1m", period="7d",
                            progress=False, auto_adjust=False, prepost=False,
                            threads=False, group_by="column")
            if df is not None and not df.empty:
                # coge el último cierre como escalar robusto (sin FutureWarning)
                return float(df["Close"].dropna().to_numpy()[-1])
        except Exception:
            pass
        df = yf.download("EURUSD=X", interval="1d", period="5d",
                        progress=False, auto_adjust=False, prepost=False,
                        threads=False, group_by="column")
        if df is not None and not df.empty:
            return float(df["Close"].dropna().to_numpy()[-1])
        return float("nan")