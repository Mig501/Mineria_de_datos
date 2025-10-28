import socket
import json
import pandas as pd
from datetime import datetime

class Model:
    def __init__(self, spark):
        self.spark = spark

    def load_from_socket(self, host="localhost", port=8080, max_rows=50) -> pd.DataFrame:
        """
        Se conecta al servidor definido en data/datos.py y recibe datos JSON.
        Devuelve un DataFrame de pandas con columnas: Date, Open, High, Low, Close, Volume, Ticker
        """
        registros = []
        print(f"Conectando al servidor de datos en {host}:{port}...")

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((host, port))
            buffer = b""
            while len(registros) < max_rows:
                chunk = s.recv(1024)
                if not chunk:
                    break
                buffer += chunk
                while b"\n" in buffer:
                    line, buffer = buffer.split(b"\n", 1)
                    try:
                        msg = json.loads(line.decode("utf-8"))
                        registros.append(msg)
                    except json.JSONDecodeError:
                        continue

        if not registros:
            print("No se recibieron datos.")
            return pd.DataFrame(columns=["Date", "Open", "High", "Low", "Close", "Volume", "Ticker"])

        df = pd.DataFrame(registros)
        df.rename(columns={"price": "Open", "high": "High", "low": "Low", "volume": "Volume"}, inplace=True)
        df["Close"] = df["Open"]
        df["Date"] = datetime.now().date()
        df = df[["Date", "Open", "High", "Low", "Close", "Volume", "ticker"]]
        df.rename(columns={"ticker": "Ticker"}, inplace=True)
        return df
