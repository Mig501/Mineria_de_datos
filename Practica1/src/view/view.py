from pyspark.sql import DataFrame
import matplotlib.pyplot as plt
import os
import pandas as pd
from typing import Optional
class View:
    def _ensure_dir(self, path: str) -> None:
        os.makedirs(path, exist_ok=True)

    def title(self, text: str) -> None:
        print("\n" + text)

    def schema(self, df: DataFrame) -> None:
        df.printSchema()

    def head(self, df: DataFrame, n: int) -> None:
        df.show(n)

    def count(self, df: DataFrame) -> None:
        cnt = df.count()
        self.title(f"Número total de registros: {cnt}")

    def save_hist_returns(self, pdf: pd.DataFrame, out_path: str, title: Optional[str] = None) -> None:
        self._ensure_dir(os.path.dirname(out_path))
        plt.figure()
        pdf[pdf["IsFriday"] == "Friday"]["Return"].plot(kind="hist", bins=40, alpha=0.6, label="Friday")
        pdf[pdf["IsFriday"] != "Friday"]["Return"].plot(kind="hist", bins=40, alpha=0.6, label="Weekday")
        plt.xlabel("Daily return")
        plt.ylabel("Frequency")
        plt.legend()
        if title: plt.title(title)
        plt.tight_layout()
        plt.savefig(out_path, dpi=140)
        plt.close()

    def save_box_returns(self, pdf: pd.DataFrame, out_path: str, title: Optional[str] = None) -> None:
        self._ensure_dir(os.path.dirname(out_path))
        plt.figure()
        data = [pdf[pdf["IsFriday"] != "Friday"]["Return"], pdf[pdf["IsFriday"] == "Friday"]["Return"]]
        plt.boxplot(data, labels=["Weekday", "Friday"], showmeans=True)
        plt.ylabel("Daily return")
        if title: plt.title(title)
        plt.tight_layout()
        plt.savefig(out_path, dpi=140)
        plt.close()

    def save_violin_returns(self, pdf: pd.DataFrame, out_path: str, title: Optional[str] = None) -> None:
        self._ensure_dir(os.path.dirname(out_path))
        plt.figure()
        data = [pdf[pdf["IsFriday"] != "Friday"]["Return"], pdf[pdf["IsFriday"] == "Friday"]["Return"]]
        plt.violinplot(data, showmeans=True, showmedians=True)
        plt.xticks([1, 2], ["Weekday", "Friday"])
        plt.ylabel("Daily return")
        if title: plt.title(title)
        plt.tight_layout()
        plt.savefig(out_path, dpi=140)
        plt.close()

    def plot_scatter_price_volume(self, pdf, ticker: str, corr_val: float, out_path: str) -> None:
        self._ensure_dir(os.path.dirname(out_path))

        plt.figure(figsize=(6, 4))
        plt.scatter(pdf["Close"], pdf["Volume"], alpha=0.5, s=10)
        plt.title(f"{ticker} — Precio vs Volumen (r={corr_val:.2f})")
        plt.xlabel("Precio de cierre (€)")
        plt.ylabel("Volumen")
        plt.tight_layout()
        plt.savefig(os.path.join(out_path, f"{ticker}_scatter.png"))
        plt.close()

    def plot_corr_bar(self, corr_map: dict[str, float], out_path: str) -> None:
        self._ensure_dir(os.path.dirname(out_path))

        tickers = list(corr_map.keys())
        values  = [corr_map[t] for t in tickers]

        plt.figure(figsize=(8, 4))
        plt.bar(tickers, values)
        plt.axhline(0, linewidth=1)
        plt.title("Correlación Precio-Volumen por Ticker")
        plt.ylabel("Coeficiente de correlación (pearson)")
        plt.xlabel("Ticker")
        plt.tight_layout()
        plt.savefig(os.path.join(out_path, "correlaciones.png"))
        plt.close()

    def plot_all_data_per_ticker(self, df:DataFrame, out_dir:str, show_sma:bool=True):
        os.makedirs(out_dir, exist_ok=True)
        for tk, g in df.groupby("Ticker"):
            if g.empty:
                continue

            fig, ax1 = plt.subplots(figsize=(11,6))
            ax1.plot(g["EventTS"], g["Close"], label="Close", linewidth=1.4)
            if "High" in g.columns and "Low" in g.columns:
                ax1.fill_between(g["EventTS"], g["Low"], g["High"], alpha=0.15, label="Rango High-Low")
            if show_sma and "SMA" in g.columns:
                ax1.plot(g["EventTS"], g["SMA"], label="SMA(10)", linewidth=1.2)

            ax1.set_title(f"{tk} — Precio (todos los datos del parquet)")
            ax1.set_xlabel("Tiempo")
            ax1.set_ylabel("Precio (€)")
            ax1.grid(True, alpha=0.25)

            ax2 = ax1.twinx()
            ax2.bar(g["EventTS"], g["Volume"].fillna(0), alpha=0.25, width=0.02, label="Volumen")
            ax2.set_ylabel("Volumen")

            h1, l1 = ax1.get_legend_handles_labels()
            h2, l2 = ax2.get_legend_handles_labels()
            ax1.legend(h1+h2, l1+l2, loc="upper left")

            fig.autofmt_xdate()
            fig.tight_layout()
            out = os.path.join(out_dir, f"{tk}.png")
            fig.savefig(out, dpi=150)
            plt.close(fig)
            print(f"Guardado: {out}")
