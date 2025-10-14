# ---------------- main.py ----------------
from controller.controller import Controller
from view.view import CLIView
from model.connection.spark_session import SparkSessionManager

def main():
    ctrl = Controller()

    # Ejemplo de uso — Ej1a con YFinance
    # sdf = ctrl.ej1a_download_history_to_spark(
    #     ticker="AAPL", start="2024-01-01", end="2024-12-31", interval="1d",
    #     parquet_path="./data/market/aapl"
    # )

    # Ejemplo de uso — Ej1a con Socket (datos.py)
    sdf = ctrl.ej1a_from_socket(
        host="localhost",
        port=8080,
        max_rows=100,
        window="1D",
        parquet_path="./data/market/socket_demo"
    )

    CLIView.show_head(sdf, n=5)
    SparkSessionManager.stop()

if __name__ == "__main__":
    main()
