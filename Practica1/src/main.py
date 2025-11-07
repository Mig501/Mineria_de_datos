import os
os.environ["HADOOP_HOME"] = "C:\\"
os.environ["hadoop.home.dir"] = "C:\\"

from model.SparkSession import *
from controller.controller import Controller
import pandas as pd

def main():
    spark = get_spark("MD_Practica1")

    ctrl = Controller(spark)
    '''
    tickers = ["BBVA.MC", "SAB.MC", "IBE.MC", "NTGY.MC", "TEF.MC", "CLNX.MC"]

    # Ejercicio 4
    ctrl.run_ej4(
        tickers=tickers,
        start="2020-01-01",
        end="2025-01-31",
        interval="1d",
        parquet_path="./data/parquet/historico",
    )
    
    '''
    #Ejercicio 5 
    ctrl.run_ej5(host="localhost", port=8080, batch_seconds=5,
                 parquet_path="./data/parquet/ej5", timeout_s=60)
    
    spark.stop()

if __name__ == "__main__":
    main()
