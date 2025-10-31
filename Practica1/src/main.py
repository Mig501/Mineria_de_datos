import os
os.environ["HADOOP_HOME"] = "C:\\"
os.environ["hadoop.home.dir"] = "C:\\"

from model.SparkSession import *
from controller.controller import Controller
import pandas as pd

def main():
    spark = get_spark("MD_Practica1")

    ctrl = Controller(spark)

    
    # Ejemplo 1: usar datos en vivo / socket / fallback a datos.py
    ctrl.run_ej1c(
        source="socket",
        source_kwargs={
            "host": "localhost",
            "port": 8080,
            "fallback_local": True
        },
        parquet_path="./data/parquet/historico"
    )

    '''
    ctrl.run_ej1c(
        source="yfinance",
        source_kwargs={
            "ticker": "IBE.MC",           
            "start": "2023-01-01",        
            "end": "2023-12-31",          
            "interval": "1d"              
        },
        parquet_path="./data/parquet/historico"
    )
    '''
    spark.stop()

if __name__ == "__main__":
    main()
