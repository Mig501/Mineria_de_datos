from pyspark.sql import SparkSession, DataFrame
from view.view import View
from model.model import Model
from model.logic import Logic

class Controller:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.view = View()
        self.model = Model(spark)
        self.logic = Logic()

    def run_ej1a(self, ticker: str, start: str, end: str, interval: str , preview_n: int) -> DataFrame:
        
        pdf = self.model.load_from_socket()

        sdf = self.logic.pandas_to_spark(self.spark, pdf, ticker)

        self.view.title("Ej1-a")
        self.view.head(sdf, n=preview_n)
        self.view.schema(sdf)

        return sdf
    
    def run_ej1b(self, sdf:DataFrame) -> DataFrame:

        if sdf.rdd.isEmpty():
            self.view.title("No se puede limpiar porque no hay datos.")
            return sdf

        sdf_clean = self.logic.clean_and_validate_data(sdf)

        if sdf_clean.rdd.isEmpty():
            self.view.title("Todos los registros fueron eliminados tras la limpieza.")
        else:
            self.view.title("Ej1-b")
            self.view.count(sdf)
            self.view.count(sdf_clean)
            self.view.title("""Comprobaciones realizadas:
                                    1. Duplicados: se eliminan registros repetidos de mismo Date y Ticker.
                                    2. Nulos: se eliminan filas con valores nulos en campos esenciales.
                                    3. Tipos: se asegura que las columnas numéricas son float (DoubleType) o long (LongType).
                                    4. Negativos: no se permiten precios o volúmenes negativos.
                                    5. Coherencia: High >= Low, y Close debe estar dentro del rango [Low, High].""")
        return sdf_clean

    def finish(self):
        self.spark.stop()
        self.view.title("Spark session stopped.\n")