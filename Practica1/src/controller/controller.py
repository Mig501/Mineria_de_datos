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

    def run_ej1a(self,
                 source: str,
                 source_kwargs: dict | None = None,
                 preview_n: int = 8,
                ) -> DataFrame:

        if source_kwargs is None:
            source_kwargs = {}

        pdf = self.model.get_raw_data(source=source, **source_kwargs)

        sdf = self.logic.pandas_to_spark(self.spark, pdf)

        if sdf.rdd.isEmpty():
            self.view.title("No se obtuvieron datos (DataFrame vacío).")
        else:
            self.view.head(sdf, n=preview_n)
        
        return sdf

    def run_ej1b(self, source: str, source_kwargs: dict | None = None) -> DataFrame:
        sdf_raw = self.run_ej1a( source=source, source_kwargs=source_kwargs, preview_n=8)

        if sdf_raw.rdd.isEmpty():
            self.view.title("No se puede limpiar porque no hay datos.")
            return sdf_raw

        sdf_clean = self.logic.clean_and_validate_data(sdf_raw)

        if sdf_clean.rdd.isEmpty():
            self.view.title("Todos los registros fueron eliminados tras la limpieza.")
        else:
            self.view.title("""Comprobaciones realizadas:
                            1. Duplicados: se eliminan registros repetidos de mismo Date y Ticker.
                            2. Nulos: se eliminan filas con valores nulos en campos esenciales.
                            3. Tipos: se asegura que las columnas numéricas son float (DoubleType) o long (LongType).
                            4. Negativos: no se permiten precios o volúmenes negativos.
                            5. Coherencia: High >= Low, y Close debe estar dentro del rango [Low, High].""")
            self.view.head(sdf_clean, n=8)

        return sdf_clean

    def run_ej1c(self,
                 source: str,
                 source_kwargs: dict | None = None,
                 parquet_path: str = "./data/parquet/historico") -> None:

        if source_kwargs is None:
            source_kwargs = {}

        sdf_raw = self.run_ej1a(source=source, source_kwargs=source_kwargs, preview_n=8)

        if sdf_raw is None or sdf_raw.rdd.isEmpty():
            self.view.title("No hay datos de entrada, no se guarda nada.")
            return

        sdf_clean = self.logic.clean_and_validate_data(sdf_raw)

        if sdf_clean.rdd.isEmpty():
            self.view.title("Tras limpieza no quedan datos válidos.")
            return

        df_existing = self.model.read_parquet_via_spark(self.spark, parquet_path)

        df_to_store = self.logic.filter_new_tickers_for_storage(sdf_clean, df_existing)

        if df_to_store.rdd.isEmpty():
            self.view.title("Todos los tickers ya estaban guardados.")
            return

        self.model.save_parquet(df_to_store, parquet_path)

        self.view.title("Datos nuevos almacenados correctamente.")
        self.view.count(df_to_store)
        self.view.head(df_to_store, n=10)
