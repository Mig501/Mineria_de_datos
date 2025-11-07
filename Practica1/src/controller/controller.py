from pyspark.sql import SparkSession, DataFrame
from view.view import View
from model.model import Model
from model.logic import Logic
from pyspark.sql import functions as F
from functools import reduce
from pyspark.streaming import StreamingContext
class Controller:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.view = View()
        self.model = Model(spark)
        self.logic = Logic()

    def _build_stream(self, host: str, port: int, batch_seconds: int):
        sc  = self.spark.sparkContext
        sc.setLogLevel("ERROR")
        ssc = StreamingContext(sc, batch_seconds)
        lines = ssc.socketTextStream(host, port)
        return ssc, lines
    
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
        sdf_raw = self.run_ej1a(source=source, source_kwargs=source_kwargs, preview_n=8)

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
        
    def run_ej2(self,
                source: str,
                source_kwargs: dict | None = None,
                parquet_path: str = "./data/parquet/historico") -> None:

        if source_kwargs is None:
            source_kwargs = {}
        
        self.run_ej1c(source=source, source_kwargs=source_kwargs, parquet_path=parquet_path)
        sdf = self.model.read_parquet_via_spark(self.spark, parquet_path)

        if sdf is None or sdf.rdd.isEmpty():
            self.view.title("No hay datos cargados para procesar.")
            return

        sdf_new = self.logic.add_day_of_week(sdf)

        self.view.head(sdf_new, n=10)
        self.view.schema(sdf_new)

    def run_ej4(self,
                tickers: list[str],
                start: str = "2020-01-01",
                end: str = "2025-01-31",
                interval: str = "1d",
                parquet_path: str = "./data/parquet/historico") -> None:

        nuevos = []
        for tk in tickers:
            try:
                pdf = self.model.get_raw_data(
                    source="yfinance",
                    ticker=tk, start=start, end=end, interval=interval
                )
                sdf_raw   = self.logic.pandas_to_spark(self.spark, pdf)
                sdf_clean = self.logic.clean_and_validate_data(sdf_raw)
                nuevos.append(sdf_clean)
            except Exception as e:
                self.view.title(f"EJ4: Error al descargar {tk}: {e}")

        if not nuevos:
            self.view.title("EJ4: No se descargó nada nuevo.")
            return

        sdf_nuevos = reduce(lambda a,b: a.unionByName(b), nuevos)

        sdf_existente = self.model.read_parquet_via_spark(self.spark, parquet_path)

        sdf_merged = (
            sdf_existente.unionByName(sdf_nuevos)
            .withColumn("Date", F.to_date("Date"))  
            .dropDuplicates(["Ticker","Date"])
            .orderBy("Ticker","Date")
        )

        self.model.save_parquet(sdf_merged, parquet_path)

        sdf_all = self.model.read_parquet_via_spark(self.spark, parquet_path)
        sdf_all = (
            sdf_all
            .withColumn("Date", F.to_date("Date"))
            .filter((F.col("Date") >= F.lit(start)) & (F.col("Date") <= F.lit(end)))
            .filter(F.col("Ticker").isin(tickers))
            .orderBy("Ticker","Date")
        )

        self.view.title("EJ4: Primeros 5 del histórico combinado")
        self.view.head(sdf_all, 5)

        self.view.title("EJ4: Primeros 5 por empresa con DayOfWeek")
        sdf_dow = self.logic.add_day_of_week(sdf_all)
        for tk in tickers:
            self.view.title(f"  • {tk}")
            self.view.head(sdf_dow.filter(F.col("Ticker")==tk), 5)

    def run_ej4_conexion(self, host="localhost", port=8080, batch_seconds=5):
        self.view.title("EJ4: Probando conexión de streaming")
        ssc, lines = self._build_stream(host, port, batch_seconds)

        lines.pprint()

        ssc.start()
        ssc.awaitTermination()

    def run_ej5(self, host="localhost", port=8080, batch_seconds=5,
                parquet_path: str | None = "./data/parquet/ej5", timeout_s: int = 60) -> None:
        ssc, lines = self._build_stream(host, port, batch_seconds)

        self.view.title("EJ5:")

        def process_batch(time, rdd):
            if rdd.isEmpty():
                print(f"[{time}] Micro-lote vacío")
                return

            df_json = self.spark.read.json(rdd)

            eurusd = self.model.get_current_eurusd()

            df_ej5 = self.logic.map_stream_ej5(df_json, eurusd)

            n = df_ej5.count()
            print(f"[{time}] filas={n} eurusd={eurusd}")
            self.view.head(df_ej5.orderBy("EventTS"), 5)

            if parquet_path and n > 0:
                self.model.save_parquet(df_ej5, parquet_path, is_streaming=True)

        lines.foreachRDD(process_batch)

        ssc.start()

        ssc.awaitTerminationOrTimeout(timeout_s)
        ssc.stop(stopSparkContext=False)
        ssc.awaitTermination()