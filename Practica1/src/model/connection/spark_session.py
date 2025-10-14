# ---------------- spark_session.py ----------------
from pyspark.sql import SparkSession

class SparkSessionManager:
    _spark = None

    @classmethod
    def get_session(cls) -> SparkSession:
        if cls._spark is None:
            cls._spark = (
                SparkSession.builder
                .appName("Practica2-MineriaDatos")
                .getOrCreate()
            )
        return cls._spark

    @classmethod
    def stop(cls):
        if cls._spark is not None:
            cls._spark.stop()
            cls._spark = None
