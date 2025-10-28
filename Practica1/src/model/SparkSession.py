from pyspark.sql import SparkSession

_spark = None

def get_spark(app_name: str = "MD_Practica1"):
    global _spark
    if _spark is None:
        _spark = (SparkSession.builder.appName(app_name).getOrCreate())
    return _spark
