from pyspark.sql import SparkSession

def get_spark() -> SparkSession:
    return (SparkSession.builder.appName("Practica-IBEX-MVC").getOrCreate())
