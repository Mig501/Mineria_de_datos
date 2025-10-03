from pyspark.sql import SparkSession
import os

def get_spark() -> SparkSession:
    return (SparkSession.builder.appName("IBEX35").config("spark.driver.extraClassPath", "lib/mysql-connector-j-9.2.0.jar").getOrCreate())
