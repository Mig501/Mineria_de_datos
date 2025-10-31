from pyspark.sql import SparkSession

_spark = None

def get_spark(app_name: str = "MD_Practica1"):
    global _spark
    if _spark is None:
        _spark = (SparkSession.builder.appName(app_name)
                .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
                .config("spark.hadoop.fs.hdfs.impl", "org.apache.hadoop.fs.LocalFileSystem")
                .config("spark.sql.execution.arrow.pyspark.enabled", "false").getOrCreate())
    return _spark
