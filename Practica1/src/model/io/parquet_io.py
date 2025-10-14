# ---------------- parquet_io.py ----------------
class ParquetIO:
    @staticmethod
    def write_parquet(df, path, mode="overwrite"):
        df.write.mode(mode).parquet(path)

    @staticmethod
    def read_parquet(spark, path, columns=None):
        df = spark.read.parquet(path)
        return df.select(*columns) if columns else df
