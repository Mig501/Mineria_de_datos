from pyspark.sql import DataFrame

class View:
    def title(self, text: str) -> None:
        print("\n" + text)

    def schema(self, df: DataFrame) -> None:
        df.printSchema()

    def head(self, df: DataFrame, n: int) -> None:
        df.show(n)

    def count(self, df: DataFrame) -> None:
        cnt = df.count()
        print(f"Total records: {cnt}")
