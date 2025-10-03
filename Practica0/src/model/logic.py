from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window

class Logic:
    def convert_to_date(self, df: DataFrame):
        df = df.withColumn("Fecha", F.to_date(F.col("Fecha"), "dd/MM/yyyy"))
        
        return df

    def remove_mc_suffix(self, df: DataFrame):
        for col in df.columns:
            if col.endswith(".MC"):
                df = df.withColumnRenamed(col, col.replace(".MC", ""))
        return df
    
    def rename_tickers(self, df: DataFrame):
        ticker_to_name = {
            "IBE.MC": "Iberdrola",
            "REP.MC": "Repsol",
            "NTGY.MC": "Naturgy",
            "ELE.MC": "Endesa",
            "ENG.MC": "Enagás",
            "RED.MC": "Red Eléctrica",
            "SAN.MC": "Banco Santander",
            "BBVA.MC": "BBVA",
            "CABK.MC": "Caixabank",
            "BKT.MC": "Bankinter",
            "SAB.MC": "Banco Sabadell",
            "UNI.MC": "Unicaja Banco",
            "MAP.MC": "Mapfre",
            "ACS.MC": "ACS",
            "ANA.MC": "Aena",
            "ANE.MC": "Aena",
            "ACX.MC": "Acciona",
            "MTS.MC": "MasMovil",
            "SCYR.MC": "Sacyr",
            "CLNX.MC": "Cellnex",
            "TEF.MC": "Telefónica",
            "AENA.MC": "Aena",
            "FER.MC": "Ferrovial",
            "ITX.MC": "Inditex",
            "AMS.MC": "Amadeus",
            "IAG.MC": "IAG",
            "GRF.MC": "Grifols",
            "FDR.MC": "Ferrovial",
            "SLR.MC": "Solaria",
            "ROVI.MC": "Rovi",
            "LOG.MC": "Logista",
            "IDR.MC": "Indra",
            "MEL.MC": "Melia",
            "PUIG.MC": "Puig",
            "COL.MC": "Colonial",
            "MRL.MC": "Merlin"
        }

        for ticker, name in ticker_to_name.items():
            df = df.withColumnRenamed(ticker, name)

        return df

    def remove_duplicates_and_empty_columns(self, df: DataFrame):
        df_no_duplicates = df.dropDuplicates()

        for column in df_no_duplicates.columns:
            if df_no_duplicates.filter(F.col(column).isNull()).count() == df_no_duplicates.count():
                df_no_duplicates = df_no_duplicates.drop(column)

        return df_no_duplicates
    
    def rows_deleted(self, df_raw: DataFrame, df_cleaned: DataFrame):
        rows_before = df_raw.count()
        rows_after = df_cleaned.count()
        rows_deleted = rows_before - rows_after
        return rows_deleted
    
    def available_companies(self, df_cleaned: DataFrame):
        available_companies = len(df_cleaned.columns) - 1 
        return available_companies

    def get_dates(self, df_date: DataFrame):
        min_date = df_date.agg({"Fecha": "min"}).collect()[0][0]
        max_date = df_date.agg({"Fecha": "max"}).collect()[0][0]
        return min_date, max_date
    
    def count_days_with_data(self, df_cleaned: DataFrame):
        days_available = df_cleaned.filter(df_cleaned["Fecha"].isNotNull()).count()
        return days_available
    
    def rename_columns(self, df_raw: DataFrame, col1, col2):
        df_renamed = df_raw.withColumnRenamed(col1, col2)
        return df_renamed
    
    def calculate_annual_stats(self, df: DataFrame):
        df = df.withColumn("Year", F.year(F.col("Dia")))

        stats = []
        for column in df.columns:
            if column != "Dia" and column != "Year":
                stats.append(
                    df.groupBy("Year").agg(
                        F.round(F.avg(column), 2).alias(f"Anual mean {column}"),
                        F.max(column).alias(f"Max anual {column}"),
                        F.min(column).alias(f"Min anual {column}")
                    )
                )

        return stats

    def create_deficiency_notice(self, df: DataFrame):
        window_spec = Window.orderBy("Dia").rowsBetween(-30, 0)

        df = df.withColumn(
            "Deficiency Notice UNI",
            F.when(
                F.col("UNI") < 1, True
            ).otherwise(False)
        )

        df = df.withColumn(
            "Deficiency Notice UNI",
            F.sum(F.when(F.col("UNI") < 1, 1).otherwise(0)).over(window_spec) >= 30
        )

        return df
    
    def calculate_variation_classification(self, df: DataFrame):
        for col in df.columns:
            if col != "Dia":
                df = df.withColumn(col, F.col(col).cast("double"))

        first_date = df.first()
        last_date = df.tail(1)[0]

        results = []

        for col in df.columns:
            if col != "Dia":
                first_value = first_date[col]
                last_value = last_date[col]

            if first_value is not None and last_value is not None:
                variation = ((last_value - first_value) / first_value) * 100

                if variation <= -15:
                    category = "Bajada Fuerte"
                elif variation < -1:
                    category = "Bajada"
                elif -1 <= variation <= 1:
                    category = "Neutra"
                elif variation < 15:
                    category = "Subida"
                else:
                    category = "Subida Fuerte"

                results.append((col, round(variation, 2), category))

        return results
    
    def assign_quartiles(self, df: DataFrame):
        for col in df.columns:
            if col != "Dia":
                df = df.withColumn(col, F.col(col).cast("double"))

        columns = [c for c in df.columns if c != "Dia"]

        for col in columns:
            df_col = df.select(col).filter(F.col(col).isNotNull())
            if df_col.count() == 0:
                continue

            quantiles = df_col.approxQuantile(col, [0.25, 0.5, 0.75], 0.01)
            if len(quantiles) < 3:
                continue 

            q1, q2, q3 = quantiles

            df = df.withColumn(
                f"{col}_cuartil",
                F.when(F.col(col) <= q1, "q1")
                 .when((F.col(col) > q1) & (F.col(col) <= q2), "q2")
                 .when((F.col(col) > q2) & (F.col(col) <= q3), "q3")
                 .otherwise("q4")
            )

        return df

    def save_to_sql(self, df, table_name, mode="overwrite"):
        """
        Guarda un DataFrame de PySpark en una tabla MySQL mediante JDBC.
        df: DataFrame a guardar
        table_name: nombre de la tabla
        mode: overwrite, append, ignore, error
        """
        url = "jdbc:mysql://localhost:3306/IBEX35"
        properties = {
            "user": "root",
            "password": "changeme",
            "driver": "com.mysql.cj.jdbc.Driver"
        }
        df.write.jdbc(
            url="jdbc:mysql://localhost:3306/IBEX35",
            table = table_name,
            mode = mode,
            properties=properties
        )