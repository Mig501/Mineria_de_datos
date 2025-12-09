from pyspark.sql import SparkSession
from view.view import View
from model.model import Model
from model.logic import Logic

class Controller:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.view = View()          
        self.model = Model(spark)   
        self.logic = Logic()       

    # Ejercicio 1a
    def ej1a(self, path_csv: str):
        self.view.title("Ej1-a")
        df_raw = self.model.load_csv(path_csv)

        self.view.schema(df_raw)

        df_mod = self.logic.convert_to_date(df_raw)

        self.view.schema(df_mod)
        self.view.head(df_mod, n=6)

        return df_raw, df_mod

    # Ejercicio 1b
    def ej1b(self, df_raw):
        self.view.title("Ej1-b")

        df_mod = self.logic.remove_mc_suffix(df_raw)

        self.view.schema(df_mod)
        self.view.head(df_mod, n=6)

        return df_mod
    
    # Ejercicio 1c
    def ej1c(self, path_csv: str):
        self.view.title("Ej1-c")

        df_raw = self.model.load_with_struct_type(path_csv)
        
        df_mod = self.logic.rename_tickers(df_raw)

        self.view.schema(df_mod)
        self.view.head(df_mod, n=6)

    # Ejercicio 2a
    def ej2a(self, df_raw) -> None:
        self.view.title("Ej2a")

        df_cleaned = self.logic.remove_duplicates_and_empty_columns(df_raw)

        rows_deleted = self.logic.rows_deleted(df_raw, df_cleaned)
        self.view.title(f"Filas eliminadas: {rows_deleted}")

        available_companies = self.logic.available_companies(df_cleaned)
        self.view.title(f"Compañías con datos disponibles: {available_companies}")

        self.view.head(df_cleaned, n=6)

        return df_cleaned

    # Ejercicio 2b
    def ej2b(self, df_no_mc) -> None:
        self.view.title("Ej2b")

        df_date = self.logic.convert_to_date(df_no_mc)

        min_date, max_date = self.logic.get_dates(df_date)
        self.view.title(f"Fecha inicial: {min_date}")
        self.view.title(f"Fecha final: {max_date}")

        days_available = self.logic.count_days_with_data(df_date)
        self.view.title(f"Días con datos disponibles: {days_available}")

        self.view.title(f"Comentario")
        self.view.title(f"Si el periodo es corto o tiene huecos, sería útil buscar más datos para completar las fechas.")

    # Ejercicio 3
    def ej3(self, df_no_mc) -> None:
        self.view.title("Ej3")

        df_renamed = self.logic.rename_columns(df_no_mc, "Fecha", "Dia")
        self.view.head(df_renamed, n=10)

        df_annual_stats = self.logic.calculate_annual_stats(df_renamed)
        for stats in df_annual_stats:
            self.view.head(stats, n=1)

        df_with_deficiency = self.logic.create_deficiency_notice(df_renamed)
        self.view.head(df_with_deficiency, n=100)

    # Ejercicio 4
    def ej4(self, df_no_mc):
        self.view.title("Ej4")

        variations = self.logic.calculate_variation_classification(df_no_mc)

        for row in variations:
            self.view.title(f"Ticker: {row[0]}, Variación: {row[1]}%, Clasificación: {row[2]}")

    # Ejercicio 5
    def ej5(self, df_no_mc):
        self.view.title("Ej5")

        df_date = self.logic.convert_to_date(df_no_mc)
        df_day = self.logic.rename_columns(df_date, "Fecha", "Dia")
        df_quartiles = self.logic.assign_quartiles(df_day)

        df_quartiles.show(1, truncate=False)

        cols_to_show = ["Dia", "AENA", "BBVA", "AENA_cuartil", "BBVA_cuartil"]
        df_quartiles.select(*cols_to_show).show(truncate=False)

    def store_raw_data(self, df_raw):
        self.logic.save_to_sql(df_raw, "Datos2024", mode="overwrite")
        print("Datos originales guardados en la tabla 'Datos2024'.")

    # Guarda CSV limpio/tratado
    def store_cleaned_data(self, df_cleaned):
        self.logic.save_to_sql(df_cleaned, "Datos2024_Tratados", mode="overwrite")
        print("Datos tratados guardados en la tabla 'Datos2024_Tratados'.")

    def finish(self):
        self.spark.stop()
        self.view.title("Spark session stopped.\n")