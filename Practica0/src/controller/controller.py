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

    def ej1a(self, path_csv: str) -> None:
        df_raw = self.model.load_csv(path_csv)

        self.view.title("Ej1-a")
        self.view.schema(df_raw)

        df_mod = self.logic.convert_to_date(df_raw)

        self.view.schema(df_mod)
        self.view.head(df_mod, n=6)

        return df_mod

    def ej1b(self, df_raw) -> None:
        df_mod = self.logic.remove_mc_suffix(df_raw)

        self.view.title("Ej1-b")
        self.view.schema(df_mod)
        self.view.head(df_mod, n=6)

        return df_mod
    
    def ej1c(self, path_csv: str) -> None:
        df_raw = self.model.load_with_struct_type(path_csv)

        df_mod = self.logic.rename_tickers(df_raw)

        self.view.title("Ej1-c")
        self.view.schema(df_mod)
        self.view.head(df_mod, n=6)

    
    def ej2a(self, df_raw) -> None:
        
        df_cleaned = self.logic.remove_duplicates_and_empty_columns(df_raw)

        self.view.title("Ej2a")
        rows_deleted = self.logic.rows_deleted(df_raw, df_cleaned)
        self.view.title(f"Filas eliminadas: {rows_deleted}")

        available_companies = self.logic.available_companies(df_cleaned)
        self.view.title(f"Compañías con datos disponibles: {available_companies}")

        self.view.head(df_cleaned, n=6)

        return df_cleaned
    
    def ej2b(self, df_no_mc) -> None:
        self.view.title("Ej2b")

        df_date = self.logic.convert_to_date(df_no_mc)

        min_date = df_date.agg({"Fecha": "min"}).collect()[0][0]
        max_date = df_date.agg({"Fecha": "max"}).collect()[0][0]

        self.view.title(f"Fecha inicial: {min_date}")
        self.view.title(f"Fecha final: {max_date}")

        days_available = self.logic.count_days_with_data(df_date)
        self.view.title(f"Días con datos disponibles: {days_available}")

        self.view.title(f"Comentario")
        self.view.title(f"Si el periodo es corto o tiene huecos, sería útil buscar más datos para completar las fechas.")

    def ej3(self, df_no_mc) -> None:
        self.view.title("Ej3")

        df_renamed = self.logic.rename_columns(df_no_mc, "Fecha", "Dia")
        self.view.head(df_renamed, n=10)

        df_annual_stats = self.logic.calculate_annual_stats(df_renamed)
        for stats in df_annual_stats:
            self.view.head(stats, n=1)

        df_with_deficiency = self.logic.create_deficiency_notice(df_renamed)
        self.view.head(df_with_deficiency, n=100)

        return df_with_deficiency
