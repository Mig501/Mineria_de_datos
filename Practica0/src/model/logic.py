from pyspark.sql import DataFrame, functions as F

class Logic:
    def convert_to_date(self, df: DataFrame) -> DataFrame:
        df = df.withColumn("Fecha", F.to_date(F.col("Fecha"), "dd/MM/yyyy"))
        
        return df

    def remove_mc_suffix(self, df):
        for col in df.columns:
            if col.endswith(".MC"):
                df = df.withColumnRenamed(col, col.replace(".MC", ""))
        return df
    
    def rename_tickers(self, df):
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

        # Renombrar columnas de tickers a nombres completos
        for ticker, name in ticker_to_name.items():
            df = df.withColumnRenamed(ticker, name)

        return df

    def remove_duplicates_and_empty_columns(self, df):
        df_no_duplicates = df.dropDuplicates()

        for column in df_no_duplicates.columns:
            if df_no_duplicates.filter(F.col(column).isNull()).count() == df_no_duplicates.count():
                df_no_duplicates = df_no_duplicates.drop(column)

        return df_no_duplicates
    
    def rows_deleted(self, df_raw, df_cleaned):
        rows_before = df_raw.count()
        rows_after = df_cleaned.count()
        rows_deleted = rows_before - rows_after
        return rows_deleted
    
    def available_companies(self, df_cleaned):
        available_companies = df_cleaned.dropna(subset=["IBE", "REP", "NTGY", "ELE", "ENG", "RED"]).count()
        return available_companies

    def count_days_with_data(self, df_cleaned):
        days_available = df_cleaned.filter(df_cleaned["Fecha"].isNotNull()).count()
        return days_available