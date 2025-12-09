from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DateType

class Model:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def load_csv(self, path: str) -> DataFrame:
        return (self.spark.read.option("header", True).option("sep", ";").option("dateFormat","dd/MM/yyyy").csv(path))

    def load_with_struct_type(self, path: str):

        schema = StructType([
            StructField("Fecha", DateType(), False),
            StructField("IBE.MC", StringType(), False),
            StructField("REP.MC", StringType(), True),
            StructField("NTGY.MC", StringType(), True),
            StructField("ELE.MC", StringType(), True),
            StructField("ENG.MC", StringType(), True),
            StructField("RED.MC", StringType(), True),
            StructField("SAN.MC", StringType(), True),
            StructField("BBVA.MC", StringType(), True),
            StructField("CABK.MC", StringType(), True),
            StructField("BKT.MC", StringType(), True),
            StructField("SAB.MC", StringType(), True),
            StructField("UNI.MC", StringType(), True),
            StructField("MAP.MC", StringType(), True),
            StructField("ACS.MC", StringType(), True),
            StructField("ANA.MC", StringType(), True),
            StructField("ANE.MC", StringType(), True),
            StructField("ACX.MC", StringType(), True),
            StructField("MTS.MC", StringType(), True),
            StructField("SCYR.MC", StringType(), True),
            StructField("CLNX.MC", StringType(), True),
            StructField("TEF.MC", StringType(), True),
            StructField("AENA.MC", StringType(), True),
            StructField("FER.MC", StringType(), True),
            StructField("ITX.MC", StringType(), True),
            StructField("AMS.MC", StringType(), True),
            StructField("IAG.MC", StringType(), True),
            StructField("GRF.MC", StringType(), True),
            StructField("FDR.MC", StringType(), True),
            StructField("SLR.MC", StringType(), True),
            StructField("ROVI.MC", StringType(), True),
            StructField("LOG.MC", StringType(), True),
            StructField("IDR.MC", StringType(), True),
            StructField("MEL.MC", StringType(), True),
            StructField("PUIG.MC", StringType(), True),
            StructField("COL.MC", StringType(), True),
            StructField("MRL.MC", StringType(), True)
        ])

        return (self.spark.read.option("header", True).schema(schema).csv(path))
    
    