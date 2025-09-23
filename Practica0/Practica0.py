from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *

# Crear una SparkSession
spark_session = (SparkSession.builder.appName("IBEX35").getOrCreate())

# Ej1-a
print("Ej1-a \n")
df = spark_session.read.option("header", True).option("sep", ";").option("dateFormat", "dd/MM/yyyy").csv("ibex35_close-2024.csv")
df.printSchema()
df = df.withColumn("Fecha", col("Fecha").cast("date"))
df.printSchema()
df.show(6)

# Ej1-b
print("Ej1-b \n")
