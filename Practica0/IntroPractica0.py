from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, when

#Create a Spark session
spark = SparkSession.builder.getOrCreate()

#Show
df = spark.read.json("Amazon_review.json")
df.show(5)

'''
primero = df.head(1)[0]
print(primero)
ultimo = df.tail(1)[0]
print(ultimo)

#Return the list of the first elements
df.head(5)

#List the columns of the dataframe
print("Columns: ", df.columns)

#With distict we can remove duplicates
df = df.distinct()
print("Number of rows without duplicates: ", df.count())

#With dropDuplicates we can remove duplicates based on specific columns, but you have to assign it to a variable because it does not modify the original dataframe
df = df.dropDuplicates(["reviewerID"])

#With select we can select specific columns
df = df.select("overall").show()
df = df.select(col["overall"]-1, col["reviewID"]).show()

df.count()
df.show(df.count())

#Filter
df.filter(df["overall"] >= 5).show()

#When
df = df.withColumn("state", when(col("overall") >= 5, "Positive").otherwise("Negative"))
df.select("overall", "state").show()

#Schema
df.printSchema()
'''
