from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline

class PipelineFactory:
    
    def create_kmeans_pipeline(self, k:int):
        assembler = VectorAssembler(
            inputCols=["Open", "Close"],
            outputCol="features"
        )

        kmeans = KMeans(
            k=k,
            seed=123,
            featuresCol="features",
            predictionCol="prediction"
        )

        pipeline = Pipeline(stages=[assembler, kmeans])
        return pipeline
