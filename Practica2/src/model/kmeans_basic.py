import mlflow
from pyspark.sql import Row
from pyspark.ml.evaluation import ClusteringEvaluator
from model.data_loader import DataLoader
from model.pipelines import PipelineFactory

class KMeansBasicExperiment:

    def __init__(self, spark):
        self.spark = spark

    def run_experiment(self):

        mlflow.set_tracking_uri("file:///C:/mlruns")
        mlflow.set_experiment("Practica2_KMeans_BBVA")

        df_pandas = DataLoader().load_bbva_data()

        rows = [
            Row(
                Date=str(r["Date"]),
                Open=float(r["Open"].item()),
                Close=float(r["Close"].item())
            )
            for _, r in df_pandas.iterrows()
        ]
        df = self.spark.createDataFrame(rows)

        best_k = None
        best_silhouette = -1

        for k in [2, 3, 4, 5]:

            with mlflow.start_run():

                pipeline = PipelineFactory().create_kmeans_pipeline(k)

                model = pipeline.fit(df)
                predictions = model.transform(df)

                evaluator = ClusteringEvaluator(
                    featuresCol="features",
                    predictionCol="prediction",
                    metricName="silhouette"
                )

                silhouette = evaluator.evaluate(predictions)

                mlflow.log_param("k", k)
                mlflow.log_metric("silhouette", silhouette)
                mlflow.spark.log_model(model, artifact_path=f"model_k_{k}")

                if silhouette > best_silhouette:
                    best_silhouette = silhouette
                    best_k = k

        return best_k, best_silhouette
