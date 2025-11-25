import mlflow
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.ml.evaluation import ClusteringEvaluator
from model.data_loader import DataLoader
from model.pipelines import PipelineFactory
from view.print_results import View

class ExperimentController:

    def __init__(self):

        self.spark = (
            SparkSession.builder
            .appName("Practica2")
            .master("local[*]")
            .config("spark.sql.execution.arrow.pyspark.enabled", "false")
            .getOrCreate()
        )

    def run_experiment_ej1(self):

        # MLflow local
        mlflow.set_tracking_uri("file:///C:/mlruns")
        mlflow.set_experiment("Practica2_KMeans_BBVA")

        # Cargar datos
        df_pandas = DataLoader().load_bbva_data()

        # Convertir a Spark
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

        # Intentamos varios k
        for k in [2, 3, 4, 5]:

            with mlflow.start_run():

                pipeline = PipelineFactory().create_kmeans_pipeline(k)

                # Entrenar
                model = pipeline.fit(df)
                predictions = model.transform(df)

                evaluator = ClusteringEvaluator(
                    featuresCol="features",
                    predictionCol="prediction",
                    metricName="silhouette"
                )

                silhouette = evaluator.evaluate(predictions)

                # Registrar parámetros y métricas
                mlflow.log_param("k", k)
                mlflow.log_metric("silhouette", silhouette)

                # Registrar el modelo
                mlflow.spark.log_model(model, artifact_path=f"model_k_{k}")

                # Mejor modelo
                if silhouette > best_silhouette:
                    best_silhouette = silhouette
                    best_k = k
        
        View.show_results_ej1(best_k, best_silhouette)
