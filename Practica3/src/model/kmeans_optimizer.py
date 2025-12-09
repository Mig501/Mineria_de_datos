import mlflow
import mlflow.spark
from hyperopt import fmin, tpe, hp, STATUS_OK, Trials
from pyspark.sql import Row
from model.data_loader import DataLoader
from model.pipelines import PipelineFactory
import pandas as pd

class KMeansOptimizer:

    def __init__(self, spark):
        self.spark = spark

    def prepare_data(self):
        df_pandas = DataLoader().load_bbva_data()

        rows = [
            Row(
                Date=str(r["Date"]),
                Open=float(r["Open"].item()),
                Close=float(r["Close"].item())
            )
            for _, r in df_pandas.iterrows()
        ]

        return self.spark.createDataFrame(rows)

    def optimize_k(self, max_evals=5):

        mlflow.set_tracking_uri("file:///C:/mlruns")
        mlflow.set_experiment("Practica2_KMeans_Hyperopt")

        df = self.prepare_data()

        space = {
            "k": hp.quniform("k", 2, 6, 1)
        }

        def objective(params):
            k = int(params["k"])

            with mlflow.start_run():

                pipeline = PipelineFactory().create_kmeans_pipeline(k)
                model = pipeline.fit(df)
                predictions = model.transform(df)

                kmeans_model = model.stages[-1]  

                cost = kmeans_model.summary.trainingCost

                mlflow.log_param("k", k)
                mlflow.log_metric("cost", cost)

                mlflow.spark.log_model(model, artifact_path=f"model_k_{k}")

                return {"loss": float(cost), "status": STATUS_OK}

        trials = Trials()
        best = fmin(
            fn=objective,
            space=space,
            algo=tpe.suggest,
            max_evals=max_evals,
            trials=trials
        )

        return int(best["k"])

    def get_all_runs(self, experiment_name):
        client = mlflow.tracking.MlflowClient()

        experiment = client.get_experiment_by_name(experiment_name)
        if experiment is None:
            return pd.DataFrame()

        runs = client.search_runs(
            experiment_ids=[experiment.experiment_id]
        )

        data = []
        for r in runs:
            data.append({
                "run_id": r.info.run_id,
                "params": r.data.params,
                "metrics": r.data.metrics
            })

        return pd.DataFrame(data)