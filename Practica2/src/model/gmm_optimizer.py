import mlflow
import mlflow.spark
from hyperopt import fmin, tpe, hp, STATUS_OK, Trials
from pyspark.sql import Row
from pyspark.ml.clustering import GaussianMixture
from pyspark.ml.feature import VectorAssembler
import pandas as pd
from model.data_loader import DataLoader

class GaussianMixtureOptimizer:

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

        df = self.spark.createDataFrame(rows)

        assembler = VectorAssembler(
            inputCols=["Open", "Close"],
            outputCol="features"
        )
        return assembler.transform(df)

    def optimize_k(self, max_evals=5):

        mlflow.set_tracking_uri("file:///C:/mlruns")
        mlflow.set_experiment("Practica2_GMM_Hyperopt")

        df = self.prepare_data()

        space = {
            "k": hp.quniform("k", 2, 6, 1)
        }

        def objective(params):
            k = int(params["k"])

            with mlflow.start_run():

                gm = GaussianMixture(
                    k=k,
                    featuresCol="features",
                    predictionCol="prediction"
                )

                model = gm.fit(df)

                log_likelihood = model.summary.logLikelihood

                loss = -float(log_likelihood)

                mlflow.log_param("k", k)
                mlflow.log_metric("log_likelihood", log_likelihood)

                mlflow.spark.log_model(model, artifact_path=f"gmm_k_{k}")

                return {"loss": loss, "status": STATUS_OK}

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
