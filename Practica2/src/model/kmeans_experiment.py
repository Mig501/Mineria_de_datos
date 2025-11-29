from model.pipelines import PipelineFactory

class KMeansExperiment:

    def __init__(self, spark, df_prepared, gmm_results):
        self.spark = spark
        self.df_prepared = df_prepared
        self.gmm_results = gmm_results

    def run_kmeans(self, k=2):
        pipeline = PipelineFactory().create_kmeans_pipeline(k)
        model = pipeline.fit(self.df_prepared)
        preds = model.transform(self.df_prepared)

        return preds

    def comparar_con_gmm(self, df_kmeans):

        df_gmm = self.gmm_results.withColumnRenamed("prediction", "gmm_cluster")
        df_kmeans = df_kmeans.withColumnRenamed("prediction", "kmeans_cluster")

        joined = df_kmeans.join(
            df_gmm.select("Ticker", "gmm_cluster"),
            on="Ticker"
        )
        
        diferencias = joined.filter(joined.kmeans_cluster != joined.gmm_cluster)

        return diferencias
