from pyspark.sql import SparkSession
from model.kmeans_basic import KMeansBasicExperiment
from view.view import View
from model.kmeans_optimizer import KMeansOptimizer
from model.gmm_optimizer import GaussianMixtureOptimizer
from model.gmm_experiment import GaussianMixtureExperiment
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

        View.show_text("\nEj1:")
        
        experiment = KMeansBasicExperiment(self.spark)

        best_k, best_silhouette = experiment.run_experiment()

        View.show_results_ej1(best_k, best_silhouette)


    def run_experiment_ej2(self):

        View.show_text("\nEj2:")

        self.optimizer = KMeansOptimizer(self.spark)

        best_k = self.optimizer.optimize_k(max_evals=5)
        View.show_best_k(best_k)

        runs_df = self.optimizer.get_all_runs("Practica2_KMeans_Hyperopt")
        View.show_all_runs(runs_df)

    def run_experiment_ej4(self):

        View.show_text("\nEj4:")

        optimizer = GaussianMixtureOptimizer(self.spark)

        best_k = optimizer.optimize_k(max_evals=5)

        runs_df = optimizer.get_all_runs("Practica2_GMM_Hyperopt")

        View.show_results_ej4(best_k)
        View.show_all_runs(runs_df)

    def run_experiment_ej5(self):

        View.show_text("\nEj5:")

        experiment = GaussianMixtureExperiment(self.spark)

        results = experiment.run_multiple_times(repetitions=5)

        View.show_gmm_results(results)
        View.show_text("\nComentarios:" \
        "El modelo genera dos grupos porque existe una clara diferencia estructural entre TEF.MC y los otros dos activos." \
        "TEF.MC presenta precios y volatilidades en escalas distintas, lo que lo sitúa en un cluster propio. " \
        "Por el contrario, NTGY.MC y ELE.MC comparten una dinámica muy similar y son agrupados sistemáticamente. " \
        "Por esta razón, el número adecuado de clusters es 2, ya que representa de forma natural la estructura real de los datos y evita particiones artificiales.")