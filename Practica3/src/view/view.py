class View:
    
    @staticmethod
    def show_text(text):
        print(text)

    @staticmethod
    def show_results_ej1(best_k, silhouette):
        print(f" MEJOR NÚMERO DE CLUSTERS: {best_k}")
        print(f" MEJOR SILHOUETTE: {silhouette}")

    @staticmethod
    def show_best_k(k):
        print(f"Mejor número de clusters: {k}\n")

    @staticmethod
    def show_all_runs(df):

        if df.empty:
            print("No hay runs registradas.")
            return

        print("\nTODAS LAS RUNS DEL EXPERIMENTO")

        for _, row in df.iterrows():

            print(f"\nRun ID: {row['run_id']}")

            print("  Parámetros:")
            for p, v in row["params"].items():
                print(f"    - {p}: {v}")

            print("  Métricas:")
            for m, v in row["metrics"].items():
                print(f"    - {m}: {v}")

    @staticmethod
    def show_results_ej4(best_k):
        print(f"Mejor número de clusters para GMM: {best_k}\n")

    @staticmethod
    def show_gmm_results(results):
        print("\nResultados Gaussian Mixture\n")

        for i, df in enumerate(results):
            print(f"Repetición {i+1}")
            df.crosstab("Ticker", "prediction").show()
            print()

    @staticmethod
    def show_clusters_kmeans(df_kmeans):
        print("\nCLUSTERS ENCONTRADOS POR KMEANS\n")

        df_kmeans.groupBy("prediction").count().show()

    @staticmethod
    def show_diff_gmm_kmeans(df_dif):
        total = df_dif.count()
        print("\nDIFERENCIAS ENTRE GMM Y KMEANS\n")
        print(f"Total de datos clasificados de distinta forma: {total}\n")
