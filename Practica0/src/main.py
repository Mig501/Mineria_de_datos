from model.SparkSession import get_spark
from controller.controller import Controller

def main():
    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")

    controller = Controller(spark)

    df_mod = controller.ej1a("data/ibex35_close-2024.csv")

    df_no_mc = controller.ej1b(df_mod)

    controller.ej1c("data/ibex35_close-2024.csv")

    controller.ej2a(df_no_mc)

    controller.ej2b(df_mod)

    controller.ej3(df_no_mc)

    controller.ej4(df_no_mc)

    controller.ej5(df_no_mc)

    controller.finish()

if __name__ == "__main__":
    main()
