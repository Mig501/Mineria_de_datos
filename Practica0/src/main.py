from model.SparkSession import get_spark
from controller.controller import Controller

def main():
    spark = get_spark()

    controller = Controller(spark)

    df_mod = controller.ej1a("data/ibex35_close-2024.csv")

    df_no_mc = controller.ej1b(df_mod)

    #df_structed = controller.ej1c("data/ibex35_close-2024.csv")

    df_cleaned = controller.ej2a(df_no_mc)

    #controller.ej2b(df_mod)

    #df_with_deficiency = controller.ej3(df_no_mc)

if __name__ == "__main__":
    main()
