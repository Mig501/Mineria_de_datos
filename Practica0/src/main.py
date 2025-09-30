from model.SparkSession import get_spark
from controller.controller import Controller

def main():
    spark = get_spark()

    controller = Controller(spark)

    df_raw = controller.ej1a("data/ibex35_close-2024.csv")

    df_no_mc = controller.ej1b(df_raw)

    #df_structed = controller.ej1c(df_raw)

    df_cleaned = controller.ej2a(df_no_mc)

    controller.ej2b(df_raw)

if __name__ == "__main__":
    main()
