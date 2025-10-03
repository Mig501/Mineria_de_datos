from model.SparkSession import get_spark
from controller.controller import Controller

def main():
    # Configurar Spark
    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")

    controller = Controller(spark)

    #Ejercicio 1a
    df_raw, df_mod = controller.ej1a("data/ibex35_close-2024.csv")

    #Ejercicio 1b
    df_no_mc = controller.ej1b(df_mod)

    #Ejercicio 1c
    controller.ej1c("data/ibex35_close-2024.csv")

    #Ejercicio 2a
    df_cleaned = controller.ej2a(df_no_mc)

    #Ejercicio 2b
    controller.ej2b(df_mod)

    #Ejercicio 3
    controller.ej3(df_no_mc)

    #Ejercicio 4
    controller.ej4(df_no_mc)

    #Ejercicio 5
    controller.ej5(df_no_mc)

    # Guardar datos en SQL
    controller.store_raw_data(df_raw)

    # Guardar datos tratados en SQL
    controller.store_cleaned_data(df_cleaned)

    controller.finish()

if __name__ == "__main__":
    main()
