from controller.controller import Controller
from model.SparkSession import get_spark

def main():
    ticker = None
    start = None
    end = None
    interval = None

    spark = get_spark()

    ctrl = Controller(spark)

    # Ej1-a
    sdf = ctrl.run_ej1a(ticker=ticker, start=start, end=end, interval=interval, preview_n=10)
    
    # Ej1-b
    sdf_clean = ctrl.run_ej1b(sdf)

    # Finalizar
    ctrl.finish()

if __name__ == "__main__":
    main()
