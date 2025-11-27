from pyspark.sql import Row
from model.data_loader import DataLoader
from model.pipelines import PipelineFactory

class GaussianMixtureExperiment:

    def __init__(self, spark):
        self.spark = spark
        self.data_loader = DataLoader()

    def prepare_data(self):
        tickers = ["TEF.MC", "NTGY.MC", "ELE.MC"]
        df = self.data_loader.load_multiple_tickers(tickers)

        for t in tickers:
            df[("RangeOpen", t)] = (df[("High", t)] - df[("Low", t)]) / df[("Open", t)]
            df[("RangeLow",  t)] = (df[("High", t)] - df[("Low", t)]) / df[("Low", t)]

        df.columns = [
            f"{col}_{tic}" if tic != "" else col
            for col, tic in df.columns
        ]

        needed = [
            "Date",
            "Ticker",
            "Open_TEF.MC", "Open_NTGY.MC", "Open_ELE.MC",
            "Close_TEF.MC", "Close_NTGY.MC", "Close_ELE.MC",
            "Volume_TEF.MC", "Volume_NTGY.MC", "Volume_ELE.MC",
            "RangeOpen_TEF.MC", "RangeOpen_NTGY.MC", "RangeOpen_ELE.MC",
            "RangeLow_TEF.MC", "RangeLow_NTGY.MC", "RangeLow_ELE.MC",
        ]

        df = df[needed]

        rows = []
        for _, r in df.iterrows():
            ticker = r["Ticker"]

            p = ticker

            rows.append(Row(
                Ticker=ticker,
                Date=str(r["Date"]),
                Open=float(r[f"Open_{p}"]),
                Close=float(r[f"Close_{p}"]),
                Volume=float(r[f"Volume_{p}"]),
                RangeOpen=float(r[f"RangeOpen_{p}"]),
                RangeLow=float(r[f"RangeLow_{p}"]),
            ))

        return self.spark.createDataFrame(rows)

    def run_multiple_times(self, repetitions=5):
        df = self.prepare_data()
        pipeline = PipelineFactory().create_gmm_pipeline(k=2)

        results = []

        for _ in range(repetitions):
            model = pipeline.fit(df)
            preds = model.transform(df)
            results.append(preds)

        return results
