from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("FlyWise-Task2-InspectInputs") \
    .getOrCreate()

paths = {
    "cleaned_prices": "/corpus/cleaned/prices/",
    "out_of_scope_prices": "/corpus/out_of_scope/prices/",
    "daily_gdelt_features": "/corpus/cleaned/gdelt/daily_features/",
}

for name, path in paths.items():
    print(f"\n===== {name} =====")
    df = spark.read.parquet(path)
    df.printSchema()
    print("Rows:", df.count())

    if "trade_date" in df.columns:
        df.select(
            F.min("trade_date").alias("min_date"),
            F.max("trade_date").alias("max_date"),
            F.countDistinct("ticker").alias("tickers")
        ).show(truncate=False)

    elif "date" in df.columns:
        df.select(
            F.min("date").alias("min_date"),
            F.max("date").alias("max_date")
        ).show(truncate=False)

    elif "available_date" in df.columns:
        df.select(
            F.min("available_date").alias("min_date"),
            F.max("available_date").alias("max_date")
        ).show(truncate=False)

spark.stop()
