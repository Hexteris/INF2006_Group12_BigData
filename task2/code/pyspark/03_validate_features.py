from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("FlyWise-Task2-ValidateFeatures")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

root = "/corpus/features/stock_direction_v1"

datasets = {
    "training": root + "/training",
    "validation": root + "/validation",
    "testing": root + "/testing"
}

feature_columns = [
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "volume",
    "daily_return",
    "return_5d",
    "volatility_20d",
    "volume_change_1d",
    "intraday_range"
]

for name, path in datasets.items():
    print(f"\n===== {name.upper()} =====")

    df = spark.read.parquet(path)

    print("Rows:", df.count())

    df.select(
        F.min("trade_date").alias("first_date"),
        F.max("trade_date").alias("last_date"),
        F.countDistinct("ticker").alias("unique_tickers")
    ).show(truncate=False)

    print("Label distribution:")
    df.groupBy("label").count().orderBy("label").show()

    print("Missing values:")
    df.select([
        F.sum(
            F.when(F.col(c).isNull(), 1).otherwise(0)
        ).alias(c)
        for c in feature_columns
    ]).show(truncate=False)

    duplicate_count = (
        df.groupBy("ticker", "trade_date")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    print("Duplicate ticker/date groups:", duplicate_count)

spark.stop()
