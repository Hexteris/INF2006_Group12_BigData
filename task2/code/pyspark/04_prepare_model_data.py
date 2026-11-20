from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("FlyWise-Task2-PrepareModelData")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

source_root = "/corpus/features/stock_direction_v1"
output_root = "/corpus/features/model_ready_v1"

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

datasets = ["training", "validation", "testing"]

for name in datasets:
    print(f"\n===== Preparing {name.upper()} =====")

    df = spark.read.parquet(f"{source_root}/{name}")

    before = df.count()

    # Remove rows without complete feature values.
    df = df.dropna(subset=feature_columns + ["label"])

    # Remove invalid floating-point values.
    for column in feature_columns:
        if column != "volume":
            df = df.filter(~F.isnan(F.col(column)))

    after = df.count()

    print("Rows before:", before)
    print("Rows after:", after)
    print("Rows removed:", before - after)

    print("Label distribution:")
    df.groupBy("label").count().orderBy("label").show()

    # Keep ticker/date for traceability, but do not use
    # them as numerical model features.
    output_path = f"{output_root}/{name}"

    df.write.mode("errorifexists").parquet(output_path)

    print("Saved:", output_path)

print("\n===== Model data preparation completed =====")

spark.stop()
