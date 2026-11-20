from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("FlyWise-Task2-CheckNonFinite")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

root = "/corpus/features/model_ready_v1"

datasets = {
    "training": root + "/training",
    "validation": root + "/validation",
    "testing": root + "/testing"
}

features = [
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

    checks = []

    for c in features:
        x = F.col(c).cast("double")

        checks.append(
            F.sum(
                F.when(
                    x.isNull() |
                    F.isnan(x) |
                    (F.abs(x) == float("inf")),
                    1
                ).otherwise(0)
            ).alias(c)
        )

    df.select(checks).show(truncate=False)

spark.stop()
