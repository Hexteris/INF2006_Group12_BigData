from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
    .appName("FlyWise-Task2-StockFeatureEngineering")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# --------------------------------------------------
# 1. Read cleaned stock prices (2013-2017)
# --------------------------------------------------

cleaned_path = "/corpus/cleaned/prices/"
context_path = "/corpus/out_of_scope/prices/"

print("\n===== Reading cleaned stock prices =====")

cleaned = spark.read.parquet(cleaned_path).select(
    F.trim("ticker").alias("ticker"),
    F.col("trade_date").cast("date").alias("trade_date"),
    F.col("open_price").cast("double").alias("open_price"),
    F.col("high_price").cast("double").alias("high_price"),
    F.col("low_price").cast("double").alias("low_price"),
    F.col("close_price").cast("double").alias("close_price"),
    F.col("volume").cast("long").alias("volume")
)

print("Cleaned stock rows:", cleaned.count())

# --------------------------------------------------
# 2. Read 2018 records for label calculation only
# --------------------------------------------------

print("\n===== Reading 2018 label-context records =====")

context_raw = spark.read.parquet(context_path)

context = context_raw.select(
    F.trim(F.col("name")).alias("ticker"),
    F.to_date(F.col("date"), "yyyy-MM-dd").alias("trade_date"),
    F.col("open").cast("double").alias("open_price"),
    F.col("high").cast("double").alias("high_price"),
    F.col("low").cast("double").alias("low_price"),
    F.col("close").cast("double").alias("close_price"),
    F.col("volume").cast("long").alias("volume")
).filter(
    F.col("ticker").isNotNull()
    & F.col("trade_date").isNotNull()
    & F.col("close_price").isNotNull()
)

print("Usable 2018 context rows:", context.count())

# --------------------------------------------------
# 3. Combine historical data and label context
# --------------------------------------------------

all_prices = cleaned.unionByName(context)

# Remove any duplicate ticker/date records before
# calculating trading-session windows.
all_prices = all_prices.dropDuplicates(["ticker", "trade_date"])

print("\n===== Combined price data =====")
print("Combined rows:", all_prices.count())

# --------------------------------------------------
# 4. Define chronological windows
# --------------------------------------------------

ticker_window = (
    Window.partitionBy("ticker")
    .orderBy("trade_date")
)

history_20 = (
    ticker_window
    .rowsBetween(-19, 0)
)

# --------------------------------------------------
# 5. Calculate future target and historical features
# --------------------------------------------------

print("\n===== Calculating features and labels =====")

featured = (
    all_prices
    .withColumn("previous_close", F.lag("close_price", 1).over(ticker_window))
    .withColumn("previous_close_5d", F.lag("close_price", 5).over(ticker_window))
    .withColumn("previous_volume", F.lag("volume", 1).over(ticker_window))
    .withColumn("future_close", F.lead("close_price", 5).over(ticker_window))
    .withColumn("future_trade_date", F.lead("trade_date", 5).over(ticker_window))
    .withColumn(
        "daily_return",
        F.when(
            F.col("previous_close") > 0,
            F.col("close_price") / F.col("previous_close") - 1
        )
    )
    .withColumn(
        "return_5d",
        F.when(
            F.col("previous_close_5d") > 0,
            F.col("close_price") / F.col("previous_close_5d") - 1
        )
    )
    .withColumn(
        "volume_change_1d",
        F.when(
            F.col("previous_volume") > 0,
            F.col("volume") / F.col("previous_volume") - 1
        )
    )
    .withColumn(
        "volatility_20d",
        F.stddev_pop("daily_return").over(history_20)
    )
    .withColumn(
        "intraday_range",
        F.when(
            F.col("close_price") > 0,
            (F.col("high_price") - F.col("low_price"))
            / F.col("close_price")
        )
    )
)

# Only use 2013-2017 as prediction dates.
# 2018 records remain label context only.
prediction_rows = featured.filter(
    (F.col("trade_date") >= F.lit("2013-01-01").cast("date"))
    & (F.col("trade_date") <= F.lit("2017-12-31").cast("date"))
    & F.col("future_close").isNotNull()
    & F.col("future_trade_date").isNotNull()
)

# --------------------------------------------------
# 6. Create labels and prevent split-boundary leakage
# --------------------------------------------------

labelled = prediction_rows.withColumn(
    "label",
    F.when(F.col("future_close") > F.col("close_price"), 1)
    .otherwise(0)
)

# Training: 2013-2015.
# Future target must also remain within 2015.
training = labelled.filter(
    (F.col("trade_date") <= F.lit("2015-12-31").cast("date"))
    & (F.col("future_trade_date") <= F.lit("2015-12-31").cast("date"))
)

# Validation: 2016.
# Future target must also remain within 2016.
validation = labelled.filter(
    (F.col("trade_date") >= F.lit("2016-01-01").cast("date"))
    & (F.col("trade_date") <= F.lit("2016-12-31").cast("date"))
    & (F.col("future_trade_date") <= F.lit("2016-12-31").cast("date"))
)

# Testing: 2017.
# 2018 prices may be used to calculate the target.
testing = labelled.filter(
    (F.col("trade_date") >= F.lit("2017-01-01").cast("date"))
    & (F.col("trade_date") <= F.lit("2017-12-31").cast("date"))
)

# --------------------------------------------------
# 7. Select model features and save separate datasets
# --------------------------------------------------

output_columns = [
    "ticker",
    "trade_date",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "volume",
    "daily_return",
    "return_5d",
    "volatility_20d",
    "volume_change_1d",
    "intraday_range",
    "label"
]

output_root = "/corpus/features/stock_direction_v1"

datasets = {
    "training": training,
    "validation": validation,
    "testing": testing
}

print("\n===== Writing feature datasets =====")

for name, df in datasets.items():
    output_path = f"{output_root}/{name}"

    result = df.select(*output_columns)

    print(f"\n{name.upper()}")
    print("Rows:", result.count())

    result.groupBy("label").count().orderBy("label").show()

    result.write.mode("errorifexists").parquet(output_path)

    print("Saved to:", output_path)

print("\n===== Feature engineering completed =====")

spark.stop()
