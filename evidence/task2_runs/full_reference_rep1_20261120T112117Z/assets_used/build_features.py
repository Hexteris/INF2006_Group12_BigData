#!/usr/bin/env python3
"""Task 2 / stage spark_features: distributed cleaning-aware feature, target and split builder.

spark-submit build_features.py --db <hive db> --out <hdfs run dir> --local-json features_summary.json

Reads the Hive tables created by the Hive stages (cleaned_prices, daily_gdelt_features and,
optionally, out_of_scope_prices), builds per-ticker rolling features with Spark window
functions, joins market-wide and (lagged) GDELT features, creates the targets, applies the
chronological split and boundary rules, and registers the results as Hive tables so that
Hive and Spark both see them. Nothing is collected to the driver except small summaries.

Prediction-date rule: every feature at trade_date t uses data up to the close of t.
GDELT enters through DATEADDED + GDELT_LAG_DAYS, so day t only sees news published before t.
Targets (label, future_return_5d, future_volatility_5d) look 5 trading sessions ahead.
A row is kept in a split only when its whole target window ends inside the same period
(training <= 2015-12-31, validation <= 2016-12-31, testing <= 2017-12-31).
Rows without a complete target are excluded and counted, never filled.
"""
import argparse
import json

from pyspark.sql import SparkSession, Window, functions as F

ap = argparse.ArgumentParser()
ap.add_argument("--db", required=True)
ap.add_argument("--out", required=True)
ap.add_argument("--local-json", default="features_summary.json")
ap.add_argument("--use-2018-labels", type=int, default=0,
                help="1 = compute late-2017 targets from 2018 rows (outside the brief's window)")
ap.add_argument("--min-vol-window", type=int, default=20,
                help="minimum return observations required for volatility_20d")
ap.add_argument("--gdelt-lag-days", type=int, default=1)
a = ap.parse_args()

H = 5  # prediction horizon in trading sessions
PERIODS = {  # split -> (start, end)
    "training": ("2013-01-01", "2015-12-31"),
    "validation": ("2016-01-01", "2016-12-31"),
    "testing": ("2017-01-01", "2017-12-31"),
}
CORE = ["open_price", "high_price", "low_price", "close_price", "volume",
        "daily_return", "return_5d", "volatility_20d", "volume_change_1d",
        "intraday_range", "volume_ratio_20d", "mkt_return_1d", "mkt_volatility_20d"]
GDELT_COLS = ["gdelt_event_count", "gdelt_avg_tone", "gdelt_avg_goldstein",
              "gdelt_total_mentions", "gdelt_available"]
TARGETS = ["label", "future_return_5d", "future_volatility_5d"]

spark = (SparkSession.builder.appName("task2_features_" + a.db)
         .enableHiveSupport().getOrCreate())
spark.sparkContext.setLogLevel("WARN")
db = a.db

# ---------------------------------------------------------------- 1. read Hive tables
prices = spark.table(db + ".cleaned_prices").select(
    F.trim("ticker").alias("ticker"),
    F.col("trade_date").cast("date").alias("trade_date"),
    F.col("open_price").cast("double"), F.col("high_price").cast("double"),
    F.col("low_price").cast("double"), F.col("close_price").cast("double"),
    F.col("volume").cast("long"))

if a.use_2018_labels:
    ctx = (spark.table(db + ".out_of_scope_prices").select(
        F.trim("name").alias("ticker"),
        F.to_date("date", "yyyy-MM-dd").alias("trade_date"),
        F.col("open").cast("double").alias("open_price"),
        F.col("high").cast("double").alias("high_price"),
        F.col("low").cast("double").alias("low_price"),
        F.col("close").cast("double").alias("close_price"),
        F.col("volume").cast("long").alias("volume"))
        .filter(F.col("ticker").isNotNull() & F.col("trade_date").isNotNull()
                & F.col("close_price").isNotNull()))
    prices = prices.unionByName(ctx)

prices = prices.dropDuplicates(["ticker", "trade_date"])

# ---------------------------------------------------------------- 2. per-ticker windows
w = Window.partitionBy("ticker").orderBy("trade_date")
hist20 = w.rowsBetween(-19, 0)      # last 20 sessions including today
prev20 = w.rowsBetween(-20, -1)     # previous 20 sessions, excluding today
fwd = w.rowsBetween(1, H)           # the next H sessions (targets only)

f = (prices
     .withColumn("prev_close", F.lag("close_price", 1).over(w))
     .withColumn("close_lag_h", F.lag("close_price", H).over(w))
     .withColumn("prev_volume", F.lag("volume", 1).over(w))
     .withColumn("future_close", F.lead("close_price", H).over(w))
     .withColumn("future_trade_date", F.lead("trade_date", H).over(w))
     .withColumn("daily_return", F.when(F.col("prev_close") > 0,
                                        F.col("close_price") / F.col("prev_close") - 1))
     .withColumn("return_5d", F.when(F.col("close_lag_h") > 0,
                                     F.col("close_price") / F.col("close_lag_h") - 1))
     .withColumn("volume_change_1d", F.when(F.col("prev_volume") > 0,
                                            F.col("volume") / F.col("prev_volume") - 1))
     .withColumn("intraday_range", F.when(F.col("close_price") > 0,
                                          (F.col("high_price") - F.col("low_price")) / F.col("close_price")))
     .withColumn("n_ret_20", F.count("daily_return").over(hist20))
     .withColumn("volatility_20d", F.when(F.col("n_ret_20") >= a.min_vol_window,
                                          F.stddev_pop("daily_return").over(hist20)))
     .withColumn("n_vol_prev20", F.count("volume").over(prev20))
     .withColumn("avg_vol_prev20", F.avg("volume").over(prev20))
     .withColumn("volume_ratio_20d", F.when((F.col("n_vol_prev20") >= 20) & (F.col("avg_vol_prev20") > 0),
                                            F.col("volume") / F.col("avg_vol_prev20")))
     .withColumn("n_fwd_ret", F.count("daily_return").over(fwd))
     .withColumn("future_volatility_5d", F.when(F.col("n_fwd_ret") == H,
                                                F.stddev_pop("daily_return").over(fwd)))
     .withColumn("future_return_5d", F.when((F.col("future_close").isNotNull()) & (F.col("close_price") > 0),
                                            F.col("future_close") / F.col("close_price") - 1))
     .withColumn("label", F.when(F.col("future_close").isNotNull(),
                                 F.when(F.col("future_close") > F.col("close_price"), 1).otherwise(0))))

# ---------------------------------------------------------------- 3. market-wide features (same-day cross-section)
mkt = f.groupBy("trade_date").agg(F.avg("daily_return").alias("mkt_return_1d"),
                                  F.avg("volatility_20d").alias("mkt_volatility_20d"))
f = f.join(F.broadcast(mkt), "trade_date", "left")

# ---------------------------------------------------------------- 4. GDELT integration (availability date + lag)
g = (spark.table(db + ".daily_gdelt_features")
     .select(F.date_add("available_date", a.gdelt_lag_days).alias("trade_date"),
             F.col("event_count").alias("gdelt_event_count"),
             F.col("avg_tone").alias("gdelt_avg_tone"),
             F.col("avg_goldstein").alias("gdelt_avg_goldstein"),
             F.col("total_mentions").alias("gdelt_total_mentions")))
f = (f.join(F.broadcast(g), "trade_date", "left")
     .withColumn("gdelt_available", F.col("gdelt_event_count").isNotNull().cast("int")))

# ---------------------------------------------------------------- 5. chronological split + boundary rule
d = F.col("trade_date")
split = F.lit(None).cast("string")
period_end = F.lit(None).cast("date")
for name, (lo, hi) in PERIODS.items():
    cond = d.between(F.lit(lo).cast("date"), F.lit(hi).cast("date"))
    split = F.when(cond, F.lit(name)).otherwise(split)
    period_end = F.when(cond, F.lit(hi).cast("date")).otherwise(period_end)

fs = (f.withColumn("split", split).withColumn("period_end", period_end)
      .filter(F.col("split").isNotNull()))
has_target = (F.col("label").isNotNull() & F.col("future_volatility_5d").isNotNull()
              & F.col("future_return_5d").isNotNull() & F.col("future_trade_date").isNotNull())
crosses = has_target & (F.col("future_trade_date") > F.col("period_end")) & \
    ~((F.col("split") == "testing") & F.lit(bool(a.use_2018_labels)))
keep = has_target & ~crosses

split_report = {r["split"]: r.asDict() for r in fs.groupBy("split").agg(
    F.count("*").alias("period_rows"),
    F.sum((~has_target).cast("int")).alias("excluded_no_complete_target"),
    F.sum(crosses.cast("int")).alias("excluded_target_crosses_period_end"),
    F.sum(keep.cast("int")).alias("kept")).collect()}

out_cols = (["ticker", "trade_date"] + CORE + GDELT_COLS
            + ["future_close", "future_trade_date"] + TARGETS)
# ---------------------------------------------------------------- 6. write + register (Hive and Spark share the tables)
def register(df, table, path):
    cols = ", ".join("`%s` %s" % (n, t) for n, t in df.dtypes)
    spark.sql("DROP TABLE IF EXISTS %s.%s" % (db, table))
    spark.sql("CREATE EXTERNAL TABLE %s.%s (%s) STORED AS PARQUET LOCATION '%s'" % (db, table, cols, path))


summary = {"db": db, "params": vars(a), "horizon_sessions": H, "periods": PERIODS,
           "core_features": CORE, "gdelt_features": GDELT_COLS, "targets": TARGETS,
           "split_report": split_report, "splits": {}}

for name in PERIODS:
    fpath = "%s/features/%s" % (a.out, name)
    part = fs.filter((F.col("split") == name) & keep).select(*out_cols)
    part.write.mode("errorifexists").parquet(fpath)
    register(spark.read.parquet(fpath), "features_" + name, fpath)

    feat = spark.table("%s.features_%s" % (db, name))
    nulls = feat.agg(*[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in CORE + GDELT_COLS[:-1]]).first().asDict()
    ready = feat.dropna(subset=CORE + TARGETS)
    for c in CORE + TARGETS:
        ready = ready.filter(~F.isnan(F.col(c).cast("double")))
    rpath = "%s/model_ready/%s" % (a.out, name)
    # the model-ready table keeps ids, core features, GDELT columns and targets only;
    # future_close / future_trade_date stay in the features table for audit, never as features.
    ready.select(["ticker", "trade_date"] + CORE + GDELT_COLS + TARGETS).write.mode("errorifexists").parquet(rpath)
    register(spark.read.parquet(rpath), "model_ready_" + name, rpath)

    mr = spark.table("%s.model_ready_%s" % (db, name))
    st = mr.agg(F.count("*").alias("rows"), F.min("trade_date").alias("first_date"),
                F.max("trade_date").alias("last_date"),
                F.countDistinct("ticker").alias("tickers"),
                F.sum("label").alias("label_1"),
                F.sum("gdelt_available").alias("rows_with_gdelt")).first().asDict()
    summary["splits"][name] = {
        "features_rows": feat.count(), "null_counts_before_dropna": nulls,
        "model_ready": {k: (str(v) if k.endswith("_date") else v) for k, v in st.items()},
        "dropped_for_missing_values": feat.count() - st["rows"]}
    print(name, json.dumps(summary["splits"][name], default=str))

with open(a.local_json, "w") as fh:
    json.dump(summary, fh, indent=2, default=str)
print("split_report", json.dumps(split_report, default=str))
spark.stop()
