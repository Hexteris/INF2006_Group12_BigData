"""Task 3 training stage for the timed Task 2 driver runs (Section 6).

Run by the driver as its `train` stage through TRAIN_CMD, so model training is inside every
timed run, as the brief requires. It reads the model-ready tables that this run's driver just
registered in Hive and refits the two FROZEN final designs from Section 4 (no model search):

  volatility: class 1 if future_volatility_5d is above its exact training 75th percentile
  direction:  class 1 if the close 5 sessions later is above today's close (Task 2 label)

Each pipeline is Imputer(median) -> VectorAssembler -> StandardScaler -> LogisticRegression,
fitted on training only. The probability cutoff is chosen on 2016 validation (maximum balanced
accuracy), then 2017 is scored once. The PipelineModel, test predictions and metrics are written
under <out>/task3/<task>/.

The driver builds 8 price features (listed below), not the 14 / 26 of the Task 3 notebooks, so
these metrics are a timing and sanity check. The results reported in Section 4 come from the
Task 3 notebooks.

  spark-submit ... train_task3.py --db $DB --out $OUT [--local-json $LOCAL_RUN/train_metrics.json]
"""
import argparse
import json
import os
import time

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import Imputer, StandardScaler, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession, functions as F

ap = argparse.ArgumentParser()
ap.add_argument("--db", required=True)
ap.add_argument("--out", required=True)
ap.add_argument("--tasks", default="volatility,direction")
ap.add_argument("--seed", type=int, default=42)
ap.add_argument("--local-json",
                default=os.path.join(os.environ.get("LOCAL_RUN", "."), "train_metrics.json"))
a = ap.parse_args()

# Driver model-ready columns that are comparable across tickers. Raw price and volume levels
# are left out because they are not comparable across tickers.
FEATURES = ["daily_return", "return_5d", "volatility_20d", "volume_change_1d", "intraday_range",
            "volume_ratio_20d", "mkt_return_1d", "mkt_volatility_20d"]
CUTOFFS = [round(0.05 + 0.01 * i, 2) for i in range(91)]  # 0.05 .. 0.95

spark = (SparkSession.builder.appName("task3_train_" + a.db)
         .enableHiveSupport().getOrCreate())
spark.sparkContext.setLogLevel("WARN")

data = {s: spark.table("%s.model_ready_%s" % (a.db, s))
        for s in ("training", "validation", "testing")}


def counts(df, cutoff):
    r = (df.withColumn("pred", (F.col("risk_score") >= cutoff).cast("int"))
           .agg(F.sum(((F.col("pred") == 1) & (F.col("target") == 1)).cast("int")).alias("tp"),
                F.sum(((F.col("pred") == 1) & (F.col("target") == 0)).cast("int")).alias("fp"),
                F.sum(((F.col("pred") == 0) & (F.col("target") == 0)).cast("int")).alias("tn"),
                F.sum(((F.col("pred") == 0) & (F.col("target") == 1)).cast("int")).alias("fn"))
           .first().asDict())
    return {k: int(v or 0) for k, v in r.items()}


def balanced_accuracy(c):
    tpr = c["tp"] / (c["tp"] + c["fn"]) if c["tp"] + c["fn"] else 0.0
    tnr = c["tn"] / (c["tn"] + c["fp"]) if c["tn"] + c["fp"] else 0.0
    return (tpr + tnr) / 2


def score(model, df):
    return (model.transform(df)
            .withColumn("risk_score", vector_to_array("probability")[1])
            .select("ticker", "trade_date", "target", "risk_score"))


auc_eval = BinaryClassificationEvaluator(labelCol="target", rawPredictionCol="risk_score",
                                         metricName="areaUnderROC")
report = {"db": a.db, "features": FEATURES, "seed": a.seed, "tasks": {}}

for task in [t.strip() for t in a.tasks.split(",") if t.strip()]:
    t0 = time.time()
    if task == "volatility":
        thr = data["training"].approxQuantile("future_volatility_5d", [0.75], 0.0)[0]  # exact
        target = (F.col("future_volatility_5d") > F.lit(thr)).cast("int")
    elif task == "direction":
        thr = None
        target = F.col("label").cast("int")
    else:
        raise SystemExit("unknown task: " + task)
    d = {s: df.withColumn("target", target).select("ticker", "trade_date", "target", *FEATURES)
         for s, df in data.items()}

    filled = [c + "_filled" for c in FEATURES]
    pipe = Pipeline(stages=[
        Imputer(inputCols=FEATURES, outputCols=filled, strategy="median"),
        VectorAssembler(inputCols=filled, outputCol="unscaled"),
        StandardScaler(inputCol="unscaled", outputCol="features", withStd=True, withMean=False),
        LogisticRegression(featuresCol="features", labelCol="target", maxIter=100, regParam=0.01),
    ])
    model = pipe.fit(d["training"])  # learned on 2013-2015 only

    val = score(model, d["validation"]).cache()
    best = max(CUTOFFS, key=lambda c: balanced_accuracy(counts(val, c)))  # chosen on 2016
    val_auc = auc_eval.evaluate(val)
    val.unpersist()

    test = score(model, d["testing"])                 # 2017 scored once, after freezing
    base = "%s/task3/%s" % (a.out, task)
    model.write().save(base + "/model")
    test.write.mode("errorifexists").parquet(base + "/predictions")
    test = spark.read.parquet(base + "/predictions")
    c = counts(test, best)
    n = sum(c.values())
    maj = int(d["training"].agg(F.avg("target")).first()[0] >= 0.5)
    report["tasks"][task] = {
        "volatility_threshold": thr, "cutoff_from_2016": best,
        "validation_auc": round(val_auc, 5),
        "test_rows": n, "test_auc": round(auc_eval.evaluate(test), 5),
        "test_balanced_accuracy": round(balanced_accuracy(c), 5),
        "test_accuracy": round((c["tp"] + c["tn"]) / n, 5) if n else None,
        "test_majority_accuracy": round(
            (c["tp"] + c["fn"] if maj else c["tn"] + c["fp"]) / n, 5) if n else None,
        "confusion": c, "model_path": base + "/model", "seconds": round(time.time() - t0, 1)}
    print(task, json.dumps(report["tasks"][task]), flush=True)

with open(a.local_json, "w") as fh:
    json.dump(report, fh, indent=2)
spark.stop()
