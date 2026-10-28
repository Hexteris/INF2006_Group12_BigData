# Task 1 - read-only profiling of the raw corpus in HDFS.
# Paste each "# %%" block into its own notebook cell on EMR (PySpark kernel).
# Nothing here modifies or cleans raw data; it only measures it. Every result is
# written as a small aggregate under /corpus/raw/_manifests/.../profile/ so the
# report can cite it by path.

# %% [1] Parameters  (edit to match what was staged)
ACQ_DATE      = "2026-10-28"
PRICES_RAW    = f"/corpus/raw/prices/v1_2013-2017/acq_{ACQ_DATE}"
PRICES_PROF   = f"/corpus/raw/_manifests/prices/v1_2013-2017/acq_{ACQ_DATE}/profile"
SEC_RAW       = f"/corpus/raw/sec_edgar/v1/acq_{ACQ_DATE}"
SEC_MAN       = f"/corpus/raw/_manifests/sec_edgar/v1/acq_{ACQ_DATE}"
SEC_PROF      = f"{SEC_MAN}/profile"
EXPECTED_FIRST, EXPECTED_LAST = "2013-02-08", "2017-12-29"   # from project spec
RUN_ID = __import__("datetime").datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

from pyspark.sql import SparkSession, functions as F, Window as W
from pyspark.sql.types import StructType, StructField, StringType
spark = SparkSession.builder.getOrCreate()
print("spark", spark.version, "app", spark.sparkContext.applicationId, "run", RUN_ID)

def save(df, name, base=PRICES_PROF):
    """Write a small aggregate as one JSON file + show it. Path is the evidence locator."""
    path = f"{base}/{RUN_ID}/{name}"
    df.coalesce(1).write.mode("errorifexists").json(path)
    print("->", path)
    df.show(50, truncate=False)

# %% [2] Physical lines vs parsed rows; every column read as STRING (no inference)
lines = spark.read.text(PRICES_RAW).withColumn("src_file", F.input_file_name())
header_line = lines.first()["value"]
cols = [c.strip() for c in header_line.split(",")]
print("header:", cols)

schema = StructType([StructField(c, StringType()) for c in cols]
                    + [StructField("_corrupt_record", StringType())])
raw = (spark.read.option("header", True).option("mode", "PERMISSIVE")
       .option("columnNameOfCorruptRecord", "_corrupt_record")
       .schema(schema).csv(PRICES_RAW)
       .withColumn("src_file", F.input_file_name())
       .cache())

counts = spark.createDataFrame([(
    lines.count(),
    lines.filter(F.col("value") == header_line).count(),
    lines.filter(F.trim("value") == "").count(),
    raw.count(),
    raw.filter(F.col("_corrupt_record").isNotNull()).count(),
    raw.select("src_file").distinct().count(),
)], "physical_lines long, header_lines long, blank_lines long, parsed_rows long, "
    "malformed_rows long, files long")
save(counts, "01_row_counts")

# %% [3] Column mapping (auto-detected; override if the header differs)
lc = {c.lower(): c for c in cols}
pick = lambda *names: next((lc[n] for n in names if n in lc), None)
C = dict(series=pick("name", "ticker", "symbol", "series"), date=pick("date", "day", "timestamp"),
         open=pick("open"), high=pick("high"), low=pick("low"),
         close=pick("close"), volume=pick("volume"))
print(C)
assert all(C.values()), f"unmapped columns: {C}"

# %% [4] Nulls / empty strings per column
null_df = raw.select([
    F.sum(F.when(F.col(c).isNull() | (F.trim(F.col(c)) == ""), 1).otherwise(0)).alias(c)
    for c in cols])
save(null_df, "02_null_or_empty_per_column")

# %% [5] Typed view for measurement only (raw strings kept alongside)
NUM = ["open", "high", "low", "close", "volume"]
t = raw.select(
    F.col(C["series"]).alias("series"),
    F.col(C["date"]).alias("date_raw"),
    F.to_date(F.col(C["date"]), "yyyy-MM-dd").alias("date"),
    *[F.col(C[k]).alias(f"{k}_raw") for k in NUM],
    *[F.col(C[k]).cast("double").alias(k) for k in NUM],
).cache()

parse = t.select(
    F.sum(F.when(F.col("date_raw").isNotNull() & F.col("date").isNull(), 1).otherwise(0)).alias("bad_date"),
    *[F.sum(F.when(F.col(f"{k}_raw").isNotNull() & (F.trim(F.col(f"{k}_raw")) != "")
                   & F.col(k).isNull(), 1).otherwise(0)).alias(f"bad_{k}") for k in NUM],
    F.min("date").cast("string").alias("min_date"), F.max("date").cast("string").alias("max_date"),
    F.sum(F.when((F.col("date") < EXPECTED_FIRST) | (F.col("date") > EXPECTED_LAST), 1)
          .otherwise(0)).alias("rows_outside_expected_range"),
    F.countDistinct("series").alias("distinct_series"),
    F.countDistinct("date").alias("distinct_dates"),
)
save(parse, "03_parse_failures_and_coverage")

# %% [6] Duplicates: exact rows, duplicate (series,date) keys, and conflicting duplicates
key = ["series", "date_raw"]
dups = spark.createDataFrame([(
    raw.count() - raw.drop("src_file").distinct().count(),
    t.groupBy(key).count().filter("count > 1").count(),
    t.groupBy(key).agg(F.countDistinct(F.concat_ws("|", *[F.coalesce(f"{k}_raw", F.lit("<null>"))
                                                          for k in NUM])).alias("v"))
     .filter("v > 1").count(),
)], "exact_duplicate_rows long, duplicate_keys long, conflicting_duplicate_keys long")
save(dups, "04_duplicates")

# %% [7] Price/volume validity and internal consistency (flags counted, nothing removed)
p = t.filter(F.col("date").isNotNull())
validity = p.select(
    F.sum(F.when((F.col("open") <= 0) | (F.col("high") <= 0) | (F.col("low") <= 0)
                 | (F.col("close") <= 0), 1).otherwise(0)).alias("nonpositive_price"),
    F.sum(F.when(F.col("high") < F.col("low"), 1).otherwise(0)).alias("high_lt_low"),
    F.sum(F.when((F.col("open") > F.col("high")) | (F.col("open") < F.col("low")), 1)
          .otherwise(0)).alias("open_outside_range"),
    F.sum(F.when((F.col("close") > F.col("high")) | (F.col("close") < F.col("low")), 1)
          .otherwise(0)).alias("close_outside_range"),
    F.sum(F.when(F.col("volume") < 0, 1).otherwise(0)).alias("negative_volume"),
    F.sum(F.when(F.col("volume") == 0, 1).otherwise(0)).alias("zero_volume"),
    F.sum(F.when(F.col("volume") != F.floor("volume"), 1).otherwise(0)).alias("fractional_volume"),
)
save(validity, "05_price_volume_validity")

# Extreme day-over-day moves (possible splits/corruption) - counted, not corrected.
w = W.partitionBy("series").orderBy("date")
moves = (p.withColumn("prev_close", F.lag("close").over(w))
          .withColumn("ret", F.col("close") / F.col("prev_close") - 1))
save(moves.select(
    F.sum(F.when(F.abs("ret") > 0.5, 1).otherwise(0)).alias("abs_close_move_gt_50pct"),
    F.sum(F.when(F.abs("ret") > 0.2, 1).otherwise(0)).alias("abs_close_move_gt_20pct"),
    F.expr("percentile_approx(ret, array(0.001,0.01,0.5,0.99,0.999))").alias("ret_quantiles")),
    "06_extreme_moves")

# %% [8] Per-series coverage and gaps against the observed market calendar
cal = p.select("date").distinct().withColumn("cal_idx", F.row_number().over(W.orderBy("date")))
per = (p.select("series", "date").distinct().join(cal, "date")
        .groupBy("series")
        .agg(F.count("*").alias("bars"), F.min("date").alias("first_date"),
             F.max("date").alias("last_date"),
             (F.max("cal_idx") - F.min("cal_idx") + 1).alias("calendar_days_in_span"))
        .withColumn("missing_days_in_span", F.col("calendar_days_in_span") - F.col("bars"))
        .withColumn("starts_late", F.col("first_date") > F.lit(EXPECTED_FIRST).cast("date"))
        .withColumn("ends_early", F.col("last_date") < F.lit(EXPECTED_LAST).cast("date")))
per.write.mode("errorifexists").parquet(f"{PRICES_PROF}/{RUN_ID}/07_per_series_coverage.parquet")
save(per.select(
    F.count("*").alias("series"),
    F.sum(F.col("starts_late").cast("int")).alias("series_starting_late"),
    F.sum(F.col("ends_early").cast("int")).alias("series_ending_early"),
    F.sum(F.when(F.col("missing_days_in_span") > 0, 1).otherwise(0)).alias("series_with_gaps"),
    F.sum("missing_days_in_span").alias("total_missing_bars_in_span"),
    F.min("bars").alias("min_bars"), F.max("bars").alias("max_bars"),
    F.expr("percentile_approx(bars, 0.5)").alias("median_bars")),
    "07_per_series_summary")
per.orderBy("bars").show(15, truncate=False)

# %% [9] SEC corpus: manifest vs HDFS, form mix, sizes, error pages served as 200
man = (spark.read.option("header", True).csv(f"{SEC_MAN}/sec_download_manifest.csv")
       .withColumn("bytes", F.col("bytes").cast("long")))
staged = (spark.read.option("header", True).csv(f"{SEC_MAN}/staging_record.csv"))
sec_files = spark.read.format("binaryFile").option("recursiveFileLookup", True) \
    .load(SEC_RAW).select(F.regexp_extract("path", r"([^/]+)$", 1).alias("local_file"), "length")

sec_summary = man.groupBy("status", "form").agg(
    F.count("*").alias("filings"), F.countDistinct("cik").alias("issuers"),
    F.sum("bytes").alias("bytes"), F.min("bytes").alias("min_bytes"), F.max("bytes").alias("max_bytes"))
save(sec_summary, "10_sec_status_by_form", SEC_PROF)

recon = spark.createDataFrame([(
    man.filter("status = 'ok'").count(), sec_files.count(),
    man.filter("status = 'ok'").join(sec_files, "local_file", "left_anti").count(),
    staged.filter("match != 'yes'").count(),
)], "manifest_ok long, files_in_hdfs long, manifest_ok_missing_in_hdfs long, staging_mismatches long")
save(recon, "11_sec_reconciliation", SEC_PROF)

# SEC sometimes serves a small HTML notice instead of the document. Only small files
# can be such notices, so scan files < 64 KB. Flag them; don't delete.
small = [f"{SEC_RAW}/{r.local_file}" for r in sec_files.filter("length < 65536").collect()]
if small:
    err_pages = (spark.read.text(small, wholetext=True).withColumn("f", F.input_file_name())
                 .filter(F.col("value").rlike(
                     "(?i)Request Rate Threshold Exceeded|Undeclared Automated Tool|<title>[^<]*Not Found"))
                 .select("f"))
else:
    err_pages = spark.createDataFrame([], "f string")
save(err_pages.agg(F.count("*").alias("sec_error_pages_detected"),
                   F.collect_list("f").alias("files")), "12_sec_error_pages", SEC_PROF)
