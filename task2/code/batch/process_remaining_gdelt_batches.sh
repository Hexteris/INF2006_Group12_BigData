#!/bin/bash
set -Eeuo pipefail

# ---------- Configuration ----------
ZIP_DIR="/mnt1/task2_gdelt/zips"
SCRIPT_DIR="$HOME/task2_hadoop_streaming_scripts"
STREAMING_JAR="/usr/lib/hadoop/hadoop-streaming-3.4.2-amzn-2.jar"

HDFS_RAW="/user/hadoop/task2/gdelt/raw"
HDFS_STREAM="/user/hadoop/task2/gdelt/streaming_output"
HDFS_EVENTS="/corpus/processed/gdelt/events"
HDFS_REJECTED="/corpus/rejected/gdelt"
HDFS_AUDIT="/corpus/audit/gdelt"

LOG_DIR="$HOME/gdelt_batch_logs"
WORK_ROOT="/tmp/gdelt_remaining_work"

# Exact remaining batches:
BATCH_IDS=(020 055 056 057)
STARTS=(204 550 560 570)
BATCH_SIZE=10

mkdir -p "$LOG_DIR" "$WORK_ROOT"
LOG_FILE="$LOG_DIR/remaining_$(date +%Y%m%d_%H%M%S).log"
exec > >(tee -a "$LOG_FILE") 2>&1

CURRENT_WORK=""

fail() {
    echo "ERROR: $*" >&2
    exit 1
}

cleanup_on_exit() {
    status=$?
    if [[ $status -ne 0 ]]; then
        echo
        echo "PROCESS STOPPED. Temporary work has been preserved."
        echo "Log: $LOG_FILE"
        [[ -n "$CURRENT_WORK" ]] && echo "Current work: $CURRENT_WORK"
    fi
}
trap cleanup_on_exit EXIT

echo "=================================================="
echo "REMAINING GDELT BATCH PROCESSING"
echo "Batches: ${BATCH_IDS[*]}"
echo "Started: $(date)"
echo "Log: $LOG_FILE"
echo "=================================================="

# ---------- Pre-flight checks ----------
[[ -d "$ZIP_DIR" ]] || fail "ZIP directory missing: $ZIP_DIR"
[[ -f "$SCRIPT_DIR/gdelt/prepare_gdelt_batch.py" ]] || fail "Preparation script missing."
[[ -f "$SCRIPT_DIR/gdelt/mapper.py" ]] || fail "Mapper missing."
[[ -f "$SCRIPT_DIR/gdelt/reducer.py" ]] || fail "Reducer missing."
[[ -f "$SCRIPT_DIR/gdelt/split_gdelt_output.py" ]] || fail "Splitter missing."
[[ -f "$STREAMING_JAR" ]] || fail "Hadoop Streaming JAR missing."

python3 -m py_compile \
    "$SCRIPT_DIR/gdelt/prepare_gdelt_batch.py" \
    "$SCRIPT_DIR/gdelt/mapper.py" \
    "$SCRIPT_DIR/gdelt/reducer.py" \
    "$SCRIPT_DIR/gdelt/split_gdelt_output.py"

ARCHIVE_COUNT=$(python3 - "$ZIP_DIR" <<'PY'
import glob, os, sys
print(len(sorted(glob.glob(os.path.join(sys.argv[1], "*", "*.zip")))))
PY
)

echo "ZIP archives found: $ARCHIVE_COUNT"
[[ "$ARCHIVE_COUNT" -eq 731 ]] || fail "Expected 731 ZIP archives."

hdfs dfs -mkdir -p \
    "$HDFS_RAW" "$HDFS_STREAM" "$HDFS_EVENTS" \
    "$HDFS_REJECTED" "$HDFS_AUDIT"

# Check all target paths before starting any batch.
for i in "${!BATCH_IDS[@]}"; do
    ID="${BATCH_IDS[$i]}"
    WORK="$WORK_ROOT/batch_$ID"

    [[ ! -e "$WORK" ]] || fail "Local work directory already exists: $WORK"

    for path in \
        "$HDFS_RAW/batch_$ID.tsv" \
        "$HDFS_STREAM/batch_$ID" \
        "$HDFS_EVENTS/batch_${ID}.tsv.gz" \
        "$HDFS_REJECTED/batch_${ID}_rejected.jsonl" \
        "$HDFS_AUDIT/batch_${ID}_audit.jsonl" \
        "$HDFS_AUDIT/batch_${ID}_manifest.json"; do

        if hdfs dfs -test -e "$path"; then
            fail "Existing HDFS path; refusing to overwrite: $path"
        fi
    done
done

echo "Pre-flight checks passed."
echo

# ---------- Process the four selected batches ----------
for i in "${!BATCH_IDS[@]}"; do
    ID="${BATCH_IDS[$i]}"
    START="${STARTS[$i]}"

    echo
    echo "=================================================="
    echo "BATCH $ID | Archive index $START | Count $BATCH_SIZE"
    echo "Started: $(date)"
    echo "=================================================="

    CURRENT_WORK="$WORK_ROOT/batch_$ID"
    mkdir -p "$CURRENT_WORK"

    INPUT_TSV="$CURRENT_WORK/batch_${ID}.tsv"
    MANIFEST="$CURRENT_WORK/batch_${ID}_manifest.json"
    SUMMARY="$CURRENT_WORK/batch_${ID}_split_summary.json"
    VALID_TSV="$CURRENT_WORK/valid.tsv"
    REJECTED_JSON="$CURRENT_WORK/rejected.jsonl"
    AUDIT_JSON="$CURRENT_WORK/audit.jsonl"
    COMPRESSED="$CURRENT_WORK/batch_${ID}.tsv.gz"

    HDFS_INPUT="$HDFS_RAW/batch_${ID}.tsv"
    HDFS_OUTPUT="$HDFS_STREAM/batch_${ID}"
    HDFS_FINAL="$HDFS_EVENTS/batch_${ID}.tsv.gz"
    HDFS_REJECT="$HDFS_REJECTED/batch_${ID}_rejected.jsonl"
    HDFS_AUDIT_FILE="$HDFS_AUDIT/batch_${ID}_audit.jsonl"
    HDFS_MANIFEST="$HDFS_AUDIT/batch_${ID}_manifest.json"

    # 1. Extract the ten ZIP archives.
    python3 "$SCRIPT_DIR/gdelt/prepare_gdelt_batch.py" \
        --input-dir "$ZIP_DIR" \
        --output "$INPUT_TSV" \
        --manifest "$MANIFEST" \
        --start "$START" \
        --count "$BATCH_SIZE"

    [[ -s "$INPUT_TSV" ]] || fail "Extracted TSV is empty."

    # Verify manifest archive count and exact date range.
python3 - "$MANIFEST" "$ID" <<'PY'
import json, sys
from datetime import datetime, timedelta

path, batch_id = sys.argv[1], sys.argv[2]

expected = {
    "020": ("20160121", "20160130"),
    "055": ("20170101", "20170110"),
    "056": ("20170111", "20170120"),
    "057": ("20170121", "20170130"),
}

m = json.load(open(path))
dates = [
    a["archive"].split(".")[0]
    for a in m["archives"]
]

start, end = expected[batch_id]
expected_dates = [
    (datetime.strptime(start, "%Y%m%d") + timedelta(days=i)).strftime("%Y%m%d")
    for i in range(10)
]

print("Batch:", batch_id)
print("Archives:", m["archive_count"])
print("Date range:", dates[0], "to", dates[-1])
print("Input records:", f'{m["total_lines"]:,}')

if m["archive_count"] != 10:
    raise SystemExit("FAIL: expected exactly 10 archives.")

if dates != expected_dates:
    raise SystemExit("FAIL: archive dates do not match expected range.")

print("Date-range validation passed.")
PY

    # 2. Upload raw batch to HDFS.
    hdfs dfs -put "$INPUT_TSV" "$HDFS_INPUT"

    # 3. Run Hadoop Streaming.
    hadoop jar "$STREAMING_JAR" \
        -D mapreduce.job.reduces=4 \
        -files "$SCRIPT_DIR/gdelt/mapper.py,$SCRIPT_DIR/gdelt/reducer.py" \
        -mapper "python3 mapper.py" \
        -reducer "python3 reducer.py" \
        -input "$HDFS_INPUT" \
        -output "$HDFS_OUTPUT"

    hdfs dfs -test -e "$HDFS_OUTPUT/_SUCCESS" || \
        fail "Hadoop Streaming failed for batch $ID."

    # 4. Split mapper/reducer output into valid, rejected and audit files.
    (
        cd "$CURRENT_WORK"
        set -o pipefail
        hdfs dfs -cat "$HDFS_OUTPUT"/part-* \
            | python3 "$SCRIPT_DIR/gdelt/split_gdelt_output.py" \
            > "$SUMMARY"
    )

    # 5. Reconcile all record and audit counts.
    python3 - "$MANIFEST" "$SUMMARY" "$VALID_TSV" \
        "$REJECTED_JSON" "$AUDIT_JSON" <<'PY'
import json, sys

manifest = json.load(open(sys.argv[1]))
summary = json.load(open(sys.argv[2]))

def lines(path):
    with open(path, "rb") as f:
        return sum(1 for _ in f)

expected = manifest["total_lines"]
valid = summary["valid_records"]
rejected = summary["rejected_records"]
audits = summary["audit_summaries"]
invalid = summary["invalid_output_lines"]

valid_lines = lines(sys.argv[3])
rejected_lines = lines(sys.argv[4])
audit_lines = lines(sys.argv[5])

print("INPUT RECORDS:", f"{expected:,}")
print("VALID RECORDS:", f"{valid:,}")
print("REJECTED RECORDS:", f"{rejected:,}")
print("AUDIT SUMMARIES:", audits)
print("INVALID OUTPUT LINES:", invalid)

checks = [
    (valid + rejected == expected, "valid + rejected must equal input"),
    (invalid == 0, "invalid Streaming output lines found"),
    (audits == manifest["archive_count"], "audit count must equal archive count"),
    (valid_lines == valid, "valid TSV line count mismatch"),
    (rejected_lines == rejected, "rejected JSONL line count mismatch"),
    (audit_lines == audits, "audit JSONL line count mismatch"),
]

for passed, message in checks:
    if not passed:
        raise SystemExit("FAIL: " + message)

print("All record reconciliation checks passed.")
PY

    # 6. Validate all valid records have exactly 58 columns.
    awk -F '\t' 'NF != 58 {bad++} END {
        if (bad > 0) {
            print "Malformed TSV rows:", bad
            exit 1
        }
        print "58-column validation passed. Rows:", NR
    }' "$VALID_TSV"

    # 7. Compress and test the valid data.
    gzip -c "$VALID_TSV" > "$COMPRESSED"
    gzip -t "$COMPRESSED"

    # 8. Upload final dataset and audit evidence.
    hdfs dfs -put "$COMPRESSED" "$HDFS_FINAL"
    hdfs dfs -put "$REJECTED_JSON" "$HDFS_REJECT"
    hdfs dfs -put "$AUDIT_JSON" "$HDFS_AUDIT_FILE"
    hdfs dfs -put "$MANIFEST" "$HDFS_MANIFEST"

    # 9. Verify the uploaded compressed dataset.
    HDFS_VALID_COUNT=$(
        set -o pipefail
        hdfs dfs -cat "$HDFS_FINAL" \
            | gzip -dc \
            | awk -F '\t' 'NF != 58 {bad++} END {
                if (bad > 0) exit 1
                print NR
            }'
    )

    LOCAL_VALID_COUNT=$(python3 -c \
        'import json,sys; print(json.load(open(sys.argv[1]))["valid_records"])' \
        "$SUMMARY")

    [[ "$HDFS_VALID_COUNT" -eq "$LOCAL_VALID_COUNT" ]] || \
        fail "HDFS row count mismatch for batch $ID."

    # Confirm all uploaded evidence files exist.
    for path in "$HDFS_REJECT" "$HDFS_AUDIT_FILE" "$HDFS_MANIFEST"; do
        hdfs dfs -test -e "$path" || fail "Uploaded evidence missing: $path"
    done

    # 10. Remove temporary HDFS input and Streaming output only after verification.
    hdfs dfs -rm "$HDFS_INPUT"
    hdfs dfs -rm -r "$HDFS_OUTPUT"

    # 11. Keep compact local evidence; remove large intermediate TSV files.
    rm -f "$INPUT_TSV" "$VALID_TSV"

    echo
    echo "BATCH $ID COMPLETED SUCCESSFULLY."
    echo "Verified valid records: $HDFS_VALID_COUNT"
    echo "Local evidence retained in: $CURRENT_WORK"
    hdfs dfs -ls -h "$HDFS_FINAL"
    echo "Finished: $(date)"

    CURRENT_WORK=""
done

echo
echo "=================================================="
echo "ALL FOUR REMAINING BATCHES COMPLETED SUCCESSFULLY"
echo "Finished: $(date)"
echo "=================================================="

echo "Processed event files:"
for ID in "${BATCH_IDS[@]}"; do
    hdfs dfs -ls -h "$HDFS_EVENTS/batch_${ID}.tsv.gz"
done

echo "Audit files:"
for ID in "${BATCH_IDS[@]}"; do
    hdfs dfs -ls -h "$HDFS_AUDIT/batch_${ID}_audit.jsonl"
    hdfs dfs -ls -h "$HDFS_AUDIT/batch_${ID}_manifest.json"
done

echo "Log: $LOG_FILE"
