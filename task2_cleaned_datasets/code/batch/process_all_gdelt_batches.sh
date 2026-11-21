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
WORK_ROOT="/tmp/gdelt_automated"
TOTAL_ARCHIVES=731
BATCH_SIZE=10
FIRST_START=10   # Batch 000 has already been completed.

mkdir -p "$LOG_DIR" "$WORK_ROOT"
LOG_FILE="$LOG_DIR/process_all_$(date +%Y%m%d_%H%M%S).log"
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
        echo "Check log: $LOG_FILE"
        [[ -n "$CURRENT_WORK" ]] && echo "Work directory: $CURRENT_WORK"
    fi
}
trap cleanup_on_exit EXIT

echo "=================================================="
echo "GDELT AUTOMATED BATCH PROCESSING"
echo "Started: $(date)"
echo "Archives expected: $TOTAL_ARCHIVES"
echo "Starting archive index: $FIRST_START"
echo "Log: $LOG_FILE"
echo "=================================================="

# ---------- Pre-flight checks ----------
[[ -d "$ZIP_DIR" ]] || fail "ZIP directory not found: $ZIP_DIR"
[[ -f "$SCRIPT_DIR/gdelt/prepare_gdelt_batch.py" ]] || fail "Batch preparation script missing."
[[ -f "$SCRIPT_DIR/gdelt/mapper.py" ]] || fail "GDELT mapper missing."
[[ -f "$SCRIPT_DIR/gdelt/reducer.py" ]] || fail "GDELT reducer missing."
[[ -f "$SCRIPT_DIR/gdelt/split_gdelt_output.py" ]] || fail "GDELT splitter missing."
[[ -f "$STREAMING_JAR" ]] || fail "Hadoop Streaming JAR not found."

python3 -m py_compile \
    "$SCRIPT_DIR/gdelt/prepare_gdelt_batch.py" \
    "$SCRIPT_DIR/gdelt/mapper.py" \
    "$SCRIPT_DIR/gdelt/reducer.py" \
    "$SCRIPT_DIR/gdelt/split_gdelt_output.py"

ARCHIVE_COUNT=$(python3 - "$ZIP_DIR" <<'PY'
import glob, os, sys
files = sorted(glob.glob(os.path.join(sys.argv[1], "*", "*.zip")))
print(len(files))
PY
)

echo "ZIP archives found: $ARCHIVE_COUNT"
[[ "$ARCHIVE_COUNT" -eq "$TOTAL_ARCHIVES" ]] || \
    fail "Expected $TOTAL_ARCHIVES ZIP archives, found $ARCHIVE_COUNT."

hdfs dfs -mkdir -p "$HDFS_RAW" "$HDFS_STREAM" \
    "$HDFS_EVENTS" "$HDFS_REJECTED" "$HDFS_AUDIT"

# ---------- Process remaining batches ----------
for ((START=FIRST_START; START<TOTAL_ARCHIVES; START+=BATCH_SIZE)); do
    BATCH_INDEX=$((START / BATCH_SIZE))
    BATCH_ID=$(printf "%03d" "$BATCH_INDEX")
    COUNT=$BATCH_SIZE
    REMAINING=$((TOTAL_ARCHIVES - START))
    (( REMAINING < COUNT )) && COUNT=$REMAINING

    echo
    echo "=================================================="
    echo "BATCH $BATCH_ID | Archive index $START | Count $COUNT"
    echo "Started: $(date)"
    echo "=================================================="

    CURRENT_WORK="$WORK_ROOT/batch_$BATCH_ID"
    mkdir -p "$CURRENT_WORK"

    INPUT_TSV="$CURRENT_WORK/batch_$BATCH_ID.tsv"
    MANIFEST="$CURRENT_WORK/batch_${BATCH_ID}_manifest.json"
    SUMMARY="$CURRENT_WORK/batch_${BATCH_ID}_split_summary.json"
    VALID_TSV="$CURRENT_WORK/valid.tsv"
    REJECTED_JSON="$CURRENT_WORK/rejected.jsonl"
    AUDIT_JSON="$CURRENT_WORK/audit.jsonl"
    COMPRESSED="$CURRENT_WORK/batch_${BATCH_ID}.tsv.gz"

    HDFS_INPUT="$HDFS_RAW/batch_$BATCH_ID.tsv"
    HDFS_OUTPUT="$HDFS_STREAM/batch_$BATCH_ID"
    HDFS_FINAL="$HDFS_EVENTS/batch_${BATCH_ID}.tsv.gz"
    HDFS_REJECT="$HDFS_REJECTED/batch_${BATCH_ID}_rejected.jsonl"
    HDFS_AUDIT_FILE="$HDFS_AUDIT/batch_${BATCH_ID}_audit.jsonl"
    HDFS_MANIFEST="$HDFS_AUDIT/batch_${BATCH_ID}_manifest.json"

    # Refuse to overwrite any existing batch artifacts.
    for path in "$HDFS_INPUT" "$HDFS_OUTPUT" "$HDFS_FINAL" \
                "$HDFS_REJECT" "$HDFS_AUDIT_FILE" "$HDFS_MANIFEST"; do
        if hdfs dfs -test -e "$path"; then
            fail "Existing HDFS path found; refusing to overwrite: $path"
        fi
    done

    # 1. Extract this batch's ZIP archives.
    python3 "$SCRIPT_DIR/gdelt/prepare_gdelt_batch.py" \
        --input-dir "$ZIP_DIR" \
        --output "$INPUT_TSV" \
        --manifest "$MANIFEST" \
        --start "$START" \
        --count "$COUNT"

    [[ -s "$INPUT_TSV" ]] || fail "Extracted TSV is empty for batch $BATCH_ID."

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
        fail "Hadoop Streaming did not create _SUCCESS for batch $BATCH_ID."

    # 4. Split Streaming JSON into valid TSV, rejects and audit summaries.
    (
        cd "$CURRENT_WORK"
        set -o pipefail
        hdfs dfs -cat "$HDFS_OUTPUT"/part-* \
            | python3 "$SCRIPT_DIR/gdelt/split_gdelt_output.py" \
            > "$SUMMARY"
    )

    # 5. Reconcile input, valid and rejected record counts.
    python3 - "$MANIFEST" "$SUMMARY" <<'PY'
import json, sys
with open(sys.argv[1]) as f:
    manifest = json.load(f)
with open(sys.argv[2]) as f:
    summary = json.load(f)

expected = manifest["total_lines"]
valid = summary["valid_records"]
rejected = summary["rejected_records"]
invalid = summary["invalid_output_lines"]

print("INPUT RECORDS:", f"{expected:,}")
print("VALID RECORDS:", f"{valid:,}")
print("REJECTED RECORDS:", f"{rejected:,}")
print("AUDIT SUMMARIES:", f'{summary["audit_summaries"]:,}')
print("INVALID OUTPUT LINES:", f"{invalid:,}")

if valid + rejected != expected:
    raise SystemExit("FAIL: valid + rejected does not equal input records.")
if invalid != 0:
    raise SystemExit("FAIL: invalid Streaming output lines detected.")
PY

    # 6. Validate TSV structure and compress.
    awk -F '\t' 'NF != 58 {bad++} END {
        if (bad > 0) {
            print "Malformed TSV rows:", bad
            exit 1
        }
        print "TSV column validation passed. Rows:", NR
    }' "$VALID_TSV"

    gzip -c "$VALID_TSV" > "$COMPRESSED"
    gzip -t "$COMPRESSED"

    # 7. Upload final data and audit evidence.
    hdfs dfs -put "$COMPRESSED" "$HDFS_FINAL"
    hdfs dfs -put "$REJECTED_JSON" "$HDFS_REJECT"
    hdfs dfs -put "$AUDIT_JSON" "$HDFS_AUDIT_FILE"
    hdfs dfs -put "$MANIFEST" "$HDFS_MANIFEST"

    # 8. Verify compressed HDFS data and its 58-column structure.
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
        fail "HDFS valid row count mismatch in batch $BATCH_ID."

    # 9. Remove temporary HDFS files only after final verification.
    hdfs dfs -rm "$HDFS_INPUT"
    hdfs dfs -rm -r "$HDFS_OUTPUT"

    # 10. Remove local intermediate data for this completed batch.
    rm -rf "$CURRENT_WORK"
    CURRENT_WORK=""

    echo "BATCH $BATCH_ID COMPLETED SUCCESSFULLY."
    echo "Verified valid records: $HDFS_VALID_COUNT"
    hdfs dfs -ls -h "$HDFS_FINAL"
    echo "Finished: $(date)"
done

echo
echo "=================================================="
echo "ALL REMAINING GDELT BATCHES COMPLETED"
echo "Finished: $(date)"
echo "=================================================="

echo "Final processed files:"
hdfs dfs -ls "$HDFS_EVENTS"

echo "Final audit files:"
hdfs dfs -ls "$HDFS_AUDIT"

echo "HDFS storage:"
hdfs dfsadmin -report | grep -E \
    'Configured Capacity|DFS Used:|DFS Remaining:'

echo "Processing log: $LOG_FILE"
