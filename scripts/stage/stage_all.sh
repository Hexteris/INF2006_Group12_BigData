#!/usr/bin/env bash
# Verify an arranged raw tree, then stage every source into HDFS with stage_raw_to_hdfs.sh.
# Uses the original acquisition dates from evidence/expected_corpus.csv, so every member's
# HDFS paths are identical. Sources already in HDFS are skipped (raw is immutable).
#
#   bash scripts/stage/stage_all.sh [/mnt/g12/raw]
set -uo pipefail
cd "$(dirname "$0")/../.."
R=${1:-/mnt/g12/raw}

python3 scripts/stage/setup_corpus.py --check-only "$R" || { echo "STOP: fix the problems above first"; exit 1; }

fail=0
while IFS=, read -r src ver acq; do
  if hdfs dfs -test -d "/corpus/raw/$src/$ver/acq_$acq"; then
    echo "skip $src (already in HDFS)"; continue
  fi
  bash scripts/stage/stage_raw_to_hdfs.sh "$src" "$ver" "$acq" "$R/$src/$ver/acq_$acq" || fail=1
done < <(tail -n +2 evidence/expected_corpus.csv | cut -d, -f1-3 | sort -u)

hdfs dfs -D dfs.replication=2 -put -f "$R/setup_report.txt" /corpus/raw/_manifests/ 2>/dev/null
hdfs dfs -du -s -h /corpus/raw
[[ $fail == 0 ]] && echo "ALL SOURCES STAGED AND VERIFIED" || { echo "SOME SOURCES FAILED"; exit 1; }
