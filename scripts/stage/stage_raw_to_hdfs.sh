#!/usr/bin/env bash
# Stage one local raw source directory into HDFS under a provenance-first path,
# then verify every file by re-reading it from HDFS and comparing sha256.
#
#   /corpus/raw/<source>/<version>/acq_<YYYY-MM-DD>/...      original bytes
#   /corpus/raw/_manifests/<source>/<version>/acq_<date>/     manifests + staging record
#
# Raw files are never overwritten (no -f). Replication is set explicitly to 2.
#
# Usage (on the EMR primary node):
#   bash stage_raw_to_hdfs.sh <source> <version> <acq_date> <local_data_dir> [local_manifest_dir]
# Examples:
#   bash stage_raw_to_hdfs.sh prices    v1_2013-2017 2026-10-28 ~/raw/prices    ~/raw/_manifests
#   bash stage_raw_to_hdfs.sh sec_edgar v1           2026-10-28 ~/raw/sec_edgar/filings ~/raw/sec_edgar/_manifests
set -euo pipefail

SOURCE=${1:?source}; VERSION=${2:?version}; ACQ=${3:?acq_date YYYY-MM-DD}
LOCAL_DIR=${4:?local data dir}; LOCAL_MAN=${5:-}
[[ "$ACQ" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || { echo "acq_date must be YYYY-MM-DD"; exit 2; }

RAW="/corpus/raw/${SOURCE}/${VERSION}/acq_${ACQ}"
MAN="/corpus/raw/_manifests/${SOURCE}/${VERSION}/acq_${ACQ}"
REC=$(mktemp -d)/staging_record.csv
HDFS_PUT="hdfs dfs -D dfs.replication=2 -put"

if hdfs dfs -test -d "$RAW"; then
  echo "ERROR: $RAW already exists. Raw corpus is immutable; use a new acq_ date or version." >&2
  exit 3
fi

echo "staged_at_utc,host,local_file,hdfs_path,local_bytes,hdfs_bytes,local_sha256,hdfs_sha256,match" > "$REC"
hdfs dfs -mkdir -p "$RAW" "$MAN"

T0=$(date +%s)
cd "$LOCAL_DIR"
find . -type f ! -name '*.part' -printf '%P\n' | sort | while read -r f; do
  dest="$RAW/$f"
  hdfs dfs -mkdir -p "$(dirname "$dest")"
  $HDFS_PUT "$f" "$dest"
  lb=$(stat -c %s "$f")
  ls_=$(sha256sum "$f" | cut -d' ' -f1)
  hb=$(hdfs dfs -stat %b "$dest")
  hs=$(hdfs dfs -cat "$dest" | sha256sum | cut -d' ' -f1)
  m=$([[ "$lb" == "$hb" && "$ls_" == "$hs" ]] && echo yes || echo NO)
  echo "$(date -u +%FT%TZ),$(hostname),$f,$dest,$lb,$hb,$ls_,$hs,$m" >> "$REC"
  echo "$m  $f"
done
T1=$(date +%s)

if [[ -n "$LOCAL_MAN" ]]; then
  $HDFS_PUT "$LOCAL_MAN"/* "$MAN/"
fi
$HDFS_PUT "$REC" "$MAN/staging_record.csv"

# Evidence for the report: sizes, replication, and the HDFS listing.
{
  echo "staging_seconds=$((T1 - T0))"
  echo "files_staged=$(($(wc -l < "$REC") - 1))"
  echo "mismatches=$(grep -c ',NO$' "$REC" || true)"
  echo "--- hdfs dfs -du -s (logical bytes, bytes incl. replication)"
  hdfs dfs -du -s "$RAW"
  echo "--- replication of first file"
  hdfs dfs -stat 'replication=%r path=%n' "$(hdfs dfs -ls -R "$RAW" | awk '$1 !~ /^d/ {print $8; exit}')"
} | tee "$(dirname "$REC")/staging_summary.txt"
$HDFS_PUT "$(dirname "$REC")/staging_summary.txt" "$MAN/staging_summary.txt"

if grep -q ',NO$' "$REC"; then
  echo "ERROR: checksum or size mismatch; see $MAN/staging_record.csv" >&2
  exit 4
fi
echo "OK: $RAW verified. Manifests in $MAN"
