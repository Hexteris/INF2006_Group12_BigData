#!/usr/bin/env bash
# Stage one local raw source directory into HDFS under a provenance-first path, then
# verify every file by reading it back from HDFS and comparing size + sha256.
# This is the procedure used for the 2026-10-28 acquisition (staging records in
# evidence/manifests_2026-10-28/<source>/<version>/acq_2026-10-28/).
#
#   /corpus/raw/<source>/<version>/acq_<YYYY-MM-DD>/          original bytes
#   /corpus/raw/_manifests/<source>/<version>/acq_<date>/      manifests + staging record
#
# One bulk -put and one bulk -get per source (not per file), so 311 SEC filings stage in
# seconds. Raw folders are immutable: the script stops if the target already exists.
# Replication is set explicitly to 2.
#
# Usage (EMR primary node):
#   bash stage_raw_to_hdfs.sh <source> <version> <acq_date> <local_dir> [local_manifest_dir]
# As run on 2026-10-28 (S=/mnt/g12/stage, a fresh copy pulled back from S3):
#   bash stage_raw_to_hdfs.sh prices              v1_2013-2017 2026-10-28 $S/prices            $S/_manifests
#   bash stage_raw_to_hdfs.sh sec_company_tickers current      2026-10-28 $S/sec_ref
#   bash stage_raw_to_hdfs.sh sec_inventory       v1           2026-10-28 $S/sec_inventory     $S/sec_inventory_man
#   bash stage_raw_to_hdfs.sh sec_edgar           v1           2026-10-28 $S/sec_edgar/filings $S/sec_edgar/_manifests
set -uo pipefail

src=${1:?source}; ver=${2:?version}; ACQ=${3:?acq_date YYYY-MM-DD}; dir=${4:?local dir}; man=${5:-}
[[ "$ACQ" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || { echo "acq_date must be YYYY-MM-DD"; exit 2; }

raw=/corpus/raw/$src/$ver/acq_$ACQ
mdir=/corpus/raw/_manifests/$src/$ver/acq_$ACQ
work=/mnt/g12/staging/$src
rec=$work/staging_record.csv

hdfs dfs -test -d "$raw" && { echo "STOP: $raw already exists (raw is immutable)"; exit 3; }
find "$dir" -name '*.part' | grep -q . && { echo "STOP: partial downloads in $dir"; exit 3; }
rm -rf "$work"; mkdir -p "$work"
hdfs dfs -mkdir -p "$(dirname "$raw")" "$mdir"

t0=$(date +%s)
hdfs dfs -D dfs.replication=2 -put "$dir" "$raw" || exit 1
t1=$(date +%s)
hdfs dfs -get "$raw" "$work/readback" || exit 1
t2=$(date +%s)

echo "staged_at_utc,host,local_file,hdfs_path,local_bytes,hdfs_bytes,local_sha256,hdfs_sha256,match" > "$rec"
(cd "$dir" && find . -type f -printf '%P\n' | sort) | while read -r f; do
  lb=$(stat -c %s "$dir/$f"); lsum=$(sha256sum "$dir/$f" | cut -d' ' -f1)
  hb=$(stat -c %s "$work/readback/$f" 2>/dev/null || echo 0)
  hsum=$(sha256sum "$work/readback/$f" 2>/dev/null | cut -d' ' -f1)
  [[ "$lb" == "$hb" && "$lsum" == "$hsum" ]] && m=yes || m=NO
  echo "$(date -u +%FT%TZ),$(hostname),$f,$raw/$f,$lb,$hb,$lsum,$hsum,$m" >> "$rec"
done

{ echo "source=$src version=$ver acq=$ACQ put_seconds=$((t1-t0)) readback_seconds=$((t2-t1))"
  echo "files=$(( $(wc -l < "$rec") - 1 )) mismatches=$(grep -c ',NO$' "$rec")"
  hdfs dfs -du -s "$raw"
  hdfs dfs -stat 'replication=%r file=%n' "$raw/$(sed -n 2p "$rec" | cut -d, -f3)"
} | tee "$work/staging_summary.txt"

[[ -n "$man" ]] && hdfs dfs -D dfs.replication=2 -put "$man"/* "$mdir/"
hdfs dfs -D dfs.replication=2 -put "$rec" "$work/staging_summary.txt" "$mdir/"
rm -rf "$work/readback"
grep -q ',NO$' "$rec" && { echo "MISMATCH - see $rec"; exit 4; }
echo "OK: $raw verified -> manifests in $mdir"
