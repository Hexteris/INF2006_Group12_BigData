#!/usr/bin/env bash
# One-command cluster setup: copy the bronze corpus from your S3 bucket to the primary node,
# verify it, stage all 5 sources into HDFS (/corpus/raw/...) and save the staging records to S3.
# Credentials come from the EMR instance role; nothing to paste.
#
#   bash scripts/stage/cluster_setup.sh inf2006-g12-<studentID>
set -uo pipefail
cd "$(dirname "$0")/../.."
B=${1:-}
[[ -n "$B" ]] || { echo "Usage: bash scripts/stage/cluster_setup.sh inf2006-g12-<studentID>"; exit 2; }
R=/mnt/g12/raw

echo "== 1/5 working folder /mnt/g12"
sudo mkdir -p /mnt/g12 && sudo chown "$(id -un)":"$(id -gn)" /mnt/g12 || { echo "FAIL: cannot create /mnt/g12"; exit 1; }

echo "== 2/5 cluster config check"
bash scripts/cluster/verify_cluster.sh || echo "WARN: verify_cluster.sh reported a problem (see above); continuing"

echo "== 3/5 copy s3://$B/raw/ -> $R"
aws s3 ls "s3://$B/raw/" >/dev/null 2>&1 || { echo "FAIL: cannot read s3://$B/raw/ (wrong bucket name, or setup.ps1 upload not done)"; exit 1; }
aws s3 sync "s3://$B/raw/" "$R/" --exclude "*.part" --only-show-errors || { echo "FAIL: S3 copy stopped; rerun this command"; exit 1; }

echo "== 4/5 verify + stage into HDFS"
bash scripts/stage/stage_all.sh "$R" || { echo "FAIL: staging did not finish (see above)"; exit 1; }

echo "== 5/5 save staging records to s3://$B/evidence/staging/"
aws s3 cp --recursive /mnt/g12/staging "s3://$B/evidence/staging/" --only-show-errors \
  || echo "WARN: could not copy staging records to S3"

echo "PASS: corpus is in HDFS under /corpus/raw (records in s3://$B/evidence/staging/)"
