#!/usr/bin/env bash
# Record that the running cluster matches the reference config (run on the primary node).
# Writes evidence/cluster_verify_<UTC timestamp>.txt and, if a bucket is given, copies it to S3.
#   bash scripts/cluster/verify_cluster.sh [s3-bucket]
set -uo pipefail
cd "$(dirname "$0")/../.."
mkdir -p evidence
OUT="evidence/cluster_verify_$(date -u +%Y%m%dT%H%M%SZ).txt"
CID=$(jq -r .jobFlowId /mnt/var/lib/info/job-flow.json 2>/dev/null || echo unknown)
REL=$(jq -r .releaseLabel /mnt/var/lib/info/extraInstanceData.json 2>/dev/null || echo unknown)

check() {  # name, expected, actual
  local s=FAIL; [[ "$2" == "$3" ]] && s=PASS
  printf '%-34s expected=%-8s actual=%-8s %s\n' "$1" "$2" "$3" "$s"
}

{
  echo "verified_at_utc=$(date -u +%FT%TZ) cluster_id=$CID release=$REL host=$(hostname)"
  echo "== checks"
  check "dfs.replication" 2 "$(hdfs getconf -confKey dfs.replication 2>/dev/null)"
  check "live datanodes" 2 "$(hdfs dfsadmin -report 2>/dev/null | sed -n 's/^Live datanodes (\([0-9]*\)).*/\1/p')"
  sd=/etc/spark/conf/spark-defaults.conf
  getp() { awk -v k="$1" '$1==k {print $2}' "$sd"; }
  check "spark.dynamicAllocation.enabled" false "$(getp spark.dynamicAllocation.enabled)"
  check "spark.executor.instances" 2  "$(getp spark.executor.instances)"
  check "spark.executor.cores"     2  "$(getp spark.executor.cores)"
  check "spark.executor.memory"    6g "$(getp spark.executor.memory)"
  check "spark.executor.memoryOverhead" 2g "$(getp spark.executor.memoryOverhead)"
  check "spark.driver.memory"      2g "$(getp spark.driver.memory)"
  echo "== datanode capacity (expect ~94 GiB configured per node: 100 GiB gp3 volume, see docs/decisions.md D2)"
  hdfs dfsadmin -report 2>/dev/null | grep -E '^(Name|Hostname|Configured Capacity|DFS Remaining):'
  echo "== yarn nodes"
  yarn node -list -all 2>/dev/null
  echo "== versions"
  hadoop version 2>/dev/null | head -1
  hive --version 2>/dev/null | head -1
  spark-submit --version 2>&1 | grep -m1 -i 'version'
  python3 --version
} | tee "$OUT"

if [[ -n "${1:-}" ]]; then
  aws s3 cp "$OUT" "s3://$1/evidence/$(basename "$OUT")"
fi
grep -q FAIL "$OUT" && { echo "Some checks FAILED - see $OUT"; exit 1; }
echo "All checks PASS -> $OUT"
