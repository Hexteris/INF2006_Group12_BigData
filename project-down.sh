#!/usr/bin/env bash
# project-down.sh - macOS/Linux port of project-down.ps1. Copy outputs to S3 first: HDFS is deleted with the cluster.
# Usage: ./project-down.sh [-c j-XXXX]
cd "$(dirname "$0")"; REGION="us-east-1"; CID=""
while getopts "c:g:" o; do case $o in c) CID=$OPTARG;; g) REGION=$OPTARG;; *) exit 1;; esac; done
[ -z "$CID" ] && [ -f .project-cluster-id ] && CID=$(tr -d '[:space:]' < .project-cluster-id)
[ -n "$CID" ] || { echo "No -c given and no .project-cluster-id file found."; exit 1; }
echo "Terminating cluster $CID ..."
aws emr terminate-clusters --cluster-ids "$CID" --region "$REGION" || { echo "Failed. Terminate it from the EMR console instead."; exit 1; }
echo "Termination requested; state now: $(aws emr describe-cluster --cluster-id "$CID" --region "$REGION" --query Cluster.Status.State --output text 2>/dev/null). Then click End Lab in AWS Academy."
rm -f .project-cluster-id
