#!/usr/bin/env bash
# project-up.sh - macOS/Linux port of project-up.ps1 (same cluster, same settings).
# Usage:
#   ./project-up.sh -b inf2006-g12-2500837                 # launch + connect
#   ./project-up.sh -c j-XXXXXXXX                          # reconnect / reopen SSH after an IP change
# Options: -b bucket  -c cluster-id  -n name  -r release  -k key-name  -p pem-file  -g region  -x (print the ssh command, do not connect)
set -u
cd "$(dirname "$0")"
NAME="G12-Reference"; BUCKET=""; RELEASE="emr-7.10.0"; IDLE=3600; KEY="vockey2"; PEM="vockey2.pem"; REGION="us-east-1"; CID=""; NOCONNECT=0
while getopts "b:c:n:r:k:p:g:x" o; do
  case $o in b) BUCKET=$OPTARG;; c) CID=$OPTARG;; n) NAME=$OPTARG;; r) RELEASE=$OPTARG;; k) KEY=$OPTARG;; p) PEM=$OPTARG;; g) REGION=$OPTARG;; x) NOCONNECT=1;; *) exit 1;; esac
done
mkdir -p evidence

echo "Checking AWS credentials..."
if ! who=$(aws sts get-caller-identity --region "$REGION" --output text 2>&1); then
  echo "AWS credentials are not valid/expired. Paste 'AWS CLI: Show' into ~/.aws/credentials"; exit 1
fi
echo "  OK: $who"
[ -f "$PEM" ] || { echo "Key file $PEM not found in $(pwd)"; exit 1; }
chmod 400 "$PEM"

if [ -z "$CID" ]; then
  [ -n "$BUCKET" ] || { echo "-b <bucket> is required when launching"; exit 1; }
  if ! aws s3api head-bucket --bucket "$BUCKET" --region "$REGION" 2>/dev/null; then
    echo "Creating bucket s3://$BUCKET ..."; aws s3 mb "s3://$BUCKET" --region "$REGION" || { echo "Could not create bucket."; exit 1; }
  fi
  # Task 3 (Spark ML) needs NumPy on every node: stage the bootstrap script in the bucket.
  aws s3 cp install-numpy.sh "s3://$BUCKET/bootstrap/install-numpy.sh" --region "$REGION" || { echo "Could not upload install-numpy.sh"; exit 1; }
  CFG=$(mktemp -t g12-emr-config).json
  python3 - "$BUCKET" "$CFG" <<'PY'
import json, sys
bucket, out = sys.argv[1], sys.argv[2]
cfg = json.load(open("config/emr-configurations.json"))
cfg.append({"Classification": "jupyter-s3-conf", "Properties": {"s3.persistence.enabled": "true", "s3.persistence.bucket": bucket}})
json.dump(cfg, open(out, "w"), indent=2)
PY
  echo "Launching '$NAME' ($RELEASE; 1 primary + 2 core m5.xlarge; 100 GiB gp3 per core)..."
  CID=$(aws emr create-cluster --name "$NAME" --release-label "$RELEASE" \
        --applications Name=Hadoop Name=Hive Name=Spark Name=Livy Name=JupyterHub \
        --instance-groups "file://config/instance-groups.json" --configurations "file://$CFG" \
        --bootstrap-actions Path="s3://$BUCKET/bootstrap/install-numpy.sh",Name=install-numpy \
        --ec2-attributes KeyName="$KEY" --ebs-root-volume-size 30 --use-default-roles \
        --log-uri "s3://$BUCKET/emr-logs/" --auto-termination-policy IdleTimeout=$IDLE \
        --tags "project=INF2006-G12" --region "$REGION" --query ClusterId --output text) \
    || { echo "create-cluster failed. If the key pair is missing try -k vockey -p vockey.pem; if the release is rejected try -r emr-7.9.0 (record it as a deviation)."; exit 1; }
  echo "  Cluster launched: $CID"
fi
echo "$CID" > .project-cluster-id

echo "Waiting for WAITING (~10-15 min)..."
while true; do
  sleep 30
  info=$(aws emr describe-cluster --cluster-id "$CID" --region "$REGION" \
    --query "Cluster.[Status.State,MasterPublicDnsName,Ec2InstanceAttributes.EmrManagedMasterSecurityGroup,Status.StateChangeReason.Message]" --output text 2>&1) \
    || { echo "  (describe not ready yet, retrying)"; continue; }
  state=$(echo "$info" | cut -f1); DNS=$(echo "$info" | cut -f2); SG=$(echo "$info" | cut -f3); reason=$(echo "$info" | cut -f4)
  echo "  $(date +%H:%M:%S)  state=$state  dns=$DNS"
  case $state in TERMINATED*) echo "Cluster terminated before becoming ready: $reason"; exit 1;; WAITING) break;; esac
done
echo "Cluster ready. DNS: $DNS  Master SG: $SG"

STAMP=$(date +%Y%m%d-%H%M%S)
aws emr describe-cluster --cluster-id "$CID" --region "$REGION" --output json > "evidence/cluster_${CID}_${STAMP}.json" 2>/dev/null
echo "  Saved evidence/cluster_${CID}_${STAMP}.json"

CIDR="$(curl -s https://api.ipify.org)/32"
echo "Opening SSH from $CIDR on $SG..."
auth=$(aws ec2 authorize-security-group-ingress --group-id "$SG" \
  --ip-permissions "IpProtocol=tcp,FromPort=22,ToPort=22,IpRanges=[{CidrIp=$CIDR}]" --region "$REGION" 2>&1)
if [ $? -ne 0 ] && ! echo "$auth" | grep -q "InvalidPermission.Duplicate"; then echo "  Could not add SSH rule: $auth"; else echo "  SSH allowed from $CIDR."; fi

echo
echo "JupyterHub: https://localhost:9443 (jovyan / jupyter; accept the self-signed cert)"
echo "YARN UI:    http://localhost:8088"
echo "Teardown:   ./project-down.sh"
SSH="ssh -i $PEM -o ServerAliveInterval=60 -o StrictHostKeyChecking=accept-new -L 9443:localhost:9443 -L 8088:localhost:8088 hadoop@$DNS"
if [ $NOCONNECT -eq 1 ]; then echo "Connect with:"; echo "  $SSH"; exit 0; fi
exec $SSH
