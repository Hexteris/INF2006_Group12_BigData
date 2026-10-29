# INF2006 Group 12: team setup

This branch only sets up your own copy of the project corpus: launch the reference EMR cluster, get the
data from the team Google Drive, verify it, and stage it into HDFS. Project code lives on `main`.

## 1. Laptop (once)

Install Git, Python 3, the AWS CLI and (recommended) Google Drive for desktop.

```powershell
git clone -b team-setup https://github.com/Hexteris/INF2006_Group12_BigData.git g12-setup
cd g12-setup
```

## 2. Get and verify the data

Follow **Route A** in `docs/team_corpus_setup.md` (Drive → laptop → your S3 bucket).
Expect `OK 1047/1047` and 1,048 objects in `s3://inf2006-g12-<studentID>/raw/`.

## 3. Launch the cluster (each Learner Lab session)

1. Start Learner Lab. Paste **AWS Details → AWS CLI: Show** into `%USERPROFILE%\.aws\credentials`.
2. Download the key from AWS Details as `labsuser.pem` into this folder, then:
   ```powershell
   icacls labsuser.pem /inheritance:r /grant:r "${env:USERNAME}:(R)"
   ```
3. Launch (about 15 min; it opens SSH when ready):
   ```powershell
   .\project-up.ps1 -Bucket inf2006-g12-<studentID> -KeyName vockey -PemFile labsuser.pem
   ```
   If SSH times out later (your IP changed):
   `.\project-up.ps1 -ClusterId (Get-Content .project-cluster-id) -KeyName vockey -PemFile labsuser.pem`

## 4. On the primary node

```bash
sudo yum install -y git tmux
git clone -b team-setup https://github.com/Hexteris/INF2006_Group12_BigData.git ~/g12 && cd ~/g12
tmux new -s g12
bash scripts/cluster/verify_cluster.sh                 # expect all PASS
export B=inf2006-g12-<studentID>
sudo mkdir -p /mnt/g12 && sudo chown hadoop:hadoop /mnt/g12
aws s3 sync s3://$B/raw/ /mnt/g12/raw/
bash scripts/stage/stage_all.sh /mnt/g12/raw            # expect ALL SOURCES STAGED AND VERIFIED
aws s3 cp --recursive /mnt/g12/staging s3://$B/evidence/staging/
```

Route B (Drive straight to the cluster) and a troubleshooting table are in `docs/team_corpus_setup.md`.

## 5. Finish

`.\project-down.ps1`, then **End Lab**. HDFS is deleted with the cluster; S3 keeps the data, so next
session repeat only steps 3 and 4.

Never commit data, keys or credentials (`.gitignore` covers `raw/`, `*.pem`, `.aws/`).
