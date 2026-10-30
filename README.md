# INF2006 Group 12: team setup

This branch gets **your own copy of the bronze corpus** (Task 1 raw data, 1,046 files, 13.08 GB)
from Daniel's Google Drive into your S3 bucket and your EMR cluster's HDFS. Project code is on `main`.

Five steps. Each one shows what you should see. If something fails, see `docs/team_corpus_setup.md`.

## 1. Download the data from Drive (once)

1. Open the `INF2006_bigdata_project` link Daniel shared (Drive → **Shared with me**).
2. Right-click `INF2006_bigdata_project` → **Download**. Drive makes several zips of about 2 GB each.
3. Extract **all** of the zips into one folder: `C:\Dev\drive_dl`.
   Do not unzip the GDELT `.zip` or Wikinews `.bz2` files inside them.

## 2. Install tools and get this branch (once)

Install [Git](https://git-scm.com/download/win), [Python 3](https://www.python.org/downloads/)
(tick *Add python.exe to PATH*) and [AWS CLI v2](https://aws.amazon.com/cli/). Then in PowerShell:

```powershell
git clone -b team-setup https://github.com/Hexteris/INF2006_Group12_BigData.git C:\Dev\g12-setup
cd C:\Dev\g12-setup
```

## 3. Verify the data and upload it to S3 (once)

1. Start Learner Lab. Copy **AWS Details → AWS CLI: Show** into `%USERPROFILE%\.aws\credentials`.
2. Run (use your own student ID):
   ```powershell
   powershell -ExecutionPolicy Bypass -File .\setup.ps1 -From C:\Dev\drive_dl -StudentId <studentID>
   ```
   Expect `OK 1046/1046`, then `S3 holds 1047 objects`, then `DONE`.
   The upload is 13 GB. If the lab session ends, paste fresh credentials and run the same command again.

## 4. Launch the cluster (every lab session)

1. AWS Details → **Download PEM**. Save it in `C:\Dev\g12-setup` as `labsuser.pem`, then:
   ```powershell
   icacls labsuser.pem /inheritance:r /grant:r "${env:USERNAME}:(R)"
   ```
2. Launch (about 15 minutes; it opens an SSH session when ready):
   ```powershell
   powershell -ExecutionPolicy Bypass -File .\project-up.ps1 -Bucket inf2006-g12-<studentID>
   ```

## 5. Stage the corpus into HDFS (every lab session)

In the SSH session on the primary node:

```bash
sudo yum install -y git tmux
git clone -b team-setup https://github.com/Hexteris/INF2006_Group12_BigData.git ~/g12 && cd ~/g12
tmux new -s g12
bash scripts/stage/cluster_setup.sh inf2006-g12-<studentID>
```

Expect `OK 1046/1046`, `mismatches=0` for each of the 5 sources, then `ALL SOURCES STAGED AND VERIFIED`
and `PASS`. Takes about 5–10 minutes.

## Finish

Run `powershell -ExecutionPolicy Bypass -File .\project-down.ps1`, then **End Lab**. HDFS is deleted
with the cluster, but S3 keeps the data: next session, repeat only steps 4 and 5.

Never commit data, keys or credentials (`.gitignore` covers `raw/`, `*.pem`, `.aws/`).
