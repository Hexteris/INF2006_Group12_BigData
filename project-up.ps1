<#
    project-up.ps1  -  Launch the INF2006 Group 12 REFERENCE cluster, open SSH from
                       your current IP, save the cluster description as evidence,
                       and connect with tunnels for JupyterHub and the YARN UI.

    Reference config (project spec, "Cloud resource constraints"):
      emr-7.10.0; 1 primary + 2 core m5.xlarge; no task nodes, no autoscaling
      one 100 GiB gp3 volume per core node, 3000 IOPS, 125 MiB/s   (config/instance-groups.json)
      DEVIATION: spec says 200 GiB; Learner Lab policy denies EBS volumes > ~100 GiB (tested 2026-10-28)
      dfs.replication=2; Spark 2 executors x 2 cores x 6g, 2g overhead,
      2g driver, dynamic allocation off                              (config/emr-configurations.json)
      Apps: Hadoop, Hive, Spark, Livy, JupyterHub (JupyterHub's PySpark kernel runs through Livy)

    Usage:
        powershell -ExecutionPolicy Bypass -File .\project-up.ps1 -Bucket inf2006-g12-<studentID>
        ... -Bucket inf2006-g12-<studentID> -Release emr-7.9.0      # fallback release (record as deviation)
        ... -ClusterId (Get-Content .project-cluster-id)            # reconnect / re-open SSH after IP change

    Key: put the Learner Lab key (AWS Details -> Download PEM) in this folder as labsuser.pem
    (or vockey.pem). It is found automatically; -PemFile overrides. Key pair name: vockey.

    -Bucket enables: EMR logs -> s3://<bucket>/emr-logs/ and JupyterHub notebook
    persistence -> s3://<bucket>/jupyter/ (notebooks survive cluster termination).
    The bucket is created if it does not exist.

    After login:  bash ~/<repo>/scripts/cluster/verify_cluster.sh      (writes evidence)
    JupyterHub:   https://localhost:9443   (default user jovyan / password jupyter)
    YARN UI:      http://localhost:8088
    Teardown:     .\project-down.ps1
#>

param(
    [string]$ClusterId,
    [string]$Name        = "G12-Reference",
    [string]$Bucket,
    [string]$Release     = "emr-7.10.0",
    [int]   $IdleTimeout = 3600,
    [string]$KeyName     = "vockey",
    [string]$PemFile     = "",
    [string]$Region      = "us-east-1"
)

# "Continue", not "Stop": in Windows PowerShell 5.1, "Stop" turns any stderr output from
# aws.exe (even with 2>$null) into a terminating error. Every aws call below checks
# $LASTEXITCODE instead, and Get-MyIp still throws on failure.
$ErrorActionPreference = "Continue"
Set-Location -Path $PSScriptRoot
New-Item -ItemType Directory -Force -Path "evidence" | Out-Null

# --- key file: -PemFile, else labsuser.pem, else vockey.pem in this folder ---
if (-not $PemFile) {
    foreach ($cand in @("labsuser.pem", "vockey.pem")) {
        if (Test-Path (Join-Path $PSScriptRoot $cand)) { $PemFile = $cand; break }
    }
}
if (-not $PemFile -or -not (Test-Path $PemFile)) {
    Write-Host "Key file not found. In Learner Lab: AWS Details -> Download PEM, save it in" -ForegroundColor Red
    Write-Host "  $PSScriptRoot  as labsuser.pem, then run:" -ForegroundColor Red
    Write-Host "  icacls labsuser.pem /inheritance:r /grant:r `"`${env:USERNAME}:(R)`"" -ForegroundColor Red
    exit 1
}
Write-Host "Using key file $PemFile (key pair '$KeyName')" -ForegroundColor DarkGray

function Get-MyIp {
    try { return (Invoke-RestMethod -Uri "https://api.ipify.org").Trim() }
    catch { throw "Could not determine your public IP (no internet?)." }
}

# --- 0. credentials ---
Write-Host "Checking AWS credentials..." -ForegroundColor Cyan
$who = aws sts get-caller-identity --region $Region --output text 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "AWS credentials are not valid/expired. Paste 'AWS CLI: Show' into $env:USERPROFILE\.aws\credentials" -ForegroundColor Red
    exit 1
}
Write-Host "  OK: $who" -ForegroundColor Green

# --- 1. launch ---
if (-not $ClusterId) {
    if (-not $Bucket) { Write-Host "-Bucket is required when launching (logs + notebook persistence)." -ForegroundColor Red; exit 1 }

    aws s3api head-bucket --bucket $Bucket --region $Region 2>$null
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Creating bucket s3://$Bucket ..." -ForegroundColor Cyan
        aws s3 mb "s3://$Bucket" --region $Region
        if ($LASTEXITCODE -ne 0) { Write-Host "Could not create bucket." -ForegroundColor Red; exit 1 }
    }

    # Merge the committed config with the bucket-specific JupyterHub persistence setting.
    $cfg = Get-Content "config\emr-configurations.json" -Raw | ConvertFrom-Json
    $cfg = @($cfg) + [pscustomobject]@{
        Classification = "jupyter-s3-conf"
        Properties     = [pscustomobject]@{
            "s3.persistence.enabled" = "true"
            "s3.persistence.bucket"  = $Bucket
        }
    }
    $cfgFile = Join-Path $env:TEMP "g12-emr-configurations.json"
    [IO.File]::WriteAllText($cfgFile, (ConvertTo-Json -InputObject $cfg -Depth 10))   # UTF-8, no BOM

    $apps = @("Name=Hadoop", "Name=Hive", "Name=Spark", "Name=Livy", "Name=JupyterHub")
    Write-Host "Launching '$Name' ($Release; 1 primary + 2 core m5.xlarge; 100 GiB gp3 per core)..." -ForegroundColor Cyan
    $ClusterId = aws emr create-cluster `
        --name $Name `
        --release-label $Release `
        --applications $apps `
        --instance-groups "file://config/instance-groups.json" `
        --configurations "file://$cfgFile" `
        --ec2-attributes KeyName=$KeyName `
        --use-default-roles `
        --log-uri "s3://$Bucket/emr-logs/" `
        --auto-termination-policy IdleTimeout=$IdleTimeout `
        --tags "project=INF2006-G12" `
        --region $Region `
        --query "ClusterId" --output text
    if ($LASTEXITCODE -ne 0) {
        Write-Host "create-cluster failed. If the release or JupyterHub is rejected, retry with -Release <newest emr-7.x> and record the deviation." -ForegroundColor Red
        exit 1
    }
    Write-Host "  Cluster launched: $ClusterId" -ForegroundColor Green
}
$ClusterId | Out-File -FilePath ".project-cluster-id" -Encoding ascii

# --- 2. wait for WAITING ---
Write-Host "Waiting for WAITING (~10-15 min)..." -ForegroundColor Cyan
$state = $null
do {
    Start-Sleep -Seconds 30
    $info = aws emr describe-cluster --cluster-id $ClusterId --region $Region `
        --query "Cluster.[Status.State,MasterPublicDnsName,Ec2InstanceAttributes.EmrManagedMasterSecurityGroup,Status.StateChangeReason.Message]" `
        --output text 2>&1
    if ($LASTEXITCODE -ne 0) { Write-Host "  (describe not ready yet, retrying)"; continue }
    $parts = $info -split "\t"
    $state = $parts[0]; $dns = $parts[1]; $masterSg = $parts[2]
    Write-Host ("  {0}  state={1}  dns={2}" -f (Get-Date -Format HH:mm:ss), $state, $dns)
    if ($state -like "TERMINATED*") {
        Write-Host "Cluster terminated before becoming ready: $($parts[3])" -ForegroundColor Red; exit 1
    }
} while ($state -ne "WAITING")
Write-Host "Cluster ready. DNS: $dns  Master SG: $masterSg" -ForegroundColor Green

# --- 3. evidence: the cluster's own description of its config ---
$stamp = Get-Date -Format "yyyyMMdd-HHmmss"
aws emr describe-cluster --cluster-id $ClusterId --region $Region --output json 2>$null | Out-File -Encoding ascii "evidence\cluster_${ClusterId}_$stamp.json"
Write-Host "  Saved evidence\cluster_${ClusterId}_$stamp.json" -ForegroundColor Green

# --- 4. SSH from current IP ---
$cidr = "$(Get-MyIp)/32"
Write-Host "Opening SSH from $cidr on $masterSg..." -ForegroundColor Cyan
$auth = aws ec2 authorize-security-group-ingress --group-id $masterSg `
    --ip-permissions "IpProtocol=tcp,FromPort=22,ToPort=22,IpRanges=[{CidrIp=$cidr}]" `
    --region $Region 2>&1
if ($LASTEXITCODE -ne 0 -and $auth -notmatch "InvalidPermission.Duplicate") {
    Write-Host "  Could not add SSH rule: $auth" -ForegroundColor Yellow
} else {
    Write-Host "  SSH allowed from $cidr." -ForegroundColor Green
}

# --- 5. connect ---
Write-Host ""
Write-Host "JupyterHub: https://localhost:9443 (jovyan / jupyter; accept the self-signed cert)" -ForegroundColor Cyan
Write-Host "YARN UI:    http://localhost:8088" -ForegroundColor Cyan
Write-Host "Teardown:   .\project-down.ps1" -ForegroundColor DarkGray
Write-Host ""
ssh -i $PemFile -o ServerAliveInterval=60 -L 9443:localhost:9443 -L 8088:localhost:8088 "hadoop@$dns"
