<#
    lab-up.ps1  -  Launch an EMR (Spark + Zeppelin) cluster for the INF2006 labs,
                   open SSH from your CURRENT public IP, and connect with the
                   Zeppelin tunnel (localhost:8890).

    Usage:
        .\lab-up.ps1                                   # launch default (MyPipelineCluster: Spark+Zeppelin)
        .\lab-up.ps1 -Name MyHiveCluster -Apps hive    # launch a Hive (Core Hadoop) cluster
        .\lab-up.ps1 -ClusterId j-XXXX                 # reuse an existing cluster, just reconnect

    -Apps values:
        spark   -> Hadoop + Spark + Zeppelin + Livy   (Labs 8 and 9)
        hive    -> Hadoop + Hive                       (Lab 10)

    Prereqs:
        - AWS CLI authenticated with your current AWS Academy lab credentials
          (paste "AWS CLI: Show" block into %USERPROFILE%\.aws\credentials each session)
        - vockey2.pem present in this folder

    The cluster id is saved to .lab-cluster-id so lab-down.ps1 can tear it down.
#>

param(
    [string]$ClusterId,
    [string]$Name     = "MyPipelineCluster",
    [ValidateSet("spark","hive")]
    [string]$Apps     = "spark",
    [string]$KeyName  = "vockey2",
    [string]$PemFile  = "vockey2.pem",
    [string]$Region   = "us-east-1"
)

$ErrorActionPreference = "Stop"
Set-Location -Path $PSScriptRoot

function Get-MyIp {
    try { return (Invoke-RestMethod -Uri "https://api.ipify.org").Trim() }
    catch { throw "Could not determine your public IP (no internet?)." }
}

# --- 0. sanity: credentials valid? ---
Write-Host "Checking AWS credentials..." -ForegroundColor Cyan
$who = aws sts get-caller-identity --output text 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "AWS credentials are not valid/expired." -ForegroundColor Red
    Write-Host "Open the AWS Academy lab -> AWS Details -> AWS CLI: Show, and paste the block into:" -ForegroundColor Yellow
    Write-Host "  $env:USERPROFILE\.aws\credentials" -ForegroundColor Yellow
    exit 1
}
Write-Host "  OK: $who" -ForegroundColor Green

# --- 1. launch (unless reusing an existing cluster) ---
if (-not $ClusterId) {
    if ($Apps -eq "hive") {
        $appList = @("Name=Hadoop","Name=Hive")
    } else {
        $appList = @("Name=Hadoop","Name=Spark","Name=Zeppelin","Name=Livy")
    }
    Write-Host "Launching new EMR cluster '$Name' (apps: $Apps)..." -ForegroundColor Cyan
    $ClusterId = aws emr create-cluster `
        --name $Name `
        --release-label emr-7.10.0 `
        --applications $appList `
        --instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m5.xlarge InstanceGroupType=CORE,InstanceCount=2,InstanceType=m5.xlarge `
        --ec2-attributes KeyName=$KeyName `
        --use-default-roles `
        --region $Region `
        --auto-termination-policy IdleTimeout=3600 `
        --query "ClusterId" --output text
    if ($LASTEXITCODE -ne 0) { Write-Host "create-cluster failed." -ForegroundColor Red; exit 1 }
    Write-Host "  Cluster launched: $ClusterId" -ForegroundColor Green
}
$ClusterId | Out-File -FilePath ".lab-cluster-id" -Encoding ascii

# --- 2. wait for WAITING, capture DNS + master security group ---
Write-Host "Waiting for cluster to reach WAITING (this takes ~7-15 min)..." -ForegroundColor Cyan
$dns = $null; $masterSg = $null; $state = $null
do {
    Start-Sleep -Seconds 30
    $info = aws emr describe-cluster --cluster-id $ClusterId --region $Region `
        --query "Cluster.[Status.State,MasterPublicDnsName,Ec2InstanceAttributes.EmrManagedMasterSecurityGroup]" `
        --output text 2>&1
    if ($LASTEXITCODE -ne 0) { Write-Host "  (describe not ready yet, retrying)"; continue }
    $parts = $info -split "\s+"
    $state = $parts[0]; $dns = $parts[1]; $masterSg = $parts[2]
    Write-Host ("  {0}  state={1}  dns={2}" -f (Get-Date -Format HH:mm:ss), $state, $dns)
    if ($state -eq "TERMINATED" -or $state -eq "TERMINATED_WITH_ERRORS") {
        Write-Host "Cluster terminated before becoming ready." -ForegroundColor Red; exit 1
    }
} while ($state -ne "WAITING")

Write-Host "Cluster ready." -ForegroundColor Green
Write-Host "  DNS:        $dns"
Write-Host "  Master SG:  $masterSg"

# --- 3. open SSH (port 22) from current IP (idempotent) ---
$myIp = Get-MyIp
$cidr = "$myIp/32"
Write-Host "Opening SSH from your current IP ($cidr) on $masterSg..." -ForegroundColor Cyan
$auth = aws ec2 authorize-security-group-ingress `
    --group-id $masterSg `
    --ip-permissions "IpProtocol=tcp,FromPort=22,ToPort=22,IpRanges=[{CidrIp=$cidr}]" `
    --region $Region 2>&1
if ($LASTEXITCODE -ne 0) {
    if ($auth -match "InvalidPermission.Duplicate") {
        Write-Host "  Rule already exists for $cidr - good." -ForegroundColor Green
    } else {
        Write-Host "  Could not add SSH rule: $auth" -ForegroundColor Yellow
        Write-Host "  Add it manually in EC2 > Security Groups > $masterSg (SSH, 22, Source $cidr)." -ForegroundColor Yellow
    }
} else {
    Write-Host "  SSH rule added for $cidr." -ForegroundColor Green
}

# --- 4. connect with Zeppelin tunnel ---
Write-Host ""
Write-Host "Connecting via SSH with Zeppelin tunnel (localhost:8890)..." -ForegroundColor Cyan
Write-Host "Once logged in, open http://localhost:8890 in your browser for Zeppelin." -ForegroundColor Cyan
Write-Host "When done, run: .\lab-down.ps1" -ForegroundColor DarkGray
Write-Host ""
ssh -i $PemFile -L 8890:localhost:8890 "hadoop@$dns"
