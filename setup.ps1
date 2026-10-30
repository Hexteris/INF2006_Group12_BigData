<#
    setup.ps1  -  One-command laptop setup for the INF2006 Group 12 bronze corpus.

    1. Checks Python, the downloaded Drive folder and free disk space.
    2. Verifies all 1,046 files (size + sha256) against evidence\expected_corpus.csv and arranges
       them into raw\<source>\<version>\acq_<date>\ (moves by default; -Copy keeps the originals).
    3. Checks AWS credentials, creates s3://inf2006-g12-<StudentId> if needed, uploads raw\ to
       s3://inf2006-g12-<StudentId>/raw/ and checks the object count (expect 1,047).

    Usage (from this folder):
        powershell -ExecutionPolicy Bypass -File .\setup.ps1 -From C:\Dev\drive_dl -StudentId 2501234
        powershell -ExecutionPolicy Bypass -File .\setup.ps1 -From C:\Dev\drive_dl -DryRun   # verify only

    Safe to rerun: verified files are skipped and the S3 upload resumes.
#>
param(
    [Parameter(Mandatory = $true)][string]$From,
    [string]$StudentId,
    [string]$To = "raw",
    [switch]$DryRun,
    [switch]$Copy,
    [string]$Region = "us-east-1"
)

$ErrorActionPreference = "Continue"   # aws.exe writes to stderr; check $LASTEXITCODE instead
Set-Location -Path $PSScriptRoot
$Expected = 1046

function Pass($msg) { Write-Host "  PASS  $msg" -ForegroundColor Green }
function Fail($msg, $fix) {
    Write-Host "  FAIL  $msg" -ForegroundColor Red
    if ($fix) { Write-Host "        Fix: $fix" -ForegroundColor Yellow }
    exit 1
}

Write-Host "INF2006 G12 corpus setup" -ForegroundColor Cyan
if (-not $DryRun -and -not $StudentId) {
    Fail "-StudentId is missing" "add -StudentId <your student ID> (or -DryRun to only verify)"
}

# --- 1. Python (skip the Microsoft Store stub, which prints nothing useful) ---
$py = $null
foreach ($cand in @(@("py", "-3"), @("python"), @("python3"))) {
    $exe = $cand[0]; $args0 = @($cand | Select-Object -Skip 1)
    if (-not (Get-Command $exe -ErrorAction SilentlyContinue)) { continue }
    $ver = & $exe @args0 --version 2>&1 | Out-String
    if ($LASTEXITCODE -eq 0 -and $ver -match "Python 3\.(\d+)" -and [int]$Matches[1] -ge 8) {
        $py = $cand; break
    }
}
if (-not $py) { Fail "Python 3.8+ not found" "install Python 3 from python.org (tick 'Add python.exe to PATH'), open a new PowerShell" }
$pyExe = $py[0]; $pyArgs = @($py | Select-Object -Skip 1)
Pass ("Python: " + (& $pyExe @pyArgs --version 2>&1))

# --- 2. download folder and disk space ---
if (-not (Test-Path -LiteralPath $From -PathType Container)) {
    Fail "folder not found: $From" "download the Drive folder, extract ALL zip parts into one folder, pass it as -From"
}
$From = (Resolve-Path -LiteralPath $From).Path
Pass "download folder: $From"

New-Item -ItemType Directory -Force -Path $To | Out-Null
$target = (Resolve-Path -LiteralPath $To).Path
$needGB = 14
if ($Copy -or ([IO.Path]::GetPathRoot($From) -ne [IO.Path]::GetPathRoot($target))) { $needGB = 27 }
$freeGB = [math]::Floor((New-Object IO.DriveInfo ([IO.Path]::GetPathRoot($target))).AvailableFreeSpace / 1GB)
if ($freeGB -lt $needGB) { Fail "only $freeGB GB free on $([IO.Path]::GetPathRoot($target)) (need about $needGB GB)" "free up space, or use -To on another drive" }
Pass "disk space: $freeGB GB free"

# --- 3. verify + arrange ---
Write-Host "Verifying $Expected files (sha256; a few minutes)..." -ForegroundColor Cyan
$sc = @("scripts\stage\setup_corpus.py", "--from", $From, "--to", $target)
if (-not $Copy) { $sc += "--move" }
& $pyExe @pyArgs @sc
if ($LASTEXITCODE -ne 0) {
    Fail "some files are missing or corrupt (listed as PROBLEM above)" "re-download the Drive zip part that holds those files, extract it into $From, rerun this command"
}
Pass "corpus verified: OK $Expected/$Expected in $target"

if ($DryRun) {
    Write-Host "Dry run: nothing uploaded. Rerun without -DryRun and with -StudentId to upload." -ForegroundColor Cyan
    exit 0
}

# --- 4. AWS ---
if (-not (Get-Command aws -ErrorAction SilentlyContinue)) {
    Fail "AWS CLI not found" "install AWS CLI v2 (https://aws.amazon.com/cli/), open a new PowerShell"
}
$who = aws sts get-caller-identity --output text 2>&1
if ($LASTEXITCODE -ne 0) {
    Fail "AWS credentials missing or expired" "Learner Lab -> Start Lab -> AWS Details -> AWS CLI: Show; paste into $env:USERPROFILE\.aws\credentials"
}
Pass "AWS credentials valid"

$Bucket = "inf2006-g12-$StudentId"
aws s3api head-bucket --bucket $Bucket 2>$null | Out-Null
if ($LASTEXITCODE -ne 0) {
    aws s3 mb "s3://$Bucket" --region $Region | Out-Null
    if ($LASTEXITCODE -ne 0) { Fail "could not create s3://$Bucket" "bucket names are global; check the student ID, or the name may be taken" }
    Pass "created s3://$Bucket"
} else { Pass "bucket s3://$Bucket exists" }

Write-Host "Uploading to s3://$Bucket/raw/ (13 GB; resumes if interrupted)..." -ForegroundColor Cyan
aws s3 sync $target "s3://$Bucket/raw/" --exclude "*.part" --only-show-errors
if ($LASTEXITCODE -ne 0) {
    Fail "upload stopped" "credentials expire with the lab session: paste fresh ones and rerun the same command"
}
$summary = aws s3 ls "s3://$Bucket/raw/" --recursive --summarize | Select-String "Total Objects"
$count = [int](($summary -replace "[^\d]", ""))
if ($count -lt $Expected + 1) { Fail "S3 has $count objects, expected $($Expected + 1)" "rerun this command" }
Pass "S3 holds $count objects ($Expected data files + setup_report.txt)"

Write-Host ""
Write-Host "DONE. Next: launch the cluster (README step 4):" -ForegroundColor Green
Write-Host "  powershell -ExecutionPolicy Bypass -File .\project-up.ps1 -Bucket $Bucket" -ForegroundColor Green
