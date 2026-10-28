<#
    lab-down.ps1  -  Terminate the EMR cluster created by lab-up.ps1 so you stop
                     being billed.

    Usage:
        .\lab-down.ps1                    # terminate the cluster recorded in .lab-cluster-id
        .\lab-down.ps1 -ClusterId j-XXXX  # terminate a specific cluster
#>

param(
    [string]$ClusterId,
    [string]$Region = "us-east-1"
)

$ErrorActionPreference = "Stop"
Set-Location -Path $PSScriptRoot

if (-not $ClusterId) {
    if (Test-Path ".lab-cluster-id") {
        $ClusterId = (Get-Content ".lab-cluster-id" -Raw).Trim()
    } else {
        Write-Host "No cluster id given and no .lab-cluster-id file found." -ForegroundColor Red
        Write-Host "Run: .\lab-down.ps1 -ClusterId j-XXXXXXXX" -ForegroundColor Yellow
        exit 1
    }
}

Write-Host "Terminating cluster $ClusterId ..." -ForegroundColor Cyan
$out = aws emr terminate-clusters --cluster-ids $ClusterId --region $Region 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "terminate-clusters failed: $out" -ForegroundColor Red
    Write-Host "Terminate it manually from the EMR console if this persists." -ForegroundColor Yellow
    exit 1
}

Write-Host "Termination requested. It will take a few minutes to fully shut down." -ForegroundColor Green
if (Test-Path ".lab-cluster-id") { Remove-Item ".lab-cluster-id" -Force }
