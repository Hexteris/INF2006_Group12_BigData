<#
    project-down.ps1  -  Terminate the project EMR cluster so it stops using lab credit.

    Usage:
        .\project-down.ps1                    # terminate the cluster recorded in .project-cluster-id
        .\project-down.ps1 -ClusterId j-XXXX  # terminate a specific cluster

    Copy outputs to S3 first: HDFS and the primary node's disks are deleted with the cluster.
#>
param(
    [string]$ClusterId,
    [string]$Region = "us-east-1"
)
# "Continue": in Windows PowerShell 5.1, "Stop" turns aws.exe stderr into a terminating error.
$ErrorActionPreference = "Continue"
Set-Location -Path $PSScriptRoot

if (-not $ClusterId) {
    if (Test-Path ".project-cluster-id") {
        $ClusterId = (Get-Content ".project-cluster-id" -Raw).Trim()
    } else {
        Write-Host "No -ClusterId given and no .project-cluster-id file found." -ForegroundColor Red
        exit 1
    }
}

Write-Host "Terminating cluster $ClusterId ..." -ForegroundColor Cyan
$out = aws emr terminate-clusters --cluster-ids $ClusterId --region $Region 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "terminate-clusters failed: $out" -ForegroundColor Red
    Write-Host "Terminate it from the EMR console instead." -ForegroundColor Yellow
    exit 1
}
$state = aws emr describe-cluster --cluster-id $ClusterId --region $Region --query Cluster.Status.State --output text 2>$null
Write-Host "Termination requested; state now: $state. Then click End Lab in AWS Academy." -ForegroundColor Green
Remove-Item ".project-cluster-id" -Force -ErrorAction SilentlyContinue
