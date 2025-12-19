# PowerShell Script for SQL Server Sink
# Usage: .\run-sync.ps1 -SourceServer "..." -SourceDb "..." -TargetServer "..." -TargetDb "..." [-TableName "dbo.Users"] [options]

param(
    [Parameter(Mandatory=$true, HelpMessage="Source database server")]
    [string]$SourceServer,

    [Parameter(Mandatory=$true, HelpMessage="Source database name")]
    [string]$SourceDb,

    [Parameter(Mandatory=$true, HelpMessage="Target database server")]
    [string]$TargetServer,

    [Parameter(Mandatory=$true, HelpMessage="Target database name")]
    [string]$TargetDb,

    [Parameter(Mandatory=$false, HelpMessage="Optional: Comma-separated list of tables/schemas to sync (e.g., 'dbo.Users, Sales'). Use 'all' or '*' for all tables.")]
    [string]$TableName = "",

    [Parameter(Mandatory=$false, HelpMessage="Optional: Batch size for processing (default: 100000)")]
    [int]$BatchSize = 100000,

    [Parameter(Mandatory=$false, HelpMessage="Optional: Number of parallel threads (default: 1)")]
    [int]$Threads = 1,

    [Parameter(Mandatory=$false, HelpMessage="Optional: Allow tables without primary keys")]
    [switch]$AllowNoPk,

    [Parameter(Mandatory=$false, HelpMessage="Optional: Use deep comparison (all columns) for no-PK tables")]
    [switch]$DeepCompare,

    [Parameter(Mandatory=$false, HelpMessage="Optional: Clear target and bulk insert")]
    [switch]$ClearTarget,

    [Parameter(Mandatory=$false, HelpMessage="Optional: Output directory for results (default: results)")]
    [string]$OutputDir = "results"
)

Write-Host "SQL Server Sink - Database Record Sync Tool" -ForegroundColor Cyan
Write-Host "===========================================" -ForegroundColor Cyan
Write-Host ""

# Build the arguments array
$args = @($SourceServer, $SourceDb, $TargetServer, $TargetDb)

if ($TableName) {
    $args += $TableName
}

# Add options
if ($BatchSize -ne 100000) {
    $args += "--batch-size", $BatchSize.ToString()
}

if ($Threads -ne 1) {
    $args += "--threads", $Threads.ToString()
}

if ($AllowNoPk) {
    $args += "--allow-no-pk"
}

if ($DeepCompare) {
    $args += "--deep-compare"
}

if ($ClearTarget) {
    $args += "--clear-target"
}

if ($OutputDir -ne "results") {
    $args += "--output-dir", $OutputDir
}

# Run the application
Write-Host "Starting sync..." -ForegroundColor Yellow
Write-Host ""

& dotnet run -- $args

if ($LASTEXITCODE -eq 0) {
    Write-Host ""
    Write-Host "Sync completed successfully!" -ForegroundColor Green
} else {
    Write-Host ""
    Write-Host "Sync failed with exit code: $LASTEXITCODE" -ForegroundColor Red
}
