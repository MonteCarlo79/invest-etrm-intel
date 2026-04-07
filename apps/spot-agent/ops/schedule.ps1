# ops/schedule.ps1
# ──────────────────────────────────────────────────────────────────
# Spot market PDF ingestion runner for Windows.
#
# TWO MODES:
#
# A) One-shot (Windows Task Scheduler — recommended for daily jobs)
#    Register once:
#      $action  = New-ScheduledTaskAction -Execute "powershell.exe" `
#                   -Argument "-ExecutionPolicy Bypass -File `"$PSScriptRoot\schedule.ps1`""
#      $trigger = New-ScheduledTaskTrigger -Daily -At "09:05AM"
#      Register-ScheduledTask -TaskName "SpotIngest" -Action $action -Trigger $trigger `
#                             -RunLevel Highest -Force
#    Each run discovers all PDFs, skips already-processed ones, ingests new ones.
#
# B) Watch mode (persistent process — e.g. via NSSM Windows Service)
#    Set $WatchMode = $true below.  The process polls every $WatchIntervalSecs seconds.
#    To install as a Windows Service with NSSM:
#      nssm install SpotIngestWatch powershell.exe `
#        "-ExecutionPolicy Bypass -File `"$PSScriptRoot\schedule.ps1`""
#      nssm set SpotIngestWatch AppParameters "-WatchMode $true"
#      nssm start SpotIngestWatch
#
# MANUAL EXECUTION:
#   powershell -ExecutionPolicy Bypass -File ops\schedule.ps1
# ──────────────────────────────────────────────────────────────────

param(
    [switch]$WatchMode,
    [int]$WatchIntervalSecs = 300,
    [switch]$Force,
    [switch]$DryRun,
    [switch]$NoLlm
)

$Root   = Split-Path $PSScriptRoot -Parent
$Python = Join-Path $Root ".venv\Scripts\python.exe"
$Script = Join-Path $Root "agent\spot_ingest.py"
$Header = Join-Path $Root "agent\spot_header_bess.yaml"

foreach ($path in @($Python, $Script, $Header)) {
    if (-not (Test-Path $path)) {
        Write-Error "Required path not found: $path"
        exit 1
    }
}

$ExtraArgs = @()
if ($Force)    { $ExtraArgs += "--force" }
if ($DryRun)   { $ExtraArgs += "--dry-run" }
if ($NoLlm)    { $ExtraArgs += "--no-llm" }

if ($WatchMode) {
    $ExtraArgs += "--watch"
    $ExtraArgs += "--interval"
    $ExtraArgs += "$WatchIntervalSecs"
    Write-Host "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Starting watch mode (interval=${WatchIntervalSecs}s) ..."
} else {
    Write-Host "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Starting one-shot ingestion ..."
}

& $Python $Script --header $Header @ExtraArgs

$ExitCode = $LASTEXITCODE
Write-Host "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Finished (exit code: $ExitCode)"
exit $ExitCode
