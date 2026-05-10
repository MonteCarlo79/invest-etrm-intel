# setup_schedule.ps1
# ---------------------------------------------------------------------------
# One-time setup: registers a Windows Task Scheduler task that runs the
# LingFeng daily collection every day at 08:00.
#
# Run this script once from PowerShell (as the same user who will run it):
#   .\services\lingfeng\setup_schedule.ps1
#
# Prerequisites — run these first if not done:
#   pip install playwright
#   playwright install chromium
# ---------------------------------------------------------------------------

param(
    [string]$RepoRoot    = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path,
    [string]$TaskName    = "BESS-LingFeng-DailyCollection",
    [string]$RunAt       = "08:00",            # daily trigger time
    [string]$PythonExe   = (Get-Command python -ErrorAction SilentlyContinue).Source
)

if (-not $PythonExe) {
    Write-Error "python not found on PATH. Install Python and try again."
    exit 1
}

$Script    = Join-Path $RepoRoot "services\lingfeng\run_daily.py"
$EnvFile   = Join-Path $RepoRoot "config\.env"
$LogFile   = Join-Path $RepoRoot "logs\lingfeng_daily.log"

# Create logs dir if needed
$LogDir = Split-Path $LogFile
if (-not (Test-Path $LogDir)) { New-Item -ItemType Directory -Path $LogDir | Out-Null }

# Build the command.  We wrap in cmd /c so stdout goes to the log file.
$Action = New-ScheduledTaskAction `
    -Execute  "cmd.exe" `
    -Argument "/c `"$PythonExe`" `"$Script`" >> `"$LogFile`" 2>&1" `
    -WorkingDirectory $RepoRoot

$Trigger = New-ScheduledTaskTrigger -Daily -At $RunAt

$Settings = New-ScheduledTaskSettingsSet `
    -ExecutionTimeLimit (New-TimeSpan -Hours 1) `
    -StartWhenAvailable `
    -RunOnlyIfNetworkAvailable

# Register (or update) the task
$existing = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($existing) {
    Set-ScheduledTask -TaskName $TaskName -Action $Action -Trigger $Trigger -Settings $Settings | Out-Null
    Write-Host "Updated existing task: $TaskName"
} else {
    Register-ScheduledTask `
        -TaskName $TaskName `
        -Action   $Action `
        -Trigger  $Trigger `
        -Settings $Settings `
        -RunLevel Highest | Out-Null
    Write-Host "Registered new task: $TaskName"
}

Write-Host ""
Write-Host "Task: $TaskName"
Write-Host "Runs: daily at $RunAt"
Write-Host "Log:  $LogFile"
Write-Host ""
Write-Host "IMPORTANT — set credentials in system environment variables before first run:"
Write-Host '  [System.Environment]::SetEnvironmentVariable("LINGFENG_USERNAME", "your_user", "User")'
Write-Host '  [System.Environment]::SetEnvironmentVariable("LINGFENG_PASSWORD", "your_pass", "User")'
Write-Host ""
Write-Host "To test immediately:"
Write-Host "  Start-ScheduledTask -TaskName '$TaskName'"
Write-Host "  Get-Content '$LogFile' -Wait"
