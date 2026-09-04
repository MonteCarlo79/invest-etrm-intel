#Requires -RunAsAdministrator
<#
.SYNOPSIS
    Register a Windows Task Scheduler task that checks for LingFeng on-demand
    backfill triggers every 15 minutes.

    The task calls run_trigger.bat, which runs run_daily.py --check-trigger.
    If hermes_settings.lingfeng_trigger_run is empty the script exits in <1s.
    If a trigger is present, the full pipeline runs and notifies you on completion.

.USAGE
    Right-click PowerShell → "Run as Administrator", then:
        .\services\lingfeng\setup_trigger_schedule.ps1
#>

$TaskName   = "LingFeng-Trigger-Check"
$BatPath    = "C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform\services\lingfeng\run_trigger.bat"
$LogDir     = "C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform\logs"

# Ensure log directory exists
if (-not (Test-Path $LogDir)) { New-Item -ItemType Directory -Path $LogDir | Out-Null }

# Remove existing task if present
if (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) {
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
    Write-Host "Removed existing task: $TaskName"
}

$Action  = New-ScheduledTaskAction -Execute "cmd.exe" -Argument "/c `"$BatPath`""
$Trigger = New-ScheduledTaskTrigger -RepetitionInterval (New-TimeSpan -Minutes 15) -Once -At (Get-Date)
$Settings = New-ScheduledTaskSettingsSet `
    -ExecutionTimeLimit (New-TimeSpan -Hours 16) `
    -MultipleInstances IgnoreNew `
    -StartWhenAvailable

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action   $Action `
    -Trigger  $Trigger `
    -Settings $Settings `
    -RunLevel Highest `
    -Description "Checks hermes_settings.lingfeng_trigger_run every 15 min and runs LingFeng backfill if triggered via Feishu/Telegram." | Out-Null

Write-Host "✅ Task '$TaskName' registered — runs every 15 minutes."
Write-Host "   Trigger via Feishu/Telegram: 'lingfeng backfill' or '/lf_run 2026-01-01'"
