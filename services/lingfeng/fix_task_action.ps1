# fix_task_action.ps1
# Re-registers the LingFeng scheduled task to call run_daily.bat.
# Self-elevates via UAC if not already running as admin.
# Run: .\services\lingfeng\fix_task_action.ps1

$isAdmin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)

if (-not $isAdmin) {
    Write-Host "Requesting elevation ..."
    $args2 = '-NonInteractive -ExecutionPolicy Bypass -File "' + $PSCommandPath + '"'
    Start-Process powershell -Verb RunAs -Wait -ArgumentList $args2
    exit
}

$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$BatFile  = Join-Path $RepoRoot "services\lingfeng\run_daily.bat"
$LogDir   = Join-Path $RepoRoot "logs"
if (-not (Test-Path $LogDir)) { New-Item -ItemType Directory -Path $LogDir | Out-Null }

$CmdArg   = '/c "' + $BatFile + '"'

$Action = New-ScheduledTaskAction -Execute "cmd.exe" -Argument $CmdArg -WorkingDirectory $RepoRoot
$Trigger  = New-ScheduledTaskTrigger -Daily -At "04:00"
$Settings = New-ScheduledTaskSettingsSet -ExecutionTimeLimit (New-TimeSpan -Hours 2) -StartWhenAvailable -RunOnlyIfNetworkAvailable

Unregister-ScheduledTask -TaskName "BESS-LingFeng-DailyCollection" -Confirm:$false -ErrorAction SilentlyContinue
Register-ScheduledTask -TaskName "BESS-LingFeng-DailyCollection" -Action $Action -Trigger $Trigger -Settings $Settings -RunLevel Highest | Out-Null

Write-Host ""
Write-Host "Task re-registered."
Write-Host "  Action  : cmd.exe $CmdArg"
Write-Host "  Trigger : daily at 04:00"
Write-Host ""
(Get-ScheduledTask -TaskName "BESS-LingFeng-DailyCollection").Actions | Format-List Execute, Arguments
pause
