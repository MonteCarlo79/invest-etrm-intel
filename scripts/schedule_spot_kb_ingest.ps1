<#
.SYNOPSIS
    Registers a Windows Task Scheduler job that runs the spot-market KB ingestion
    daily at 10:00 AM, even if the laptop was off at that time (run-if-missed).

.DESCRIPTION
    The task runs:
        py scripts/ingest_knowledge_bulk.py
            --dir "data/market-fundamentals"
            --exclude "..."
            --ext "pdf,pptx,ppt,docx,doc,txt,png,jpg,jpeg,webp"
            --workers 2
            --digest

    "Run as soon as possible after a scheduled start is missed" is enabled so
    that if the laptop is powered off at 10:00 AM, the job runs at next login.

.USAGE
    # Run once (Administrator is NOT required):
    .\scripts\schedule_spot_kb_ingest.ps1

    # To remove the task later:
    Unregister-ScheduledTask -TaskName "BESS Spot KB Ingest" -Confirm:$false
#>

$TaskName = "BESS Spot KB Ingest"

# $PSScriptRoot = bess-platform\scripts\  =>  parent = bess-platform\
$RepoRoot = Split-Path -Parent $PSScriptRoot

# Detect Python executable (prefer 'py' launcher, fall back to 'python')
if (Get-Command "py" -ErrorAction SilentlyContinue) {
    $PyExe = "py"
} else {
    $PyExe = "python"
}

# Store Chinese exclude pattern in a variable to keep the argument string clean
$Exclude = "各省现货价格及边界数据,节点电价数据"
$Ext     = "pdf,pptx,ppt,docx,doc,txt,png,jpg,jpeg,webp"

$ScriptArgs = "scripts/ingest_knowledge_bulk.py" +
              " --dir `"data/market-fundamentals`"" +
              " --exclude `"$Exclude`"" +
              " --ext `"$Ext`"" +
              " --workers 2" +
              " --digest"

# ── Action: run Python in the repo root ──────────────────────────────────────
$Action = New-ScheduledTaskAction `
    -Execute $PyExe `
    -Argument $ScriptArgs `
    -WorkingDirectory $RepoRoot

# ── Trigger: daily at 10:00 AM ───────────────────────────────────────────────
$Trigger = New-ScheduledTaskTrigger -Daily -At "10:00AM"

# ── Settings ─────────────────────────────────────────────────────────────────
# NOTE: backtick line-continuation requires backtick as the LAST character on
#       the line -- no inline comments after backticks.
$Settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -WakeToRun:$false `
    -ExecutionTimeLimit (New-TimeSpan -Hours 4) `
    -MultipleInstances IgnoreNew

# ── Principal: run as current user, only when logged on ─────────────────────
$Principal = New-ScheduledTaskPrincipal `
    -UserId ([System.Security.Principal.WindowsIdentity]::GetCurrent().Name) `
    -LogonType Interactive `
    -RunLevel Limited

# ── Register (replace if already exists) ─────────────────────────────────────
$Existing = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($Existing) {
    Write-Host "Updating existing task '$TaskName'..."
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
}

Register-ScheduledTask `
    -TaskName    $TaskName `
    -Action      $Action `
    -Trigger     $Trigger `
    -Settings    $Settings `
    -Principal   $Principal `
    -Description "Daily BESS spot-market KB ingestion + digest at 10:00 AM (run-if-missed)"

Write-Host ""
Write-Host "Task '$TaskName' registered successfully." -ForegroundColor Green
Write-Host "  Schedule : daily at 10:00 AM (runs at next login if missed)"
Write-Host "  Working  : $RepoRoot"
Write-Host "  Command  : $PyExe $ScriptArgs"
Write-Host ""
Write-Host "To view   : Open Task Scheduler -> Task Scheduler Library"
Write-Host "To run now: Start-ScheduledTask -TaskName '$TaskName'"
Write-Host "To remove : Unregister-ScheduledTask -TaskName '$TaskName' -Confirm:`$false"
