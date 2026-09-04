#Requires -RunAsAdministrator
<#
.SYNOPSIS
    Register a Windows Task Scheduler task that runs the installed-capacity
    scanner on the 5th of every month at 08:00.

    The task calls scan_installed_capacity.py, which reads all province Excel
    files from data/market-fundamentals/各省份装机数据/ and upserts to
    marketdata.province_installed_monthly.

.USAGE
    Right-click PowerShell → "Run as Administrator", then:
        .\scripts\setup_monthly_capacity_scan.ps1
#>

$TaskName   = "BESS-CapacityScan-Monthly"
$PythonExe  = "C:\Users\dipeng.chen\AppData\Local\Programs\Python\Python313\python.exe"
$ScriptPath = "C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform\scripts\scan_installed_capacity.py"
$RepoRoot   = "C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform"
$LogDir     = "$RepoRoot\logs"

# Ensure log directory exists
if (-not (Test-Path $LogDir)) { New-Item -ItemType Directory -Path $LogDir | Out-Null }

# Remove existing task if present
if (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) {
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
    Write-Host "Removed existing task: $TaskName"
}

# Wrapper: cd to repo root (so PYTHONPATH and dotenv resolve correctly), run script, redirect log
$Cmd = @"
cd /d "$RepoRoot" && "$PythonExe" "$ScriptPath" >> "$LogDir\capacity_scan.log" 2>&1
"@

$Action   = New-ScheduledTaskAction -Execute "cmd.exe" -Argument "/c `"$Cmd`""
$Settings = New-ScheduledTaskSettingsSet `
    -ExecutionTimeLimit (New-TimeSpan -Hours 2) `
    -MultipleInstances  IgnoreNew `
    -StartWhenAvailable

# New-ScheduledTaskTrigger -Monthly is not available on all Windows builds.
# Build the monthly trigger via XML and register with full task XML.
$TaskXml = @"
<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.4" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <RegistrationInfo>
    <Description>Monthly scan of province installed-capacity Excel files → marketdata.province_installed_monthly</Description>
  </RegistrationInfo>
  <Triggers>
    <CalendarTrigger>
      <StartBoundary>2026-07-05T08:00:00</StartBoundary>
      <ExecutionTimeLimit>PT2H</ExecutionTimeLimit>
      <Enabled>true</Enabled>
      <ScheduleByMonth>
        <DaysOfMonth><Day>5</Day></DaysOfMonth>
        <Months>
          <January/><February/><March/><April/><May/><June/>
          <July/><August/><September/><October/><November/><December/>
        </Months>
      </ScheduleByMonth>
    </CalendarTrigger>
  </Triggers>
  <Principals>
    <Principal id="Author">
      <RunLevel>HighestAvailable</RunLevel>
    </Principal>
  </Principals>
  <Settings>
    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>
    <StartWhenAvailable>true</StartWhenAvailable>
    <ExecutionTimeLimit>PT2H</ExecutionTimeLimit>
    <Enabled>true</Enabled>
  </Settings>
  <Actions>
    <Exec>
      <Command>cmd.exe</Command>
      <Arguments>/c "$Cmd"</Arguments>
    </Exec>
  </Actions>
</Task>
"@

Register-ScheduledTask -TaskName $TaskName -Xml $TaskXml | Out-Null

Write-Host "✅ Task '$TaskName' registered — runs on 5th of each month at 08:00."
Write-Host "   Log: $LogDir\capacity_scan.log"
Write-Host ""
Write-Host "To run immediately for testing (dry-run, no DB writes):"
Write-Host "   cd $RepoRoot"
Write-Host "   python scripts\scan_installed_capacity.py --dry-run"
Write-Host ""
Write-Host "To run immediately and write to DB:"
Write-Host "   python scripts\scan_installed_capacity.py"
