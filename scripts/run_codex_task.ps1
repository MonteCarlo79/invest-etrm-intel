param(
    [Parameter(Mandatory=$true)][string]$TaskJson,
    [Parameter(Mandatory=$true)][string]$RepoPath,
    [Parameter(Mandatory=$true)][string]$OutputSummary
)

$ErrorActionPreference = 'Stop'
$task = Get-Content -Raw -Path $TaskJson | ConvertFrom-Json
New-Item -ItemType Directory -Force -Path (Split-Path -Parent $OutputSummary) | Out-Null

$prompt = @"
Task ID: $($task.task_id)
Owner: codex
Business goal: $($task.business_goal)
Scope: $($task.scope)
Branch: $($task.branch)
Inputs: $((($task.inputs | ForEach-Object { $_.ToString() }) -join '; '))
Deliverables: $((($task.deliverables | ForEach-Object { $_.ToString() }) -join '; '))
Assumptions: $((($task.assumptions | ForEach-Object { $_.ToString() }) -join '; '))
Validation required: $((($task.validation_required | ForEach-Object { $_.ToString() }) -join '; '))

Instructions:
- Work only within the assigned scope.
- Do not switch to another branch unless explicitly instructed.
- After changes, produce a concise patch summary, assumptions, runtime risks, and a test/check plan.
"@

# Replace the placeholder command below with your actual Codex CLI invocation.
# Example pattern only:
# codex exec --cwd "$RepoPath" --prompt "$prompt"

$summary = @"
# Codex task summary

- task_id: $($task.task_id)
- status: placeholder_success
- repo_path: $RepoPath
- branch: $($task.branch)

## Prompt sent
$prompt

## Next step
Replace scripts/run_codex_task.ps1 placeholder command with your real Codex CLI command.
"@

Set-Content -Path $OutputSummary -Value $summary -Encoding UTF8
Write-Host "Prepared Codex task wrapper output at $OutputSummary"
exit 0
