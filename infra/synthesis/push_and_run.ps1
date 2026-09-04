<#
.SYNOPSIS
    Build, push, and run the knowledge synthesis pipeline as an ECS Fargate task.

.DESCRIPTION
    Builds the synthesis Docker image, pushes to ECR, registers/updates the
    task definition, then launches it on the existing bess-platform-cluster.
    Credentials (PGURL, ANTHROPIC_API_KEY) are read from config/.env and
    injected as environment variable overrides at run time — never baked
    into the image.

.PARAMETER Phase
    Space-separated phase numbers to run, e.g. "1" or "1 2 3" (default: "1")

.PARAMETER App
    Knowledge app scope: shared | trader (default: shared)

.PARAMETER Workers
    Parallel synthesis workers (default: 1 — safe for burst API limits)

.PARAMETER Limit
    Max documents to process (optional — useful for test runs, e.g. 50)

.PARAMETER BuildOnly
    Build and push the image but don't launch a task

.PARAMETER RunOnly
    Skip build/push — just register task definition and run

.EXAMPLE
    # Full shared-doc synthesis (overnight run)
    .\push_and_run.ps1

    # Test run: first 50 shared docs
    .\push_and_run.ps1 -Limit 50

    # Trader docs
    .\push_and_run.ps1 -App trader

    # Build knowledge graph + policy timeline after Phase 1 completes
    .\push_and_run.ps1 -Phase "2 3" -RunOnly

    # Phase 4: extract insights from today's sessions
    .\push_and_run.ps1 -Phase "4" -RunOnly
#>
param(
    [string]$Phase   = "1",
    [string]$App     = "shared",
    [int]$Workers    = 1,
    [string]$Limit   = "",
    [switch]$BuildOnly,
    [switch]$RunOnly
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# ── Constants ──────────────────────────────────────────────────────────────────
$ACCOUNT  = "319383842493"
$REGION   = "ap-southeast-1"
$REPO     = "bess-synthesis"
$CLUSTER  = "bess-platform-cluster"
$ECR_URI  = "$ACCOUNT.dkr.ecr.$REGION.amazonaws.com/$REPO"
$REPO_ROOT = (Resolve-Path "$PSScriptRoot\..\..").Path

# ── Load credentials from config/.env ─────────────────────────────────────────
$EnvFile = Join-Path $REPO_ROOT "config\.env"
if (-not (Test-Path $EnvFile)) {
    Write-Error "config/.env not found at $EnvFile"
    exit 1
}
$EnvVars = @{}
Get-Content $EnvFile | ForEach-Object {
    if ($_ -match "^([A-Za-z_][A-Za-z0-9_]*)=(.+)$") {
        $EnvVars[$Matches[1]] = $Matches[2].Trim('"').Trim("'")
    }
}
$PGURL             = $EnvVars["PGURL"]
$ANTHROPIC_API_KEY = $EnvVars["ANTHROPIC_API_KEY"]

if (-not $PGURL)             { Write-Error "PGURL not found in config/.env"; exit 1 }
if (-not $ANTHROPIC_API_KEY) { Write-Error "ANTHROPIC_API_KEY not found in config/.env"; exit 1 }

# ── Build + push ───────────────────────────────────────────────────────────────
if (-not $RunOnly) {
    Write-Host "`n==> Ensuring ECR repository exists..." -ForegroundColor Cyan
    aws ecr create-repository --repository-name $REPO --region $REGION 2>$null
    if ($LASTEXITCODE -ne 0) {
        Write-Host "    (repository already exists — OK)" -ForegroundColor DarkGray
    }

    Write-Host "`n==> Logging in to ECR..." -ForegroundColor Cyan
    $loginPwd = aws ecr get-login-password --region $REGION
    $loginPwd | docker login --username AWS --password-stdin "$ACCOUNT.dkr.ecr.$REGION.amazonaws.com"

    Write-Host "`n==> Building Docker image..." -ForegroundColor Cyan
    Push-Location $REPO_ROOT
    docker build -t $REPO -f infra/synthesis/Dockerfile .
    if ($LASTEXITCODE -ne 0) { Write-Error "Docker build failed"; exit 1 }
    Pop-Location

    Write-Host "`n==> Tagging and pushing to ECR..." -ForegroundColor Cyan
    docker tag "${REPO}:latest" "${ECR_URI}:latest"
    docker push "${ECR_URI}:latest"
    if ($LASTEXITCODE -ne 0) { Write-Error "Docker push failed"; exit 1 }

    Write-Host "    Image pushed: ${ECR_URI}:latest" -ForegroundColor Green
}

if ($BuildOnly) {
    Write-Host "`nBuild complete. Use -RunOnly to launch without rebuilding." -ForegroundColor Green
    exit 0
}

# ── Build command override ─────────────────────────────────────────────────────
# Convert "1 2 3" → --phase 1 --phase 2 --phase 3
$PhaseArgs = ($Phase.Trim() -split "\s+") | ForEach-Object { "--phase", $_ }
$CmdArgs = @("python", "scripts/run_synthesis_pipeline.py") `
         + $PhaseArgs `
         + @("--app", $App, "--workers", $Workers.ToString())
if ($Limit) { $CmdArgs += @("--limit", $Limit) }

Write-Host "`n==> Command: $($CmdArgs -join ' ')" -ForegroundColor Cyan

# ── Register task definition with injected secrets ────────────────────────────
Write-Host "`n==> Registering task definition..." -ForegroundColor Cyan

$TaskDef = Get-Content "$PSScriptRoot\task-definition.json" | ConvertFrom-Json

# Inject credentials + command
$TaskDef.containerDefinitions[0].image   = "${ECR_URI}:latest"
$TaskDef.containerDefinitions[0].command = $CmdArgs
$TaskDef.containerDefinitions[0].environment = @(
    @{ name = "PYTHONPATH";        value = "/app"              },
    @{ name = "PYTHONUNBUFFERED";  value = "1"                 },
    @{ name = "PGURL";             value = $PGURL              },
    @{ name = "ANTHROPIC_API_KEY"; value = $ANTHROPIC_API_KEY  }
)

# Write BOM-free UTF-8 (Set-Content -Encoding utf8 adds BOM in PS 5.1 → invalid JSON)
# file://C:/path (2 slashes): botocore strips "file://" → valid Windows path.
$TmpJson    = Join-Path $PSScriptRoot "tmp-taskdef.json"
$TaskDefStr = $TaskDef | ConvertTo-Json -Depth 20
[System.IO.File]::WriteAllText($TmpJson, $TaskDefStr)
$TmpUri = "file://" + ($TmpJson -replace "\\", "/")
try {
    $RegisterResult = aws ecs register-task-definition --cli-input-json $TmpUri
} finally {
    Remove-Item $TmpJson -ErrorAction SilentlyContinue
}

$TaskDefArn = ($RegisterResult | ConvertFrom-Json).taskDefinition.taskDefinitionArn
if (-not $TaskDefArn) {
    Write-Error "Failed to register task definition:`n$RegisterResult"
    exit 1
}
Write-Host "    Registered: $TaskDefArn" -ForegroundColor Green

# ── Launch ECS task ────────────────────────────────────────────────────────────
Write-Host "`n==> Launching ECS Fargate task..." -ForegroundColor Cyan

$NetworkConfig = "awsvpcConfiguration={" +
    "subnets=[subnet-04eef3891262d543a,subnet-0d561ea9ef0242812]," +
    "securityGroups=[sg-08576f2bea0274a81]," +
    "assignPublicIp=ENABLED}"

$RunResult = aws ecs run-task `
    --cluster    $CLUSTER `
    --task-definition $TaskDefArn `
    --launch-type FARGATE `
    --network-configuration $NetworkConfig `
    | ConvertFrom-Json

if ($RunResult.failures) {
    Write-Error "ECS run-task failed: $($RunResult.failures | ConvertTo-Json)"
    exit 1
}

$Task     = $RunResult.tasks[0]
$TaskArn  = $Task.taskArn
$TaskId   = $TaskArn.Split("/")[-1]

Write-Host "`n✓ Task launched!" -ForegroundColor Green
Write-Host "  Task ID  : $TaskId"
Write-Host "  Task ARN : $TaskArn"
Write-Host ""
Write-Host "── Monitor ───────────────────────────────────────────────────────" -ForegroundColor Cyan
Write-Host "  Status  : aws ecs describe-tasks --cluster $CLUSTER --tasks $TaskId --query 'tasks[0].lastStatus'"
Write-Host "  Logs    : aws logs tail /ecs/bess-platform --follow --log-stream-prefix synthesis"
Write-Host "  Stop    : aws ecs stop-task --cluster $CLUSTER --task $TaskId"
Write-Host "─────────────────────────────────────────────────────────────────" -ForegroundColor Cyan
