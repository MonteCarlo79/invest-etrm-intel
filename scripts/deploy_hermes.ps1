# Deploy hermes-service to ECR + force-new ECS deployment
# Usage:  pwsh scripts/deploy_hermes.ps1   (env vars must be set beforehand)
param()
$ErrorActionPreference = "Stop"

$Region  = "ap-southeast-1"
$Account = "319383842493"
$Repo    = "bess-platform-hermes"
$Cluster = "bess-platform-cluster"
$Service = "bess-platform-hermes-svc"
$Image   = "${Account}.dkr.ecr.${Region}.amazonaws.com/${Repo}:latest"

$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

Write-Host "=== Logging in to ECR ===" -ForegroundColor Cyan
$ecrPassword = aws ecr get-login-password --region $Region
$ecrPassword | docker login --username AWS --password-stdin "${Account}.dkr.ecr.${Region}.amazonaws.com"

Write-Host "=== Building image (minimal context) ===" -ForegroundColor Cyan

# Create a temp directory with ONLY what hermes needs (~few MB instead of 3GB+)
$ctx = Join-Path ([System.IO.Path]::GetTempPath()) "hermes-build-context"
if (Test-Path $ctx) { Remove-Item -Recurse -Force $ctx }
New-Item -ItemType Directory -Path "$ctx/services" | Out-Null
New-Item -ItemType Directory -Path "$ctx/apps"     | Out-Null

Copy-Item -Recurse "$Root/services/hermes"       "$ctx/services/hermes"
Copy-Item -Recurse "$Root/apps/hermes-service"   "$ctx/apps/hermes-service"
"" | Set-Content "$ctx/services/__init__.py"

# Place the Dockerfile at the context root so relative paths work
Copy-Item "$Root/apps/hermes-service/Dockerfile" "$ctx/Dockerfile"

docker build `
    -t $Image `
    --platform linux/amd64 `
    $ctx

Remove-Item -Recurse -Force $ctx

Write-Host "=== Pushing image ===" -ForegroundColor Cyan
docker push $Image

Write-Host "=== Registering new task definition with OneDrive env vars ===" -ForegroundColor Cyan

# Fetch current task definition ARN from the service
$taskDefArn = (aws ecs describe-services `
    --cluster $Cluster --services $Service --region $Region `
    --query "services[0].taskDefinition" --output text)

$taskDef = aws ecs describe-task-definition `
    --task-definition $taskDefArn --region $Region | ConvertFrom-Json

$containers = $taskDef.taskDefinition.containerDefinitions

foreach ($c in $containers) {
    $env = @{}
    foreach ($e in $c.environment) { $env[$e.name] = $e.value }

    # Inject OneDrive vars from PowerShell env
    $env["ONEDRIVE_CLIENT_ID"]     = if ($env:ONEDRIVE_CLIENT_ID)     { $env:ONEDRIVE_CLIENT_ID }     else { $env["ONEDRIVE_CLIENT_ID"] }
    $env["ONEDRIVE_CLIENT_SECRET"] = if ($env:ONEDRIVE_CLIENT_SECRET) { $env:ONEDRIVE_CLIENT_SECRET } else { $env["ONEDRIVE_CLIENT_SECRET"] }
    $env["ONEDRIVE_REFRESH_TOKEN"] = if ($env:ONEDRIVE_REFRESH_TOKEN) { $env:ONEDRIVE_REFRESH_TOKEN } else { $env["ONEDRIVE_REFRESH_TOKEN"] }

    # Remove legacy WeChat var
    $env.Remove("WECHAT_OWNER_ID") | Out-Null

    $c.environment = $env.GetEnumerator() | ForEach-Object {
        [PSCustomObject]@{ name = $_.Key; value = $_.Value }
    }
}

$td = $taskDef.taskDefinition
$payload = @{
    family                  = $td.family
    taskRoleArn             = $td.taskRoleArn
    executionRoleArn        = $td.executionRoleArn
    networkMode             = $td.networkMode
    containerDefinitions    = $containers
    requiresCompatibilities = $td.requiresCompatibilities
    cpu                     = $td.cpu
    memory                  = $td.memory
} | ConvertTo-Json -Depth 20

$tmpFile = [System.IO.Path]::GetTempFileName()
$payload | Set-Content -Path $tmpFile -Encoding UTF8

$newArn = (aws ecs register-task-definition `
    --region $Region `
    --cli-input-json "file://$tmpFile" `
    --query "taskDefinition.taskDefinitionArn" --output text)

Remove-Item $tmpFile

Write-Host "New task definition: $newArn" -ForegroundColor Green

Write-Host "=== Force-deploying ECS service ===" -ForegroundColor Cyan
aws ecs update-service `
    --cluster $Cluster `
    --service $Service `
    --task-definition $newArn `
    --force-new-deployment `
    --region $Region `
    --query "service.deployments[0].status" `
    --output text

Write-Host "=== Done — deployment started ===" -ForegroundColor Green
