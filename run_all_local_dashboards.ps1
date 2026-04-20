param(
[string]$RepoRoot = (Get-Location).Path,
[string]$Email = "[chen_dpeng@hotmail.com](mailto:chen_dpeng@hotmail.com)",
[string]$Role = "Admin",
[string]$AwsProfile = "",
[switch]$EnableInnerMongoliaEcsTrigger
)

$dsn = "postgresql://postgres:!BESSmap2026@bess-platform-pg.cjs000o4wn2w.ap-southeast-1.rds.amazonaws.com:5432/marketdata?sslmode=require"

function Start-AppWindow {
param(
[string]$Title,
[string]$Command
)

```
Start-Process powershell -ArgumentList @(
    "-NoExit",
    "-ExecutionPolicy", "Bypass",
    "-Command",
    "Set-Location '$RepoRoot'; `$Host.UI.RawUI.WindowTitle = '$Title'; $Command"
)
```



}

# Optional AWS profile for apps/features that need AWS access

$awsProfileBlock = ""
if ($AwsProfile -ne "") {
$awsProfileBlock = @"
`$env:AWS_PROFILE = '$AwsProfile'
`$env:AWS_DEFAULT_REGION = 'ap-southeast-1'
"@
}

# 1) portal :8500

Start-AppWindow "portal-8500" @"
`$env:PYTHONPATH      = '$RepoRoot'
`$env:AUTH_MODE       = 'dev'
`$env:DEV_USER_EMAIL  = '$Email'
`$env:DEV_USER_ROLE   = '$Role'
`$env:DB_DSN          = '$dsn'
`$env:APP_URL_MAP     = 'inner-mongolia=[http://localhost:8504,bess-map=http://localhost:8503,uploader=http://localhost:8501,model-catalogue=http://localhost:8506,pnl-attribution=http://localhost:8502,spot-markets=http://localhost:8505](http://localhost:8504,bess-map=http://localhost:8503,uploader=http://localhost:8501,model-catalogue=http://localhost:8506,pnl-attribution=http://localhost:8502,spot-markets=http://localhost:8505)'
$awsProfileBlock
py -m streamlit run apps/portal/app.py --server.port 8500
"@

# 2) uploader :8501

Start-AppWindow "uploader-8501" @"
`$env:PYTHONPATH      = '$RepoRoot'
`$env:AUTH_MODE       = 'dev'
`$env:DEV_USER_EMAIL  = '$Email'
`$env:DEV_USER_ROLE   = '$Role'
`$env:S3_BUCKET       = 'bess-uploader-data-chen-singp-2026'
`$env:PGURL           = '$dsn'
`$env:DB_DSN          = '$dsn'
$awsProfileBlock
py -m streamlit run apps/uploader/app.py --server.port 8501
"@

# 3) pnl-attribution :8502

Start-AppWindow "pnl-attribution-8502" @"
`$env:AUTH_MODE       = 'dev'
`$env:DB_DSN          = '$dsn'
py -m streamlit run apps/trading/bess/mengxi/pnl_attribution/app.py --server.port 8502
"@

# 4) bess-map :8503

Start-AppWindow "bess-map-8503" @"
`$env:AUTH_MODE       = 'dev'
`$env:DEV_USER_EMAIL  = '$Email'
`$env:DEV_USER_ROLE   = '$Role'
`$env:PGURL           = '$dsn'
py -m streamlit run .\services\bess_map\streamlit_bess_profit_dashboard_v14.1_consistent_full2.py --server.port 8503 -- --env services/bess_map/.env
"@

# 5) inner-mongolia :8504

$imExtra = ""
if ($EnableInnerMongoliaEcsTrigger) {
$imExtra = @"
`$env:ECS_CLUSTER           = 'bess-platform-cluster'
`$env:PIPELINE_TASK_DEF     = 'arn:aws:ecs:ap-southeast-1:319383842493:task-definition/bess-platform-inner-pipeline:35'
`$env:PRIVATE_SUBNETS       = 'subnet-04eef3891262d543a,subnet-0d561ea9ef0242812'
`$env:TASK_SECURITY_GROUPS  = 'sg-08576f2bea0274a81'
"@
}
Start-AppWindow "inner-mongolia-8504" @"
`$env:PYTHONPATH      = '$RepoRoot;$RepoRoot\apps\bess-inner-mongolia'
`$env:AUTH_MODE       = 'dev'
`$env:DEV_USER_EMAIL  = '$Email'
`$env:DEV_USER_ROLE   = '$Role'
`$env:PGURL           = '$dsn'
$awsProfileBlock
$imExtra
py -m streamlit run apps/bess-inner-mongolia/im/app.py --server.port 8504
"@

# 6) spot-markets :8505

Start-AppWindow "spot-markets-8505" @"
`$env:DB_URL          = '$dsn'
py -m streamlit run apps/spot-agent/ui/spot_dashboard.py --server.port 8505
"@

# 7) model-catalogue :8506

Start-AppWindow "model-catalogue-8506" @"
`$env:PYTHONPATH      = '$RepoRoot'
`$env:AUTH_MODE       = 'dev'
`$env:DEV_USER_EMAIL  = '$Email'
`$env:DEV_USER_ROLE   = '$Role'
`$env:PGURL           = '$dsn'
`$env:DB_DSN          = '$dsn'
py -m streamlit run libs/decision_models/adapters/app/catalogue_app.py --server.port 8506
"@

Write-Host "Launched local dashboards:"
Write-Host "  portal            http://localhost:8500"
Write-Host "  uploader          http://localhost:8501"
Write-Host "  pnl-attribution   http://localhost:8502"
Write-Host "  bess-map          http://localhost:8503"
Write-Host "  inner-mongolia    http://localhost:8504"
Write-Host "  spot-markets      http://localhost:8505"
Write-Host "  model-catalogue   http://localhost:8506"
Write-Host ""
Write-Host "Notes:"
Write-Host "  - Ensure RDS access from your laptop is open on 5432."
Write-Host "  - Use an existing AWS profile/session for uploader and optional inner-mongolia ECS trigger."
Write-Host "  - Uploader pipeline may still depend on local copied model scripts under apps/uploader/models."
