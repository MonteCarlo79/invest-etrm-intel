$repo = (Get-Location).Path
$dsn  = "postgresql://postgres:%21BESSmap2026@bess-platform-pg.cjs000o4wn2w.ap-southeast-1.rds.amazonaws.com:5432/marketdata?sslmode=require"

function Start-AppWindow {
    param(
        [string]$Title,
        [string]$Command
    )

    Start-Process powershell -ArgumentList @(
        "-NoExit",
        "-Command",
        "Set-Location '$repo'; `$Host.UI.RawUI.WindowTitle = '$Title'; $Command"
    )
}

# Portal
Start-AppWindow "portal-8500" @"
`$env:PYTHONPATH      = '$repo'
`$env:AUTH_MODE       = 'dev'
`$env:DEV_USER_EMAIL  = 'chen_dpeng@hotmail.com'
`$env:DEV_USER_ROLE   = 'Admin'
`$env:DB_DSN          = '$dsn'
`$env:APP_URL_MAP     = 'inner-mongolia=http://localhost:8504,bess-map=http://localhost:8503,uploader=http://localhost:8501,model-catalogue=http://localhost:8506,pnl-attribution=http://localhost:8502,spot-markets=http://localhost:8505'
py -m streamlit run apps/portal/app.py --server.port 8500
"@

# Uploader
Start-AppWindow "uploader-8501" @"
`$env:AUTH_MODE       = 'dev'
`$env:DEV_USER_EMAIL  = 'chen_dpeng@hotmail.com'
`$env:DEV_USER_ROLE   = 'Admin'
`$env:S3_BUCKET       = 'bess-uploader-data-chen-singp-2026'
py -m streamlit run apps/uploader/app.py --server.port 8501
"@

# PnL Attribution
Start-AppWindow "pnl-attribution-8502" @"
`$env:AUTH_MODE       = 'dev'
`$env:DB_DSN          = '$dsn'
py -m streamlit run apps/trading/bess/mengxi/pnl_attribution/app.py --server.port 8502
"@

# BESS Map
Start-AppWindow "bess-map-8503" @"
`$env:AUTH_MODE       = 'dev'
`$env:DEV_USER_EMAIL  = 'chen_dpeng@hotmail.com'
`$env:DEV_USER_ROLE   = 'Admin'
`$env:PGURL           = '$dsn'
py -m streamlit run .\services\bess_map\streamlit_bess_profit_dashboard_v14.1_consistent_full2.py --server.port 8503 -- --env services/bess_map/.env
"@

# Inner Mongolia
Start-AppWindow "inner-mongolia-8504" @"
`$env:PYTHONPATH      = '$repo;$repo\apps\bess-inner-mongolia'
`$env:AUTH_MODE       = 'dev'
`$env:DEV_USER_EMAIL  = 'chen_dpeng@hotmail.com'
`$env:DEV_USER_ROLE   = 'Admin'
`$env:PGURL           = '$dsn'
py -m streamlit run apps/bess-inner-mongolia/im/app.py --server.port 8504
"@

# Spot Markets
Start-AppWindow "spot-markets-8505" @"
`$env:DB_URL          = '$dsn'
py -m streamlit run apps/spot-agent/ui/spot_dashboard.py --server.port 8505
"@

# Model Catalogue
Start-AppWindow "model-catalogue-8506" @"
`$env:PYTHONPATH      = '$repo'
`$env:AUTH_MODE       = 'dev'
`$env:DEV_USER_EMAIL  = 'chen_dpeng@hotmail.com'
`$env:DEV_USER_ROLE   = 'Admin'
`$env:PGURL           = '$dsn'
`$env:DB_DSN          = '$dsn'
py -m streamlit run libs/decision_models/adapters/app/catalogue_app.py --server.port 8506
"@