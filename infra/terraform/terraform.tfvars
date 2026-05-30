region = "ap-southeast-1"
name   = "bess-platform"

vpc_id = "vpc-0e44e77436492fc1a"

public_subnet_ids = [
  "subnet-04eef3891262d543a",
  "subnet-0d561ea9ef0242812"
]

private_subnet_ids = [
  "subnet-04eef3891262d543a",
  "subnet-0d561ea9ef0242812"
]

image_bess_map = "319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-map:v42"
image_uploader = "319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-uploader:v20"
image_portal   = "319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-platform-portal:v7"

db_password = "!BESSmap2026"

uploads_bucket_name = "bess-uploader-data-chen-singp-2026"

investor_password = "StrongInvestorPass123!"
internal_password = "StrongerInternalPass456!"
admin_password    = "UltraSecureAdmin789!"

db_dsn = "postgresql://postgres:%21BESSmap2026@bess-platform-pg.cjs000o4wn2w.ap-southeast-1.rds.amazonaws.com:5432/marketdata"

image_inner_mongolia   = "319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-inner-mongolia:v52"
image_inner_pipeline   = "319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-inner-pipeline:v16"
image_mengxi_dashboard = "319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-mengxi-dashboard:v8"
image_model_catalogue  = "319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-model-catalogue:v1"
image_options_cockpit  = "319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-options-cockpit:v5-debug"

image_strategy_agent  = "319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-strategy-agent:latest"
image_portfolio_agent = "319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-portfolio-agent:latest"
image_execution_agent = "319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-execution-agent:latest"
image_dev_agent       = "319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-it-dev-agent:latest"
image_trading_performance_agent = "319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-trading-performance-agent:latest"

anthropic_api_key = "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"

scheduler_role_arn = "arn:aws:iam::319383842493:role/EventBridgeSchedulerRole"

desired_count_inner_mongolia = 1

# Non-essential dashboards suspended to reduce Fargate cost.
# Set back to 1 to restore. No resources are deleted.
desired_count_bess_map        = 1
desired_count_uploader        = 0
enable_uploader_service       = false
desired_count_model_catalogue = 0
desired_count_spot_markets    = 1
pnl_attribution_desired_count = 0
ai_enabled                   = "true"

show_aws_debug = false

duration_h              = "4"
subsidy_per_mwh         = "350"
capex_yuan_per_kwh      = "600"
degradation_rate        = "0.04"
om_cost_per_mw_per_year = "24000"
life_years              = "10"
conversion_factor       = "4"

acm_certificate_arn = "arn:aws:acm:ap-southeast-1:319383842493:certificate/8a7d08c9-d008-48a0-a1d4-25e125bd0ab8"

logout_redirect_uri = "https://www.pjh-etrm.ai/signed-out"

cognito_callback_urls = [
  "https://pjh-etrm.ai/oauth2/idpresponse",
  "https://www.pjh-etrm.ai/oauth2/idpresponse"
]

cognito_default_redirect_uri = "https://www.pjh-etrm.ai/oauth2/idpresponse"

# pnl-attribution service (enable when image is built and pushed to ECR)
enable_pnl_attribution_service = false
pnl_attribution_image          = "319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-pnl-attribution:v4"
pnl_attribution_pgurl          = "postgresql://postgres:%21BESSmap2026@bess-platform-pg.cjs000o4wn2w.ap-southeast-1.rds.amazonaws.com:5432/marketdata"

# trading-bess-mengxi scheduled jobs (TT loaders + Mengxi P&L refresh)
enable_trading_bess_mengxi_schedules = true
image_trading_jobs                   = "319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-trading-jobs:v20260405-1"

tt_app_key    = "5bb8ebc4-9bbe-4d05-84de-f108c237c8ce"
tt_app_secret = "d75ef900-15c6-451c-943e-0cc835e89f38"
db_host = "bess-platform-pg.cjs000o4wn2w.ap-southeast-1.rds.amazonaws.com"
image_mengxi_ingest  = "319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-mengxi-ingestion:v19"
image_spot_markets   = "319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-spot-markets:v30"

modo_api_key  = "c456dba24fe6e6b302ac926d7da993c2e47ed54dddd67d20653b4a148225"
modo_email    = "dipeng.chen@envision-energy.com"   # fill in: your Modo Energy web login email
modo_password = "!Ariesqq0409"   # fill in: your Modo Energy web login password

image_gb_market = "319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-gb-market:v68"

image_au_market    = "319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-au-market:v3"
image_ercot_market = "319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-ercot-market:v1"
image_pjm_market   = "319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-pjm-market:v1"
image_caiso_market = "319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-caiso-market:v1"

desired_count_au_market    = 1
desired_count_ercot_market = 1
desired_count_pjm_market   = 1
desired_count_caiso_market = 1
# All three deployed 2026-05-25 — images pushed to ECR, services ACTIVE/COMPLETED

wecom_webhook_url = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=dddedcc5-a364-4a7c-9467-cb90fb7d563a"

# Spot market daily report — set to your desired WeCom group webhook key
spot_market_wecom_webhook_url = ""
desired_count_gb_market  = 1

LINGFENG_USERNAME="bfsjcs"
LINGFENG_PASSWORD="P+2jGuuE5WD"

fengxing_api_key = "b5d38061-d965-6039d74b69ff"

# Crystal-Ball Fortune Teller
image_crystal_ball             = "319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/crystal-ball-fortune:v6"
desired_count_crystal_ball     = 1
crystal_ball_wecom_webhook_url = ""

# Daily market report email (06:00 SGT)
# Use Gmail with an App Password: https://myaccount.google.com/apppasswords
smtp_host      = "smtp.gmail.com"
smtp_port      = "587"
smtp_user      = "dipengchen@gmail.com"
smtp_password  = "enpmlhytdltnjpfn"
# Comma-separated for multiple recipients, e.g. "a@b.com,c@d.com"
report_email_to = "chen_dpeng@hotmail.com"