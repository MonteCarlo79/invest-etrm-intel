variable "region" {
  type    = string
  default = "ap-southeast-1"
}

variable "name" {
  type    = string
  default = "bess-platform"
}

# Provide 2 public subnets and 2 private subnets in the same VPC
variable "vpc_id" {
  type = string
}

variable "public_subnet_ids" {
  type = list(string)
}

variable "private_subnet_ids" {
  type = list(string)
}

# ECR image URIs you pushed (latest tags ok to start)
variable "image_bess_map" {
  type = string
}

variable "image_uploader" {
  type = string
}

variable "image_portal" {
  description = "Docker image for portal service"
  type        = string
}

# Streamlit ports
variable "port_bess_map" {
  type    = number
  default = 8503
}

variable "port_uploader" {
  type    = number
  default = 8501
}

variable "port_portal" {
  type    = number
  default = 8500
}

# RDS settings (start small, scale later)
variable "db_name" {
  type    = string
  default = "marketdata"
}

variable "db_username" {
  type    = string
  default = "postgres"
}

# Use a strong password; terraform will store it in state.
variable "db_password" {
  type      = string
  sensitive = true
}

variable "db_instance_class" {
  type    = string
  default = "db.t4g.micro"
}

variable "db_allocated_storage" {
  type    = number
  default = 20
}

# S3 uploads bucket name (must be globally unique)
variable "uploads_bucket_name" {
  type = string
}

# Desired task sizing (tune later)
variable "task_cpu" {
  type    = number
  default = 512
}

variable "task_memory" {
  type    = number
  default = 1024
}

# Desired count
variable "desired_count_bess_map" {
  type    = number
  default = 1
}

variable "desired_count_uploader" {
  type    = number
  default = 1
}


variable "investor_password" {
  description = "Password for investor login"
  type        = string
  sensitive   = true
}

variable "internal_password" {
  description = "Password for internal login"
  type        = string
  sensitive   = true
}

variable "admin_password" {
  description = "Password for admin login"
  type        = string
  sensitive   = true
}

variable "db_dsn" {
  description = "Postgres DSN for portal"
  type        = string
  sensitive   = true
}

#################################################
# Inner Mongolia Dashboard Image
#################################################
variable "image_inner_mongolia" {
  description = "Docker image for Inner Mongolia Streamlit dashboard"
  type        = string
}

#################################################
# Inner Mongolia Pipeline Image
#################################################
variable "image_inner_pipeline" {
  description = "Docker image for Inner Mongolia pipeline"
  type        = string
}

#################################################
# Portal Service Desired Count
#################################################
variable "desired_count_portal" {
  description = "Desired task count for the portal ECS service. Set to 0 to scale down without destroying."
  type        = number
  default     = 1
}

#################################################
# Inner Mongolia Service Desired Count
#################################################
variable "desired_count_inner_mongolia" {
  description = "Number of running tasks for inner-mongolia service"
  type        = number
  default     = 1
}

#################################################
# PnL Attribution Service
#################################################
variable "enable_pnl_attribution_service" {
  description = "Set to true to deploy the Mengxi P&L attribution Streamlit service"
  type        = bool
  default     = false

  validation {
    condition     = !var.enable_pnl_attribution_service || length(trimspace(var.pnl_attribution_image)) > 0
    error_message = "pnl_attribution_image must be non-empty when enable_pnl_attribution_service is true."
  }
}

variable "pnl_attribution_image" {
  description = "Docker image for the Mengxi P&L attribution service"
  type        = string
  default     = ""
}

variable "pnl_attribution_pgurl" {
  description = "Optional PGURL override for Mengxi P&L attribution ECS task. If empty, defaults to stack RDS DSN."
  type        = string
  sensitive   = true
  default     = ""
}

variable "pnl_attribution_container_port" {
  description = "Container port for Mengxi P&L attribution Streamlit app"
  type        = number
  default     = 8502
}

variable "pnl_attribution_path" {
  description = "ALB base path for Mengxi P&L attribution app"
  type        = string
  default     = "/pnl-attribution"
}

variable "pnl_attribution_cpu" {
  description = "Fargate task CPU units for Mengxi P&L attribution app"
  type        = number
  default     = 512
}

variable "pnl_attribution_memory" {
  description = "Fargate task memory (MiB) for Mengxi P&L attribution app"
  type        = number
  default     = 1024
}

variable "pnl_attribution_desired_count" {
  description = "Desired ECS task count for Mengxi P&L attribution app"
  type        = number
  default     = 1
}

#################################################
# Non-essential Dashboard Desired Counts
# Set to 0 to scale down without destroying resources.
#################################################
variable "desired_count_spot_markets" {
  description = "Desired task count for the spot-markets dashboard ECS service."
  type        = number
  default     = 1
}

variable "desired_count_mengxi_dashboard" {
  description = "Desired task count for the mengxi-dashboard ECS service."
  type        = number
  default     = 1
}

variable "desired_count_model_catalogue" {
  description = "Desired task count for the model-catalogue ECS service."
  type        = number
  default     = 1
}

#################################################
# trading-bess-mengxi scheduled jobs (TT loaders + Mengxi P&L refresh)
#################################################
variable "enable_trading_bess_mengxi_schedules" {
  description = "Enable EventBridge/ECS schedules for TT province loader, TT asset loader, and Mengxi P&L refresh."
  type        = bool
  default     = false

  validation {
    condition     = !var.enable_trading_bess_mengxi_schedules || length(trimspace(var.image_trading_jobs)) > 0
    error_message = "image_trading_jobs must be non-empty when enable_trading_bess_mengxi_schedules is true."
  }
}

variable "image_trading_jobs" {
  description = "Docker image for Mengxi trading jobs (TT loaders + P&L refresh)"
  type        = string
  default     = ""
}

variable "trading_jobs_db_dsn" {
  description = "Optional DB DSN override for scheduled trading jobs. If empty, defaults to stack RDS DSN."
  type        = string
  sensitive   = true
  default     = ""
}

variable "trading_jobs_log_retention_days" {
  description = "CloudWatch log retention for trading-bess-mengxi scheduled job log groups."
  type        = number
  default     = 14
}

variable "tt_app_key" {
  description = "TT REST API application key for province and asset loaders."
  type        = string
  sensitive   = true
  default     = ""
}

variable "tt_app_secret" {
  description = "TT REST API application secret for province and asset loaders."
  type        = string
  sensitive   = true
  default     = ""
}

variable "db_host" {
  description = "RDS hostname for focused_assets_data.py (DB_HOST env var)."
  type        = string
  default     = ""
}

variable "image_mengxi_ingest" {
  description = "Docker image for the Mengxi Excel ingest job."
  type        = string
  default     = ""
}

#################################################
# China Spot Market Dashboard
#################################################
variable "image_spot_markets" {
  description = "Docker image for China Spot Market dashboard"
  type        = string
  default     = ""
}

#################################################
# Model Catalogue Image
#################################################
variable "image_model_catalogue" {
  description = "Docker image for model catalogue Streamlit app"
  type        = string
}

#################################################
# Options Cockpit Image
#################################################
variable "image_options_cockpit" {
  description = "Docker image for Options Cockpit Streamlit app"
  type        = string
  default     = ""
}

variable "desired_count_options_cockpit" {
  description = "Desired task count for the options-cockpit ECS service."
  type        = number
  default     = 1
}

#################################################
# Mengxi Dashboard Image
#################################################
variable "image_mengxi_dashboard" {
  description = "Docker image for Mengxi 15-min market data dashboard"
  type        = string
}

variable "pipeline_image_tag" {
  description = "Docker tag for pipeline image"
  type        = string
  default     = "v2"
}

variable "ai_enabled" {
  type    = string
  default = "false"
}

############################################
# Inner Mongolia BESS Parameters
############################################

variable "conversion_factor" {
  description = "Settlement conversion factor (4 for 15-min data)"
  type        = string
}

variable "duration_h" {
  description = "BESS duration in hours"
  type        = string
}

variable "subsidy_per_mwh" {
  description = "Subsidy per MWh"
  type        = string
}

variable "capex_yuan_per_kwh" {
  description = "Capex per kWh"
  type        = string
}

variable "degradation_rate" {
  description = "Annual degradation rate"
  type        = string
}

variable "om_cost_per_mw_per_year" {
  description = "O&M cost per MW per year"
  type        = string
}

variable "life_years" {
  description = "Project life in years"
  type        = string
}


variable "project" {
  type    = string
  default = "bess-platform"
}

variable "environment" {
  type    = string
  default = "prod"
}



variable "openai_api_key" {
  type      = string
  sensitive = true
  default   = ""
}

variable "slack_webhook_url" {
  type      = string
  sensitive = true
  default   = ""
}

variable "report_email_to" {
  type    = string
  default = ""
}

variable "smtp_host" {
  type    = string
  default = ""
}

variable "smtp_port" {
  type    = string
  default = "587"
}

variable "smtp_user" {
  type      = string
  sensitive = true
  default   = ""
}

variable "smtp_password" {
  type      = string
  sensitive = true
  default   = ""
}

variable "smtp_from" {
  type    = string
  default = ""
}

variable "image_strategy_agent" {
  description = "Docker image for strategy agent"
  type        = string
}

variable "image_portfolio_agent" {
  description = "Docker image for portfolio risk agent"
  type        = string
}

variable "image_execution_agent" {
  description = "Docker image for execution agent"
  type        = string
}

variable "image_dev_agent" {
  description = "Docker image for IT dev agent"
  type        = string
}

variable "image_trading_performance_agent" {
  description = "Docker image for BESS trading performance agent"
  type        = string
}

variable "anthropic_api_key" {
  description = "Anthropic Claude API key for the trading performance agent"
  type        = string
  sensitive   = true
  default     = ""
}

variable "scheduler_role_arn" {
  description = "IAM role used by EventBridge Scheduler to trigger ECS tasks"
  type        = string
}

variable "show_aws_debug" {
  description = "Enable AWS debug in container"
  type        = bool
  default     = false
}

variable "acm_certificate_arn" {
  description = "ACM certificate ARN for the ALB HTTPS listener"
  type        = string
}

variable "logout_redirect_uri" {
  type = string
}

variable "cognito_callback_urls" {
  type = list(string)
}

variable "cognito_default_redirect_uri" {
  type = string
}