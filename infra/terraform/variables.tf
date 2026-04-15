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
# Inner Mongolia Service Desired Count
#################################################
variable "desired_count_inner_mongolia" {
  description = "Number of running tasks for inner-mongolia service"
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