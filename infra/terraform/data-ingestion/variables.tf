# Variable names and types aligned with trading-bess-mengxi/schedules.tf conventions.
# The root module (infra/terraform/main.tf) must pass:
#   name                    = var.name                              (already "bess-platform")
#   region                  = var.region
#   ecs_cluster_arn         = aws_ecs_cluster.this.arn
#   ecs_cluster_name        = aws_ecs_cluster.this.name
#   private_subnet_ids      = var.private_subnet_ids
#   task_security_group_id  = aws_security_group.ecs_tasks.id
#   ecs_execution_role_arn  = aws_iam_role.task_execution.arn
#   ecs_task_role_arn       = aws_iam_role.task_role.arn
#   events_invoke_ecs_role_arn = aws_iam_role.eventbridge_ecs.arn
#   db_dsn                  = "postgresql://..."
#   container_image         = "<ecr-uri>:latest"   (new variable needed in root)
#   tt_app_key              = "<key>"               (new variable needed in root)
#   tt_app_secret           = "<secret>"            (new variable needed in root)

variable "name" {
  type        = string
  default     = "bess-platform"
  description = "Project name prefix (matches root var.name)"
}

variable "region" {
  type    = string
  default = "ap-southeast-1"
}

variable "ecs_cluster_arn" {
  type        = string
  description = "ARN of the ECS cluster (e.g. aws_ecs_cluster.this.arn from root module)"
}

variable "ecs_cluster_name" {
  type        = string
  description = "Name of the ECS cluster (for ECS_CLUSTER env var in freshness_monitor)"
}

variable "private_subnet_ids" {
  type        = list(string)
  description = "Private subnet IDs for Fargate tasks (var.private_subnet_ids from root)"
}

variable "task_security_group_id" {
  type        = string
  description = "Security group ID for ECS tasks (aws_security_group.ecs_tasks.id from root)"
}

variable "ecs_execution_role_arn" {
  type        = string
  description = "IAM execution role ARN (aws_iam_role.task_execution.arn from root)"
}

variable "ecs_task_role_arn" {
  type        = string
  description = "IAM task role ARN (aws_iam_role.task_role.arn from root)"
}

variable "events_invoke_ecs_role_arn" {
  type        = string
  description = "IAM role for EventBridge to invoke ECS (aws_iam_role.eventbridge_ecs.arn from root)"
}

variable "container_image" {
  type        = string
  description = "Full ECR image URI for bess-data-ingestion (e.g. <account>.dkr.ecr.ap-southeast-1.amazonaws.com/bess-data-ingestion:latest)"
}

# ---------------------------------------------------------------------------
# DB credentials (plain env var — no Secrets Manager, matching repo convention)
# ---------------------------------------------------------------------------

variable "db_dsn" {
  type        = string
  sensitive   = true
  description = "PostgreSQL DSN passed as both PGURL and DB_DSN env vars"
}

# ---------------------------------------------------------------------------
# TT DAAS API credentials (plain env var — same as running locally)
# ---------------------------------------------------------------------------

variable "tt_app_key" {
  type        = string
  sensitive   = true
  description = "APP_KEY for TT DAAS Poseidon SDK"
}

variable "tt_app_secret" {
  type        = string
  sensitive   = true
  description = "APP_SECRET for TT DAAS Poseidon SDK"
}

# ---------------------------------------------------------------------------
# Lingfeng portal credentials (leave empty until URL is confirmed)
# ---------------------------------------------------------------------------

variable "lingfeng_base_url" {
  type        = string
  default     = ""
  description = "Base URL of the Lingfeng portal. Leave empty — collector will fail-fast with clear error."
}

variable "lingfeng_username" {
  type        = string
  sensitive   = true
  default     = ""
  description = "Lingfeng portal username"
}

variable "lingfeng_password" {
  type        = string
  sensitive   = true
  default     = ""
  description = "Lingfeng portal password"
}

variable "lingfeng_province_list" {
  type        = string
  default     = ""
  description = "Comma-separated Lingfeng province names (e.g. 山东,安徽)"
}

# ---------------------------------------------------------------------------
# S3 landing bucket (for Lingfeng raw Excel files)
# ---------------------------------------------------------------------------

variable "s3_bucket" {
  type        = string
  default     = ""
  description = "S3 bucket name for raw landing zone. Leave empty to skip S3 upload."
}
