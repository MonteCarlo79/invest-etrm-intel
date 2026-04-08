variable "aws_region" {
  type    = string
  default = "ap-southeast-1"
}

variable "ecs_cluster_name" {
  type = string
}

variable "vpc_id" {
  type = string
}

variable "private_subnet_ids" {
  type = list(string)
}

variable "rds_security_group_id" {
  type = string
}

variable "ecs_task_execution_role_arn" {
  type = string
}

variable "container_image" {
  type = string
}

variable "pgurl" {
  type      = string
  sensitive = true
}

variable "db_schema" {
  type    = string
  default = "marketdata"
}

variable "province" {
  type    = string
  default = "mengxi"
}

variable "start_date" {
  type    = string
  default = "2026-01-01"
}

variable "end_date" {
  description = "End date for reconciliation"
  type        = string
  default     = ""
}

variable "reconcile_days" {
  description = "Number of days to reconcile from start_date"
  type        = number
  default     = 1
}

variable "force_reload" {
  type    = string
  default = "false"
}

variable "schedule_expression" {
  type    = string
  default = "cron(0 22 * * ? *)"
}

variable "ecs_task_role_arn" {
  description = "IAM role used by ECS task containers"
  type        = string
}

variable "MARKET_LAG_DAYS" {
  description = "Lag days before market data is available"
  type        = number
  default     = 1
}

variable "REQUEST_DELAY" {
  description = "Delay between API requests to avoid throttling"
  type        = number
  default     = 2
}

variable "MAX_DOWNLOAD_WORKERS" {
  description = "Parallel download workers"
  type        = number
  default     = 1
}

variable "alert_webhook_url" {
  description = "Optional webhook URL for Mengxi ingestion failure alerts"
  type        = string
  default     = ""
  sensitive   = true
}

variable "alert_context" {
  description = "Context label included in Mengxi ingestion alerts"
  type        = string
  default     = "bess-mengxi-ingestion"
}