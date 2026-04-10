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

variable "remediation_schedule_expression" {
  description = "Schedule for recurring targeted Mengxi remediation"
  type        = string
  default     = "cron(0 1 ? * SAT *)"
}

variable "remediation_start_date" {
  description = "Start date for recurring remediation window"
  type        = string
  default     = "2025-07-01"
}

variable "remediation_end_date" {
  description = "Optional end date for recurring remediation window"
  type        = string
  default     = "2025-12-31"
}

variable "remediation_reconcile_days" {
  description = "Fallback remediation window width when remediation_end_date is empty"
  type        = number
  default     = 30
}

variable "remediation_batch_size" {
  description = "Chunk size for exact-date remediation loops"
  type        = number
  default     = 7
}

variable "ecs_task_role_arn" {
  description = "IAM role used by ECS task containers"
  type        = string
}

variable "scheduler_role_arn" {
  description = "IAM role assumed by EventBridge for direct ECS targets"
  type        = string
  default     = ""
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
  description = "Optional webhook URL for terminal Mengxi ingestion alerts"
  type        = string
  default     = ""
  sensitive   = true
}

variable "alert_context" {
  description = "Optional context label included in Mengxi ingestion alerts"
  type        = string
  default     = "mengxi-ingestion"
}
