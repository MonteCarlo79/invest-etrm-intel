output "ecr_repository_url" {
  value       = aws_ecr_repository.data_ingestion.repository_url
  description = "ECR URL to push bess-data-ingestion image to"
}

output "enos_market_task_def_arn" {
  value       = aws_ecs_task_definition.enos_market.arn
  description = "ARN of the enos_market ECS task definition (for manual RunTask or gap dispatch)"
}

output "tt_api_task_def_arn" {
  value       = aws_ecs_task_definition.tt_api.arn
  description = "ARN of the tt_api ECS task definition"
}

output "lingfeng_task_def_arn" {
  value       = aws_ecs_task_definition.lingfeng.arn
  description = "ARN of the lingfeng ECS task definition (NOT scheduled — run manually)"
}

output "freshness_monitor_task_def_arn" {
  value       = aws_ecs_task_definition.freshness_monitor.arn
  description = "ARN of the freshness_monitor ECS task definition"
}
