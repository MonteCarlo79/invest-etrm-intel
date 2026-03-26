output "ecs_task_definition_arn" {
  value = aws_ecs_task_definition.mengxi_ingestion.arn
}

output "ecs_ingestion_security_group_id" {
  value = aws_security_group.ecs_ingestion.id
}

output "eventbridge_rule_name" {
  value = aws_cloudwatch_event_rule.mengxi_daily.name
}
