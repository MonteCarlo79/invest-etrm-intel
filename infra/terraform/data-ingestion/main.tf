###############################################################################
# bess-data-ingestion — ECS tasks + EventBridge schedules
#
# New standalone module. Does NOT touch:
#   - infra/terraform/main.tf (root)
#   - infra/terraform/trading-bess-mengxi/schedules.tf
#   - infra/terraform/mengxi-ingestion/
#
# Conventions matched to infra/terraform/trading-bess-mengxi/schedules.tf:
#   - Variable names: private_subnet_ids, task_security_group_id,
#                     ecs_execution_role_arn, ecs_task_role_arn,
#                     events_invoke_ecs_role_arn, db_dsn
#   - Log groups: /ecs/${var.name}/<task-name>
#   - Task families: ${var.name}-<task-name>
#   - PGURL and API credentials passed as plain env vars (no Secrets Manager)
#   - assign_public_ip = false (outbound via NAT, matching trading-bess-mengxi)
###############################################################################

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0"
    }
  }
}

provider "aws" {
  region = var.region
}

data "aws_caller_identity" "current" {}

locals {
  common_env = [
    { name = "AWS_REGION", value = var.region },
    { name = "DB_DSN",     value = var.db_dsn },
    { name = "PGURL",      value = var.db_dsn },
    { name = "PYTHONPATH", value = "/app" },
  ]
}

# ---------------------------------------------------------------------------
# ECR repository
# ---------------------------------------------------------------------------

resource "aws_ecr_repository" "data_ingestion" {
  name                 = "bess-data-ingestion"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = {
    Project = var.name
  }
}

resource "aws_ecr_lifecycle_policy" "data_ingestion" {
  repository = aws_ecr_repository.data_ingestion.name

  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "keep last 10 images"
      selection = {
        tagStatus   = "any"
        countType   = "imageCountMoreThan"
        countNumber = 10
      }
      action = { type = "expire" }
    }]
  })
}

# ---------------------------------------------------------------------------
# CloudWatch log groups  (pattern: /ecs/${var.name}/<task-name>)
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_log_group" "enos_market" {
  name              = "/ecs/${var.name}/enos-market-collector"
  retention_in_days = 30
  tags              = { Project = var.name }
}

resource "aws_cloudwatch_log_group" "tt_api" {
  name              = "/ecs/${var.name}/tt-api-collector"
  retention_in_days = 30
  tags              = { Project = var.name }
}

resource "aws_cloudwatch_log_group" "lingfeng" {
  name              = "/ecs/${var.name}/lingfeng-collector"
  retention_in_days = 30
  tags              = { Project = var.name }
}

resource "aws_cloudwatch_log_group" "freshness" {
  name              = "/ecs/${var.name}/freshness-monitor"
  retention_in_days = 14
  tags              = { Project = var.name }
}

# ---------------------------------------------------------------------------
# ECS task definitions  (family: ${var.name}-<task-name>)
# ---------------------------------------------------------------------------

resource "aws_ecs_task_definition" "enos_market" {
  family                   = "${var.name}-enos-market-collector"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "512"
  memory                   = "1024"
  execution_role_arn       = var.ecs_execution_role_arn
  task_role_arn            = var.ecs_task_role_arn

  container_definitions = jsonencode([{
    name      = "enos-market-collector"
    image     = var.container_image
    essential = true
    command   = ["python", "services/data_ingestion/enos_market_collector.py"]

    environment = concat(local.common_env, [
      { name = "RUN_MODE",    value = "daily" },
      { name = "DB_SCHEMA",   value = "marketdata" },
      { name = "LOG_LEVEL",   value = "INFO" },
    ])

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = aws_cloudwatch_log_group.enos_market.name
        awslogs-region        = var.region
        awslogs-stream-prefix = "ecs"
      }
    }
  }])

  tags = { Project = var.name }
}

resource "aws_ecs_task_definition" "tt_api" {
  family                   = "${var.name}-tt-api-collector"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "1024"
  memory                   = "2048"
  execution_role_arn       = var.ecs_execution_role_arn
  task_role_arn            = var.ecs_task_role_arn

  container_definitions = jsonencode([{
    name      = "tt-api-collector"
    image     = var.container_image
    essential = true
    command   = ["python", "services/data_ingestion/tt_api_collector.py"]

    environment = concat(local.common_env, [
      { name = "RUN_MODE",        value = "daily" },
      { name = "MARKET_LIST",     value = "Mengxi,Anhui,Shandong,Jiangsu" },
      { name = "FULL_HISTORY",    value = "false" },
      { name = "DB_LOOKBACK_DAYS",value = "2" },
      { name = "RUN_INHOUSE_WIND",value = "true" },
      { name = "LOG_LEVEL",       value = "INFO" },
      { name = "APP_KEY",         value = var.tt_app_key },
      { name = "APP_SECRET",      value = var.tt_app_secret },
    ])

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = aws_cloudwatch_log_group.tt_api.name
        awslogs-region        = var.region
        awslogs-stream-prefix = "ecs"
      }
    }
  }])

  tags = { Project = var.name }
}

resource "aws_ecs_task_definition" "lingfeng" {
  family                   = "${var.name}-lingfeng-collector"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "1024"
  memory                   = "2048"
  execution_role_arn       = var.ecs_execution_role_arn
  task_role_arn            = var.ecs_task_role_arn

  container_definitions = jsonencode([{
    name      = "lingfeng-collector"
    image     = var.container_image
    essential = true
    command   = ["python", "services/data_ingestion/lingfeng_export_collector.py"]

    environment = concat(local.common_env, [
      { name = "RUN_MODE",                value = "daily" },
      { name = "LINGFENG_BASE_URL",       value = var.lingfeng_base_url },
      { name = "LINGFENG_USERNAME",       value = var.lingfeng_username },
      { name = "LINGFENG_PASSWORD",       value = var.lingfeng_password },
      { name = "LINGFENG_PROVINCE_LIST",  value = var.lingfeng_province_list },
      { name = "S3_BUCKET",               value = var.s3_bucket },
      { name = "LOG_LEVEL",               value = "INFO" },
    ])

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = aws_cloudwatch_log_group.lingfeng.name
        awslogs-region        = var.region
        awslogs-stream-prefix = "ecs"
      }
    }
  }])

  tags = { Project = var.name }
}

resource "aws_ecs_task_definition" "freshness_monitor" {
  family                   = "${var.name}-freshness-monitor"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "256"
  memory                   = "512"
  execution_role_arn       = var.ecs_execution_role_arn
  task_role_arn            = var.ecs_task_role_arn

  container_definitions = jsonencode([{
    name      = "freshness-monitor"
    image     = var.container_image
    essential = true
    command   = ["python", "services/data_ingestion/freshness_monitor.py"]

    environment = concat(local.common_env, [
      # ECS_DISPATCH=false: gap detection only on first deploy.
      # Set to "true" after confirming IAM permissions (ecs:RunTask + iam:PassRole
      # on task_role) and at least one collector is producing data.
      { name = "ECS_DISPATCH",              value = "false" },
      { name = "ECS_CLUSTER",               value = var.ecs_cluster_name },
      { name = "PRIVATE_SUBNETS",           value = join(",", var.private_subnet_ids) },
      { name = "TASK_SECURITY_GROUPS",      value = var.task_security_group_id },
      { name = "ENOS_MARKET_TASK_DEF",      value = "${var.name}-enos-market-collector" },
      { name = "TT_API_TASK_DEF",           value = "${var.name}-tt-api-collector" },
      { name = "LINGFENG_TASK_DEF",         value = "${var.name}-lingfeng-collector" },
      { name = "LOG_LEVEL",                 value = "INFO" },
    ])

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = aws_cloudwatch_log_group.freshness.name
        awslogs-region        = var.region
        awslogs-stream-prefix = "ecs"
      }
    }
  }])

  tags = { Project = var.name }
}

# ---------------------------------------------------------------------------
# EventBridge schedules  (rule names: ${var.name}-<task-name>-daily)
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_event_rule" "enos_market_daily" {
  name                = "${var.name}-enos-market-daily"
  description         = "Daily EnOS Mengxi market data collection"
  schedule_expression = "cron(5 20 * * ? *)"   # 04:05 SGT
  tags                = { Project = var.name }
}

resource "aws_cloudwatch_event_rule" "tt_api_daily" {
  name                = "${var.name}-tt-api-daily"
  description         = "Daily TT DAAS API collection (province + node tables)"
  schedule_expression = "cron(55 0 * * ? *)"   # 08:55 SGT — before existing tt-province-loader at 09:10
  tags                = { Project = var.name }
}

resource "aws_cloudwatch_event_rule" "freshness_monitor_daily" {
  name                = "${var.name}-freshness-monitor-daily"
  description         = "Daily freshness check and gap remediation dispatch"
  schedule_expression = "cron(0 3 * * ? *)"    # 11:00 SGT
  tags                = { Project = var.name }
}

# ---------------------------------------------------------------------------
# EventBridge targets
# (pattern from trading-bess-mengxi: arn = var.ecs_cluster_arn,
#  role_arn = var.events_invoke_ecs_role_arn)
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_event_target" "enos_market" {
  rule      = aws_cloudwatch_event_rule.enos_market_daily.name
  target_id = "enos-market-collector"
  arn       = var.ecs_cluster_arn
  role_arn  = var.events_invoke_ecs_role_arn

  ecs_target {
    launch_type         = "FARGATE"
    task_count          = 1
    task_definition_arn = aws_ecs_task_definition.enos_market.arn
    network_configuration {
      subnets          = var.private_subnet_ids
      security_groups  = [var.task_security_group_id]
      assign_public_ip = true
    }
  }
}

resource "aws_cloudwatch_event_target" "tt_api" {
  rule      = aws_cloudwatch_event_rule.tt_api_daily.name
  target_id = "tt-api-collector"
  arn       = var.ecs_cluster_arn
  role_arn  = var.events_invoke_ecs_role_arn

  ecs_target {
    launch_type         = "FARGATE"
    task_count          = 1
    task_definition_arn = aws_ecs_task_definition.tt_api.arn
    network_configuration {
      subnets          = var.private_subnet_ids
      security_groups  = [var.task_security_group_id]
      assign_public_ip = true
    }
  }
}

resource "aws_cloudwatch_event_target" "freshness_monitor" {
  rule      = aws_cloudwatch_event_rule.freshness_monitor_daily.name
  target_id = "freshness-monitor"
  arn       = var.ecs_cluster_arn
  role_arn  = var.events_invoke_ecs_role_arn

  ecs_target {
    launch_type         = "FARGATE"
    task_count          = 1
    task_definition_arn = aws_ecs_task_definition.freshness_monitor.arn
    network_configuration {
      subnets          = var.private_subnet_ids
      security_groups  = [var.task_security_group_id]
      assign_public_ip = true
    }
  }
}
