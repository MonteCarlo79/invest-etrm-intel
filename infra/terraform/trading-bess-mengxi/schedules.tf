# infra/terraform/trading-bess-mengxi/schedules.tf

variable "region" {}
variable "name" {}
variable "ecs_cluster_arn" {}
variable "private_subnet_ids" { type = list(string) }
variable "task_security_group_id" {}
variable "ecs_execution_role_arn" {}
variable "ecs_task_role_arn" {}
variable "events_invoke_ecs_role_arn" {}
variable "image_trading_jobs" {}
variable "db_dsn" { sensitive = true }
variable "log_retention_days" {
  type    = number
  default = 14
}

locals {
  common_env = [
    { name = "AWS_REGION", value = var.region },
    { name = "DB_DSN", value = var.db_dsn },
    { name = "PGURL", value = var.db_dsn },
    { name = "PYTHONPATH", value = "/app" }
  ]
}

resource "aws_cloudwatch_log_group" "tt_province_loader" {
  name              = "/ecs/${var.name}/tt-province-loader"
  retention_in_days = var.log_retention_days
}

resource "aws_cloudwatch_log_group" "tt_asset_loader" {
  name              = "/ecs/${var.name}/tt-asset-loader"
  retention_in_days = var.log_retention_days
}

resource "aws_cloudwatch_log_group" "mengxi_pnl_refresh" {
  name              = "/ecs/${var.name}/mengxi-pnl-refresh"
  retention_in_days = var.log_retention_days
}

resource "aws_ecs_task_definition" "tt_province_loader" {
  family                   = "${var.name}-tt-province-loader"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "1024"
  memory                   = "2048"
  execution_role_arn       = var.ecs_execution_role_arn
  task_role_arn            = var.ecs_task_role_arn

  container_definitions = jsonencode([
    {
      name      = "tt-province-loader"
      image     = var.image_trading_jobs
      essential = true
      command   = ["python", "services/loader/province_misc_to_db_v2.py"]

      environment = concat(local.common_env, [
        { name = "MARKET_LIST", value = "Mengxi,Anhui,Shandong,Jiangsu" },
        { name = "FULL_HISTORY", value = "false" },
        { name = "DB_LOOKBACK_DAYS", value = "2" },
        { name = "RUN_INHOUSE_WIND", value = "true" },
        { name = "LOG_LEVEL", value = "INFO" }
      ])

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = aws_cloudwatch_log_group.tt_province_loader.name
          awslogs-region        = var.region
          awslogs-stream-prefix = "ecs"
        }
      }
    }
  ])
}

resource "aws_ecs_task_definition" "tt_asset_loader" {
  family                   = "${var.name}-tt-asset-loader"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "1024"
  memory                   = "2048"
  execution_role_arn       = var.ecs_execution_role_arn
  task_role_arn            = var.ecs_task_role_arn

  container_definitions = jsonencode([
    {
      name      = "tt-asset-loader"
      image     = var.image_trading_jobs
      essential = true
      command   = ["python", "services/common/focused_assets_data.py"]

      environment = concat(local.common_env, [
        { name = "MARKET_LIST", value = "Mengxi_SuYou,Mengxi_WuLaTe,Mengxi_WuHai,Mengxi_WuLanChaBu,Shandong_BinZhou,Anhui_DingYuan,Jiangsu_SheYang" },
        { name = "FULL_HISTORY", value = "false" },
        { name = "DB_LOOKBACK_DAYS", value = "2" },
        { name = "LOG_LEVEL", value = "INFO" }
      ])

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = aws_cloudwatch_log_group.tt_asset_loader.name
          awslogs-region        = var.region
          awslogs-stream-prefix = "ecs"
        }
      }
    }
  ])
}

resource "aws_ecs_task_definition" "mengxi_pnl_refresh" {
  family                   = "${var.name}-mengxi-pnl-refresh"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "1024"
  memory                   = "2048"
  execution_role_arn       = var.ecs_execution_role_arn
  task_role_arn            = var.ecs_task_role_arn

  container_definitions = jsonencode([
    {
      name      = "mengxi-pnl-refresh"
      image     = var.image_trading_jobs
      essential = true
      command   = ["python", "services/trading/bess/mengxi/run_pnl_refresh.py"]

      environment = concat(local.common_env, [
        { name = "DEFAULT_COMPENSATION_YUAN_PER_MWH", value = "350" },
        { name = "PNL_REFRESH_LOOKBACK_DAYS", value = "7" },
        { name = "LOG_LEVEL", value = "INFO" }
      ])

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = aws_cloudwatch_log_group.mengxi_pnl_refresh.name
          awslogs-region        = var.region
          awslogs-stream-prefix = "ecs"
        }
      }
    }
  ])
}

resource "aws_cloudwatch_event_rule" "tt_province_loader_daily" {
  name                = "${var.name}-tt-province-loader-daily"
  description         = "Daily TT province loader"
  schedule_expression = "cron(10 1 * * ? *)" # 09:10 China time
}

resource "aws_cloudwatch_event_rule" "tt_asset_loader_daily" {
  name                = "${var.name}-tt-asset-loader-daily"
  description         = "Daily TT asset loader"
  schedule_expression = "cron(35 1 * * ? *)" # 09:35 China time
}

resource "aws_cloudwatch_event_rule" "mengxi_pnl_refresh_daily" {
  name                = "${var.name}-mengxi-pnl-refresh-daily"
  description         = "Daily Mengxi P&L refresh"
  schedule_expression = "cron(10 2 * * ? *)" # 10:10 China time
}

resource "aws_cloudwatch_event_target" "tt_province_loader_daily" {
  rule      = aws_cloudwatch_event_rule.tt_province_loader_daily.name
  target_id = "tt-province-loader"
  arn       = var.ecs_cluster_arn
  role_arn  = var.events_invoke_ecs_role_arn

  ecs_target {
    launch_type         = "FARGATE"
    task_count          = 1
    task_definition_arn = aws_ecs_task_definition.tt_province_loader.arn
    network_configuration {
      subnets          = var.private_subnet_ids
      security_groups  = [var.task_security_group_id]
      assign_public_ip = false
    }
  }
}

resource "aws_cloudwatch_event_target" "tt_asset_loader_daily" {
  rule      = aws_cloudwatch_event_rule.tt_asset_loader_daily.name
  target_id = "tt-asset-loader"
  arn       = var.ecs_cluster_arn
  role_arn  = var.events_invoke_ecs_role_arn

  ecs_target {
    launch_type         = "FARGATE"
    task_count          = 1
    task_definition_arn = aws_ecs_task_definition.tt_asset_loader.arn
    network_configuration {
      subnets          = var.private_subnet_ids
      security_groups  = [var.task_security_group_id]
      assign_public_ip = false
    }
  }
}

resource "aws_cloudwatch_event_target" "mengxi_pnl_refresh_daily" {
  rule      = aws_cloudwatch_event_rule.mengxi_pnl_refresh_daily.name
  target_id = "mengxi-pnl-refresh"
  arn       = var.ecs_cluster_arn
  role_arn  = var.events_invoke_ecs_role_arn

  ecs_target {
    launch_type         = "FARGATE"
    task_count          = 1
    task_definition_arn = aws_ecs_task_definition.mengxi_pnl_refresh.arn
    network_configuration {
      subnets          = var.private_subnet_ids
      security_groups  = [var.task_security_group_id]
      assign_public_ip = false
    }
  }
}
