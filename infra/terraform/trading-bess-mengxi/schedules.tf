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
variable "image_mengxi_ingest" {}
variable "db_dsn" { sensitive = true }
variable "tt_app_key"    { sensitive = true }
variable "tt_app_secret" { sensitive = true }
# Individual DB vars for focused_assets_data.py (reads DB_DEFAULTS, not DB_DSN)
variable "db_host"     {}
variable "db_port"     { default = "5432" }
variable "db_user"     { default = "postgres" }
variable "db_password" { sensitive = true }
variable "db_name"     { default = "marketdata" }
variable "log_retention_days" {
  type    = number
  default = 14
}

locals {
  common_env = [
    { name = "AWS_REGION", value = var.region },
    { name = "DB_DSN",     value = var.db_dsn },
    { name = "PGURL",      value = var.db_dsn },
    { name = "PYTHONPATH", value = "/app" }
  ]
}

# Log groups for tt-province-loader, tt-asset-loader, and mengxi-pnl-refresh
# already exist in AWS (created outside this module). They are not managed here
# to avoid ResourceAlreadyExistsException. Names are inlined in task definitions.

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
        { name = "APP_KEY",            value = var.tt_app_key },
        { name = "APP_SECRET",         value = var.tt_app_secret },
        { name = "MARKET_LIST",        value = "Mengxi,Anhui,Shandong,Jiangsu" },
        { name = "FULL_HISTORY",       value = "false" },
        { name = "DB_LOOKBACK_DAYS",   value = "2" },
        { name = "RUN_INHOUSE_WIND",   value = "true" },
        { name = "LOG_LEVEL",          value = "INFO" }
      ])

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = "/ecs/${var.name}/tt-province-loader"
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
        { name = "APP_KEY",          value = var.tt_app_key },
        { name = "APP_SECRET",       value = var.tt_app_secret },
        # Individual DB_* vars required by focused_assets_data.py (_db_engine uses
        # DB_DEFAULTS which reads these; it does not check DB_DSN/PGURL directly).
        # Remove once bess-trading-jobs image is rebuilt with the DB_DSN-aware fix.
        { name = "DB_HOST",          value = var.db_host },
        { name = "DB_PORT",          value = var.db_port },
        { name = "DB_USER",          value = var.db_user },
        { name = "DB_PASSWORD",      value = var.db_password },
        { name = "DB_NAME",          value = var.db_name },
        { name = "MARKET_LIST",      value = "Mengxi_SuYou,Mengxi_WuLaTe,Mengxi_WuHai,Mengxi_WuLanChaBu,Shandong_BinZhou,Anhui_DingYuan,Jiangsu_SheYang" },
        { name = "FULL_HISTORY",     value = "false" },
        { name = "DB_LOOKBACK_DAYS", value = "2" },
        { name = "LOG_LEVEL",        value = "INFO" }
      ])

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = "/ecs/${var.name}/tt-asset-loader"
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
        { name = "DEFAULT_COMPENSATION_YUAN_PER_MWH",  value = "350" },
        { name = "PNL_REFRESH_LOOKBACK_DAYS",          value = "7" },
        { name = "PNL_ENABLE_CANON_COMPAT_VIEWS",      value = "1" },
        { name = "LOG_LEVEL",                          value = "INFO" }
      ])

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = "/ecs/${var.name}/mengxi-pnl-refresh"
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
      assign_public_ip = true
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
      assign_public_ip = true
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
      assign_public_ip = true
    }
  }
}

# ---------------------------------------------------------------------------
# Mengxi Excel ingest — downloads daily Excel from Mengxi portal and loads
# md_id_cleared_energy (dispatch volumes) + md_rt_nodal_price into marketdata
# schema. Must run before the province loader (09:10) and pnl-refresh (10:10).
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_log_group" "mengxi_excel_ingest" {
  name              = "/ecs/${var.name}/mengxi-excel-ingest"
  retention_in_days = 30
}

resource "aws_ecs_task_definition" "mengxi_excel_ingest" {
  family                   = "${var.name}-mengxi-excel-ingest"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "1024"
  memory                   = "2048"
  execution_role_arn       = var.ecs_execution_role_arn
  task_role_arn            = var.ecs_task_role_arn

  container_definitions = jsonencode([
    {
      name      = "mengxi-excel-ingest"
      image     = var.image_mengxi_ingest
      essential = true
      command   = ["python", "run_pipeline.py"]

      environment = concat(local.common_env, [
        { name = "RUN_MODE",        value = "daily" },
        { name = "MARKET_LAG_DAYS", value = "1" },
        { name = "LOG_LEVEL",       value = "INFO" }
      ])

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = aws_cloudwatch_log_group.mengxi_excel_ingest.name
          awslogs-region        = var.region
          awslogs-stream-prefix = "ecs"
        }
      }
    }
  ])
}

resource "aws_cloudwatch_event_rule" "mengxi_excel_ingest_daily" {
  name                = "${var.name}-mengxi-excel-ingest-daily"
  description         = "Daily Mengxi Excel ingest (md_id_cleared_energy + md_rt_nodal_price)"
  schedule_expression = "cron(30 0 * * ? *)" # 08:30 China time — before province loader at 09:10
}

resource "aws_cloudwatch_event_target" "mengxi_excel_ingest_daily" {
  rule      = aws_cloudwatch_event_rule.mengxi_excel_ingest_daily.name
  target_id = "mengxi-excel-ingest"
  arn       = var.ecs_cluster_arn
  role_arn  = var.events_invoke_ecs_role_arn

  ecs_target {
    launch_type         = "FARGATE"
    task_count          = 1
    task_definition_arn = aws_ecs_task_definition.mengxi_excel_ingest.arn
    network_configuration {
      subnets          = var.private_subnet_ids
      security_groups  = [var.task_security_group_id]
      assign_public_ip = true
    }
  }
}
