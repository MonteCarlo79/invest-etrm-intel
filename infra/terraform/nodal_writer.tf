############################################
# Nodal Trading daily writer (L3 asset agents)
#
# One-shot Fargate task on an EventBridge cron (16:00 CST = 08:00 UTC, before
# the 17:00 D+1 nomination cutoff). Runs services/nodal_agents/daily_job.py:
#   run_day(tomorrow) + register_strategies(yesterday).
# Image tracks the mengxi-dashboard app image so app deploys carry the writer.
############################################

resource "aws_ecs_task_definition" "nodal_writer" {
  family                   = "${var.name}-nodal-writer"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  # LP fleet solve OOM-killed at 0.5 vCPU / 2 GB in the first dry-runs (exit
  # 137); 1 vCPU / 4 GB completed 23-plant runs with headroom.
  cpu                      = "1024"
  memory                   = "4096"

  execution_role_arn = aws_iam_role.task_execution.arn
  task_role_arn      = aws_iam_role.task_role.arn

  container_definitions = jsonencode([
    {
      name    = "nodal-writer"
      image   = var.image_mengxi_dashboard
      command = ["python", "-m", "services.nodal_agents.daily_job"]
      environment = [
        {
          name  = "PGURL"
          value = "postgresql://${var.db_username}:${var.db_password}@${aws_db_instance.pg.address}:5432/${var.db_name}?sslmode=require"
        },
        { name = "AWS_REGION", value = var.region },
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = local.log_group
          awslogs-region        = var.region
          awslogs-stream-prefix = "nodal-writer"
        }
      }
    }
  ])
}

resource "aws_cloudwatch_event_rule" "nodal_writer_daily" {
  name                = "${var.name}-nodal-writer-daily"
  description         = "L3 nodal asset agents: D+1 strategies + D-1 promote-loop evaluation"
  schedule_expression = "cron(0 8 * * ? *)" # 16:00 Asia/Shanghai, before the 17:00 nomination cutoff
}

resource "aws_iam_role" "nodal_writer_scheduler" {
  name = "${var.name}-nodal-writer-scheduler"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "events.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "nodal_writer_scheduler" {
  name = "${var.name}-nodal-writer-scheduler"
  role = aws_iam_role.nodal_writer_scheduler.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "ecs:RunTask"
        Resource = aws_ecs_task_definition.nodal_writer.arn
      },
      {
        Effect   = "Allow"
        Action   = "iam:PassRole"
        Resource = [aws_iam_role.task_execution.arn, aws_iam_role.task_role.arn]
      }
    ]
  })
}

resource "aws_cloudwatch_event_target" "nodal_writer_daily" {
  rule     = aws_cloudwatch_event_rule.nodal_writer_daily.name
  arn      = aws_ecs_cluster.this.arn
  role_arn = aws_iam_role.nodal_writer_scheduler.arn

  ecs_target {
    task_definition_arn = aws_ecs_task_definition.nodal_writer.arn
    launch_type         = "FARGATE"
    network_configuration {
      subnets          = var.private_subnet_ids
      security_groups  = [aws_security_group.ecs_tasks.id]
      assign_public_ip = false
    }
  }
}
