############################################
# Professor weekly topic card (胖橘花丈量电价)
# One-shot Fargate task on an EventBridge cron (Mondays 08:50 Beijing = 00:50 UTC).
# Gathers KB recent docs + weekly price anomaly scan, asks the editor model for
# 3-5 topic proposals, stores them in marketdata.professor_topic_proposals,
# and sends a Feishu card to the owner. See services/professor/topic_card.py
# and power-academy/columns/xiyangjing/topics/CHOOSING.md for the pick contract.
############################################

resource "aws_ecs_task_definition" "professor_topic_card" {
  family                   = "${var.name}-professor-topic-card"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "512"
  memory                   = "1024"

  execution_role_arn = aws_iam_role.task_execution.arn
  task_role_arn      = aws_iam_role.task_role.arn

  container_definitions = jsonencode([
    {
      name    = "professor-topic-card"
      image   = var.image_professor_topic_card
      command = ["python", "services/professor/topic_card.py", "--send"]
      environment = [
        {
          name  = "PGURL"
          value = "postgresql://${var.db_username}:${var.db_password}@${aws_db_instance.pg.address}:5432/${var.db_name}?sslmode=require"
        },
        { name = "AWS_REGION", value = var.region },
        { name = "ANTHROPIC_API_KEY", value = var.anthropic_api_key },
        { name = "FEISHU_APP_ID", value = var.feishu_app_id },
        { name = "FEISHU_APP_SECRET", value = var.feishu_app_secret },
        { name = "FEISHU_OWNER_OPEN_ID", value = var.feishu_owner_open_id },
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = local.log_group
          awslogs-region        = var.region
          awslogs-stream-prefix = "professor-topic-card"
        }
      }
    }
  ])
}

resource "aws_cloudwatch_event_rule" "professor_topic_card_weekly" {
  name                = "${var.name}-professor-topic-card-weekly"
  description         = "胖橘花 weekly topic proposal card (Mondays 08:50 Beijing)"
  schedule_expression = "cron(50 0 ? * MON *)"
}

resource "aws_iam_role" "professor_topic_card_scheduler" {
  name = "${var.name}-professor-topic-card-scheduler"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "events.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "professor_topic_card_scheduler" {
  name = "${var.name}-professor-topic-card-scheduler"
  role = aws_iam_role.professor_topic_card_scheduler.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "ecs:RunTask"
      Resource = aws_ecs_task_definition.professor_topic_card.arn
    }]
  })
}

resource "aws_cloudwatch_event_target" "professor_topic_card" {
  rule      = aws_cloudwatch_event_rule.professor_topic_card_weekly.name
  target_id = "professor-topic-card"
  arn       = aws_ecs_cluster.this.arn
  role_arn  = aws_iam_role.professor_topic_card_scheduler.arn

  ecs_target {
    task_count          = 1
    task_definition_arn = aws_ecs_task_definition.professor_topic_card.arn
    launch_type         = "FARGATE"
    network_configuration {
      subnets          = var.private_subnet_ids
      security_groups  = [aws_security_group.ecs_tasks.id]
      assign_public_ip = false
    }
  }
}
