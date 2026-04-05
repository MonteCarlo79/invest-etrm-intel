# infra/terraform/trading-bess-mengxi/dashboard.tf
#
# Mengxi P&L Attribution dashboard — ECS Fargate service + ALB routing.
#
# The ECS service, target group, and listener rule were created prior to this
# module being written. Before the first `terraform apply`, run terraform import
# for each resource (see README or import commands in DEPLOYMENT.md).

# ---------------------------------------------------------------------------
# Variables (dashboard-specific; shared infra vars are in schedules.tf)
# ---------------------------------------------------------------------------

variable "image_pnl_attribution" {}

variable "alb_https_listener_arn" {}

variable "vpc_id" {}

variable "cognito_user_pool_arn" {}

variable "cognito_user_pool_client_id" {}

variable "cognito_user_pool_domain" {}

# ---------------------------------------------------------------------------
# CloudWatch log group
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_log_group" "pnl_attribution" {
  name              = "/ecs/${var.name}/pnl-attribution"
  retention_in_days = var.log_retention_days
}

# ---------------------------------------------------------------------------
# ECS task definition
# ---------------------------------------------------------------------------

resource "aws_ecs_task_definition" "pnl_attribution" {
  family                   = "${var.name}-pnl-attribution"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "512"
  memory                   = "1024"
  execution_role_arn       = var.ecs_execution_role_arn
  task_role_arn            = var.ecs_task_role_arn

  container_definitions = jsonencode([
    {
      name      = "pnl-attribution"
      image     = var.image_pnl_attribution
      essential = true

      portMappings = [
        { containerPort = 8502, protocol = "tcp" }
      ]

      environment = [
        { name = "AWS_REGION", value = var.region },
        { name = "DB_DSN",     value = var.db_dsn },
        { name = "PGURL",      value = var.db_dsn },
      ]

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = aws_cloudwatch_log_group.pnl_attribution.name
          awslogs-region        = var.region
          awslogs-stream-prefix = "ecs"
        }
      }
    }
  ])
}

# ---------------------------------------------------------------------------
# ALB target group
# ---------------------------------------------------------------------------

resource "aws_lb_target_group" "pnl_attribution" {
  name        = "${var.name}-pnl-attr-tg"
  port        = 8502
  protocol    = "HTTP"
  target_type = "ip"
  vpc_id      = var.vpc_id

  health_check {
    path                = "/pnl-attribution/_stcore/health"
    interval            = 30
    healthy_threshold   = 2
    unhealthy_threshold = 3
    timeout             = 5
    matcher             = "200"
  }
}

# ---------------------------------------------------------------------------
# ALB listener rule — Cognito auth then forward
# ---------------------------------------------------------------------------

resource "aws_lb_listener_rule" "pnl_attribution" {
  listener_arn = var.alb_https_listener_arn
  priority     = 25

  condition {
    path_pattern {
      values = ["/pnl-attribution", "/pnl-attribution/", "/pnl-attribution/*"]
    }
  }

  # Step 1: authenticate via Cognito (ALB-managed, no code in app)
  action {
    type  = "authenticate-cognito"
    order = 1

    authenticate_cognito {
      user_pool_arn              = var.cognito_user_pool_arn
      user_pool_client_id        = var.cognito_user_pool_client_id
      user_pool_domain           = var.cognito_user_pool_domain
      on_unauthenticated_request = "authenticate"
      scope                      = "openid"
    }
  }

  # Step 2: forward authenticated traffic to the container
  action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.pnl_attribution.arn
    order            = 2
  }
}

# ---------------------------------------------------------------------------
# ECS service
# ---------------------------------------------------------------------------

resource "aws_ecs_service" "pnl_attribution" {
  name            = "${var.name}-pnl-attribution-svc"
  cluster         = var.ecs_cluster_arn
  task_definition = aws_ecs_task_definition.pnl_attribution.arn
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = var.private_subnet_ids
    security_groups  = [var.task_security_group_id]
    assign_public_ip = true
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.pnl_attribution.arn
    container_name   = "pnl-attribution"
    container_port   = 8502
  }

  # Allow rolling deploy: bring up new task before draining old one
  deployment_minimum_healthy_percent = 100
  deployment_maximum_percent         = 200

  # Auto-rollback on failed deploy
  deployment_circuit_breaker {
    enable   = true
    rollback = true
  }

  # Grace period so ALB doesn't deregister the task before Streamlit is ready
  health_check_grace_period_seconds = 30

  # Don't replace service when task_definition revision increments via deploy
  lifecycle {
    ignore_changes = [task_definition]
  }
}
