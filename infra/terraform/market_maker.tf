# ─────────────────────────────────────────────────────────────────────────────
# Market Maker — central risk book (port 8530, Cognito auth)
# Image deploys after v1: jq-swap from the service's current tdArn
# (container_definitions ignored after creation, as asset-risk).
# ─────────────────────────────────────────────────────────────────────────────
resource "aws_ecr_repository" "market_maker" {
  name                 = "bess-market-maker"
  image_tag_mutability = "MUTABLE"
  image_scanning_configuration { scan_on_push = false }
  tags = local.tags
}

resource "aws_lb_target_group" "market_maker" {
  name        = "bess-platform-market-maker"
  port        = 8530
  protocol    = "HTTP"
  vpc_id      = var.vpc_id
  target_type = "ip"
  lifecycle { create_before_destroy = true }
  health_check {
    path                = "/market-maker/_stcore/health"
    protocol            = "HTTP"
    matcher             = "200-399"
    interval            = 30
    timeout             = 10
    healthy_threshold   = 2
    unhealthy_threshold = 3
  }
  tags = local.tags
}

resource "aws_lb_listener_rule" "market_maker_path" {
  listener_arn = aws_lb_listener.https.arn
  priority     = 59

  action {
    type  = "authenticate-cognito"
    order = 1
    authenticate_cognito {
      user_pool_arn       = aws_cognito_user_pool.bess_users.arn
      user_pool_client_id = aws_cognito_user_pool_client.bess_client.id
      user_pool_domain    = aws_cognito_user_pool_domain.main.domain
    }
  }

  action {
    type             = "forward"
    order            = 2
    target_group_arn = aws_lb_target_group.market_maker.arn
  }
  condition {
    path_pattern { values = ["/market-maker", "/market-maker/", "/market-maker/*"] }
  }
}

resource "aws_ecs_task_definition" "market_maker" {
  family                   = "${var.name}-market-maker"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "512"
  memory                   = "2048"
  execution_role_arn       = aws_iam_role.task_execution.arn
  task_role_arn            = aws_iam_role.task_role.arn

  container_definitions = jsonencode([{
    name         = "market-maker"
    image        = var.image_market_maker
    essential    = true
    portMappings = [{ containerPort = 8530, protocol = "tcp" }]
    environment = [
      { name = "PGURL", value = local.db_pgurl_direct },
      { name = "ANTHROPIC_API_KEY", value = var.anthropic_api_key },
      { name = "BEDROCK_REGION", value = "us-east-1" },
      { name = "AWS_REGION", value = var.region },
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = local.log_group
        awslogs-region        = var.region
        awslogs-stream-prefix = "market-maker"
      }
    }
  }])

  lifecycle { ignore_changes = [container_definitions] }
  tags = local.tags
}

resource "aws_ecs_service" "market_maker" {
  name            = "${var.name}-market-maker-svc"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.market_maker.arn
  desired_count   = var.desired_count_market_maker
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = var.private_subnet_ids
    security_groups  = [aws_security_group.ecs_tasks.id]
    assign_public_ip = true
  }
  load_balancer {
    target_group_arn = aws_lb_target_group.market_maker.arn
    container_name   = "market-maker"
    container_port   = 8530
  }
  depends_on = [aws_lb_listener.https]
  tags       = local.tags

  lifecycle { ignore_changes = [task_definition, desired_count] }
}
