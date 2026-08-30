# ─────────────────────────────────────────────────────────────────────────────
# Hermes — messaging hub (Feishu/Telegram/WeChat/OneDrive/Claude bridge)
# Port 8000 on /hermes/*
# Restored 2026-06-19: definition was accidentally removed from main.tf.
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_ecr_repository" "hermes" {
  name                 = "bess-platform-hermes"
  image_tag_mutability = "MUTABLE"
  image_scanning_configuration { scan_on_push = false }
  tags = local.tags
}

import {
  to = aws_ecr_repository.hermes
  id = "bess-platform-hermes"
}

resource "aws_ecr_lifecycle_policy" "hermes" {
  repository = aws_ecr_repository.hermes.name
  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "Keep last 5 images"
      selection    = { tagStatus = "any", countType = "imageCountMoreThan", countNumber = 5 }
      action       = { type = "expire" }
    }]
  })
}

import {
  to = aws_ecr_lifecycle_policy.hermes
  id = "bess-platform-hermes"
}

resource "aws_lb_target_group" "hermes" {
  name_prefix = "tghrm-"
  port        = 8000
  protocol    = "HTTP"
  vpc_id      = var.vpc_id
  target_type = "ip"
  lifecycle { create_before_destroy = true }
  health_check {
    path                = "/hermes/health"
    protocol            = "HTTP"
    matcher             = "200"
    interval            = 30
    timeout             = 5
    healthy_threshold   = 2
    unhealthy_threshold = 3
  }
  tags = local.tags
}

import {
  to = aws_lb_target_group.hermes
  id = "arn:aws:elasticloadbalancing:ap-southeast-1:319383842493:targetgroup/tghrm-20260613091905680300000001/7d89e3fb0cc9f2c3"
}

# Webhook endpoint — no Cognito (must be publicly reachable for WeChat/Feishu callbacks)
resource "aws_lb_listener_rule" "hermes_wecom_webhook" {
  listener_arn = aws_lb_listener.https.arn
  priority     = 53

  action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.hermes.arn
  }
  condition {
    path_pattern { values = ["/hermes/inbound/wecom", "/hermes/inbound/wecom/*"] }
  }
}

import {
  to = aws_lb_listener_rule.hermes_wecom_webhook
  id = "arn:aws:elasticloadbalancing:ap-southeast-1:319383842493:listener-rule/app/bess-platform-alb/bed7b373d923c365/a5747829a4d3e921/b96db8f169b72e8d"
}

# Main hermes UI — Cognito auth
resource "aws_lb_listener_rule" "hermes_path" {
  listener_arn = aws_lb_listener.https.arn
  priority     = 55

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
    target_group_arn = aws_lb_target_group.hermes.arn
  }
  condition {
    path_pattern { values = ["/hermes", "/hermes/*"] }
  }
}

import {
  to = aws_lb_listener_rule.hermes_path
  id = "arn:aws:elasticloadbalancing:ap-southeast-1:319383842493:listener-rule/app/bess-platform-alb/bed7b373d923c365/a5747829a4d3e921/50e01c00bf56f66a"
}

resource "aws_ecs_task_definition" "hermes" {
  family                   = "${var.name}-hermes"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "512"
  memory                   = "2048"  # 1024 OOM-killed (exit 137) on concurrent KB synthesis + monthly PDF parse, 2026-08-11
  execution_role_arn       = aws_iam_role.task_execution.arn
  task_role_arn            = aws_iam_role.task_role.arn

  container_definitions = jsonencode([{
    name      = "hermes"
    image     = "319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-platform-hermes:${var.hermes_image_tag}"
    essential = true
    portMappings = [{ containerPort = 8000, protocol = "tcp" }]
    environment = [
      { name = "PGURL",              value = local.db_pgurl_direct },
      { name = "ANTHROPIC_API_KEY",  value = var.hermes_anthropic_api_key },
      { name = "BEDROCK_REGION",     value = "us-east-1" },
      { name = "WECOM_CORP_ID",      value = var.hermes_wecom_corp_id },
      { name = "WECOM_AGENT_ID",     value = var.hermes_wecom_agent_id },
      { name = "WECOM_SECRET",       value = var.hermes_wecom_secret },
      { name = "WECOM_USER_ID",      value = var.hermes_wecom_user_id },
      { name = "PLANKA_BASE_URL",    value = var.planka_base_url },
      { name = "PLANKA_EMAIL",       value = var.hermes_planka_email },
      { name = "PLANKA_PASSWORD",    value = var.hermes_planka_password },
      { name = "AWS_DEFAULT_REGION", value = var.region },
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = local.log_group
        awslogs-region        = var.region
        awslogs-stream-prefix = "hermes"
      }
    }
  }])

  # Container definitions are updated via ECS direct deploy (image pushes),
  # not via Terraform. Terraform only manages the service lifecycle.
  lifecycle { ignore_changes = [container_definitions] }
  tags = local.tags
}

import {
  to = aws_ecs_task_definition.hermes
  id = "arn:aws:ecs:ap-southeast-1:319383842493:task-definition/bess-platform-hermes:1"
}

resource "aws_ecs_service" "hermes" {
  name            = "${var.name}-hermes-svc"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.hermes.arn
  desired_count   = var.desired_count_hermes
  launch_type     = "FARGATE"

  network_configuration {
    # Dedicated private subnets behind the NAT gateway (nat.tf) — stable
    # egress IP for the Fengxing whitelist. Applied out-of-band via
    # aws ecs update-service; never blanket-apply this drifted service.
    subnets          = [for s in aws_subnet.hermes_private : s.id]
    security_groups  = [aws_security_group.ecs_tasks.id]
    assign_public_ip = false
  }
  load_balancer {
    target_group_arn = aws_lb_target_group.hermes.arn
    container_name   = "hermes"
    container_port   = 8000
  }
  depends_on = [aws_lb_listener.https]
  tags       = local.tags

  lifecycle { ignore_changes = [task_definition] }
}

import {
  to = aws_ecs_service.hermes
  id = "bess-platform-cluster/bess-platform-hermes-svc"
}

# EC2 security group rule — allows Hermes ECS to be reached from Wechaty bridge EC2
resource "aws_security_group_rule" "ec2_to_hermes" {
  type              = "ingress"
  description       = "Hermes Wechaty bridge EC2 to Hermes ECS"
  from_port         = 8000
  to_port           = 8000
  protocol          = "tcp"
  cidr_blocks       = ["172.31.30.155/32"]
  security_group_id = aws_security_group.ecs_tasks.id
}

import {
  to = aws_security_group_rule.ec2_to_hermes
  id = "sgrule-1214278503"
}

# IAM role for Wechaty bridge EC2 (SSM access)
resource "aws_iam_role" "hermes_ec2_ssm" {
  name = "${var.name}-hermes-ec2-ssm"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { Service = "ec2.amazonaws.com" }
    }]
  })
  tags = local.tags
}

import {
  to = aws_iam_role.hermes_ec2_ssm
  id = "bess-platform-hermes-ec2-ssm"
}

resource "aws_iam_instance_profile" "hermes_ec2_ssm" {
  name = "${var.name}-hermes-ec2-ssm"
  role = aws_iam_role.hermes_ec2_ssm.name
  tags = local.tags
}

import {
  to = aws_iam_instance_profile.hermes_ec2_ssm
  id = "bess-platform-hermes-ec2-ssm"
}
