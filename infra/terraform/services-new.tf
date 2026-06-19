# ─────────────────────────────────────────────────────────────────────────────
# services-new.tf
# Services added after the initial Terraform bootstrap that are NOT yet in
# main.tf. All other services that were previously here have been removed
# because they were duplicates of definitions already in main.tf.
# ─────────────────────────────────────────────────────────────────────────────

locals {
  db_pgurl_direct = "postgresql://${var.db_username}:${var.db_password}@${aws_db_instance.pg.address}:5432/${var.db_name}?sslmode=require"
}

# ─────────────────────────────────────────────────────────────────────────────
# PH Market  (port 8510, simple forward — no Cognito)
# ─────────────────────────────────────────────────────────────────────────────
resource "aws_ecr_repository" "ph_market" {
  name                 = "bess-ph-market"
  image_tag_mutability = "MUTABLE"
  image_scanning_configuration { scan_on_push = false }
  tags = local.tags
}

import {
  to = aws_ecr_repository.ph_market
  id = "bess-ph-market"
}

resource "aws_lb_target_group" "ph_market" {
  name        = "bess-platform-ph-market"
  port        = 8510
  protocol    = "HTTP"
  vpc_id      = var.vpc_id
  target_type = "ip"
  lifecycle { create_before_destroy = true }
  health_check {
    path                = "/ph-market/_stcore/health"
    protocol            = "HTTP"
    matcher             = "200-399"
    interval            = 30
    timeout             = 10
    healthy_threshold   = 2
    unhealthy_threshold = 3
  }
  tags = local.tags
}

import {
  to = aws_lb_target_group.ph_market
  id = "arn:aws:elasticloadbalancing:ap-southeast-1:319383842493:targetgroup/bess-platform-ph-market/5a313c19ad9bb4ab"
}

resource "aws_lb_listener_rule" "ph_market_path" {
  listener_arn = aws_lb_listener.https.arn
  priority     = 51

  action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.ph_market.arn
  }
  condition {
    path_pattern { values = ["/ph-market", "/ph-market/", "/ph-market/*"] }
  }
}

import {
  to = aws_lb_listener_rule.ph_market_path
  id = "arn:aws:elasticloadbalancing:ap-southeast-1:319383842493:listener-rule/app/bess-platform-alb/bed7b373d923c365/a5747829a4d3e921/4f8d972e87e3cd48"
}

resource "aws_ecs_task_definition" "ph_market" {
  family                   = "${var.name}-ph-market"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "512"
  memory                   = "1024"
  execution_role_arn       = aws_iam_role.task_execution.arn
  task_role_arn            = aws_iam_role.task_role.arn

  container_definitions = jsonencode([{
    name      = "ph-market"
    image     = var.image_ph_market
    essential = true
    portMappings = [{ containerPort = 8510, protocol = "tcp" }]
    environment = [
      { name = "PGURL",             value = local.db_pgurl_direct },
      { name = "ANTHROPIC_API_KEY", value = var.anthropic_api_key },
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = local.log_group
        awslogs-region        = var.region
        awslogs-stream-prefix = "ph-market"
      }
    }
  }])

  lifecycle { ignore_changes = [container_definitions] }
  tags = local.tags
}

import {
  to = aws_ecs_task_definition.ph_market
  id = "arn:aws:ecs:ap-southeast-1:319383842493:task-definition/bess-platform-ph-market:6"
}

resource "aws_ecs_service" "ph_market" {
  name            = "${var.name}-ph-market-svc"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.ph_market.arn
  desired_count   = var.desired_count_ph_market
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = var.private_subnet_ids
    security_groups  = [aws_security_group.ecs_tasks.id]
    assign_public_ip = true
  }
  load_balancer {
    target_group_arn = aws_lb_target_group.ph_market.arn
    container_name   = "ph-market"
    container_port   = 8510
  }
  depends_on = [aws_lb_listener.https]
  tags       = local.tags

  lifecycle { ignore_changes = [task_definition] }
}

import {
  to = aws_ecs_service.ph_market
  id = "bess-platform-cluster/bess-platform-ph-market-svc"
}

# ─────────────────────────────────────────────────────────────────────────────
# PO Market  (port 8511, simple forward — no Cognito)
# ─────────────────────────────────────────────────────────────────────────────
resource "aws_ecr_repository" "po_market" {
  name                 = "bess-po-market"
  image_tag_mutability = "MUTABLE"
  image_scanning_configuration { scan_on_push = false }
  tags = local.tags
}

import {
  to = aws_ecr_repository.po_market
  id = "bess-po-market"
}

resource "aws_lb_target_group" "po_market" {
  name        = "bess-platform-po-market"
  port        = 8511
  protocol    = "HTTP"
  vpc_id      = var.vpc_id
  target_type = "ip"
  lifecycle { create_before_destroy = true }
  health_check {
    path                = "/po-market/_stcore/health"
    protocol            = "HTTP"
    matcher             = "200-399"
    interval            = 30
    timeout             = 10
    healthy_threshold   = 2
    unhealthy_threshold = 3
  }
  tags = local.tags
}

import {
  to = aws_lb_target_group.po_market
  id = "arn:aws:elasticloadbalancing:ap-southeast-1:319383842493:targetgroup/bess-platform-po-market/5e68c6525a8d67ac"
}

resource "aws_lb_listener_rule" "po_market_path" {
  listener_arn = aws_lb_listener.https.arn
  priority     = 52

  action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.po_market.arn
  }
  condition {
    path_pattern { values = ["/po-market", "/po-market/", "/po-market/*"] }
  }
}

import {
  to = aws_lb_listener_rule.po_market_path
  id = "arn:aws:elasticloadbalancing:ap-southeast-1:319383842493:listener-rule/app/bess-platform-alb/bed7b373d923c365/a5747829a4d3e921/73145eb6c4fc154c"
}

resource "aws_ecs_task_definition" "po_market" {
  family                   = "${var.name}-po-market"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "512"
  memory                   = "1024"
  execution_role_arn       = aws_iam_role.task_execution.arn
  task_role_arn            = aws_iam_role.task_role.arn

  container_definitions = jsonencode([{
    name      = "po-market"
    image     = var.image_po_market
    essential = true
    portMappings = [{ containerPort = 8511, protocol = "tcp" }]
    environment = [
      { name = "PGURL",             value = local.db_pgurl_direct },
      { name = "ANTHROPIC_API_KEY", value = var.anthropic_api_key },
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = local.log_group
        awslogs-region        = var.region
        awslogs-stream-prefix = "po-market"
      }
    }
  }])

  lifecycle { ignore_changes = [container_definitions] }
  tags = local.tags
}

import {
  to = aws_ecs_task_definition.po_market
  id = "arn:aws:ecs:ap-southeast-1:319383842493:task-definition/bess-platform-po-market:5"
}

resource "aws_ecs_service" "po_market" {
  name            = "${var.name}-po-market-svc"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.po_market.arn
  desired_count   = var.desired_count_po_market
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = var.private_subnet_ids
    security_groups  = [aws_security_group.ecs_tasks.id]
    assign_public_ip = true
  }
  load_balancer {
    target_group_arn = aws_lb_target_group.po_market.arn
    container_name   = "po-market"
    container_port   = 8511
  }
  depends_on = [aws_lb_listener.https]
  tags       = local.tags

  lifecycle { ignore_changes = [task_definition] }
}

import {
  to = aws_ecs_service.po_market
  id = "bess-platform-cluster/bess-platform-po-market-svc"
}

# ─────────────────────────────────────────────────────────────────────────────
# Crystal Ball Client Terminal  (port 8521, simple forward)
# ─────────────────────────────────────────────────────────────────────────────
resource "aws_ecr_repository" "crystal_ball_client" {
  name                 = "crystal-ball-client"
  image_tag_mutability = "MUTABLE"
  image_scanning_configuration { scan_on_push = false }
  tags = local.tags
}

import {
  to = aws_ecr_repository.crystal_ball_client
  id = "crystal-ball-client"
}

resource "aws_lb_target_group" "crystal_ball_client" {
  name        = "bess-cb-client-tg"
  port        = 8521
  protocol    = "HTTP"
  vpc_id      = var.vpc_id
  target_type = "ip"
  lifecycle { create_before_destroy = true }
  health_check {
    path                = "/crystal-ball-client/_stcore/health"
    protocol            = "HTTP"
    matcher             = "200-399"
    interval            = 30
    timeout             = 10
    healthy_threshold   = 2
    unhealthy_threshold = 3
  }
  tags = local.tags
}

import {
  to = aws_lb_target_group.crystal_ball_client
  id = "arn:aws:elasticloadbalancing:ap-southeast-1:319383842493:targetgroup/bess-cb-client-tg/dc8492cda08f414c"
}

resource "aws_lb_listener_rule" "crystal_ball_client_path" {
  listener_arn = aws_lb_listener.https.arn
  priority     = 26

  action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.crystal_ball_client.arn
  }
  condition {
    path_pattern { values = ["/crystal-ball-client", "/crystal-ball-client/", "/crystal-ball-client/*"] }
  }
}

import {
  to = aws_lb_listener_rule.crystal_ball_client_path
  id = "arn:aws:elasticloadbalancing:ap-southeast-1:319383842493:listener-rule/app/bess-platform-alb/bed7b373d923c365/a5747829a4d3e921/2e9267e259f21101"
}

resource "aws_ecs_task_definition" "crystal_ball_client" {
  family                   = "${var.name}-crystal-ball-client"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "512"
  memory                   = "1024"
  execution_role_arn       = aws_iam_role.task_execution.arn
  task_role_arn            = aws_iam_role.task_role.arn

  container_definitions = jsonencode([{
    name      = "crystal-ball-client"
    image     = var.image_crystal_ball_client
    essential = true
    portMappings = [{ containerPort = 8521, protocol = "tcp" }]
    environment = [
      { name = "PGURL",             value = local.db_pgurl_direct },
      { name = "ANTHROPIC_API_KEY", value = var.anthropic_api_key },
      { name = "AWS_REGION",        value = var.region },
      { name = "TIMEZONE",          value = "Asia/Shanghai" },
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = local.log_group
        awslogs-region        = var.region
        awslogs-stream-prefix = "crystal-ball-client"
      }
    }
  }])

  lifecycle { ignore_changes = [container_definitions] }
  tags = local.tags
}

import {
  to = aws_ecs_task_definition.crystal_ball_client
  id = "arn:aws:ecs:ap-southeast-1:319383842493:task-definition/bess-platform-crystal-ball-client:2"
}

resource "aws_ecs_service" "crystal_ball_client" {
  name            = "${var.name}-crystal-ball-client-svc"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.crystal_ball_client.arn
  desired_count   = var.desired_count_crystal_ball_client
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = var.private_subnet_ids
    security_groups  = [aws_security_group.ecs_tasks.id]
    assign_public_ip = true
  }
  load_balancer {
    target_group_arn = aws_lb_target_group.crystal_ball_client.arn
    container_name   = "crystal-ball-client"
    container_port   = 8521
  }
  depends_on = [aws_lb_listener.https]
  tags       = local.tags

  lifecycle { ignore_changes = [task_definition] }
}

import {
  to = aws_ecs_service.crystal_ball_client
  id = "bess-platform-cluster/bess-platform-crystal-ball-client-svc"
}
