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
    name         = "ph-market"
    image        = var.image_ph_market
    essential    = true
    portMappings = [{ containerPort = 8510, protocol = "tcp" }]
    environment = [
      { name = "PGURL", value = local.db_pgurl_direct },
      { name = "ANTHROPIC_API_KEY", value = var.anthropic_api_key },
      { name = "BEDROCK_REGION", value = "us-east-1" },
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
    name         = "po-market"
    image        = var.image_po_market
    essential    = true
    portMappings = [{ containerPort = 8511, protocol = "tcp" }]
    environment = [
      { name = "PGURL", value = local.db_pgurl_direct },
      { name = "ANTHROPIC_API_KEY", value = var.anthropic_api_key },
      { name = "BEDROCK_REGION", value = "us-east-1" },
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
# Deal Structurer  (port 8522, Cognito-protected)
# ─────────────────────────────────────────────────────────────────────────────
resource "aws_ecr_repository" "deal_structurer" {
  name                 = "bess-platform-deal-structurer"
  image_tag_mutability = "MUTABLE"
  image_scanning_configuration { scan_on_push = false }
  tags = local.tags
}

resource "aws_lb_target_group" "deal_structurer" {
  name        = "bess-platform-deal-structurer"
  port        = 8522
  protocol    = "HTTP"
  vpc_id      = var.vpc_id
  target_type = "ip"
  lifecycle { create_before_destroy = true }
  health_check {
    path                = "/deal-structurer/_stcore/health"
    protocol            = "HTTP"
    matcher             = "200-399"
    interval            = 30
    timeout             = 10
    healthy_threshold   = 2
    unhealthy_threshold = 3
  }
  tags = local.tags
}

resource "aws_lb_listener_rule" "deal_structurer_path" {
  listener_arn = aws_lb_listener.https.arn
  priority     = 56

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
    target_group_arn = aws_lb_target_group.deal_structurer.arn
  }

  condition {
    path_pattern { values = ["/deal-structurer", "/deal-structurer/", "/deal-structurer/*"] }
  }
}

resource "aws_ecs_task_definition" "deal_structurer" {
  family                   = "${var.name}-deal-structurer"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "1024"
  memory                   = "2048"
  execution_role_arn       = aws_iam_role.task_execution.arn
  task_role_arn            = aws_iam_role.task_role.arn

  container_definitions = jsonencode([{
    name         = "deal-structurer"
    image        = var.image_deal_structurer
    essential    = true
    portMappings = [{ containerPort = 8522, protocol = "tcp" }]
    environment = [
      { name = "PGURL", value = local.db_pgurl_direct },
      { name = "ANTHROPIC_API_KEY", value = var.anthropic_api_key },
      { name = "BEDROCK_REGION", value = "us-east-1" },
      # thread caps (match live td rev 18; prevent numpy/scipy CPU oversubscription on Fargate)
      { name = "OPENBLAS_NUM_THREADS", value = "1" },
      { name = "OMP_NUM_THREADS", value = "1" },
      { name = "MKL_NUM_THREADS", value = "1" },
      { name = "VECLIB_MAXIMUM_THREADS", value = "1" },
      { name = "NUMEXPR_NUM_THREADS", value = "1" },
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = local.log_group
        awslogs-region        = var.region
        awslogs-stream-prefix = "deal-structurer"
      }
    }
  }])

  lifecycle { ignore_changes = [container_definitions] }
  tags = local.tags
}

import {
  to = aws_ecs_task_definition.deal_structurer
  id = "arn:aws:ecs:ap-southeast-1:319383842493:task-definition/bess-platform-deal-structurer:18"
}

resource "aws_ecs_service" "deal_structurer" {
  name            = "${var.name}-deal-structurer-svc"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.deal_structurer.arn
  desired_count   = var.desired_count_deal_structurer
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = var.private_subnet_ids
    security_groups  = [aws_security_group.ecs_tasks.id]
    assign_public_ip = true
  }
  load_balancer {
    target_group_arn = aws_lb_target_group.deal_structurer.arn
    container_name   = "deal-structurer"
    container_port   = 8522
  }
  depends_on = [aws_lb_listener.https]
  tags       = local.tags

  lifecycle { ignore_changes = [task_definition] }
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
    name         = "crystal-ball-client"
    image        = var.image_crystal_ball_client
    essential    = true
    portMappings = [{ containerPort = 8521, protocol = "tcp" }]
    environment = [
      { name = "PGURL", value = local.db_pgurl_direct },
      { name = "ANTHROPIC_API_KEY", value = var.anthropic_api_key },
      { name = "BEDROCK_REGION", value = "us-east-1" },
      { name = "AWS_REGION", value = var.region },
      { name = "TIMEZONE", value = "Asia/Shanghai" },
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

# ─────────────────────────────────────────────────────────────────────────────
# Asset Risk Management  (port 8512, Cognito auth)
# ─────────────────────────────────────────────────────────────────────────────
resource "aws_lb_target_group" "asset_risk" {
  name        = "bess-platform-asset-risk"
  port        = 8512
  protocol    = "HTTP"
  vpc_id      = var.vpc_id
  target_type = "ip"
  lifecycle { create_before_destroy = true }
  health_check {
    path                = "/asset-risk/_stcore/health"
    protocol            = "HTTP"
    matcher             = "200-399"
    interval            = 30
    timeout             = 10
    healthy_threshold   = 2
    unhealthy_threshold = 3
  }
  tags = local.tags
}

resource "aws_lb_listener_rule" "asset_risk_path" {
  listener_arn = aws_lb_listener.https.arn
  priority     = 57

  action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.asset_risk.arn
  }
  condition {
    path_pattern { values = ["/asset-risk", "/asset-risk/", "/asset-risk/*"] }
  }
}

resource "aws_ecs_task_definition" "asset_risk" {
  family                   = "${var.name}-asset-risk"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "512"
  memory                   = "1024"
  execution_role_arn       = aws_iam_role.task_execution.arn
  task_role_arn            = aws_iam_role.task_role.arn

  container_definitions = jsonencode([{
    name         = "asset-risk"
    image        = var.image_asset_risk
    essential    = true
    portMappings = [{ containerPort = 8512, protocol = "tcp" }]
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
        awslogs-stream-prefix = "asset-risk"
      }
    }
  }])

  lifecycle { ignore_changes = [container_definitions] }
  tags = local.tags
}

resource "aws_ecs_service" "asset_risk" {
  name            = "${var.name}-asset-risk-svc"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.asset_risk.arn
  desired_count   = var.desired_count_asset_risk
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = var.private_subnet_ids
    security_groups  = [aws_security_group.ecs_tasks.id]
    assign_public_ip = true
  }
  load_balancer {
    target_group_arn = aws_lb_target_group.asset_risk.arn
    container_name   = "asset-risk"
    container_port   = 8512
  }
  depends_on = [aws_lb_listener.https]
  tags       = local.tags

  lifecycle { ignore_changes = [task_definition] }
}

# ─────────────────────────────────────────────────────────────────────────────
# Retail Risk Management  (port 8513, Cognito auth)
# ─────────────────────────────────────────────────────────────────────────────
resource "aws_lb_target_group" "retail_risk" {
  name        = "bess-platform-retail-risk"
  port        = 8513
  protocol    = "HTTP"
  vpc_id      = var.vpc_id
  target_type = "ip"
  lifecycle { create_before_destroy = true }
  health_check {
    path                = "/retail-risk/_stcore/health"
    protocol            = "HTTP"
    matcher             = "200-399"
    interval            = 30
    timeout             = 10
    healthy_threshold   = 2
    unhealthy_threshold = 3
  }
  tags = local.tags
}

resource "aws_lb_listener_rule" "retail_risk_path" {
  listener_arn = aws_lb_listener.https.arn
  priority     = 58

  action {
    type = "authenticate-cognito"
    authenticate_cognito {
      user_pool_arn       = aws_cognito_user_pool.bess_users.arn
      user_pool_client_id = aws_cognito_user_pool_client.bess_client.id
      user_pool_domain    = aws_cognito_user_pool_domain.main.domain
    }
  }
  action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.retail_risk.arn
  }
  condition {
    path_pattern { values = ["/retail-risk", "/retail-risk/", "/retail-risk/*"] }
  }
}

resource "aws_ecs_task_definition" "retail_risk" {
  family                   = "${var.name}-retail-risk"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "512"
  memory                   = "1024"
  execution_role_arn       = aws_iam_role.task_execution.arn
  task_role_arn            = aws_iam_role.task_role.arn

  container_definitions = jsonencode([{
    name         = "retail-risk"
    image        = var.image_retail_risk
    essential    = true
    portMappings = [{ containerPort = 8513, protocol = "tcp" }]
    environment = [
      { name = "PGURL", value = local.db_pgurl_direct },
      { name = "ANTHROPIC_API_KEY", value = var.anthropic_api_key },
      { name = "BEDROCK_REGION", value = "ap-southeast-1" },
      { name = "AWS_REGION", value = var.region },
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = local.log_group
        awslogs-region        = var.region
        awslogs-stream-prefix = "retail-risk"
      }
    }
  }])

  lifecycle { ignore_changes = [container_definitions] }
  tags = local.tags
}

resource "aws_ecs_service" "retail_risk" {
  name            = "${var.name}-retail-risk-svc"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.retail_risk.arn
  desired_count   = var.desired_count_retail_risk
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = var.private_subnet_ids
    security_groups  = [aws_security_group.ecs_tasks.id]
    assign_public_ip = true
  }
  load_balancer {
    target_group_arn = aws_lb_target_group.retail_risk.arn
    container_name   = "retail-risk"
    container_port   = 8513
  }
  depends_on = [aws_lb_listener.https]
  tags       = local.tags

  lifecycle { ignore_changes = [task_definition] }
}
