# ─────────────────────────────────────────────────────────────────────────────
# services-new.tf
# All services added after the initial Terraform bootstrap.
# Import blocks bring existing AWS resources into state so terraform plan
# shows 0 changes after import. Run: terraform apply (import happens first).
# ─────────────────────────────────────────────────────────────────────────────

locals {
  db_pgurl_direct = "postgresql://${var.db_username}:${var.db_password}@${aws_db_instance.pg.address}:5432/${var.db_name}?sslmode=require"
}

# ─────────────────────────────────────────────────────────────────────────────
# IAM: Bedrock policy on task role
# ─────────────────────────────────────────────────────────────────────────────
resource "aws_iam_role_policy" "task_bedrock" {
  name = "${var.name}-task-bedrock"
  role = aws_iam_role.task_role.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["bedrock:InvokeModel", "bedrock:InvokeModelWithResponseStream"]
      Resource = "*"
    }]
  })
}

import {
  to = aws_iam_role_policy.task_bedrock
  id = "bess-platform-task-role:bess-platform-task-bedrock"
}

# ─────────────────────────────────────────────────────────────────────────────
# Options Cockpit  (port 8507)
# ─────────────────────────────────────────────────────────────────────────────
resource "aws_ecr_repository" "options_cockpit" {
  name                 = "bess-options-cockpit"
  image_tag_mutability = "MUTABLE"
  image_scanning_configuration { scan_on_push = false }
  tags = local.tags
}

import {
  to = aws_ecr_repository.options_cockpit
  id = "bess-options-cockpit"
}

resource "aws_lb_target_group" "options_cockpit" {
  name_prefix = "tgopc-"
  port        = 8507
  protocol    = "HTTP"
  vpc_id      = var.vpc_id
  target_type = "ip"
  lifecycle { create_before_destroy = true }
  health_check {
    path                = "/options-cockpit/_stcore/health"
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
  to = aws_lb_target_group.options_cockpit
  id = "arn:aws:elasticloadbalancing:ap-southeast-1:319383842493:targetgroup/tgopc-20260426005325424500000001/3e6c34caa6843329"
}

resource "aws_lb_listener_rule" "options_cockpit_path" {
  listener_arn = aws_lb_listener.https.arn
  priority     = 29

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
    target_group_arn = aws_lb_target_group.options_cockpit.arn
  }
  condition {
    path_pattern { values = ["/options-cockpit", "/options-cockpit/", "/options-cockpit/*"] }
  }
}

import {
  to = aws_lb_listener_rule.options_cockpit_path
  id = "arn:aws:elasticloadbalancing:ap-southeast-1:319383842493:listener-rule/app/bess-platform-alb/bed7b373d923c365/a5747829a4d3e921/50c5a3da08a32893"
}

resource "aws_ecs_task_definition" "options_cockpit" {
  family                   = "${var.name}-options-cockpit"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "512"
  memory                   = "1024"
  execution_role_arn       = aws_iam_role.task_execution.arn
  task_role_arn            = aws_iam_role.task_role.arn

  container_definitions = jsonencode([{
    name      = "options-cockpit"
    image     = var.image_options_cockpit
    essential = true
    portMappings = [{ containerPort = 8507, protocol = "tcp" }]
    environment = [
      { name = "AWS_REGION", value = var.region }
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = local.log_group
        awslogs-region        = var.region
        awslogs-stream-prefix = "options-cockpit"
      }
    }
  }])

  lifecycle { ignore_changes = [container_definitions] }
  tags = local.tags
}

import {
  to = aws_ecs_task_definition.options_cockpit
  id = "arn:aws:ecs:ap-southeast-1:319383842493:task-definition/bess-platform-options-cockpit:7"
}

resource "aws_ecs_service" "options_cockpit" {
  name            = "${var.name}-options-cockpit-svc"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.options_cockpit.arn
  desired_count   = var.desired_count_options_cockpit
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = var.private_subnet_ids
    security_groups  = [aws_security_group.ecs_tasks.id]
    assign_public_ip = true
  }
  load_balancer {
    target_group_arn = aws_lb_target_group.options_cockpit.arn
    container_name   = "options-cockpit"
    container_port   = 8507
  }
  depends_on = [aws_lb_listener.https]
  tags       = local.tags

  lifecycle { ignore_changes = [task_definition] }
}

import {
  to = aws_ecs_service.options_cockpit
  id = "bess-platform-cluster/bess-platform-options-cockpit-svc"
}

# ─────────────────────────────────────────────────────────────────────────────
# GB Market  (port 8508)
# ─────────────────────────────────────────────────────────────────────────────
resource "aws_ecr_repository" "gb_market" {
  name                 = "bess-gb-market"
  image_tag_mutability = "MUTABLE"
  image_scanning_configuration { scan_on_push = false }
  tags = local.tags
}

import {
  to = aws_ecr_repository.gb_market
  id = "bess-gb-market"
}

resource "aws_lb_target_group" "gb_market" {
  name_prefix = "tggb-"
  port        = 8508
  protocol    = "HTTP"
  vpc_id      = var.vpc_id
  target_type = "ip"
  lifecycle { create_before_destroy = true }
  health_check {
    path                = "/gb-market/_stcore/health"
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
  to = aws_lb_target_group.gb_market
  id = "arn:aws:elasticloadbalancing:ap-southeast-1:319383842493:targetgroup/tggb-20260510162441233200000001/2e5fb817a1472c53"
}

resource "aws_lb_listener_rule" "gb_market_path" {
  listener_arn = aws_lb_listener.https.arn
  priority     = 45

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
    target_group_arn = aws_lb_target_group.gb_market.arn
  }
  condition {
    path_pattern { values = ["/gb-market", "/gb-market/", "/gb-market/*"] }
  }
}

import {
  to = aws_lb_listener_rule.gb_market_path
  id = "arn:aws:elasticloadbalancing:ap-southeast-1:319383842493:listener-rule/app/bess-platform-alb/bed7b373d923c365/a5747829a4d3e921/cdff8829f1bdad05"
}

resource "aws_ecs_task_definition" "gb_market" {
  family                   = "${var.name}-gb-market"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "512"
  memory                   = "1024"
  execution_role_arn       = aws_iam_role.task_execution.arn
  task_role_arn            = aws_iam_role.task_role.arn

  container_definitions = jsonencode([{
    name      = "gb-market"
    image     = var.image_gb_market
    essential = true
    portMappings = [{ containerPort = 8508, protocol = "tcp" }]
    environment = [
      { name = "PGURL",            value = local.db_pgurl_direct },
      { name = "ANTHROPIC_API_KEY", value = var.anthropic_api_key },
      { name = "MODO_API_KEY",     value = var.modo_api_key },
      { name = "MODO_EMAIL",       value = var.modo_email },
      { name = "MODO_PASSWORD",    value = var.modo_password },
      { name = "WECOM_WEBHOOK_URL", value = var.wecom_webhook_url },
      { name = "SMTP_HOST",        value = var.smtp_host },
      { name = "SMTP_PORT",        value = var.smtp_port },
      { name = "SMTP_USER",        value = var.smtp_user },
      { name = "SMTP_PASSWORD",    value = var.smtp_password },
      { name = "REPORT_FROM_EMAIL", value = var.smtp_user },
      { name = "REPORT_TO_EMAIL",  value = var.report_email_to },
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = local.log_group
        awslogs-region        = var.region
        awslogs-stream-prefix = "gb-market"
      }
    }
  }])

  lifecycle { ignore_changes = [container_definitions] }
  tags = local.tags
}

import {
  to = aws_ecs_task_definition.gb_market
  id = "arn:aws:ecs:ap-southeast-1:319383842493:task-definition/bess-platform-gb-market:73"
}

resource "aws_ecs_service" "gb_market" {
  name            = "${var.name}-gb-market-svc"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.gb_market.arn
  desired_count   = var.desired_count_gb_market
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = var.private_subnet_ids
    security_groups  = [aws_security_group.ecs_tasks.id]
    assign_public_ip = true
  }
  load_balancer {
    target_group_arn = aws_lb_target_group.gb_market.arn
    container_name   = "gb-market"
    container_port   = 8508
  }
  depends_on = [aws_lb_listener.https]
  tags       = local.tags

  lifecycle { ignore_changes = [task_definition] }
}

import {
  to = aws_ecs_service.gb_market
  id = "bess-platform-cluster/bess-platform-gb-market-svc"
}

# ─────────────────────────────────────────────────────────────────────────────
# AU Market  (port 8509)
# ─────────────────────────────────────────────────────────────────────────────
resource "aws_ecr_repository" "au_market" {
  name                 = "bess-au-market"
  image_tag_mutability = "MUTABLE"
  image_scanning_configuration { scan_on_push = false }
  tags = local.tags
}

import {
  to = aws_ecr_repository.au_market
  id = "bess-au-market"
}

resource "aws_lb_target_group" "au_market" {
  name     = "bess-platform-au-market"
  port     = 8509
  protocol = "HTTP"
  vpc_id   = var.vpc_id
  target_type = "ip"
  lifecycle { create_before_destroy = true }
  health_check {
    path                = "/au-market/_stcore/health"
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
  to = aws_lb_target_group.au_market
  id = "arn:aws:elasticloadbalancing:ap-southeast-1:319383842493:targetgroup/bess-platform-au-market/be0d2bb967017494"
}

resource "aws_lb_listener_rule" "au_market_path" {
  listener_arn = aws_lb_listener.https.arn
  priority     = 46

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
    target_group_arn = aws_lb_target_group.au_market.arn
  }
  condition {
    path_pattern { values = ["/au-market", "/au-market/", "/au-market/*"] }
  }
}

import {
  to = aws_lb_listener_rule.au_market_path
  id = "arn:aws:elasticloadbalancing:ap-southeast-1:319383842493:listener-rule/app/bess-platform-alb/bed7b373d923c365/a5747829a4d3e921/b9b9cf246d7dc611"
}

resource "aws_ecs_task_definition" "au_market" {
  family                   = "${var.name}-au-market"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "512"
  memory                   = "1024"
  execution_role_arn       = aws_iam_role.task_execution.arn
  task_role_arn            = aws_iam_role.task_role.arn

  container_definitions = jsonencode([{
    name      = "au-market"
    image     = var.image_au_market
    essential = true
    portMappings = [{ containerPort = 8509, protocol = "tcp" }]
    environment = [
      { name = "PGURL",            value = local.db_pgurl_direct },
      { name = "ANTHROPIC_API_KEY", value = var.anthropic_api_key },
      { name = "MODO_API_KEY",     value = var.modo_api_key },
      { name = "MODO_EMAIL",       value = var.modo_email },
      { name = "MODO_PASSWORD",    value = var.modo_password },
      { name = "SMTP_HOST",        value = var.smtp_host },
      { name = "SMTP_PORT",        value = var.smtp_port },
      { name = "SMTP_USER",        value = var.smtp_user },
      { name = "SMTP_PASSWORD",    value = var.smtp_password },
      { name = "REPORT_TO_EMAIL",  value = var.report_email_to },
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = local.log_group
        awslogs-region        = var.region
        awslogs-stream-prefix = "au-market"
      }
    }
  }])

  lifecycle { ignore_changes = [container_definitions] }
  tags = local.tags
}

import {
  to = aws_ecs_task_definition.au_market
  id = "arn:aws:ecs:ap-southeast-1:319383842493:task-definition/bess-platform-au-market:7"
}

resource "aws_ecs_service" "au_market" {
  name            = "${var.name}-au-market-svc"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.au_market.arn
  desired_count   = var.desired_count_au_market
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = var.private_subnet_ids
    security_groups  = [aws_security_group.ecs_tasks.id]
    assign_public_ip = true
  }
  load_balancer {
    target_group_arn = aws_lb_target_group.au_market.arn
    container_name   = "au-market"
    container_port   = 8509
  }
  depends_on = [aws_lb_listener.https]
  tags       = local.tags

  lifecycle { ignore_changes = [task_definition] }
}

import {
  to = aws_ecs_service.au_market
  id = "bess-platform-cluster/bess-platform-au-market-svc"
}

# ─────────────────────────────────────────────────────────────────────────────
# ERCOT Market  (port 8510)
# ─────────────────────────────────────────────────────────────────────────────
resource "aws_ecr_repository" "ercot_market" {
  name                 = "bess-ercot-market"
  image_tag_mutability = "MUTABLE"
  image_scanning_configuration { scan_on_push = false }
  tags = local.tags
}

import {
  to = aws_ecr_repository.ercot_market
  id = "bess-ercot-market"
}

resource "aws_lb_target_group" "ercot_market" {
  name     = "bess-platform-ercot-market"
  port     = 8510
  protocol = "HTTP"
  vpc_id   = var.vpc_id
  target_type = "ip"
  lifecycle { create_before_destroy = true }
  health_check {
    path                = "/ercot-market/_stcore/health"
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
  to = aws_lb_target_group.ercot_market
  id = "arn:aws:elasticloadbalancing:ap-southeast-1:319383842493:targetgroup/bess-platform-ercot-market/bf89ee9bf88a0428"
}

resource "aws_lb_listener_rule" "ercot_market_path" {
  listener_arn = aws_lb_listener.https.arn
  priority     = 47

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
    target_group_arn = aws_lb_target_group.ercot_market.arn
  }
  condition {
    path_pattern { values = ["/ercot-market", "/ercot-market/", "/ercot-market/*"] }
  }
}

import {
  to = aws_lb_listener_rule.ercot_market_path
  id = "arn:aws:elasticloadbalancing:ap-southeast-1:319383842493:listener-rule/app/bess-platform-alb/bed7b373d923c365/a5747829a4d3e921/992aeb902baa1a52"
}

resource "aws_ecs_task_definition" "ercot_market" {
  family                   = "${var.name}-ercot-market"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "512"
  memory                   = "1024"
  execution_role_arn       = aws_iam_role.task_execution.arn
  task_role_arn            = aws_iam_role.task_role.arn

  container_definitions = jsonencode([{
    name      = "ercot-market"
    image     = var.image_ercot_market
    essential = true
    portMappings = [{ containerPort = 8510, protocol = "tcp" }]
    environment = [
      { name = "PGURL",            value = local.db_pgurl_direct },
      { name = "ANTHROPIC_API_KEY", value = var.anthropic_api_key },
      { name = "MODO_API_KEY",     value = var.modo_api_key },
      { name = "MODO_EMAIL",       value = var.modo_email },
      { name = "MODO_PASSWORD",    value = var.modo_password },
      { name = "SMTP_HOST",        value = var.smtp_host },
      { name = "SMTP_PORT",        value = var.smtp_port },
      { name = "SMTP_USER",        value = var.smtp_user },
      { name = "SMTP_PASSWORD",    value = var.smtp_password },
      { name = "REPORT_TO_EMAIL",  value = var.report_email_to },
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = local.log_group
        awslogs-region        = var.region
        awslogs-stream-prefix = "ercot-market"
      }
    }
  }])

  lifecycle { ignore_changes = [container_definitions] }
  tags = local.tags
}

import {
  to = aws_ecs_task_definition.ercot_market
  id = "arn:aws:ecs:ap-southeast-1:319383842493:task-definition/bess-platform-ercot-market:5"
}

resource "aws_ecs_service" "ercot_market" {
  name            = "${var.name}-ercot-market-svc"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.ercot_market.arn
  desired_count   = var.desired_count_ercot_market
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = var.private_subnet_ids
    security_groups  = [aws_security_group.ecs_tasks.id]
    assign_public_ip = true
  }
  load_balancer {
    target_group_arn = aws_lb_target_group.ercot_market.arn
    container_name   = "ercot-market"
    container_port   = 8510
  }
  depends_on = [aws_lb_listener.https]
  tags       = local.tags

  lifecycle { ignore_changes = [task_definition] }
}

import {
  to = aws_ecs_service.ercot_market
  id = "bess-platform-cluster/bess-platform-ercot-market-svc"
}

# ─────────────────────────────────────────────────────────────────────────────
# PJM Market  (port 8511)
# ─────────────────────────────────────────────────────────────────────────────
resource "aws_ecr_repository" "pjm_market" {
  name                 = "bess-pjm-market"
  image_tag_mutability = "MUTABLE"
  image_scanning_configuration { scan_on_push = false }
  tags = local.tags
}

import {
  to = aws_ecr_repository.pjm_market
  id = "bess-pjm-market"
}

resource "aws_lb_target_group" "pjm_market" {
  name     = "bess-platform-pjm-market"
  port     = 8511
  protocol = "HTTP"
  vpc_id   = var.vpc_id
  target_type = "ip"
  lifecycle { create_before_destroy = true }
  health_check {
    path                = "/pjm-market/_stcore/health"
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
  to = aws_lb_target_group.pjm_market
  id = "arn:aws:elasticloadbalancing:ap-southeast-1:319383842493:targetgroup/bess-platform-pjm-market/5748a45d49797c54"
}

resource "aws_lb_listener_rule" "pjm_market_path" {
  listener_arn = aws_lb_listener.https.arn
  priority     = 48

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
    target_group_arn = aws_lb_target_group.pjm_market.arn
  }
  condition {
    path_pattern { values = ["/pjm-market", "/pjm-market/", "/pjm-market/*"] }
  }
}

import {
  to = aws_lb_listener_rule.pjm_market_path
  id = "arn:aws:elasticloadbalancing:ap-southeast-1:319383842493:listener-rule/app/bess-platform-alb/bed7b373d923c365/a5747829a4d3e921/d3a455f9afc644b5"
}

resource "aws_ecs_task_definition" "pjm_market" {
  family                   = "${var.name}-pjm-market"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "512"
  memory                   = "1024"
  execution_role_arn       = aws_iam_role.task_execution.arn
  task_role_arn            = aws_iam_role.task_role.arn

  container_definitions = jsonencode([{
    name      = "pjm-market"
    image     = var.image_pjm_market
    essential = true
    portMappings = [{ containerPort = 8511, protocol = "tcp" }]
    environment = [
      { name = "PGURL",            value = local.db_pgurl_direct },
      { name = "ANTHROPIC_API_KEY", value = var.anthropic_api_key },
      { name = "MODO_API_KEY",     value = var.modo_api_key },
      { name = "MODO_EMAIL",       value = var.modo_email },
      { name = "MODO_PASSWORD",    value = var.modo_password },
      { name = "SMTP_HOST",        value = var.smtp_host },
      { name = "SMTP_PORT",        value = var.smtp_port },
      { name = "SMTP_USER",        value = var.smtp_user },
      { name = "SMTP_PASSWORD",    value = var.smtp_password },
      { name = "REPORT_TO_EMAIL",  value = var.report_email_to },
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = local.log_group
        awslogs-region        = var.region
        awslogs-stream-prefix = "pjm-market"
      }
    }
  }])

  lifecycle { ignore_changes = [container_definitions] }
  tags = local.tags
}

import {
  to = aws_ecs_task_definition.pjm_market
  id = "arn:aws:ecs:ap-southeast-1:319383842493:task-definition/bess-platform-pjm-market:5"
}

resource "aws_ecs_service" "pjm_market" {
  name            = "${var.name}-pjm-market-svc"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.pjm_market.arn
  desired_count   = var.desired_count_pjm_market
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = var.private_subnet_ids
    security_groups  = [aws_security_group.ecs_tasks.id]
    assign_public_ip = true
  }
  load_balancer {
    target_group_arn = aws_lb_target_group.pjm_market.arn
    container_name   = "pjm-market"
    container_port   = 8511
  }
  depends_on = [aws_lb_listener.https]
  tags       = local.tags

  lifecycle { ignore_changes = [task_definition] }
}

import {
  to = aws_ecs_service.pjm_market
  id = "bess-platform-cluster/bess-platform-pjm-market-svc"
}

# ─────────────────────────────────────────────────────────────────────────────
# CAISO Market  (port 8512)
# ─────────────────────────────────────────────────────────────────────────────
resource "aws_ecr_repository" "caiso_market" {
  name                 = "bess-caiso-market"
  image_tag_mutability = "MUTABLE"
  image_scanning_configuration { scan_on_push = false }
  tags = local.tags
}

import {
  to = aws_ecr_repository.caiso_market
  id = "bess-caiso-market"
}

resource "aws_lb_target_group" "caiso_market" {
  name     = "bess-platform-caiso-market"
  port     = 8512
  protocol = "HTTP"
  vpc_id   = var.vpc_id
  target_type = "ip"
  lifecycle { create_before_destroy = true }
  health_check {
    path                = "/caiso-market/_stcore/health"
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
  to = aws_lb_target_group.caiso_market
  id = "arn:aws:elasticloadbalancing:ap-southeast-1:319383842493:targetgroup/bess-platform-caiso-market/6dfac06e9b6c4ee4"
}

resource "aws_lb_listener_rule" "caiso_market_path" {
  listener_arn = aws_lb_listener.https.arn
  priority     = 49

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
    target_group_arn = aws_lb_target_group.caiso_market.arn
  }
  condition {
    path_pattern { values = ["/caiso-market", "/caiso-market/", "/caiso-market/*"] }
  }
}

import {
  to = aws_lb_listener_rule.caiso_market_path
  id = "arn:aws:elasticloadbalancing:ap-southeast-1:319383842493:listener-rule/app/bess-platform-alb/bed7b373d923c365/a5747829a4d3e921/6900eda696fefa2a"
}

resource "aws_ecs_task_definition" "caiso_market" {
  family                   = "${var.name}-caiso-market"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "512"
  memory                   = "1024"
  execution_role_arn       = aws_iam_role.task_execution.arn
  task_role_arn            = aws_iam_role.task_role.arn

  container_definitions = jsonencode([{
    name      = "caiso-market"
    image     = var.image_caiso_market
    essential = true
    portMappings = [{ containerPort = 8512, protocol = "tcp" }]
    environment = [
      { name = "PGURL",            value = local.db_pgurl_direct },
      { name = "ANTHROPIC_API_KEY", value = var.anthropic_api_key },
      { name = "MODO_API_KEY",     value = var.modo_api_key },
      { name = "MODO_EMAIL",       value = var.modo_email },
      { name = "MODO_PASSWORD",    value = var.modo_password },
      { name = "SMTP_HOST",        value = var.smtp_host },
      { name = "SMTP_PORT",        value = var.smtp_port },
      { name = "SMTP_USER",        value = var.smtp_user },
      { name = "SMTP_PASSWORD",    value = var.smtp_password },
      { name = "REPORT_TO_EMAIL",  value = var.report_email_to },
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = local.log_group
        awslogs-region        = var.region
        awslogs-stream-prefix = "caiso-market"
      }
    }
  }])

  lifecycle { ignore_changes = [container_definitions] }
  tags = local.tags
}

import {
  to = aws_ecs_task_definition.caiso_market
  id = "arn:aws:ecs:ap-southeast-1:319383842493:task-definition/bess-platform-caiso-market:6"
}

resource "aws_ecs_service" "caiso_market" {
  name            = "${var.name}-caiso-market-svc"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.caiso_market.arn
  desired_count   = var.desired_count_caiso_market
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = var.private_subnet_ids
    security_groups  = [aws_security_group.ecs_tasks.id]
    assign_public_ip = true
  }
  load_balancer {
    target_group_arn = aws_lb_target_group.caiso_market.arn
    container_name   = "caiso-market"
    container_port   = 8512
  }
  depends_on = [aws_lb_listener.https]
  tags       = local.tags

  lifecycle { ignore_changes = [task_definition] }
}

import {
  to = aws_ecs_service.caiso_market
  id = "bess-platform-cluster/bess-platform-caiso-market-svc"
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
  name     = "bess-platform-ph-market"
  port     = 8510
  protocol = "HTTP"
  vpc_id   = var.vpc_id
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
      { name = "PGURL",            value = local.db_pgurl_direct },
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
  name     = "bess-platform-po-market"
  port     = 8511
  protocol = "HTTP"
  vpc_id   = var.vpc_id
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
      { name = "PGURL",            value = local.db_pgurl_direct },
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
# Crystal Ball Fortune Teller  (port 8520)
# ─────────────────────────────────────────────────────────────────────────────
resource "aws_ecr_repository" "crystal_ball" {
  name                 = "crystal-ball-fortune"
  image_tag_mutability = "MUTABLE"
  image_scanning_configuration { scan_on_push = false }
  tags = local.tags
}

import {
  to = aws_ecr_repository.crystal_ball
  id = "crystal-ball-fortune"
}

resource "aws_lb_target_group" "crystal_ball" {
  name_prefix = "tgcb-"
  port        = 8520
  protocol    = "HTTP"
  vpc_id      = var.vpc_id
  target_type = "ip"
  lifecycle { create_before_destroy = true }
  health_check {
    path                = "/crystal-ball/_stcore/health"
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
  to = aws_lb_target_group.crystal_ball
  id = "arn:aws:elasticloadbalancing:ap-southeast-1:319383842493:targetgroup/tgcb-20260530112936841000000001/7406af6bc8ff6ceb"
}

resource "aws_lb_listener_rule" "crystal_ball_path" {
  listener_arn = aws_lb_listener.https.arn
  priority     = 50

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
    target_group_arn = aws_lb_target_group.crystal_ball.arn
  }
  condition {
    path_pattern { values = ["/crystal-ball", "/crystal-ball/", "/crystal-ball/*"] }
  }
}

import {
  to = aws_lb_listener_rule.crystal_ball_path
  id = "arn:aws:elasticloadbalancing:ap-southeast-1:319383842493:listener-rule/app/bess-platform-alb/bed7b373d923c365/a5747829a4d3e921/5ab668ad85e63c53"
}

resource "aws_ecs_task_definition" "crystal_ball" {
  family                   = "${var.name}-crystal-ball"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "512"
  memory                   = "1024"
  execution_role_arn       = aws_iam_role.task_execution.arn
  task_role_arn            = aws_iam_role.task_role.arn

  container_definitions = jsonencode([{
    name      = "crystal-ball"
    image     = var.image_crystal_ball
    essential = true
    portMappings = [{ containerPort = 8520, protocol = "tcp" }]
    environment = [
      { name = "PGURL",             value = local.db_pgurl_direct },
      { name = "ANTHROPIC_API_KEY", value = var.anthropic_api_key },
      { name = "AWS_REGION",        value = var.region },
      { name = "SMTP_HOST",         value = var.smtp_host },
      { name = "SMTP_PORT",         value = var.smtp_port },
      { name = "SMTP_USER",         value = var.smtp_user },
      { name = "SMTP_PASSWORD",     value = var.smtp_password },
      { name = "REPORT_FROM_EMAIL", value = var.smtp_user },
      { name = "REPORT_TO_EMAIL",   value = var.report_email_to },
      { name = "WECOM_WEBHOOK_URL", value = var.crystal_ball_wecom_webhook_url },
      { name = "REPORT_HOUR",       value = "7" },
      { name = "TIMEZONE",          value = "Asia/Shanghai" },
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = local.log_group
        awslogs-region        = var.region
        awslogs-stream-prefix = "crystal-ball"
      }
    }
  }])

  lifecycle { ignore_changes = [container_definitions] }
  tags = local.tags
}

import {
  to = aws_ecs_task_definition.crystal_ball
  id = "arn:aws:ecs:ap-southeast-1:319383842493:task-definition/bess-platform-crystal-ball:34"
}

resource "aws_ecs_service" "crystal_ball" {
  name            = "${var.name}-crystal-ball-svc"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.crystal_ball.arn
  desired_count   = var.desired_count_crystal_ball
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = var.private_subnet_ids
    security_groups  = [aws_security_group.ecs_tasks.id]
    assign_public_ip = true
  }
  load_balancer {
    target_group_arn = aws_lb_target_group.crystal_ball.arn
    container_name   = "crystal-ball"
    container_port   = 8520
  }
  depends_on = [aws_lb_listener.https]
  tags       = local.tags

  lifecycle { ignore_changes = [task_definition] }
}

import {
  to = aws_ecs_service.crystal_ball
  id = "bess-platform-cluster/bess-platform-crystal-ball-svc"
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
  name     = "bess-cb-client-tg"
  port     = 8521
  protocol = "HTTP"
  vpc_id   = var.vpc_id
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

# ─────────────────────────────────────────────────────────────────────────────
# Trading Performance Agent  (scheduled ECS task, no ALB)
# ─────────────────────────────────────────────────────────────────────────────
resource "aws_ecr_repository" "trading_performance_agent" {
  name                 = "bess-trading-performance-agent"
  image_tag_mutability = "MUTABLE"
  image_scanning_configuration { scan_on_push = false }
  tags = local.tags
}

import {
  to = aws_ecr_repository.trading_performance_agent
  id = "bess-trading-performance-agent"
}

resource "aws_ecs_task_definition" "trading_performance_agent" {
  family                   = "${var.name}-trading-performance-agent"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "1024"
  memory                   = "2048"
  execution_role_arn       = aws_iam_role.task_execution.arn
  task_role_arn            = aws_iam_role.task_role.arn

  container_definitions = jsonencode([{
    name      = "trading-performance-agent"
    image     = var.image_trading_performance_agent
    essential = true
    environment = [
      { name = "PGURL",             value = local.db_pgurl_direct },
      { name = "DB_DSN",            value = local.db_pgurl_direct },
      { name = "ANTHROPIC_API_KEY", value = var.anthropic_api_key },
      { name = "SMTP_HOST",         value = var.smtp_host },
      { name = "SMTP_PORT",         value = var.smtp_port },
      { name = "SMTP_USER",         value = var.smtp_user },
      { name = "SMTP_PASSWORD",     value = var.smtp_password },
      { name = "SMTP_FROM",         value = var.smtp_from },
      { name = "REPORT_EMAIL_TO",   value = var.report_email_to },
      { name = "UPLOADS_BUCKET_NAME", value = var.uploads_bucket_name },
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = local.log_group
        awslogs-region        = var.region
        awslogs-stream-prefix = "trading-performance-agent"
      }
    }
  }])

  lifecycle { ignore_changes = [container_definitions] }
  tags = local.tags
}

import {
  to = aws_ecs_task_definition.trading_performance_agent
  id = "arn:aws:ecs:ap-southeast-1:319383842493:task-definition/bess-platform-trading-performance-agent:9"
}

resource "aws_cloudwatch_event_rule" "trading_performance_agent_daily" {
  name                = "${var.name}-trading-performance-agent-daily"
  schedule_expression = "cron(30 22 * * ? *)"
  description         = "Daily trading performance agent run (06:30 SGT)"
}

import {
  to = aws_cloudwatch_event_rule.trading_performance_agent_daily
  id = "bess-platform-trading-performance-agent-daily"
}

resource "aws_cloudwatch_event_target" "trading_performance_agent_target" {
  rule     = aws_cloudwatch_event_rule.trading_performance_agent_daily.name
  arn      = aws_ecs_cluster.this.arn
  role_arn = aws_iam_role.eventbridge_ecs.arn
  target_id = "trading-performance-agent-target"

  ecs_target {
    launch_type         = "FARGATE"
    task_definition_arn = aws_ecs_task_definition.trading_performance_agent.arn
    task_count          = 1

    network_configuration {
      subnets          = var.private_subnet_ids
      security_groups  = [aws_security_group.ecs_tasks.id]
      assign_public_ip = false
    }
  }
}

import {
  to = aws_cloudwatch_event_target.trading_performance_agent_target
  id = "bess-platform-trading-performance-agent-daily/trading-performance-agent-target"
}
