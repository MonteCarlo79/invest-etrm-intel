provider "aws" {
  region = var.region
}

data "aws_caller_identity" "current" {}

locals {
  tags = {
    Project = var.name
  }

  log_group = "/ecs/${var.name}"
}

# -------------------------
# CloudWatch Logs
# -------------------------
resource "aws_cloudwatch_log_group" "ecs" {
  name              = local.log_group
  retention_in_days = 14
  tags              = local.tags
}

# -------------------------
# S3 for uploads (Fargate-safe storage)
# -------------------------
resource "aws_s3_bucket" "uploads" {
  bucket = var.uploads_bucket_name
  tags   = local.tags
}

resource "aws_s3_bucket_public_access_block" "uploads" {
  bucket                  = aws_s3_bucket.uploads.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "uploads" {
  bucket = aws_s3_bucket.uploads.id
  versioning_configuration {
    status = "Enabled"
  }
}


# -------------------------
# Networking: Security Groups
# -------------------------
# ALB SG: allow inbound 80 from internet, outbound all
resource "aws_security_group" "alb" {
  name        = "${var.name}-alb-sg"
  description = "ALB SG"
  vpc_id      = var.vpc_id
  tags        = local.tags

  ingress {
     description = "HTTPS"
     from_port   = 443
     to_port     = 443
     protocol    = "tcp"
     cidr_blocks = ["0.0.0.0/0"]
   
    #    cidr_blocks = [ "138.113.14.246/32", "223.104.5.51/32", "39.144.40.138/32", "103.130.145.210/32"]
  }

  ingress {
     description = "HTTP"
     from_port   = 80
     to_port     = 80
     protocol    = "tcp"
     cidr_blocks = ["0.0.0.0/0"]
   }
  egress {
    description = "All outbound"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# ECS tasks SG: allow inbound from ALB only on both ports; outbound all
resource "aws_security_group" "ecs_tasks" {
  name        = "${var.name}-ecs-tasks-sg"
  description = "ECS tasks SG"
  vpc_id      = var.vpc_id
  tags        = local.tags

  ingress {
    description     = "Streamlit services from ALB"
    from_port       = 8500
    to_port         = 8504
    protocol        = "tcp"
    security_groups = [aws_security_group.alb.id]
  }

  egress {
    description = "All outbound"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# RDS SG: allow 5432 from ECS tasks SG only
resource "aws_security_group" "rds" {
  name        = "${var.name}-rds-sg"
  description = "RDS SG"
  vpc_id      = var.vpc_id
  tags        = local.tags

  ingress {
    description     = "Postgres from ECS tasks"
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [aws_security_group.ecs_tasks.id]
  }

  egress {
    description = "All outbound"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# -------------------------
# RDS Postgres (private)
# -------------------------
resource "aws_db_subnet_group" "pg" {
  name       = "${var.name}-pg-subnets"
  subnet_ids = var.private_subnet_ids
  tags       = local.tags
}

resource "aws_db_instance" "pg" {
  identifier                   = "${var.name}-pg"
  engine                       = "postgres"
  engine_version               = "18.2"
  instance_class               = var.db_instance_class
  allocated_storage            = 100
  storage_type                 = "gp3"   # was gp2; gp3 saves ~$1.90/month, same 3000 IOPS, in-place change
  db_name                      = var.db_name
  username                     = var.db_username
  password                     = var.db_password
  port                         = 5432
  publicly_accessible          = false
  multi_az                     = false
  storage_encrypted            = true
  skip_final_snapshot          = true
  deletion_protection          = false
  db_subnet_group_name         = aws_db_subnet_group.pg.name
  vpc_security_group_ids       = [aws_security_group.rds.id]
  tags                         = local.tags
  backup_retention_period      = 7
  performance_insights_enabled = true
}

# -------------------------
# ALB + Target Groups + Listener Rules
# -------------------------
resource "aws_lb" "app" {
  name               = "${var.name}-alb"
  internal           = false
  load_balancer_type = "application"
  security_groups    = [aws_security_group.alb.id]
  subnets            = var.public_subnet_ids
  idle_timeout       = 300
  tags               = local.tags
  access_logs {
    bucket  = aws_s3_bucket.uploads.bucket
    prefix  = "alb"
    enabled = true
  }
}

resource "aws_lb_listener_rule" "signed_out_page" {
  listener_arn = aws_lb_listener.https.arn
  priority     = 3

  action {
    type = "fixed-response"

    fixed_response {
      content_type = "text/html"
      status_code  = "200"
      message_body = <<EOF
<html>
  <head><title>Signed Out</title></head>
  <body style="font-family: Arial, sans-serif; padding: 40px;">
    <h2>You have been signed out.</h2>
    <p><a href="/portal/">Sign in again</a></p>
  </body>
</html>
EOF
    }
  }

  condition {
    path_pattern {
      values = ["/signed-out"]
    }
  }
}

resource "aws_lb_target_group" "bess_map" {
  name_prefix = "tgmap-"
  port        = 8503
  protocol    = "HTTP"
  vpc_id      = var.vpc_id
  target_type = "ip"
  lifecycle {
    create_before_destroy = true
  }
  health_check {
    path                = "/bess-map/_stcore/health"
    protocol            = "HTTP"
    matcher             = "200-399"
    interval            = 30
    timeout             = 5
    healthy_threshold   = 2
    unhealthy_threshold = 3
  }

  tags = local.tags
}

resource "aws_lb_target_group" "uploader" {
  name_prefix = "tgupl-"
  port        = 8501
  protocol    = "HTTP"
  vpc_id      = var.vpc_id
  target_type = "ip"

  lifecycle {
    create_before_destroy = true
  }

  health_check {
    path                = "/uploader/_stcore/health"
    protocol            = "HTTP"
    matcher             = "200-399"
    interval            = 30
    timeout             = 5
    healthy_threshold   = 2
    unhealthy_threshold = 3
  }

  tags = local.tags
}


resource "aws_lb_target_group" "inner_mongolia" {
  name_prefix = "tgim-"
  port        = 8504
  protocol    = "HTTP"
  vpc_id      = var.vpc_id
  target_type = "ip"

  health_check {
    path                = "/inner-mongolia/_stcore/health"
    protocol            = "HTTP"
    matcher             = "200-399"
    interval            = 30
    timeout             = 5
    healthy_threshold   = 2
    unhealthy_threshold = 3
  }
  lifecycle {
    create_before_destroy = true
  }
  tags = local.tags
}


resource "aws_lb_listener" "https" {
  load_balancer_arn = aws_lb.app.arn
  port              = 443
  protocol          = "HTTPS"
  ssl_policy        = "ELBSecurityPolicy-TLS13-1-2-Res-PQ-2025-09"
  certificate_arn   = var.acm_certificate_arn

  default_action {
    type = "authenticate-cognito"

    authenticate_cognito {
      user_pool_arn       = aws_cognito_user_pool.bess_users.arn
      user_pool_client_id = aws_cognito_user_pool_client.bess_client.id
      user_pool_domain    = aws_cognito_user_pool_domain.main.domain
    }
  }

  default_action {
    type = "fixed-response"

    fixed_response {
      content_type = "text/plain"
      message_body = "Not Found"
      status_code  = "404"
    }
  }
}

resource "aws_lb_listener" "http" {
  load_balancer_arn = aws_lb.app.arn
  port              = 80
  protocol          = "HTTP"

  default_action {
    type = "redirect"

    redirect {
      protocol    = "HTTPS"
      port        = "443"
      host        = "#{host}"
      path        = "/portal/"
      query       = "#{query}"
      status_code = "HTTP_302"
    }
  }
}

resource "aws_lb_listener_rule" "root_redirect_to_portal" {
  listener_arn = aws_lb_listener.https.arn
  priority     = 2

  action {
    type = "redirect"

    redirect {
      protocol    = "HTTPS"
      port        = "443"
      host        = "#{host}"
      path        = "/portal/"
      query       = "#{query}"
      status_code = "HTTP_302"
    }
  }

  condition {
    path_pattern {
      values = ["/"]
    }
  }
}

resource "aws_lb_listener_rule" "portal_path" {
  listener_arn = aws_lb_listener.https.arn
  priority     = 5

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
    target_group_arn = aws_lb_target_group.portal.arn
  }

  condition {
    path_pattern {
      values = ["/portal", "/portal/", "/portal/*"]
    }
  }
}

resource "aws_lb_listener_rule" "uploader_path" {
  listener_arn = aws_lb_listener.https.arn
  priority     = 10

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
    target_group_arn = aws_lb_target_group.uploader.arn
  }

  condition {
    path_pattern {
      values = ["/uploader", "/uploader/", "/uploader/*"]
    }
  }
}
resource "aws_lb_listener_rule" "inner_mongolia" {
  listener_arn = aws_lb_listener.https.arn
  priority     = 15

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
    target_group_arn = aws_lb_target_group.inner_mongolia.arn
  }

  condition {
    path_pattern {
      values = ["/inner-mongolia", "/inner-mongolia/", "/inner-mongolia/*"]
    }
  }
}

resource "aws_lb_listener_rule" "bess_map_path" {
  listener_arn = aws_lb_listener.https.arn
  priority     = 20

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
    target_group_arn = aws_lb_target_group.bess_map.arn
  }

  condition {
    path_pattern {
      values = ["/bess-map", "/bess-map/", "/bess-map/*"]
    }
  }
}

resource "aws_lb_target_group" "portal" {
  name        = "${var.name}-tg-portal"
  port        = 8500
  protocol    = "HTTP"
  vpc_id      = var.vpc_id
  target_type = "ip"

  health_check {
    path                = "/portal/_stcore/health"
    protocol            = "HTTP"
    matcher             = "200-399"
    interval            = 30
    timeout             = 10
    healthy_threshold   = 2
    unhealthy_threshold = 3
  }
  lifecycle {
    create_before_destroy = true
  }
  tags = local.tags
}


# -------------------------
# ECS Cluster
# -------------------------
resource "aws_ecs_cluster" "this" {
  name = "${var.name}-cluster"
  setting {
    name  = "containerInsights"
    value = "enabled"
  }

  tags = local.tags
}

# -------------------------
# IAM: Task Execution Role + Task Role
# -------------------------
data "aws_iam_policy_document" "ecs_task_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "task_execution" {
  name               = "${var.name}-task-exec"
  assume_role_policy = data.aws_iam_policy_document.ecs_task_assume.json
  tags               = local.tags
}

resource "aws_iam_role_policy_attachment" "task_exec_policy" {
  role       = aws_iam_role.task_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}




# Task role for app permissions (S3 uploads)
resource "aws_iam_role" "task_role" {
  name               = "${var.name}-task-role"
  assume_role_policy = data.aws_iam_policy_document.ecs_task_assume.json
  tags               = local.tags
}

data "aws_iam_policy_document" "task_s3" {
  statement {
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.uploads.arn]
  }

  statement {
    actions   = ["s3:PutObject", "s3:GetObject", "s3:DeleteObject"]
    resources = ["${aws_s3_bucket.uploads.arn}/*"]
  }
}

resource "aws_iam_role_policy" "task_s3" {
  name   = "${var.name}-task-s3"
  role   = aws_iam_role.task_role.id
  policy = data.aws_iam_policy_document.task_s3.json
}

data "aws_iam_policy_document" "task_cognito_admin" {
  statement {
    actions = [
      "cognito-idp:ListUsers",
      "cognito-idp:AdminListGroupsForUser",
      "cognito-idp:AdminAddUserToGroup",
      "cognito-idp:AdminRemoveUserFromGroup"
    ]
    resources = [
      aws_cognito_user_pool.bess_users.arn
    ]
  }
}

resource "aws_iam_role_policy" "task_cognito_admin" {
  name   = "${var.name}-task-cognito-admin"
  role   = aws_iam_role.task_role.id
  policy = data.aws_iam_policy_document.task_cognito_admin.json
}

resource "aws_iam_policy" "ecs_run_task_policy" {
  name = "${var.name}-ecs-run-task"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [

      {
        Effect = "Allow"
        Action = [
          "ecs:RunTask",
          "ecs:DescribeTasks"
        ]
        Resource = "*"
      },

      {
        Effect = "Allow"
        Action = ["iam:PassRole"]
        Resource = [
          aws_iam_role.task_execution.arn,
          aws_iam_role.task_role.arn
        ]
      }

    ]
  })
}

resource "aws_iam_role_policy_attachment" "attach_run_task" {
  role       = aws_iam_role.task_role.name
  policy_arn = aws_iam_policy.ecs_run_task_policy.arn
}


# -------------------------
# ECS Task Definitions
# -------------------------

############################################
# BESS MAP TASK
# 7-day metrics: avg mem 10.1%, peak 18.2% of 1024 MB = 186 MB peak.
# 256/512 gives 2.7x headroom over observed peak. Apply after confirming no
# memory spike pattern. Rolling deployment; roll back by reverting + apply.
############################################
resource "aws_ecs_task_definition" "bess_map" {
  family                   = "bess-platform-bess-map"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "256"
  memory                   = "512"

  execution_role_arn = aws_iam_role.task_execution.arn
  task_role_arn      = aws_iam_role.task_role.arn

  container_definitions = jsonencode([
  {
    name      = "bess-map"
    image     = var.image_bess_map
    essential = true

    portMappings = [
      {
        containerPort = 8503
        protocol      = "tcp"
      }
    ]

    command = [
  	"streamlit",
  	"run",
  	"streamlit_bess_profit_dashboard_v14.1_consistent_full2.py",
  	"--server.port=8503",
  	"--server.address=0.0.0.0",
  	"--server.baseUrlPath=bess-map",
  	"--server.enableCORS=false",
  	"--server.enableXsrfProtection=false",
  	"--",
  	"--env",
  	"/apps/.env",
  	"--schema",
  	"marketdata"
     ]

    environment = [
      {
        name  = "PGURL"
        value = "postgresql://${var.db_username}:${var.db_password}@${aws_db_instance.pg.address}:5432/${var.db_name}?sslmode=require"
      },
      {
        name  = "PGHOST"
        value = aws_db_instance.pg.address
      },
      {
        name  = "PGPORT"
        value = "5432"
      },
      {
        name  = "PGDATABASE"
        value = var.db_name
      },
      {
        name  = "PGUSER"
        value = var.db_username
      },
       {
         name  = "AWS_REGION"
         value = var.region
       },
       {
         name  = "COGNITO_USER_POOL_ID"
         value = aws_cognito_user_pool.bess_users.id
       },
      {
        name  = "PGPASSWORD"
        value = var.db_password
      }
    ]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = local.log_group
        awslogs-region        = var.region
        awslogs-stream-prefix = "bess-map"
      }
    }
  }
])
}


############################################
# BESS UPLOADER TASK
# 7-day metrics: avg mem 5.9%, peak 7.4% of 1024 MB = 75.8 MB peak.
# 256/512 gives 6.7x headroom over observed peak. Apply after confirming no
# memory spike during large Excel uploads. Rolling deployment; roll back by reverting + apply.
############################################
resource "aws_ecs_task_definition" "uploader" {
  family                   = "${var.name}-uploader"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "256"
  memory                   = "512"

  execution_role_arn = aws_iam_role.task_execution.arn
  task_role_arn      = aws_iam_role.task_role.arn

  container_definitions = jsonencode([
  {
    name      = "bess-uploader"
    image     = var.image_uploader
    essential = true

    portMappings = [
      {
        containerPort = 8501
        protocol      = "tcp"
      }
    ]

    command = [
  	"streamlit",
  	"run",
  	"app.py",
  	"--server.port=8501",
  	"--server.address=0.0.0.0",
  	"--server.baseUrlPath=uploader",
  	"--server.enableCORS=false",
  	"--server.enableXsrfProtection=false"
     ]

    environment = [
      {
        name  = "PGURL"
        value = "postgresql://${var.db_username}:${var.db_password}@${aws_db_instance.pg.address}:5432/${var.db_name}?sslmode=require"
      },
      {
        name  = "PGHOST"
        value = aws_db_instance.pg.address
      },
      {
        name  = "PGPORT"
        value = "5432"
      },
      {
        name  = "PGDATABASE"
        value = var.db_name
      },
      {
        name  = "PGUSER"
        value = var.db_username
      },
      {
        name  = "PGPASSWORD"
        value = var.db_password
      },
      {
        name  = "DB_SCHEMA"
        value = "marketdata"
      },
      {
        name  = "UPLOAD_DIR"
        value = "/tmp/uploads"
      },
      {
        name  = "LOG_DIR"
        value = "/tmp/logs"
      },

       {
         name  = "AWS_REGION"
         value = var.region
       },
       {
         name  = "COGNITO_USER_POOL_ID"
         value = aws_cognito_user_pool.bess_users.id
       },

      {
        name  = "S3_BUCKET"
        value = aws_s3_bucket.uploads.bucket
      }
    ]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = local.log_group
        awslogs-region        = var.region
        awslogs-stream-prefix = "bess-uploader"
      }
    }
  }
])
}



resource "aws_ecs_task_definition" "portal" {
  family                   = "bess-platform-portal"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "256"
  memory                   = "512"
  execution_role_arn       = aws_iam_role.task_execution.arn
  task_role_arn            = aws_iam_role.task_role.arn

  container_definitions = jsonencode([
    {
      name      = "portal"
      image     = var.image_portal
      essential = true

      portMappings = [
        {
          containerPort = 8500
          protocol      = "tcp"
        }
      ]

      environment = [
        {
          name  = "DB_DSN"
          value = "postgresql://${var.db_username}:${var.db_password}@${aws_db_instance.pg.address}:5432/${var.db_name}?sslmode=require"
        },
        {
          name  = "INVESTOR_PASSWORD"
          value = var.investor_password
        },
        {
          name  = "INTERNAL_PASSWORD"
          value = var.internal_password
        },
        {
          name  = "ADMIN_PASSWORD"
          value = var.admin_password
        },
        {
          name  = "AI_ENABLED"
          value = var.ai_enabled
        },
        {
          name  = "ECS_CLUSTER"
          value = aws_ecs_cluster.this.name
        },
        {
          name  = "PRIVATE_SUBNETS"
          value = join(",", var.private_subnet_ids)
        },
        {
          name  = "TASK_SECURITY_GROUPS"
          value = aws_security_group.ecs_tasks.id
        },
        {
          name  = "AWS_REGION"
          value = var.region
        },
        {
          name  = "EMAIL_ROLE_MAP"
          value = "chen_dpeng@hotmail.com=Admin"
        },
         {
          name  = "COGNITO_DOMAIN"
          value = "https://${aws_cognito_user_pool_domain.main.domain}.auth.${var.region}.amazoncognito.com"
        },
        {
          name  = "COGNITO_CLIENT_ID"
          value = aws_cognito_user_pool_client.bess_client.id
        },
        {
          name  = "COGNITO_USER_POOL_ID"
          value = aws_cognito_user_pool.bess_users.id
        },
        {
          name  = "SHOW_AWS_DEBUG"
          value = tostring(var.show_aws_debug)
        },
        {
          name  = "LOGOUT_REDIRECT_URI"
          value = var.logout_redirect_uri
        }

      ]

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = local.log_group
          awslogs-region        = var.region
          awslogs-stream-prefix = "portal"
        }
      }
    }
  ])

  tags = local.tags
}
# Live task def (v43) runs at 2048/8192 — manually scaled up at some point.
# 7-day metrics: avg CPU 0.01%, avg mem 1.78% (149 MB peak out of 8192 MB).
# Terraform target: 1024/2048 gives 6.8x headroom over observed peak — safe.
# Applying this will trigger a rolling ECS service deployment. Validate metrics
# are stable before applying; roll back with: terraform apply after reverting.
resource "aws_ecs_task_definition" "inner_mongolia" {
  family                   = "${var.name}-inner-mongolia"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 1024
  memory                   = 2048

  execution_role_arn = aws_iam_role.task_execution.arn
  task_role_arn      = aws_iam_role.task_role.arn

  container_definitions = jsonencode([
    {
      name      = "inner-mongolia"
      image     = var.image_inner_mongolia
      essential = true

      portMappings = [
        {
          containerPort = 8504
          protocol      = "tcp"
        }
      ]

      command = [
  	"streamlit",
  	"run",
  	"app.py",
  	"--server.port=8504",
  	"--server.address=0.0.0.0",
  	"--server.baseUrlPath=inner-mongolia",
  	"--server.enableCORS=false",
  	"--server.enableXsrfProtection=false"
      ]

      environment = [
        {
          name  = "PGURL"
          value = "postgresql://${var.db_username}:${var.db_password}@${aws_db_instance.pg.address}:5432/${var.db_name}?sslmode=require"
        },
        {
          name  = "AWS_REGION"
          value = var.region
        },
        {
          name  = "ECS_CLUSTER"
          value = aws_ecs_cluster.this.name
        },
        {
          name  = "PIPELINE_TASK_DEF"
          value = aws_ecs_task_definition.inner_pipeline.arn
        },
        {
          name  = "PRIVATE_SUBNETS"
          value = join(",", var.private_subnet_ids)
        },
        {
          name  = "TASK_SECURITY_GROUPS"
          value = aws_security_group.ecs_tasks.id
        },
          {
           name  = "COGNITO_USER_POOL_ID"
           value = aws_cognito_user_pool.bess_users.id
           },

        { name = "CONVERSION_FACTOR", value = var.conversion_factor },
        { name = "DURATION_H", value = var.duration_h },
        { name = "SUBSIDY_PER_MWH", value = var.subsidy_per_mwh },
        { name = "CAPEX_YUAN_PER_KWH", value = var.capex_yuan_per_kwh },
        { name = "DEGRADATION_RATE", value = var.degradation_rate },
        { name = "OM_COST_PER_MW_PER_YEAR", value = var.om_cost_per_mw_per_year },
        { name = "LIFE_YEARS", value = var.life_years }

      ]

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = local.log_group
          awslogs-region        = var.region
          awslogs-stream-prefix = "inner-mongolia"
        }
      }
    }
  ])
}

# Live task def (v35): 4096 vCPU / 16384 MB.
# Container Insights confirms this sizing is CORRECT — not legacy padding.
# Observed peak: 12,570 MB (Mar 28 backfill run, 76.7% of 16384 MB).
# Typical daily runs: 134–1,332 MB peak. DO NOT reduce memory — large runs will OOM.
# DO NOT apply any right-sizing change to this task definition.
resource "aws_ecs_task_definition" "inner_pipeline" {
  family                   = "${var.name}-inner-pipeline"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 4096
  memory                   = 16384

  execution_role_arn = aws_iam_role.task_execution.arn
  task_role_arn      = aws_iam_role.task_role.arn

  container_definitions = jsonencode([
    {
      name      = "pipeline"
      image     = var.image_inner_pipeline
      essential = true

      command = ["python", "inner_pipeline.py"]

      environment = [
        {
          name  = "PGURL"
          value = "postgresql://${var.db_username}:${var.db_password}@${aws_db_instance.pg.address}:5432/${var.db_name}?sslmode=require"
        },
        {
          name  = "DB_DSN"
          value = "postgresql://${var.db_username}:${var.db_password}@${aws_db_instance.pg.address}:5432/${var.db_name}?sslmode=require"
        },
        {
          name  = "CONVERSION_FACTOR"
          value = var.conversion_factor
        },
        {
          name  = "DURATION_H"
          value = var.duration_h
        },
        {
          name  = "PYTHONPATH"
          value = "/app"
        },
        {
          name  = "SUBSIDY_PER_MWH"
          value = var.subsidy_per_mwh
        },
        {
          name  = "CAPEX_YUAN_PER_KWH"
          value = var.capex_yuan_per_kwh
        },
        {
          name  = "DEGRADATION_RATE"
          value = var.degradation_rate
        },
        {
          name  = "OM_COST_PER_MW_PER_YEAR"
          value = var.om_cost_per_mw_per_year
        },
        {
          name  = "LIFE_YEARS"
          value = var.life_years
        },

        # province_misc_to_db_v2.py runtime controls
        {
          name  = "MARKET_LIST"
          value = "Mengxi,Anhui,Shandong,Jiangsu"
        },
        {
          name  = "FULL_HISTORY"
          value = "false"
        },
        {
          name  = "DB_LOOKBACK_DAYS"
          value = "2"
        },
        {
          name  = "RUN_INHOUSE_WIND"
          value = "true"
        }
      ]

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = local.log_group
          awslogs-region        = var.region
          awslogs-stream-prefix = "inner-pipeline"
        }
      }
    }
  ])
}
resource "aws_ecs_task_definition" "inner_agent" {
  family                   = "${var.name}-inner-agent"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "512"
  memory                   = "1024"

  execution_role_arn = aws_iam_role.task_execution.arn
  task_role_arn      = aws_iam_role.task_role.arn

  container_definitions = jsonencode([
    {
      name      = "inner-agent"
      image     = "${aws_ecr_repository.inner_pipeline.repository_url}:${var.pipeline_image_tag}"
      essential = true

      command = ["python", "run_agent.py"]

      environment = [
        {
          name  = "PGURL"
          value = "postgresql://${var.db_username}:${var.db_password}@${aws_db_instance.pg.address}:5432/${var.db_name}?sslmode=require"
        },
        { name = "DB_SCHEMA", value = "marketdata" },

        # gating & anomaly
        { name = "MIN_FILE_SIZE_MB", value = "7" },
        { name = "BACKFILL_DAYS", value = "7" }
      ]

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = local.log_group
          awslogs-region        = var.region
          awslogs-stream-prefix = "inner-agent"
        }
      }
    }
  ])
}


resource "aws_ecr_repository" "inner_mongolia" {
  name = "bess-inner-mongolia"

  image_scanning_configuration {
    scan_on_push = false
  }

  image_tag_mutability = "MUTABLE"
}

resource "aws_ecr_lifecycle_policy" "inner_mongolia" {
  repository = aws_ecr_repository.inner_mongolia.name
  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "keep last 5 images"
      selection    = { tagStatus = "any", countType = "imageCountMoreThan", countNumber = 5 }
      action       = { type = "expire" }
    }]
  })
}

data "aws_iam_policy_document" "eventbridge_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }
  }
}

resource "aws_ecr_repository" "inner_pipeline" {
  name                 = "bess-inner-pipeline"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  encryption_configuration {
    encryption_type = "AES256"
  }

  tags = {
    Project = "bess-platform"
  }
}

resource "aws_ecr_lifecycle_policy" "inner_pipeline" {
  repository = aws_ecr_repository.inner_pipeline.name
  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "keep last 5 images"
      selection    = { tagStatus = "any", countType = "imageCountMoreThan", countNumber = 5 }
      action       = { type = "expire" }
    }]
  })
}

resource "aws_iam_role" "eventbridge_ecs" {
  name = "bess-platform-eventbridge-ecs"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Service = "events.amazonaws.com"
      }
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "eventbridge_ecs_run_task" {
  name = "${var.name}-eventbridge-ecs-run-task"
  role = aws_iam_role.eventbridge_ecs.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["ecs:RunTask"]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = ["iam:PassRole"]
        Resource = [
          aws_iam_role.task_execution.arn,
          aws_iam_role.task_role.arn
        ]
      }
    ]
  })
}



resource "aws_cloudwatch_event_rule" "inner_agent_daily" {
  name                = "${var.name}-inner-agent-daily"
  schedule_expression = "cron(0 21 * * ? *)"
}

resource "aws_cloudwatch_event_target" "inner_agent_target" {
  rule     = aws_cloudwatch_event_rule.inner_agent_daily.name
  arn      = aws_ecs_cluster.this.arn
  role_arn = aws_iam_role.eventbridge_ecs.arn

  ecs_target {
    launch_type         = "FARGATE"
    task_definition_arn = aws_ecs_task_definition.inner_agent.arn

    network_configuration {
      subnets          = var.private_subnet_ids
      security_groups  = [aws_security_group.ecs_tasks.id]
      assign_public_ip = true
    }
  }
}

# -------------------------
# ECS Services (attach to ALB target groups)
# -------------------------
resource "aws_ecs_service" "bess_map" {
  name            = "${var.name}-bess-map-svc"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.bess_map.arn
  desired_count   = var.desired_count_bess_map
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = var.private_subnet_ids
    security_groups  = [aws_security_group.ecs_tasks.id]
    assign_public_ip = true
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.bess_map.arn
    container_name   = "bess-map"
    container_port   = 8503
  }

  depends_on = [aws_lb_listener.https]
  tags       = local.tags
}

resource "aws_ecs_service" "uploader" {
  name            = "${var.name}-uploader-svc"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.uploader.arn
  desired_count   = var.desired_count_uploader
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = var.private_subnet_ids
    security_groups  = [aws_security_group.ecs_tasks.id]
    assign_public_ip = true
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.uploader.arn
    container_name   = "bess-uploader"
    container_port   = 8501
  }

  depends_on = [aws_lb_listener.https]
  tags       = local.tags
}

resource "aws_ecs_service" "portal" {
  name            = "bess-platform-portal-svc"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.portal.arn
  desired_count   = 1
  launch_type     = "FARGATE"

  health_check_grace_period_seconds = 60

  network_configuration {
    subnets          = var.private_subnet_ids
    security_groups  = [aws_security_group.ecs_tasks.id]
    assign_public_ip = true
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.portal.arn
    container_name   = "portal"
    container_port   = 8500
  }

  depends_on = [
    aws_lb_listener.https
  ]

  tags = local.tags
}

resource "aws_ecs_service" "inner_mongolia" {
  name            = "${var.name}-inner-mongolia-svc"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.inner_mongolia.arn
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = var.private_subnet_ids
    security_groups  = [aws_security_group.ecs_tasks.id]
    assign_public_ip = true
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.inner_mongolia.arn
    container_name   = "inner-mongolia"
    container_port   = 8504
  }

  depends_on = [aws_lb_listener.https]
  tags       = local.tags
}

resource "aws_ecs_task_definition" "execution_report" {
  family                   = "${var.name}-execution-report"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 512
  memory                   = 1024

  execution_role_arn = aws_iam_role.task_execution.arn
  task_role_arn      = aws_iam_role.task_role.arn

  container_definitions = jsonencode([
    {
      name      = "execution-report"
      image     = var.image_execution_agent
      essential = true

      command = ["python", "-m", "shared.agents.run_daily_report"]

      environment = [
        {
          name  = "PGURL"
          value = "postgresql://${var.db_username}:${var.db_password}@${aws_db_instance.pg.address}:5432/${var.db_name}?sslmode=require"
        },
        {
          name  = "SLACK_WEBHOOK_URL"
          value = var.slack_webhook_url
        }
      ]

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = local.log_group
          awslogs-region        = var.region
          awslogs-stream-prefix = "execution-report"
        }
      }
    }
  ])
}

resource "aws_cloudwatch_event_rule" "execution_report_daily" {
  name                = "${var.name}-execution-report-daily"
  schedule_expression = "cron(0 22 * * ? *)"
}

resource "aws_cloudwatch_event_target" "execution_report_target" {
  rule     = aws_cloudwatch_event_rule.execution_report_daily.name
  arn      = aws_ecs_cluster.this.arn
  role_arn = aws_iam_role.eventbridge_ecs.arn

  ecs_target {
    launch_type         = "FARGATE"
    task_definition_arn = aws_ecs_task_definition.execution_report.arn

    network_configuration {
      subnets          = var.private_subnet_ids
      security_groups  = [aws_security_group.ecs_tasks.id]
      assign_public_ip = true
    }
  }
}

resource "aws_ecr_repository" "strategy_agent" {
  name = "bess-strategy-agent"
}
resource "aws_ecr_lifecycle_policy" "strategy_agent" {
  repository = aws_ecr_repository.strategy_agent.name
  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "keep last 5 images"
      selection    = { tagStatus = "any", countType = "imageCountMoreThan", countNumber = 5 }
      action       = { type = "expire" }
    }]
  })
}
resource "aws_ecr_repository" "portfolio_agent" {
  name = "bess-portfolio-agent"
}

resource "aws_ecr_lifecycle_policy" "portfolio_agent" {
  repository = aws_ecr_repository.portfolio_agent.name
  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "keep last 5 images"
      selection    = { tagStatus = "any", countType = "imageCountMoreThan", countNumber = 5 }
      action       = { type = "expire" }
    }]
  })
}

resource "aws_ecr_repository" "execution_agent" {
  name = "bess-execution-agent"
}

resource "aws_ecr_lifecycle_policy" "execution_agent" {
  repository = aws_ecr_repository.execution_agent.name
  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "keep last 5 images"
      selection    = { tagStatus = "any", countType = "imageCountMoreThan", countNumber = 5 }
      action       = { type = "expire" }
    }]
  })
}

resource "aws_ecr_repository" "it_dev_agent" {
  name = "bess-it-dev-agent"
}

resource "aws_ecr_lifecycle_policy" "it_dev_agent" {
  repository = aws_ecr_repository.it_dev_agent.name
  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "keep last 5 images"
      selection    = { tagStatus = "any", countType = "imageCountMoreThan", countNumber = 5 }
      action       = { type = "expire" }
    }]
  })
}


resource "aws_ecs_task_definition" "strategy_agent" {
  family                   = "${var.name}-strategy-agent"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "512"
  memory                   = "1024"

  execution_role_arn = aws_iam_role.task_execution.arn
  task_role_arn      = aws_iam_role.task_role.arn

  container_definitions = jsonencode([
    {
      name      = "strategy-agent"
      image     = var.image_strategy_agent
      essential = true



      environment = [
        {
          name  = "PGURL"
          value = "postgresql://${var.db_username}:${var.db_password}@${aws_db_instance.pg.address}:5432/${var.db_name}?sslmode=require"
        },
        {
          name  = "OPENAI_API_KEY"
          value = var.openai_api_key
        }
      ]

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = local.log_group
          awslogs-region        = var.region
          awslogs-stream-prefix = "strategy-agent"
        }
      }
    }
  ])
}


resource "aws_ecs_task_definition" "portfolio_agent" {
  family                   = "${var.name}-portfolio-agent"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "512"
  memory                   = "1024"

  execution_role_arn = aws_iam_role.task_execution.arn
  task_role_arn      = aws_iam_role.task_role.arn

  container_definitions = jsonencode([
    {
      name      = "portfolio-agent"
      image     = var.image_portfolio_agent
      essential = true

      environment = [
        {
          name  = "PGURL"
          value = "postgresql://${var.db_username}:${var.db_password}@${aws_db_instance.pg.address}:5432/${var.db_name}?sslmode=require"
        },
        {
          name  = "OPENAI_API_KEY"
          value = var.openai_api_key
        }
      ]

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = local.log_group
          awslogs-region        = var.region
          awslogs-stream-prefix = "portfolio-agent"
        }
      }
    }
  ])
}


resource "aws_ecs_task_definition" "execution_agent" {
  family                   = "${var.name}-execution-agent"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "512"
  memory                   = "1024"

  execution_role_arn = aws_iam_role.task_execution.arn
  task_role_arn      = aws_iam_role.task_role.arn

  container_definitions = jsonencode([
    {
      name      = "execution-agent"
      image     = var.image_execution_agent
      essential = true



      environment = [
        {
          name  = "PGURL"
          value = "postgresql://${var.db_username}:${var.db_password}@${aws_db_instance.pg.address}:5432/${var.db_name}?sslmode=require"
        },
        {
          name  = "SLACK_WEBHOOK_URL"
          value = var.slack_webhook_url
        }
      ]

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = local.log_group
          awslogs-region        = var.region
          awslogs-stream-prefix = "execution-agent"
        }
      }
    }
  ])
}


resource "aws_ecs_task_definition" "dev_agent" {
  family                   = "${var.name}-dev-agent"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "512"
  memory                   = "1024"

  execution_role_arn = aws_iam_role.task_execution.arn
  task_role_arn      = aws_iam_role.task_role.arn

  container_definitions = jsonencode([
    {
      name      = "dev-agent"
      image     = var.image_dev_agent
      essential = true


      environment = [
        {
          name  = "OPENAI_API_KEY"
          value = var.openai_api_key
        },
        {
          name  = "PGURL"
          value = "postgresql://${var.db_username}:${var.db_password}@${aws_db_instance.pg.address}:5432/${var.db_name}?sslmode=require"
        }
      ]

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = local.log_group
          awslogs-region        = var.region
          awslogs-stream-prefix = "dev-agent"
        }
      }
    }
  ])
}


resource "aws_s3_bucket_policy" "alb_logs" {
  bucket = aws_s3_bucket.uploads.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [

      {
        Effect = "Allow"
        Principal = {
          Service = "logdelivery.elasticloadbalancing.amazonaws.com"
        }
        Action = "s3:PutObject"
        Resource = "${aws_s3_bucket.uploads.arn}/alb/AWSLogs/${data.aws_caller_identity.current.account_id}/*"
        Condition = {
          StringEquals = {
            "s3:x-amz-acl" = "bucket-owner-full-control"
          }
        }
      },

      {
        Effect = "Allow"
        Principal = {
          Service = "logdelivery.elasticloadbalancing.amazonaws.com"
        }
        Action = "s3:GetBucketAcl"
        Resource = aws_s3_bucket.uploads.arn
      }

    ]
  })
}

resource "aws_cloudwatch_event_rule" "strategy_agent_daily" {
  name                = "${var.name}-strategy-agent-daily"
  schedule_expression = "cron(0 21 * * ? *)"
}

resource "aws_cloudwatch_event_target" "strategy_agent_target" {
  rule = aws_cloudwatch_event_rule.strategy_agent_daily.name
  arn  = aws_ecs_cluster.this.arn

  role_arn = aws_iam_role.eventbridge_ecs.arn

  ecs_target {
    launch_type         = "FARGATE"
    task_definition_arn = aws_ecs_task_definition.strategy_agent.arn
    task_count          = 1

    network_configuration {
      subnets   =  var.private_subnet_ids
      security_groups  = [aws_security_group.ecs_tasks.id]
      assign_public_ip = false
    }
  }
}

resource "aws_cloudwatch_event_rule" "portfolio_agent_daily" {
  name                = "${var.name}-portfolio-agent-daily"
  schedule_expression = "cron(30 21 * * ? *)"
}

resource "aws_cloudwatch_event_target" "portfolio_agent_target" {
  rule     = aws_cloudwatch_event_rule.portfolio_agent_daily.name
  arn      = aws_ecs_cluster.this.arn
  role_arn = aws_iam_role.eventbridge_ecs.arn

  ecs_target {
    launch_type         = "FARGATE"
    task_definition_arn = aws_ecs_task_definition.portfolio_agent.arn

    network_configuration {
      subnets          = var.private_subnet_ids
      security_groups  = [aws_security_group.ecs_tasks.id]
      assign_public_ip = false
    }
  }
}

resource "aws_cloudwatch_event_rule" "execution_agent_daily" {
  name                = "${var.name}-execution-agent-daily"
  schedule_expression = "cron(0 22 * * ? *)"
}

resource "aws_cloudwatch_event_target" "execution_agent_target" {
  rule     = aws_cloudwatch_event_rule.execution_agent_daily.name
  arn      = aws_ecs_cluster.this.arn
  role_arn = aws_iam_role.eventbridge_ecs.arn

  ecs_target {
    launch_type         = "FARGATE"
    task_definition_arn = aws_ecs_task_definition.execution_agent.arn

    network_configuration {
      subnets          = var.private_subnet_ids
      security_groups  = [aws_security_group.ecs_tasks.id]
      assign_public_ip = false
    }
  }
}

resource "aws_cloudwatch_event_rule" "dev_agent_daily" {
  name                = "${var.name}-dev-agent-daily"
  schedule_expression = "cron(30 22 * * ? *)"
}

resource "aws_cloudwatch_event_target" "dev_agent_target" {
  rule     = aws_cloudwatch_event_rule.dev_agent_daily.name
  arn      = aws_ecs_cluster.this.arn
  role_arn = aws_iam_role.eventbridge_ecs.arn

  ecs_target {
    launch_type         = "FARGATE"
    task_definition_arn = aws_ecs_task_definition.dev_agent.arn

    network_configuration {
      subnets          = var.private_subnet_ids
      security_groups  = [aws_security_group.ecs_tasks.id]
      assign_public_ip = false
    }
  }
}
