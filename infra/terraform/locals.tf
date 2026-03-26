locals {
  service_definitions = {
    portal = {
      route          = "/portal*"
      priority       = 100
      cpu            = 512
      memory         = 1024
      desired_count  = 1
      health_path    = "/"
      image_name     = "portal"
      container_name = "portal"
      env = {
        DB_DSN          = var.db_dsn
        OPENAI_API_KEY  = var.openai_api_key
        AI_ENABLED      = var.openai_api_key != "" ? "true" : "false"
        SHOW_AUTH_DEBUG = "false"
      }
    }

    strategy-agent = {
      route          = "/strategy-agent*"
      priority       = 110
      cpu            = 512
      memory         = 1024
      desired_count  = 1
      health_path    = "/"
      image_name     = "strategy-agent"
      container_name = "strategy-agent"
      env = {
        DB_DSN          = var.db_dsn
        OPENAI_API_KEY  = var.openai_api_key
        AI_ENABLED      = var.openai_api_key != "" ? "true" : "false"
        SHOW_AUTH_DEBUG = "false"
      }
    }

    portfolio-risk-agent = {
      route          = "/portfolio-risk-agent*"
      priority       = 120
      cpu            = 512
      memory         = 1024
      desired_count  = 1
      health_path    = "/"
      image_name     = "portfolio-risk-agent"
      container_name = "portfolio-risk-agent"
      env = {
        DB_DSN          = var.db_dsn
        OPENAI_API_KEY  = var.openai_api_key
        AI_ENABLED      = var.openai_api_key != "" ? "true" : "false"
        SHOW_AUTH_DEBUG = "false"
      }
    }

    execution-agent = {
      route          = "/execution-agent*"
      priority       = 130
      cpu            = 512
      memory         = 1024
      desired_count  = 1
      health_path    = "/"
      image_name     = "execution-agent"
      container_name = "execution-agent"
      env = {
        DB_DSN            = var.db_dsn
        OPENAI_API_KEY    = var.openai_api_key
        AI_ENABLED        = var.openai_api_key != "" ? "true" : "false"
        SHOW_AUTH_DEBUG   = "false"
        REPORT_OUTPUT_DIR = "reports"
        SLACK_WEBHOOK_URL = var.slack_webhook_url
        REPORT_EMAIL_TO   = var.report_email_to
        SMTP_HOST         = var.smtp_host
        SMTP_PORT         = var.smtp_port
        SMTP_USER         = var.smtp_user
        SMTP_PASSWORD     = var.smtp_password
        SMTP_FROM         = var.smtp_from
      }
    }

    it-dev-agent = {
      route          = "/it-dev-agent*"
      priority       = 140
      cpu            = 512
      memory         = 1024
      desired_count  = 1
      health_path    = "/"
      image_name     = "it-dev-agent"
      container_name = "it-dev-agent"
      env = {
        DB_DSN          = var.db_dsn
        OPENAI_API_KEY  = var.openai_api_key
        AI_ENABLED      = var.openai_api_key != "" ? "true" : "false"
        SHOW_AUTH_DEBUG = "false"
      }
    }

    uploader = {
      route          = "/upload*"
      priority       = 150
      cpu            = 512
      memory         = 1024
      desired_count  = 1
      health_path    = "/"
      image_name     = "uploader"
      container_name = "uploader"
      env = {
        DB_DSN          = var.db_dsn
        OPENAI_API_KEY  = var.openai_api_key
        AI_ENABLED      = var.openai_api_key != "" ? "true" : "false"
        SHOW_AUTH_DEBUG = "false"
      }
    }

    inner-mongolia = {
      route          = "/inner-mongolia*"
      priority       = 160
      cpu            = 512
      memory         = 1024
      desired_count  = 1
      health_path    = "/"
      image_name     = "inner-mongolia"
      container_name = "inner-mongolia"
      env = {
        DB_DSN          = var.db_dsn
        OPENAI_API_KEY  = var.openai_api_key
        AI_ENABLED      = var.openai_api_key != "" ? "true" : "false"
        SHOW_AUTH_DEBUG = "false"
      }
    }

    bess-map = {
      route          = "/bess-map*"
      priority       = 170
      cpu            = 1024
      memory         = 2048
      desired_count  = 1
      health_path    = "/"
      image_name     = "bess-map"
      container_name = "bess-map"
      env = {
        DB_DSN          = var.db_dsn
        OPENAI_API_KEY  = var.openai_api_key
        AI_ENABLED      = var.openai_api_key != "" ? "true" : "false"
        SHOW_AUTH_DEBUG = "false"
      }
    }
  }
}