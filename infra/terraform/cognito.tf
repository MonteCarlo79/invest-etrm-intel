resource "aws_cognito_user_pool" "bess_users" {
  name = "bess-platform-users"

  username_attributes = ["email"]

  password_policy {
    minimum_length = 8
  }

  auto_verified_attributes = ["email"]
}

resource "aws_cognito_user_pool_client" "bess_client" {
  name         = "bess-platform-alb-client"
  user_pool_id = aws_cognito_user_pool.bess_users.id

  generate_secret = true

  allowed_oauth_flows_user_pool_client = true
  allowed_oauth_flows                  = ["code"]
  allowed_oauth_scopes                 = ["openid"]

  supported_identity_providers = ["COGNITO"]

  callback_urls = var.cognito_callback_urls

  logout_urls = [
    "https://pjh-etrm.ai",
    "https://pjh-etrm.ai/portal/",
    "https://pjh-etrm.ai/signed-out",
    "https://www.pjh-etrm.ai",
    "https://www.pjh-etrm.ai/portal/",
    "https://www.pjh-etrm.ai/signed-out"
  ]

  default_redirect_uri = var.cognito_default_redirect_uri
}

resource "aws_cognito_user_pool_domain" "main" {
  domain       = "bess-platform-auth"
  user_pool_id = aws_cognito_user_pool.bess_users.id
}

resource "aws_cognito_user_group" "admin" {
  name         = "Admin"
  user_pool_id = aws_cognito_user_pool.bess_users.id
}

resource "aws_cognito_user_group" "trader" {
  name         = "Trader"
  user_pool_id = aws_cognito_user_pool.bess_users.id
}

resource "aws_cognito_user_group" "quant" {
  name         = "Quant"
  user_pool_id = aws_cognito_user_pool.bess_users.id
}

resource "aws_cognito_user_group" "analyst" {
  name         = "Analyst"
  user_pool_id = aws_cognito_user_pool.bess_users.id
}

resource "aws_cognito_user_group" "viewer" {
  name         = "Viewer"
  user_pool_id = aws_cognito_user_pool.bess_users.id
}