output "alb_dns_name" {
  value = aws_lb.app.dns_name
}

output "rds_endpoint" {
  value = aws_db_instance.pg.address
}


output "uploads_bucket" {
  value = aws_s3_bucket.uploads.bucket
}
