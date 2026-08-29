# s3_backup.tf
# ---------------------------------------------------------------------------
# Versioned S3 bucket for MacBook local-only criticals that git does not cover:
#   config/.env, terraform.tfvars, terraform.tfstate,
#   .claude/settings.local.json, ~/.claude auto-memory
# Written by scripts/backup_criticals_to_s3.sh (launchd: ai.pjh-etrm.s3-backup).
# Backup semantics: upload-only, never deletes — versioning keeps 30d of history.
# ---------------------------------------------------------------------------

resource "aws_s3_bucket" "macbook_backup" {
  bucket = "bess-platform-macbook-backup"
  tags   = local.tags
}

resource "aws_s3_bucket_public_access_block" "macbook_backup" {
  bucket                  = aws_s3_bucket.macbook_backup.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "macbook_backup" {
  bucket = aws_s3_bucket.macbook_backup.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "macbook_backup" {
  bucket = aws_s3_bucket.macbook_backup.id

  rule {
    id     = "expire-old-versions"
    status = "Enabled"

    noncurrent_version_expiration {
      noncurrent_days = 30
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

output "macbook_backup_bucket" {
  value = aws_s3_bucket.macbook_backup.bucket
}
