-- db/ddl/raw_data/file_registry.sql
-- Raw document landing zone: file registry
-- Created: 2026-03-26
-- Author: Matrix Agent
--
-- Purpose: Track all raw files uploaded to the platform before parsing.
-- Pattern: Raw originals in S3 landing zone, metadata in Postgres.
-- This is the foundation for settlement PDFs, compensation files, and similar documents.

CREATE SCHEMA IF NOT EXISTS raw_data;

CREATE TABLE IF NOT EXISTS raw_data.file_registry (
    file_id             uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- S3 location
    s3_bucket           text        NOT NULL,
    s3_key              text        NOT NULL,
    
    -- File metadata
    original_filename   text        NOT NULL,
    file_extension      text,
    file_size_bytes     bigint,
    content_md5         text,
    mime_type           text,
    
    -- Classification
    document_type       text        NOT NULL,  -- 'settlement_pdf', 'compensation_file', 'dispatch_report', etc.
    province            text,                  -- 'Mengxi', 'Anhui', etc.
    asset_code          text,                  -- If file is asset-specific
    settlement_month    date,                  -- For monthly settlement files
    
    -- Processing status
    upload_status       text        NOT NULL DEFAULT 'uploaded',  -- 'uploaded', 'processing', 'parsed', 'failed'
    parse_status        text,                  -- 'pending', 'success', 'partial', 'failed'
    parse_error         text,
    
    -- Audit
    uploaded_by         text,
    uploaded_at         timestamptz NOT NULL DEFAULT now(),
    processed_at        timestamptz,
    
    -- Soft delete
    active_flag         boolean     NOT NULL DEFAULT TRUE,
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),
    
    UNIQUE (s3_bucket, s3_key)
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_file_registry_document_type ON raw_data.file_registry (document_type);
CREATE INDEX IF NOT EXISTS idx_file_registry_province ON raw_data.file_registry (province);
CREATE INDEX IF NOT EXISTS idx_file_registry_settlement_month ON raw_data.file_registry (settlement_month);
CREATE INDEX IF NOT EXISTS idx_file_registry_upload_status ON raw_data.file_registry (upload_status);
CREATE INDEX IF NOT EXISTS idx_file_registry_uploaded_at ON raw_data.file_registry (uploaded_at DESC);

COMMENT ON TABLE raw_data.file_registry IS 'Registry of all raw files uploaded to the platform landing zone (S3)';
COMMENT ON COLUMN raw_data.file_registry.document_type IS 'Type classification: settlement_pdf, compensation_file, dispatch_report, grid_notice, etc.';
COMMENT ON COLUMN raw_data.file_registry.upload_status IS 'Upload workflow status: uploaded -> processing -> parsed/failed';
