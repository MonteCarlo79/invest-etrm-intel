-- db/ddl/raw_data/file_manifest.sql
-- Raw document landing zone: file manifest (extracted content tracking)
-- Created: 2026-03-26
-- Author: Matrix Agent
--
-- Purpose: Track extracted/parsed content from raw files.
-- Links to file_registry and stores extracted data pointers.

CREATE SCHEMA IF NOT EXISTS raw_data;

CREATE TABLE IF NOT EXISTS raw_data.file_manifest (
    manifest_id         uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    file_id             uuid        NOT NULL REFERENCES raw_data.file_registry(file_id),
    
    -- Content identification
    content_type        text        NOT NULL,  -- 'settlement_summary', 'compensation_detail', 'discharge_data', etc.
    sequence_num        int         DEFAULT 1, -- For multi-part extractions
    
    -- Extracted data reference
    -- Option A: Inline JSON for small extractions
    extracted_json      jsonb,
    
    -- Option B: Pointer to downstream table for structured data
    target_schema       text,
    target_table        text,
    target_key_column   text,
    target_key_value    text,
    
    -- Extraction metadata
    extraction_method   text,       -- 'manual', 'ocr', 'tabula', 'pdfplumber', etc.
    extraction_version  text,
    confidence_score    numeric,
    
    -- Validation
    validation_status   text,       -- 'pending', 'validated', 'rejected'
    validated_by        text,
    validated_at        timestamptz,
    
    -- Audit
    active_flag         boolean     NOT NULL DEFAULT TRUE,
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_file_manifest_file_id ON raw_data.file_manifest (file_id);
CREATE INDEX IF NOT EXISTS idx_file_manifest_content_type ON raw_data.file_manifest (content_type);

COMMENT ON TABLE raw_data.file_manifest IS 'Tracks extracted/parsed content from raw files in file_registry';
COMMENT ON COLUMN raw_data.file_manifest.extracted_json IS 'Inline JSON for small extractions; use target_* columns for large structured data';
