-- db/ddl/core/document_registry.sql
-- Business-level document registry
-- Created: 2026-03-26
-- Author: Matrix Agent
--
-- Purpose: Business-level view of documents across the platform.
-- Links to raw_data.file_registry for physical files.
-- Supports settlement invoices, compensation reports, policy documents, grid notices, etc.

CREATE SCHEMA IF NOT EXISTS core;

CREATE TABLE IF NOT EXISTS core.document_registry (
    document_id         uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Business classification
    document_category   text        NOT NULL,  -- 'settlement', 'compensation', 'policy', 'grid_notice', 'report'
    document_subcategory text,                 -- 'monthly_settlement', 'capacity_compensation', etc.
    
    -- Business context
    title               text        NOT NULL,
    description         text,
    province            text,
    asset_codes         text[],     -- Array of related asset codes
    
    -- Time context
    effective_date      date,
    period_start        date,
    period_end          date,
    settlement_month    date,       -- First day of settlement month
    
    -- Source tracking
    source_system       text,       -- 'grid_dispatch_center', 'manual_upload', etc.
    source_reference    text,       -- External document number/reference
    
    -- Physical file link
    file_id             uuid        REFERENCES raw_data.file_registry(file_id),
    
    -- Processing status
    processing_status   text        NOT NULL DEFAULT 'pending',  -- 'pending', 'extracted', 'validated', 'published'
    
    -- Key extracted values (denormalized for quick access)
    extracted_summary   jsonb,      -- Quick-access summary of key values
    
    -- Audit
    created_by          text,
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),
    active_flag         boolean     NOT NULL DEFAULT TRUE
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_document_registry_category ON core.document_registry (document_category);
CREATE INDEX IF NOT EXISTS idx_document_registry_province ON core.document_registry (province);
CREATE INDEX IF NOT EXISTS idx_document_registry_settlement_month ON core.document_registry (settlement_month);
CREATE INDEX IF NOT EXISTS idx_document_registry_file_id ON core.document_registry (file_id);

COMMENT ON TABLE core.document_registry IS 'Business-level document registry for settlements, compensation reports, policy docs, etc.';
COMMENT ON COLUMN core.document_registry.asset_codes IS 'Array of asset_code values this document relates to';
COMMENT ON COLUMN core.document_registry.extracted_summary IS 'Denormalized key values for quick access without joining to file_manifest';
