-- db/ddl/staging/mengxi_compensation_extracted.sql
-- Staging table for extracted compensation data from Mengxi settlement PDFs
-- Created: 2026-03-26
-- Author: Matrix Agent

CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.mengxi_compensation_extracted (
    extraction_id       uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    source_file_id      uuid,
    source_filename     text        NOT NULL,
    settlement_month    date        NOT NULL,
    station_name_raw    text        NOT NULL,
    asset_code          text,
    compensation_yuan   numeric     NOT NULL,
    parse_confidence    text        DEFAULT 'high',
    parse_notes         text,
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_mengxi_comp_ext_month 
    ON staging.mengxi_compensation_extracted(settlement_month);
CREATE INDEX IF NOT EXISTS idx_mengxi_comp_ext_asset 
    ON staging.mengxi_compensation_extracted(asset_code);

COMMENT ON TABLE staging.mengxi_compensation_extracted IS 
    'Extracted BESS compensation data from Mengxi settlement PDFs';
