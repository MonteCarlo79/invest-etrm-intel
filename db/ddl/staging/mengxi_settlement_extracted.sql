-- db/ddl/staging/mengxi_settlement_extracted.sql
-- Staging table for extracted settlement data (上网电量) from Mengxi settlement PDFs
-- Created: 2026-03-26
-- Author: Matrix Agent

CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.mengxi_settlement_extracted (
    extraction_id       uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    source_file_id      uuid,                           -- FK to raw_data.file_registry if registered
    source_filename     text        NOT NULL,
    settlement_month    date        NOT NULL,           -- First day of month
    station_name_raw    text        NOT NULL,           -- Raw station name from PDF
    asset_code          text,                           -- Normalized asset code (NULL if not mapped)
    discharge_mwh       numeric,                        -- 上网电量 in MWh
    discharge_kwh       numeric,                        -- 上网电量 in kWh (original unit if provided)
    settlement_yuan     numeric,                        -- 结算金额 if available
    parse_confidence    text        DEFAULT 'high',     -- 'high', 'medium', 'low'
    parse_notes         text,
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_mengxi_sett_ext_month 
    ON staging.mengxi_settlement_extracted(settlement_month);
CREATE INDEX IF NOT EXISTS idx_mengxi_sett_ext_asset 
    ON staging.mengxi_settlement_extracted(asset_code);

COMMENT ON TABLE staging.mengxi_settlement_extracted IS 
    'Extracted BESS settlement data (上网电量) from Mengxi settlement PDFs (上网结算单)';
