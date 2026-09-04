-- db/ddl/marketdata/ops_bess_dispatch.sql
--
-- Tables for Inner Mongolia BESS daily operations Excel ingestion.
-- All timestamps stored as TIMESTAMPTZ (China Standard Time = UTC+8, no DST).
--
-- Relationship between tables:
--   ops_dispatch_file_registry  (one row per ingested file)
--     └── ops_dispatch_asset_sheet_map  (one row per (file, sheet) pair)
--   ops_bess_dispatch_15min  (one row per (asset_code, interval_start) — superseded in-place)
--
-- Supersession model:
--   When a corrected file arrives for the same report_date:
--     1. New file inserted with ingest_version = MAX(previous) + 1, is_current = TRUE
--     2. Old file updated: is_current = FALSE (preserved for audit trail)
--     3. Fact rows updated via ON CONFLICT (asset_code, interval_start) DO UPDATE
--        → source_file_id updated to point at the new file
--
-- Interval semantics:
--   interval_start / interval_end are TIMESTAMPTZ with explicit +08:00 offset.
--   Excel times are naive local CST. Parser applies +08:00 when constructing timestamps.
--   Example: 2026-02-10 00:00:00+08 is the first 15-min slot of 2026-02-10 CST.

-- ---------------------------------------------------------------------------
-- File registry
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS marketdata.ops_dispatch_file_registry (
    id                   BIGSERIAL PRIMARY KEY,
    source_file_name     TEXT        NOT NULL,
    source_file_path     TEXT        NOT NULL,
    file_hash            TEXT        NOT NULL,   -- SHA-256 hex of raw file bytes
    report_date          DATE        NOT NULL,
    asset_code           TEXT,                   -- NULL = multi-asset file; per-sheet link in sheet_map
    ingested_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    parse_status         TEXT        NOT NULL DEFAULT 'pending',
    -- parse_status values: 'pending' | 'success' | 'partial' | 'failed'
    is_current           BOOLEAN     NOT NULL DEFAULT TRUE,
    -- TRUE = this file is the authoritative source for its report_date.
    -- FALSE = superseded by a later corrected file (kept for audit trail).
    supersedes_file_id   BIGINT      REFERENCES marketdata.ops_dispatch_file_registry(id),
    -- FK to the file being replaced. NULL for first ingest.
    ingest_version       INTEGER     NOT NULL DEFAULT 1,
    -- Monotonically increasing per report_date. Version 1 = first ingest, 2 = first correction, etc.
    sheet_count          INTEGER,
    row_count            INTEGER,
    notes                TEXT,
    UNIQUE (file_hash)
);

COMMENT ON TABLE marketdata.ops_dispatch_file_registry IS
    'Registry of all ingested Inner Mongolia BESS operations Excel files. '
    'One row per file ingest attempt. Corrected files supersede previous versions '
    'via is_current flag; old rows are never deleted.';

COMMENT ON COLUMN marketdata.ops_dispatch_file_registry.file_hash IS
    'SHA-256 hex digest of the raw file bytes. Used for exact-duplicate detection. '
    'Files with the same hash are silently skipped unless --force is passed.';

COMMENT ON COLUMN marketdata.ops_dispatch_file_registry.is_current IS
    'TRUE = this file is the authoritative source for its report_date. '
    'FALSE = superseded by a later corrected file. Superseded rows are preserved for audit trail.';

COMMENT ON COLUMN marketdata.ops_dispatch_file_registry.ingest_version IS
    'Monotonically increasing per report_date. Version 1 = first ingest, 2 = first correction, etc.';

COMMENT ON COLUMN marketdata.ops_dispatch_file_registry.supersedes_file_id IS
    'FK to the previous file entry being replaced. NULL for the first ingest of a report_date.';

CREATE INDEX IF NOT EXISTS idx_ops_file_registry_report_date
    ON marketdata.ops_dispatch_file_registry (report_date);

CREATE INDEX IF NOT EXISTS idx_ops_file_registry_is_current
    ON marketdata.ops_dispatch_file_registry (is_current, report_date);


-- ---------------------------------------------------------------------------
-- Asset sheet map (per file, per sheet)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS marketdata.ops_dispatch_asset_sheet_map (
    id                       BIGSERIAL   PRIMARY KEY,
    source_file_id           BIGINT      NOT NULL REFERENCES marketdata.ops_dispatch_file_registry(id),
    sheet_name               TEXT        NOT NULL,
    asset_nickname_cn        TEXT,        -- text before bracket, e.g. '苏右'
    asset_bracket_name_cn    TEXT,        -- text in bracket, e.g. '景蓝乌尔图'
    matched_asset_code       TEXT,        -- e.g. 'suyou'
    matched_dispatch_unit    TEXT,        -- full CN name in md_id_cleared_energy
    matched_plant_name       TEXT,
    match_method             TEXT        NOT NULL,
    -- match_method values: 'exact' | 'partial' | 'nickname' | 'db_config' | 'unmatched'
    price_match_n            INTEGER,     -- number of intervals matched to md_id_cleared_energy
    price_match_mae          NUMERIC(10,3), -- mean |excel_nodal_price - db_cleared_price| CNY/MWh
    price_match_r            NUMERIC(6,4),  -- Pearson correlation coefficient
    price_verification_level TEXT,
    -- 'high' (MAE < 5 CNY/MWh and n >= 80) | 'medium' (MAE < 20) | 'low' | 'unverified'
    price_verification_notes TEXT,         -- e.g. "96/96 matched, MAE=1.2 CNY/MWh, r=0.999"
    notes                    TEXT,
    UNIQUE (source_file_id, sheet_name)
);

COMMENT ON TABLE marketdata.ops_dispatch_asset_sheet_map IS
    'Per-sheet parse results for each ingested file. One row per (file, sheet) pair. '
    'Includes asset matching result and optional price verification against md_id_cleared_energy.';

COMMENT ON COLUMN marketdata.ops_dispatch_asset_sheet_map.price_match_mae IS
    'Mean absolute error between Excel nodal price and md_id_cleared_energy.cleared_price (CNY/MWh). '
    'NULL until price verification has been run (requires --verify-prices flag or manual call).';

COMMENT ON COLUMN marketdata.ops_dispatch_asset_sheet_map.price_verification_level IS
    'Summary quality level: high (MAE<5 and n>=80), medium (MAE<20), low, unverified. '
    'Thresholds defined as named constants in price_verifier.py.';

CREATE INDEX IF NOT EXISTS idx_ops_sheet_map_asset_code
    ON marketdata.ops_dispatch_asset_sheet_map (matched_asset_code);


-- ---------------------------------------------------------------------------
-- Fact table: 15-min dispatch intervals
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS marketdata.ops_bess_dispatch_15min (
    id                    BIGSERIAL   PRIMARY KEY,

    -- Operational identity (supersession key — unique per asset × interval)
    asset_code            TEXT        NOT NULL,
    interval_start        TIMESTAMPTZ NOT NULL,
    -- China Standard Time, UTC+8, no DST.
    -- Example: 2026-02-10T00:00:00+08:00 = first 15-min slot of 2026-02-10 CST.
    -- Excel times are naive local; parser applies +08:00 when constructing this field.
    interval_end          TIMESTAMPTZ NOT NULL,   -- = interval_start + 15 minutes
    data_date             DATE        NOT NULL,   -- local date in CST

    -- Provenance (updated in-place when a replacement file is ingested)
    source_file_id        BIGINT      NOT NULL REFERENCES marketdata.ops_dispatch_file_registry(id),
    sheet_name            TEXT        NOT NULL,
    dispatch_unit_name    TEXT,        -- matched CN dispatch unit name; NULL if sheet unmatched

    -- Normalized operational values
    nominated_dispatch_mw NUMERIC(10,3),
    -- 申报曲线 (nominated dispatch schedule) in MW.
    -- Sign convention (grid-operator / ops-file convention):
    --   Negative = discharge (BESS outputting power to grid, shown as negative load).
    --   Positive = charge    (BESS consuming power from grid, shown as positive load).
    -- NOTE: this is the BESS operator's nomination to the grid operator.
    -- It is NOT the same as md_id_cleared_energy.cleared_energy_mwh (DA market-cleared trading energy).
    actual_dispatch_mw    NUMERIC(10,3),
    -- 实际充放曲线 (actual dispatch / physical output) in MW.
    -- Sign convention same as nominated_dispatch_mw:
    --   Negative = discharge (BESS outputting), positive = charge (BESS consuming).
    -- NOTE: this is the physical output as reported in the operations file.
    -- It is NOT the same as md_id_cleared_energy.cleared_energy_mwh (DA market-cleared trading energy).
    nodal_price_excel     NUMERIC(10,3),
    -- 节点电价 (nodal real-time electricity price) in CNY/MWh.
    -- Sourced from Excel column E; cross-checked against md_id_cleared_energy.cleared_price
    -- during optional price verification.

    -- Raw source values (for audit and debugging)
    raw_nominated         TEXT,        -- cell value from col B before numeric coercion, e.g. "-37", "--", ""
    raw_actual            TEXT,        -- cell value from col D before numeric coercion
    raw_nodal_price       TEXT,        -- cell value from col E before numeric coercion
    raw_payload           JSONB,       -- full parsed row dict including row_number from openpyxl

    -- Price cross-reference (populated by price_verifier; NULL until verified)
    cleared_price_db      NUMERIC(10,3),  -- corresponding price from md_id_cleared_energy
    price_diff_abs        NUMERIC(10,3),  -- |nodal_price_excel - cleared_price_db|
    price_match_flag      BOOLEAN,        -- TRUE if within tolerance threshold

    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- NOT updated on replacement — preserves original first-ingest timestamp.

    -- Supersession key: one row per asset × 15-min slot.
    -- When a corrected file arrives, source_file_id and all value columns are updated in-place
    -- via ON CONFLICT (asset_code, interval_start) DO UPDATE. created_at is preserved.
    UNIQUE (asset_code, interval_start)
);

COMMENT ON TABLE marketdata.ops_bess_dispatch_15min IS
    'Inner Mongolia BESS 15-min operational dispatch intervals from daily Excel reports. '
    'Unique key is (asset_code, interval_start). Rows are updated in-place when a corrected '
    'file is ingested; source_file_id always points to the current authoritative file. '
    'All timestamps in TIMESTAMPTZ with explicit +08:00 offset (CST, no DST).';

COMMENT ON COLUMN marketdata.ops_bess_dispatch_15min.interval_start IS
    'Interval start in TIMESTAMPTZ. China Standard Time = UTC+8, no DST. '
    'Example: 2026-02-10 00:00:00+08 = first 15-min slot of 2026-02-10 in CST. '
    'Excel times are naive local; +08:00 offset is applied by the parser.';

COMMENT ON COLUMN marketdata.ops_bess_dispatch_15min.nominated_dispatch_mw IS
    'MW from 申报曲线 (nominated dispatch schedule). '
    'Sign convention (ops-file / grid-operator perspective): '
    'Negative=discharge (BESS outputting power), positive=charge (BESS consuming power). '
    'This is the BESS operator nomination to the grid operator — NOT the same as '
    'md_id_cleared_energy.cleared_energy_mwh (DA market-cleared trading energy).';

COMMENT ON COLUMN marketdata.ops_bess_dispatch_15min.actual_dispatch_mw IS
    'MW from 实际充放曲线 (actual dispatch). Physical output as reported in operations file. '
    'Sign convention same as nominated_dispatch_mw: '
    'Negative=discharge (BESS outputting), positive=charge (BESS consuming). NOT the same as '
    'md_id_cleared_energy.cleared_energy_mwh (DA market-cleared trading energy). '
    'Actual output may differ from cleared energy due to asset constraints or grid intervention.';

COMMENT ON COLUMN marketdata.ops_bess_dispatch_15min.nodal_price_excel IS
    '节点电价 (nodal real-time electricity price) in CNY/MWh from Excel column E. '
    'Cross-checked against md_id_cleared_energy.cleared_price during price verification.';

COMMENT ON COLUMN marketdata.ops_bess_dispatch_15min.source_file_id IS
    'FK to ops_dispatch_file_registry. Updated in-place when a corrected file supersedes '
    'the original. Always points to the current authoritative file (is_current=TRUE).';

COMMENT ON COLUMN marketdata.ops_bess_dispatch_15min.created_at IS
    'Timestamp of the FIRST ingest of this (asset_code, interval_start). '
    'NOT updated when a replacement file overwrites the row values.';

CREATE INDEX IF NOT EXISTS idx_ops_dispatch_15min_asset_date
    ON marketdata.ops_bess_dispatch_15min (asset_code, data_date);

CREATE INDEX IF NOT EXISTS idx_ops_dispatch_15min_data_date
    ON marketdata.ops_bess_dispatch_15min (data_date);

CREATE INDEX IF NOT EXISTS idx_ops_dispatch_15min_source_file
    ON marketdata.ops_bess_dispatch_15min (source_file_id);


-- ---------------------------------------------------------------------------
-- Future-ready asset sheet config stub
-- ---------------------------------------------------------------------------
-- Parser falls back to STATIC_ASSET_SHEET_MAP in matcher.py if this table is
-- absent or empty. When populated, this table overrides the static map without
-- requiring any code changes in the parser or matcher.

CREATE TABLE IF NOT EXISTS marketdata.ops_asset_sheet_config (
    asset_code           TEXT    NOT NULL,
    dispatch_unit_name   TEXT    NOT NULL,  -- full CN name in md_id_cleared_energy
    plant_name           TEXT,
    sheet_nickname_cn    TEXT    NOT NULL,  -- text before bracket, e.g. '苏右'
    sheet_bracket_cn     TEXT    NOT NULL,  -- text inside bracket, e.g. '景蓝乌尔图'
    active               BOOLEAN NOT NULL DEFAULT TRUE,
    notes                TEXT,
    PRIMARY KEY (asset_code)
);

COMMENT ON TABLE marketdata.ops_asset_sheet_config IS
    'Governed mapping from Excel sheet names to asset/dispatch-unit identities. '
    'When this table is populated, it overrides STATIC_ASSET_SHEET_MAP in matcher.py '
    'via load_asset_map(engine). This allows the asset map to be managed in the DB '
    'without changing any parser code. Falls back to static map if table is empty.';

COMMENT ON COLUMN marketdata.ops_asset_sheet_config.sheet_nickname_cn IS
    'Text before the bracket in the sheet name, e.g. "苏右" from "苏右（景蓝乌尔图）". '
    'Used as the primary key for sheet name matching (disambiguation token).';

COMMENT ON COLUMN marketdata.ops_asset_sheet_config.sheet_bracket_cn IS
    'Text inside the bracket in the sheet name, e.g. "景蓝乌尔图" from "苏右（景蓝乌尔图）". '
    'Brackets may be ASCII () or full-width （）; parser normalises before matching.';
