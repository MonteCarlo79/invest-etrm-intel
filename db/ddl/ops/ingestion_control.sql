-- Unified data-ingestion ops control tables
-- Appends to existing ops schema (created by mengxi_agent4_reliability.sql)
-- Run: psql $PGURL -f db/ddl/ops/ingestion_control.sql

CREATE TABLE IF NOT EXISTS ops.ingestion_job_runs (
    id              BIGSERIAL PRIMARY KEY,
    collector       TEXT NOT NULL,            -- enos_market | tt_api | lingfeng
    run_mode        TEXT NOT NULL,            -- daily | reconcile | backfill
    start_date      DATE,
    end_date        DATE,
    dataset_filter  TEXT,
    dry_run         BOOLEAN NOT NULL DEFAULT FALSE,
    status          TEXT NOT NULL DEFAULT 'running',  -- running | success | failed | skipped
    rows_written    INTEGER,
    error_message   TEXT,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ,
    ecs_task_id     TEXT
);

CREATE TABLE IF NOT EXISTS ops.ingestion_dataset_status (
    collector       TEXT NOT NULL,
    dataset         TEXT NOT NULL,            -- table name or logical dataset key
    last_run_at     TIMESTAMPTZ,
    last_success_at TIMESTAMPTZ,
    last_date_seen  DATE,
    failure_count   INTEGER NOT NULL DEFAULT 0,
    notes           TEXT,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (collector, dataset)
);

CREATE TABLE IF NOT EXISTS ops.ingestion_expected_freshness (
    dataset         TEXT PRIMARY KEY,         -- target table name
    collector       TEXT NOT NULL,
    date_column     TEXT NOT NULL,            -- e.g. data_date, time, date
    max_lag_days    INTEGER NOT NULL DEFAULT 2,
    active          BOOLEAN NOT NULL DEFAULT TRUE,
    notes           TEXT
);

CREATE TABLE IF NOT EXISTS ops.ingestion_gap_queue (
    id              BIGSERIAL PRIMARY KEY,
    dataset         TEXT NOT NULL,
    collector       TEXT NOT NULL,
    gap_start       DATE NOT NULL,
    gap_end         DATE NOT NULL,
    detected_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    status          TEXT NOT NULL DEFAULT 'pending',  -- pending | dispatched | resolved | suppressed
    dispatched_at   TIMESTAMPTZ,
    ecs_task_id     TEXT,
    resolved_at     TIMESTAMPTZ,
    notes           TEXT,
    UNIQUE (dataset, gap_start, gap_end, status)
);

-- Seed freshness config for known tables
INSERT INTO ops.ingestion_expected_freshness (dataset, collector, date_column, max_lag_days) VALUES
  ('marketdata.md_da_cleared_energy',        'enos_market', 'data_date', 2),
  ('marketdata.md_da_fuel_summary',          'enos_market', 'data_date', 2),
  ('marketdata.md_id_cleared_energy',        'enos_market', 'data_date', 2),
  ('marketdata.md_id_fuel_summary',          'enos_market', 'data_date', 2),
  ('marketdata.md_rt_nodal_price',           'enos_market', 'data_date', 2),
  ('marketdata.md_rt_total_cleared_energy',  'enos_market', 'data_date', 2),
  ('marketdata.md_settlement_ref_price',     'enos_market', 'data_date', 2),
  ('public.hist_mengxi_suyou_clear',         'tt_api', 'date', 3),
  ('public.hist_mengxi_wulate_clear',        'tt_api', 'date', 3),
  ('public.hist_shandong_binzhou_clear',     'tt_api', 'date', 3),
  ('public.hist_anhui_dingyuan_clear',       'tt_api', 'date', 3)
ON CONFLICT (dataset) DO NOTHING;
