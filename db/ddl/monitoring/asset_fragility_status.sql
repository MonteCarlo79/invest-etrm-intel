-- db/ddl/monitoring/asset_fragility_status.sql
--
-- Daily fragility status snapshot per BESS asset.
-- Depends on monitoring.asset_realization_status being populated first.
-- Populated by services/monitoring/run_fragility_monitor.py.
-- Agents read this table directly (Pattern A) — no model dispatch.

CREATE SCHEMA IF NOT EXISTS monitoring;

CREATE TABLE IF NOT EXISTS monitoring.asset_fragility_status (
    fragility_id          uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    asset_code            text        NOT NULL,
    snapshot_date         date        NOT NULL,

    -- Component scores (0.0 = healthy, 1.0 = most fragile)
    -- realization_score: derived from realization_ratio
    --   NORMAL → 0.0, WARN → 0.33, ALERT → 0.67, CRITICAL → 1.0
    realization_score     numeric,

    -- trend_score: worsening trend over last 7d vs prior 7d window
    --   improving → 0.0, stable → 0.2, deteriorating → 0.7, sharp decline → 1.0
    trend_score           numeric,

    -- Composite score: weighted combination of component scores
    -- weights: realization=0.70, trend=0.30
    composite_score       numeric,

    -- Fragility classification
    -- LOW      composite_score < 0.25
    -- MEDIUM   composite_score in [0.25, 0.50)
    -- HIGH     composite_score in [0.50, 0.75)
    -- CRITICAL composite_score >= 0.75
    fragility_level       text        NOT NULL,

    -- Supporting data (copied from realization_status for convenience)
    realization_ratio            numeric,
    realization_status_level     text,
    days_in_window               integer,

    -- Trend data
    recent_ratio          numeric,    -- avg ratio last 7d
    prior_ratio           numeric,    -- avg ratio prior 7d
    ratio_delta           numeric,    -- recent_ratio - prior_ratio

    -- Which factor contributes most to composite_score
    dominant_factor       text,

    narrative             text,
    computed_at           timestamptz NOT NULL DEFAULT now(),

    UNIQUE (asset_code, snapshot_date)
);

CREATE INDEX IF NOT EXISTS idx_fragility_status_asset_date
    ON monitoring.asset_fragility_status (asset_code, snapshot_date DESC);

CREATE INDEX IF NOT EXISTS idx_fragility_status_level
    ON monitoring.asset_fragility_status (snapshot_date DESC, fragility_level);

COMMENT ON TABLE monitoring.asset_fragility_status IS
    'Daily composite fragility score for BESS assets. '
    'Combines realization ratio and trend into a single actionable status. '
    'Updated by services/monitoring/run_fragility_monitor.py after realization monitor.';
