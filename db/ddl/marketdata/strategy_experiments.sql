-- Strategy promote loop: rolling evaluation of capture/forecast strategies.
-- Additive table (rollback: DROP TABLE marketdata.strategy_experiments).
CREATE TABLE IF NOT EXISTS marketdata.strategy_experiments (
    id                      BIGSERIAL PRIMARY KEY,
    scope                   TEXT NOT NULL DEFAULT 'bess_capture',
    model                   TEXT NOT NULL,
    province                TEXT NOT NULL,
    duration_h              DOUBLE PRECISION NOT NULL,
    power_mw                DOUBLE PRECISION NOT NULL,
    roundtrip_eff           DOUBLE PRECISION NOT NULL,
    window_days             INT NOT NULL,
    window_end              DATE NOT NULL,
    days                    INT NOT NULL,
    mean_capture_rate       DOUBLE PRECISION,
    mean_realized_per_mwh   DOUBLE PRECISION,
    mean_theoretical_per_mwh DOUBLE PRECISION,
    delta_vs_champion       DOUBLE PRECISION,
    status                  TEXT NOT NULL DEFAULT 'candidate'
        CHECK (status IN ('champion', 'candidate', 'retired-candidate')),
    note                    TEXT,
    evaluated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (scope, model, province, duration_h, power_mw, roundtrip_eff, window_days, window_end)
);
CREATE INDEX IF NOT EXISTS ix_strategy_experiments_lb
    ON marketdata.strategy_experiments (scope, province, window_end DESC);
