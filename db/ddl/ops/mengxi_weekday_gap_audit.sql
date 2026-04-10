-- Reproducible Mengxi weekday gap audit across all source tables.
-- Source-file date semantics:
--   - data_date (same day): md_rt_nodal_price, md_rt_total_cleared_energy,
--     md_avg_bid_price, md_id_cleared_energy, md_id_fuel_summary, md_settlement_ref_price
--   - data_date - 1 day (source file date): md_da_cleared_energy, md_da_fuel_summary

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.mengxi_source_table_rules (
    table_name TEXT PRIMARY KEY,
    source_date_offset_days INTEGER NOT NULL
);

INSERT INTO ops.mengxi_source_table_rules (table_name, source_date_offset_days) VALUES
    ('marketdata.md_rt_nodal_price', 0),
    ('marketdata.md_rt_total_cleared_energy', 0),
    ('marketdata.md_da_cleared_energy', -1),
    ('marketdata.md_da_fuel_summary', -1),
    ('marketdata.md_avg_bid_price', 0),
    ('marketdata.md_id_cleared_energy', 0),
    ('marketdata.md_id_fuel_summary', 0),
    ('marketdata.md_settlement_ref_price', 0)
ON CONFLICT (table_name) DO UPDATE
SET source_date_offset_days = EXCLUDED.source_date_offset_days;

CREATE OR REPLACE VIEW ops.mengxi_weekday_gap_audit AS
WITH params AS (
    SELECT
        DATE '2025-07-01' AS audit_start_date,
        (CURRENT_DATE - INTERVAL '1 day')::date AS audit_end_date
),
weekdays AS (
    SELECT d::date AS dt
    FROM params p
    CROSS JOIN generate_series(
        p.audit_start_date,
        p.audit_end_date,
        interval '1 day'
    ) d
    WHERE EXTRACT(ISODOW FROM d) < 6
),
missing_union AS (
    SELECT
        'marketdata.md_rt_nodal_price'::text AS table_name,
        0::int AS source_date_offset_days,
        w.dt AS expected_data_date,
        w.dt AS source_file_date
    FROM weekdays w
    LEFT JOIN (SELECT DISTINCT data_date FROM marketdata.md_rt_nodal_price) t
      ON t.data_date = w.dt
    WHERE t.data_date IS NULL

    UNION ALL

    SELECT
        'marketdata.md_rt_total_cleared_energy'::text,
        0::int,
        w.dt,
        w.dt
    FROM weekdays w
    LEFT JOIN (SELECT DISTINCT data_date FROM marketdata.md_rt_total_cleared_energy) t
      ON t.data_date = w.dt
    WHERE t.data_date IS NULL

    UNION ALL

    SELECT
        'marketdata.md_da_cleared_energy'::text,
        -1::int,
        w.dt,
        (w.dt - interval '1 day')::date
    FROM weekdays w
    LEFT JOIN (SELECT DISTINCT data_date FROM marketdata.md_da_cleared_energy) t
      ON t.data_date = w.dt
    WHERE t.data_date IS NULL

    UNION ALL

    SELECT
        'marketdata.md_da_fuel_summary'::text,
        -1::int,
        w.dt,
        (w.dt - interval '1 day')::date
    FROM weekdays w
    LEFT JOIN (SELECT DISTINCT data_date FROM marketdata.md_da_fuel_summary) t
      ON t.data_date = w.dt
    WHERE t.data_date IS NULL

    UNION ALL

    SELECT
        'marketdata.md_avg_bid_price'::text,
        0::int,
        w.dt,
        w.dt
    FROM weekdays w
    LEFT JOIN (SELECT DISTINCT data_date FROM marketdata.md_avg_bid_price) t
      ON t.data_date = w.dt
    WHERE t.data_date IS NULL

    UNION ALL

    SELECT
        'marketdata.md_id_cleared_energy'::text,
        0::int,
        w.dt,
        w.dt
    FROM weekdays w
    LEFT JOIN (SELECT DISTINCT data_date FROM marketdata.md_id_cleared_energy) t
      ON t.data_date = w.dt
    WHERE t.data_date IS NULL

    UNION ALL

    SELECT
        'marketdata.md_id_fuel_summary'::text,
        0::int,
        w.dt,
        w.dt
    FROM weekdays w
    LEFT JOIN (SELECT DISTINCT data_date FROM marketdata.md_id_fuel_summary) t
      ON t.data_date = w.dt
    WHERE t.data_date IS NULL

    UNION ALL

    SELECT
        'marketdata.md_settlement_ref_price'::text,
        0::int,
        w.dt,
        w.dt
    FROM weekdays w
    LEFT JOIN (SELECT DISTINCT data_date FROM marketdata.md_settlement_ref_price) t
      ON t.data_date = w.dt
    WHERE t.data_date IS NULL
)
SELECT
    table_name,
    source_date_offset_days,
    expected_data_date,
    source_file_date
FROM missing_union
ORDER BY source_file_date, table_name;
