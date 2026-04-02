-- db/ddl/core/canon_mengxi_compat_views.sql
-- Compatibility canonical views for Mengxi trading refresh.
-- Additive bridge only: keeps existing hist_* and md_id_* tables as source-of-truth.

CREATE SCHEMA IF NOT EXISTS canon;

DO $$
DECLARE
    nodal_sql text := '';
BEGIN
    IF to_regclass('public.hist_mengxi_suyou_clear_15min') IS NOT NULL THEN
        nodal_sql := nodal_sql || '
            SELECT time::timestamptz AS time, ''suyou''::text AS asset_code, price::numeric AS price
            FROM public."hist_mengxi_suyou_clear_15min"
            WHERE time IS NOT NULL
            UNION ALL';
    END IF;
    IF to_regclass('public.hist_mengxi_wulate_clear_15min') IS NOT NULL THEN
        nodal_sql := nodal_sql || '
            SELECT time::timestamptz AS time, ''wulate''::text AS asset_code, price::numeric AS price
            FROM public."hist_mengxi_wulate_clear_15min"
            WHERE time IS NOT NULL
            UNION ALL';
    END IF;
    IF to_regclass('public.hist_mengxi_wuhai_clear_15min') IS NOT NULL THEN
        nodal_sql := nodal_sql || '
            SELECT time::timestamptz AS time, ''wuhai''::text AS asset_code, price::numeric AS price
            FROM public."hist_mengxi_wuhai_clear_15min"
            WHERE time IS NOT NULL
            UNION ALL';
    END IF;
    IF to_regclass('public.hist_mengxi_wulanchabu_clear_15min') IS NOT NULL THEN
        nodal_sql := nodal_sql || '
            SELECT time::timestamptz AS time, ''wulanchabu''::text AS asset_code, price::numeric AS price
            FROM public."hist_mengxi_wulanchabu_clear_15min"
            WHERE time IS NOT NULL
            UNION ALL';
    END IF;

    IF nodal_sql = '' THEN
        EXECUTE '
            CREATE OR REPLACE VIEW canon.nodal_rt_price_15min AS
            SELECT NULL::timestamptz AS time, NULL::text AS asset_code, NULL::numeric AS price
            WHERE FALSE';
    ELSE
        nodal_sql := left(nodal_sql, length(nodal_sql) - length('UNION ALL'));
        EXECUTE 'CREATE OR REPLACE VIEW canon.nodal_rt_price_15min AS ' || nodal_sql;
    END IF;
END $$;

DO $$
BEGIN
    IF to_regclass('marketdata.md_id_cleared_energy') IS NOT NULL
       AND to_regclass('core.asset_alias_map') IS NOT NULL THEN
        EXECUTE '
            CREATE OR REPLACE VIEW canon.scenario_dispatch_15min AS
            WITH mapped AS (
                SELECT
                    m.datetime::timestamptz AS time,
                    a.asset_code::text AS asset_code,
                    MAX((m.cleared_energy_mwh::numeric) * 4.0) AS dispatch_mw
                FROM marketdata.md_id_cleared_energy m
                JOIN core.asset_alias_map a
                  ON a.active_flag = TRUE
                 AND (
                    LOWER(TRIM(a.alias_value)) = LOWER(TRIM(COALESCE(m.plant_name, '''')))
                    OR LOWER(TRIM(a.alias_value)) = LOWER(TRIM(COALESCE(m.dispatch_unit_name, '''')))
                 )
                WHERE a.asset_code IN (
                    ''suyou'', ''wulate'', ''wuhai'', ''wulanchabu'',
                    ''hetao'', ''hangjinqi'', ''siziwangqi'', ''gushanliang''
                )
                  AND m.datetime IS NOT NULL
                  AND m.cleared_energy_mwh IS NOT NULL
                GROUP BY m.datetime, a.asset_code
            )
            SELECT time, asset_code, ''cleared_actual''::text AS scenario_name, dispatch_mw
            FROM mapped';
    ELSIF to_regclass('marketdata.md_id_cleared_energy') IS NOT NULL THEN
        EXECUTE '
            CREATE OR REPLACE VIEW canon.scenario_dispatch_15min AS
            SELECT
                m.datetime::timestamptz AS time,
                CASE
                    WHEN LOWER(COALESCE(m.plant_name, '''') || '' '' || COALESCE(m.dispatch_unit_name, '''')) LIKE ''%suyou%'' THEN ''suyou''
                    WHEN LOWER(COALESCE(m.plant_name, '''') || '' '' || COALESCE(m.dispatch_unit_name, '''')) LIKE ''%wulate%'' THEN ''wulate''
                    WHEN LOWER(COALESCE(m.plant_name, '''') || '' '' || COALESCE(m.dispatch_unit_name, '''')) LIKE ''%wuhai%'' THEN ''wuhai''
                    WHEN LOWER(COALESCE(m.plant_name, '''') || '' '' || COALESCE(m.dispatch_unit_name, '''')) LIKE ''%wulanchabu%'' THEN ''wulanchabu''
                    ELSE NULL
                END::text AS asset_code,
                ''cleared_actual''::text AS scenario_name,
                (m.cleared_energy_mwh::numeric) * 4.0 AS dispatch_mw
            FROM marketdata.md_id_cleared_energy m
            WHERE m.datetime IS NOT NULL
              AND m.cleared_energy_mwh IS NOT NULL
              AND (
                LOWER(COALESCE(m.plant_name, '''') || '' '' || COALESCE(m.dispatch_unit_name, '''')) LIKE ''%suyou%''
                OR LOWER(COALESCE(m.plant_name, '''') || '' '' || COALESCE(m.dispatch_unit_name, '''')) LIKE ''%wulate%''
                OR LOWER(COALESCE(m.plant_name, '''') || '' '' || COALESCE(m.dispatch_unit_name, '''')) LIKE ''%wuhai%''
                OR LOWER(COALESCE(m.plant_name, '''') || '' '' || COALESCE(m.dispatch_unit_name, '''')) LIKE ''%wulanchabu%''
              )';
    ELSE
        EXECUTE '
            CREATE OR REPLACE VIEW canon.scenario_dispatch_15min AS
            SELECT
                NULL::timestamptz AS time,
                NULL::text AS asset_code,
                NULL::text AS scenario_name,
                NULL::numeric AS dispatch_mw
            WHERE FALSE';
    END IF;
END $$;
