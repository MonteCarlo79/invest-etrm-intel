-- db/ddl/monitoring/migrations/001_add_status_check_constraints.sql
--
-- B1/B2 hardening: add CHECK constraints for the two new status values
-- DATA_ABSENT and INDETERMINATE on monitoring.asset_realization_status, and
-- ensure fragility_level on monitoring.asset_fragility_status covers its
-- full set.
--
-- Idempotent: uses DO $$ ... EXCEPTION WHEN duplicate_object pattern.
-- Run order: after tables exist (asset_realization_status.sql, asset_fragility_status.sql).

DO $$
BEGIN
    ALTER TABLE monitoring.asset_realization_status
        ADD CONSTRAINT chk_realization_status_level
        CHECK (status_level IN (
            'NORMAL', 'WARN', 'ALERT', 'CRITICAL', 'DATA_ABSENT', 'INDETERMINATE'
        ));
EXCEPTION
    WHEN duplicate_object THEN NULL;
END;
$$;

DO $$
BEGIN
    ALTER TABLE monitoring.asset_fragility_status
        ADD CONSTRAINT chk_fragility_level
        CHECK (fragility_level IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL'));
EXCEPTION
    WHEN duplicate_object THEN NULL;
END;
$$;

DO $$
BEGIN
    ALTER TABLE monitoring.asset_fragility_status
        ADD CONSTRAINT chk_fragility_realization_status_level
        CHECK (realization_status_level IN (
            'NORMAL', 'WARN', 'ALERT', 'CRITICAL', 'DATA_ABSENT', 'INDETERMINATE'
        ));
EXCEPTION
    WHEN duplicate_object THEN NULL;
END;
$$;
