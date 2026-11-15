-- Market-maker central risk book: additive, idempotent migration.
-- Applied by scripts/market_maker/migrate.py (single transaction). The source
-- CHECK constraint name is looked up in pg_constraint by the script; the DROP
-- below uses the conventional name for documentation only.
ALTER TABLE marketdata.rm_positions ADD COLUMN IF NOT EXISTS source TEXT;
ALTER TABLE marketdata.rm_positions ADD COLUMN IF NOT EXISTS tou_block TEXT;
ALTER TABLE marketdata.rm_positions ADD COLUMN IF NOT EXISTS basis TEXT;
UPDATE marketdata.rm_positions SET source = 'bilateral' WHERE source IS NULL;
ALTER TABLE marketdata.rm_forward_curves ADD COLUMN IF NOT EXISTS tou_block TEXT;
ALTER TABLE marketdata.rm_forward_curves DROP CONSTRAINT IF EXISTS rm_forward_curves_source_check;
ALTER TABLE marketdata.rm_forward_curves ADD CONSTRAINT rm_forward_curves_source_check
  CHECK (source IN ('lingfeng','manual','exchange','spot_seasonal','traded','venue'));
CREATE UNIQUE INDEX IF NOT EXISTS uq_rm_fc_reference ON marketdata.rm_forward_curves
  (province, product, curve_date, delivery_date, COALESCE(tou_block,'base'), source) WHERE delivery_hour IS NULL;
