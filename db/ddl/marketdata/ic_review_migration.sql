-- IC case + 复盘 review store: additive, idempotent migration.
-- Applied by scripts/asset_risk/migrate_ic_review.py (single transaction).
CREATE TABLE IF NOT EXISTS marketdata.rm_ic_assumptions (
  asset_code TEXT NOT NULL,
  op_year INT NOT NULL,
  soh DOUBLE PRECISION,
  cycles_per_year DOUBLE PRECISION,
  charge_price DOUBLE PRECISION,
  discharge_price DOUBLE PRECISION,
  dod DOUBLE PRECISION,
  rte DOUBLE PRECISION,
  capcomp_rate DOUBLE PRECISION,
  capcomp_years INT,
  grid_fee DOUBLE PRECISION,
  capacity_mw DOUBLE PRECISION,
  energy_mwh DOUBLE PRECISION,
  ic_cod DATE,
  case_label TEXT,
  source_file TEXT,
  uploaded_at TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (asset_code, op_year)
);
CREATE TABLE IF NOT EXISTS marketdata.rm_review_metrics (
  asset_code TEXT NOT NULL,
  month DATE NOT NULL,
  metric TEXT NOT NULL,
  value DOUBLE PRECISION,
  unit TEXT,
  source_file TEXT,
  file_hash TEXT,
  extracted_by TEXT,
  uploaded_at TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (asset_code, month, metric)
);
