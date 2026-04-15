"""DB helpers — thin wrappers over existing patterns in services/bess_map/db.py."""
from __future__ import annotations
import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


def get_engine() -> Engine:
    url = os.environ.get("PGURL") or os.environ.get("DB_DSN")
    if not url:
        raise SystemExit("PGURL environment variable is required")
    return create_engine(url, pool_pre_ping=True)


def upsert_staging(engine: Engine, target_table: str, staging_df, conflict_cols: list[str]):
    """Temp-table + INSERT ... ON CONFLICT DO UPDATE — same pattern as services/bess_map/db.py."""
    tmp = f"_stg_{target_table.replace('.', '_')}_{os.getpid()}"
    with engine.begin() as conn:
        staging_df.to_sql(tmp, conn, if_exists="replace", index=False)
        cols = ", ".join(f'"{c}"' for c in staging_df.columns)
        conflict = ", ".join(f'"{c}"' for c in conflict_cols)
        updates = ", ".join(
            f'"{c}" = EXCLUDED."{c}"'
            for c in staging_df.columns if c not in conflict_cols
        )
        conn.execute(text(f"""
            INSERT INTO {target_table} ({cols})
            SELECT {cols} FROM {tmp}
            ON CONFLICT ({conflict}) DO UPDATE SET {updates};
            DROP TABLE IF EXISTS {tmp};
        """))
    return len(staging_df)


def delete_append(engine: Engine, table: str, date_col: str, dates: list):
    """Delete existing rows for given dates then let caller append."""
    if not dates:
        return
    with engine.begin() as conn:
        conn.execute(text(
            f"DELETE FROM {table} WHERE {date_col} = ANY(:dates)"
        ), {"dates": [str(d) for d in dates]})
