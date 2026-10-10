"""Read helpers for staging.openinfra_* — geographic grid underlay data.

Tables are written by services/openinfra/extract_gpkg.py (one-shot per gpkg
edition, run via ECS run-task). All functions degrade to empty frames when the
tables don't exist yet (local dev without the staging load).
"""
from __future__ import annotations

import json

import pandas as pd
from sqlalchemy import text

from services.openinfra.extents import EXTENTS, TAB_PROVINCE_TO_TAG

_SUBS_SQL = """
SELECT s.name, s.voltages, s.max_voltage, s.lon, s.lat
FROM staging.openinfra_substations s
JOIN (SELECT extent_tag, MAX(edition_tag) AS ed FROM staging.openinfra_substations
      WHERE extent_tag = :tag GROUP BY extent_tag) latest
  ON s.extent_tag = latest.extent_tag AND s.edition_tag = latest.ed
WHERE s.extent_tag = :tag
"""

_LINES_SQL = """
SELECT l.max_voltage, l.path_json
FROM staging.openinfra_lines l
JOIN (SELECT extent_tag, MAX(edition_tag) AS ed FROM staging.openinfra_lines
      WHERE extent_tag = :tag GROUP BY extent_tag) latest
  ON l.extent_tag = latest.extent_tag AND l.edition_tag = latest.ed
WHERE l.extent_tag = :tag
"""


def _run(engine, sql: str, tag: str) -> pd.DataFrame:
    try:
        with engine.connect() as conn:
            return pd.read_sql(text(sql), conn, params={"tag": tag})
    except Exception:
        return pd.DataFrame()


def tag_for_province(province: str) -> str | None:
    return TAB_PROVINCE_TO_TAG.get(province)


def get_substations(engine, tag: str) -> pd.DataFrame:
    return _run(engine, _SUBS_SQL, tag)


def get_line_paths(engine, tag: str) -> list[dict]:
    """[{max_voltage, path: [[lon,lat],...]}] — path_json decoded."""
    df = _run(engine, _LINES_SQL, tag)
    return [{"max_voltage": r.max_voltage, "path": json.loads(r.path_json)}
            for r in df.itertuples()]


def extent_center(tag: str) -> tuple[float, float]:
    lon_min, lon_max, lat_min, lat_max = EXTENTS[tag]["extent"]
    return (lon_min + lon_max) / 2.0, (lat_min + lat_max) / 2.0
