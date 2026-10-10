"""Extract OpenInfraMap CHN.gpkg -> staging.openinfra_substations / _lines.

One-shot per edition. The gpkg stays on disk (or S3); staging holds plain
floats + JSON paths (no PostGIS). PK (oim_fid, extent_tag) — a feature inside
two overlapping extents lands once per extent.

Usage:
    python -m services.openinfra.extract_gpkg --gpkg /path/CHN.gpkg \
        --extent mengxi --edition 2026-10 --dry-run          # counts only
    python -m services.openinfra.extract_gpkg --gpkg s3://bucket/CHN.gpkg \
        --extent all --edition 2026-10                        # full load
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path

from services.openinfra.extents import EXTENTS
from services.openinfra.gpkg import downsample, parse_wkb_coords, read_gpb_wkb

DDL = """
CREATE TABLE IF NOT EXISTS staging.openinfra_substations (
    oim_fid     BIGINT      NOT NULL,
    extent_tag  TEXT        NOT NULL,
    name        TEXT        NOT NULL,
    voltages    TEXT,
    max_voltage INTEGER,
    operator    TEXT,
    lon         DOUBLE PRECISION NOT NULL,
    lat         DOUBLE PRECISION NOT NULL,
    geom_type   TEXT        NOT NULL,
    edition_tag TEXT        NOT NULL,
    PRIMARY KEY (oim_fid, extent_tag)
);
CREATE INDEX IF NOT EXISTS idx_oim_subs_extent ON staging.openinfra_substations (extent_tag);

CREATE TABLE IF NOT EXISTS staging.openinfra_lines (
    oim_fid     BIGINT      NOT NULL,
    extent_tag  TEXT        NOT NULL,
    path_seq    INTEGER     NOT NULL,
    name        TEXT,
    voltages    TEXT,
    max_voltage INTEGER,
    path_json   TEXT        NOT NULL,
    edition_tag TEXT        NOT NULL,
    PRIMARY KEY (oim_fid, extent_tag, path_seq)
);
CREATE INDEX IF NOT EXISTS idx_oim_lines_extent ON staging.openinfra_lines (extent_tag);
"""


def _fetch_gpkg(src: str, workdir: str = "/tmp") -> str:
    """Local path passthrough; s3://bucket/key downloaded via boto3."""
    if not src.startswith("s3://"):
        return src
    import boto3
    bucket, key = src[5:].split("/", 1)
    dest = str(Path(workdir) / Path(key).name)
    boto3.client("s3").download_file(bucket, key, dest)
    return dest


def extract_extent(db: sqlite3.Connection, tag: str, cfg: dict) -> tuple[list[dict], list[dict]]:
    """Return (substation rows, line rows) for one extent."""
    lon_min, lon_max, lat_min, lat_max = cfg["extent"]
    subs: list[dict] = []
    for table, rtree, gtype in (
            ("power_substation_point", "rtree_power_substation_point_geometry", "point"),
            ("power_substation_polygon", "rtree_power_substation_polygon_geometry", "polygon")):
        for row in db.execute(
                f"""SELECT p.fid, p.name, p.voltages, p.max_voltage, p.operator,
                           (r.minx + r.maxx)/2.0 AS lon, (r.miny + r.maxy)/2.0 AS lat
                    FROM {table} p JOIN {rtree} r ON p.fid = r.id
                    WHERE r.minx >= ? AND r.maxx <= ? AND r.miny >= ? AND r.maxy <= ?
                      AND p.name IS NOT NULL AND p.name != ''""",
                (lon_min, lon_max, lat_min, lat_max)):
            subs.append({"oim_fid": row[0], "extent_tag": tag, "name": row[1],
                         "voltages": row[2], "max_voltage": row[3], "operator": row[4],
                         "lon": row[5], "lat": row[6], "geom_type": gtype})

    lines: list[dict] = []
    min_v = cfg.get("min_voltage", 500000)
    for row in db.execute(
            """SELECT p.fid, p.geometry, p.name, p.voltages, p.max_voltage
               FROM power_line p JOIN rtree_power_line_geometry r ON p.fid = r.id
               WHERE r.minx >= ? AND r.maxx <= ? AND r.miny >= ? AND r.maxy <= ?
                 AND p.max_voltage >= ?""",
            (lon_min, lon_max, lat_min, lat_max, min_v)):
        seq = 0
        for path in parse_wkb_coords(read_gpb_wkb(row[1])):
            if len(path) >= 2:
                lines.append({"oim_fid": row[0], "extent_tag": tag, "path_seq": seq,
                              "name": row[2] or "",
                              "voltages": row[3], "max_voltage": row[4],
                              "path_json": json.dumps(downsample([list(p) for p in path]))})
                seq += 1
    return subs, lines


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--gpkg", required=True, help="local path or s3://bucket/key")
    ap.add_argument("--extent", required=True, help="extent tag, or 'all'")
    ap.add_argument("--edition", required=True, help="edition tag, e.g. 2026-10")
    ap.add_argument("--dry-run", action="store_true", help="print counts, no DB write")
    args = ap.parse_args()

    path = _fetch_gpkg(args.gpkg)
    db = sqlite3.connect(path)
    db.row_factory = sqlite3.Row

    tags = list(EXTENTS) if args.extent == "all" else [args.extent]
    for tag in tags:
        if tag not in EXTENTS:
            print(f"unknown extent {tag}", file=sys.stderr)
            return 2
    results = {tag: extract_extent(db, tag, EXTENTS[tag]) for tag in tags}
    for tag, (subs, lines) in results.items():
        print(f"{tag}: substations={len(subs)} line_paths={len(lines)}")

    if args.dry_run:
        return 0

    import os
    from sqlalchemy import create_engine, text
    engine = create_engine(os.environ["DB_DSN"])
    with engine.begin() as conn:
        for stmt in DDL.split(";"):
            if stmt.strip():
                conn.execute(text(stmt))
        for tag, (subs, lines) in results.items():
            conn.execute(text("DELETE FROM staging.openinfra_substations "
                              "WHERE extent_tag = :t AND edition_tag = :e"), {"t": tag, "e": args.edition})
            conn.execute(text("DELETE FROM staging.openinfra_lines "
                              "WHERE extent_tag = :t AND edition_tag = :e"), {"t": tag, "e": args.edition})
            for r in subs:
                r["edition_tag"] = args.edition
            for r in lines:
                r["edition_tag"] = args.edition
            if subs:
                conn.execute(text(
                    "INSERT INTO staging.openinfra_substations "
                    "(oim_fid, extent_tag, name, voltages, max_voltage, operator, lon, lat, geom_type, edition_tag) "
                    "VALUES (:oim_fid, :extent_tag, :name, :voltages, :max_voltage, :operator, :lon, :lat, :geom_type, :edition_tag)"),
                    subs)
            if lines:
                conn.execute(text(
                    "INSERT INTO staging.openinfra_lines "
                    "(oim_fid, extent_tag, path_seq, name, voltages, max_voltage, path_json, edition_tag) "
                    "VALUES (:oim_fid, :extent_tag, :path_seq, :name, :voltages, :max_voltage, :path_json, :edition_tag)"),
                    lines)
        print(f"loaded edition {args.edition} into staging.openinfra_*")
    return 0


if __name__ == "__main__":
    sys.exit(main())
