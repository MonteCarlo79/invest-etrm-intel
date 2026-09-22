# services/nodal_forecast/gridmap_extract.py
"""Parse/validate vision-extracted 网架图 node rows, seed the node registry,
and link registry substations to Fengxing node_name values.

Data flow: knowledge/mengxi/substation_capacity.md (human-reviewed) → rows →
parse_substation_rows → seed_node_registry → link_fengxing_names.
This module is data-independent: it never reads the wiring diagram itself.
"""
from __future__ import annotations

import re

# ---------------------------------------------------------------------------
# name normalization
# ---------------------------------------------------------------------------
_REGION_PREFIXES = ("内蒙古.", "内蒙.")          # Fengxing names: '内蒙.汗海站/500kV.1M'
_SUFFIXES = ("变电站", "变", "站")               # longest first, single pass


def _norm(s) -> str:
    """Normalize a substation / Fengxing node name for matching: strip region
    prefix (内蒙.), drop Fengxing '/500kV.1M' bus suffix, strip 变电站/变/站."""
    s = str(s or "").strip()
    for pre in _REGION_PREFIXES:
        if s.startswith(pre):
            s = s[len(pre):]
            break
    s = s.split("/")[0].strip()
    for suf in _SUFFIXES:
        if s.endswith(suf) and len(s) > len(suf):
            s = s[:-len(suf)]
            break
    return s


def _to_int(v):
    if v is None or v == "":
        return None
    m = re.match(r"\s*(\d+)", str(v).replace("kV", "").replace("kv", ""))
    return int(m.group(1)) if m else None


def _to_float(v):
    if v is None or v == "":
        return None
    try:
        return float(str(v).replace("kV", "").strip())
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# 1. parse / validate / dedupe
# ---------------------------------------------------------------------------
def parse_substation_rows(rows: list[dict]) -> list[dict]:
    """Validate and normalize vision-extracted substation rows.

    Input keys: {name, voltage_kv, substation, transformers, rated_mva,
    n1_firm_mva, connected_plants, source}. Numbers are coerced from strings
    ('500kV' → 500; '4×1200' → transformers=4, count only). A missing/blank
    substation is derived from name with 变电站/变 suffixes stripped. Rows with
    neither name nor substation are dropped. connected_plants lists are joined
    to a comma string for the TEXT column.

    Dedupe by (substation, voltage_kv); a later row wins over an earlier one.

    Capacity note: rated_mva / n1_firm_mva stay in MVA exactly as extracted
    from the wiring diagram. For BESS cap purposes MVA≈MW (near-unity power
    factor at the point of interconnection) — no MVA→MW conversion is applied.
    n1_firm_mva is NOT derived here (kept as extracted; the review gate owns
    any (n−1)×unit estimates).
    """
    out: dict[tuple, dict] = {}
    for r in rows:
        name = (r.get("name") or "").strip()
        substation = (r.get("substation") or "").strip() or _norm(name)
        if not name and not substation:
            continue
        plants = r.get("connected_plants")
        if isinstance(plants, (list, tuple)):
            plants = ",".join(str(p) for p in plants)
        node = dict(
            name=name or substation,
            voltage_kv=_to_int(r.get("voltage_kv")),
            substation=substation,
            transformers=_to_int(r.get("transformers")),
            rated_mva=_to_float(r.get("rated_mva")),
            n1_firm_mva=_to_float(r.get("n1_firm_mva")),
            connected_plants="" if plants is None else str(plants),
            source=r.get("source"),
        )
        out[(substation, node["voltage_kv"])] = node   # later row wins
    return list(out.values())


# ---------------------------------------------------------------------------
# 2. seed the registry
# ---------------------------------------------------------------------------
_SEED_SQL = """INSERT INTO marketdata.nodal_node_registry
    (name, voltage_kv, substation, transformers, rated_mva, n1_firm_mva,
     zone, fengxing_node_name, connected_plants, source)
    VALUES (%(name)s, %(voltage_kv)s, %(substation)s, %(transformers)s,
            %(rated_mva)s, %(n1_firm_mva)s, %(zone)s, %(fengxing_node_name)s,
            %(connected_plants)s, %(source)s)
    ON CONFLICT (name) DO UPDATE SET
        voltage_kv = EXCLUDED.voltage_kv, substation = EXCLUDED.substation,
        transformers = EXCLUDED.transformers, rated_mva = EXCLUDED.rated_mva,
        n1_firm_mva = EXCLUDED.n1_firm_mva, zone = EXCLUDED.zone,
        fengxing_node_name = COALESCE(EXCLUDED.fengxing_node_name,
                                      nodal_node_registry.fengxing_node_name),
        connected_plants = EXCLUDED.connected_plants, source = EXCLUDED.source,
        updated_at = NOW()"""


def seed_node_registry(conn, nodes: list[dict]) -> int:
    """Upsert parsed nodes into marketdata.nodal_node_registry (PK = name).
    A NULL fengxing_node_name in the seed never wipes an existing link
    (COALESCE guard). Returns the number of rows upserted."""
    params = [dict(name=n["name"], voltage_kv=n.get("voltage_kv"),
                   substation=n.get("substation"), transformers=n.get("transformers"),
                   rated_mva=n.get("rated_mva"), n1_firm_mva=n.get("n1_firm_mva"),
                   zone=n.get("zone"), fengxing_node_name=n.get("fengxing_node_name"),
                   connected_plants=n.get("connected_plants"), source=n.get("source"))
              for n in nodes if n.get("name")]
    if params:
        cur = conn.cursor()
        cur.executemany(_SEED_SQL, params)
        conn.commit()
    return len(params)


# ---------------------------------------------------------------------------
# 3. link registry substations to Fengxing node names
# ---------------------------------------------------------------------------
_SUB_SQL = """SELECT substation FROM marketdata.nodal_node_registry
              WHERE substation IS NOT NULL AND substation <> ''"""
_FX_SQL = """SELECT DISTINCT node_name FROM marketdata.md_mengxi_nodal_price_96
             WHERE node_name IS NOT NULL"""
_LINK_SQL = """UPDATE marketdata.nodal_node_registry
               SET fengxing_node_name = %s, updated_at = NOW()
               WHERE substation = %s
                 AND (fengxing_node_name IS NULL OR fengxing_node_name = '')"""


def _best_match(sub_norm: str, fx_by_norm: dict[str, list[str]]):
    """Exact normalized hit first; else unique-ish substring containment
    (either direction). Multiple Fengxing buses of one substation collapse to
    a deterministic pick (shortest, then alphabetical)."""
    if sub_norm in fx_by_norm:
        return sorted(fx_by_norm[sub_norm])[0]
    cands = [orig for fnorm, origs in fx_by_norm.items()
             if sub_norm in fnorm or fnorm in sub_norm for orig in origs]
    return sorted(cands, key=lambda s: (len(s), s))[0] if cands else None


def link_fengxing_names(conn) -> dict[str, str]:
    """Map registry substations to Fengxing node_name values from
    marketdata.md_mengxi_nodal_price_96 (exact + normalized-substring match).

    Returns {normalized_substation: matched Fengxing node_name, or the
    original substation string when unresolved}. Matched links are written
    back to nodal_node_registry.fengxing_node_name only where that column is
    still empty — manually reviewed links are never overwritten.

    Matching is at substation granularity: a Fengxing name like
    '内蒙.汗海站/500kV.1M' normalizes to '汗海'. Voltage-level linking (500kV
    vs 220kV buses of the same substation) is out of scope for this contract.
    """
    cur = conn.cursor()
    cur.execute(_SUB_SQL)
    substations = [r[0] for r in cur.fetchall()]
    cur.execute(_FX_SQL)
    fx_by_norm: dict[str, list[str]] = {}
    for (node_name,) in cur.fetchall():
        fx_by_norm.setdefault(_norm(node_name), []).append(node_name)

    links: dict[str, str] = {}
    updates = []
    for sub in substations:
        key = _norm(sub)
        if not key:
            continue
        matched = _best_match(key, fx_by_norm)
        links[key] = matched if matched else sub
        if matched:
            updates.append((matched, sub))
    if updates:
        cur.executemany(_LINK_SQL, updates)
    conn.commit()
    return links
