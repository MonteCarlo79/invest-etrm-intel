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
_REG_SQL = """SELECT name, substation, voltage_kv FROM marketdata.nodal_node_registry
              WHERE substation IS NOT NULL AND substation <> ''"""
_FX_SQL = """SELECT DISTINCT node_name FROM marketdata.md_mengxi_nodal_price_96
             WHERE node_name IS NOT NULL"""
# Per-row write-back keyed on the PK (name); never overwrites a non-empty link.
_LINK_SQL = """UPDATE marketdata.nodal_node_registry
               SET fengxing_node_name = %s, updated_at = NOW()
               WHERE name = %s
                 AND (fengxing_node_name IS NULL OR fengxing_node_name = '')"""


def _fx_voltage(fx_name) -> int | None:
    """Parse the kV number from a Fengxing bus name ('xxx/500kV.1M' → 500)."""
    m = re.search(r"/(\d+)\s*kV", str(fx_name))
    return int(m.group(1)) if m else None


def _pick_exact(originals: list[str], voltage_kv) -> str:
    """Multi-bus exact hit: prefer the bus whose kV matches the registry row's
    voltage_kv; deterministic (shortest, then alphabetical) fallback when no
    bus voltage matches (or the row's voltage is unknown)."""
    names = sorted(originals, key=lambda s: (len(s), s))
    if voltage_kv is not None:
        for n in names:
            if _fx_voltage(n) == voltage_kv:
                return n
    return names[0]


def link_fengxing_names(conn) -> dict:
    """Map registry rows to Fengxing node_name values from
    marketdata.md_mengxi_nodal_price_96.

    Returns {"links": {norm_substation: fengxing_name_or_None},
             "candidates": {norm_substation: [possible_fengxing_names, ...]}}.

    - links: EXACT-normalized matches only (after stripping region prefixes,
      变电站/变/站 suffixes, and the '/500kV.1M' bus suffix). None = no exact
      match. For substations with several registry rows (different voltage
      levels), links reflects the highest-voltage row's pick; the authoritative
      per-row matches are what get written to the DB.
    - candidates: substring-only matches (either-direction containment),
      exposed for human review. They are NEVER written back — auto-persisting
      a containment guess would make a sticky false positive that the
      empty-guard then protects as if human-curated.
    - Multi-bus exact hits are resolved PER REGISTRY ROW: the bus whose kV
      (parsed from '.../500kV.1M') matches the row's voltage_kv wins; falls
      back to the deterministic pick only when no kV matches.
    - Write-back: one UPDATE per registry row (PK name), exact matches only,
      and only where fengxing_node_name is still empty (human overrides kept).
    """
    cur = conn.cursor()
    cur.execute(_REG_SQL)
    reg_rows = [(r[0], r[1], r[2]) for r in cur.fetchall()]
    cur.execute(_FX_SQL)
    fx_by_norm: dict[str, list[str]] = {}
    for (node_name,) in cur.fetchall():
        fx_by_norm.setdefault(_norm(node_name), []).append(node_name)

    links: dict[str, str | None] = {}
    candidates: dict[str, list[str]] = {}
    updates: list[tuple] = []
    seen: set[str] = set()
    # highest-voltage row of each substation first (None voltage last)
    reg_rows.sort(key=lambda t: (_norm(t[1]), 0 if t[2] is None else -t[2], str(t[0])))
    for name, sub, kv in reg_rows:
        key = _norm(sub)
        if not key:
            continue
        exact = fx_by_norm.get(key)
        if exact:
            pick = _pick_exact(exact, kv)
            links.setdefault(key, pick)
            updates.append((pick, name))                 # per-row write-back
        else:
            links.setdefault(key, None)
            if key not in seen:
                seen.add(key)
                cands = sorted({o for fn, origs in fx_by_norm.items()
                                if key in fn or fn in key for o in origs},
                               key=lambda s: (len(s), s))
                if cands:
                    candidates[key] = cands
    if updates:
        cur.executemany(_LINK_SQL, updates)
    conn.commit()
    return {"links": links, "candidates": candidates}
