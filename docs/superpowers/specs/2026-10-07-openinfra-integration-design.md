# OpenInfraMap Grid Data Integration — Design

**Date:** 2026-10-07
**Status:** Pending user review
**Data:** `data/nodal/open-infra-map/CHN.gpkg` (823 MB GeoPackage, purchased 2026-10-06) + `License.pdf` (terms NOT yet reviewed — see open question 1)
**Preview artifact:** `data/nodal/open-infra-map/preview.html` (built from `scripts/openinfra_preview.py`)

---

## 1. What the preview established

Mengxi extent extraction (100–117.5°E, 37–44.8°N):

- **341 named substations** (249 at ≥220 kV); 500 kV backbone well covered with names + voltages + polygon/point geometry
- **3,521 line paths at ≥500 kV** (500 kV ×2,639, 750 kV ×560, 1000 kV ×268, ±800/±660 kV DC ×54)
- **Vision-map match: 45 OK / 16 missing** (of 61 stations from the 2025-12-22 主接线图 extraction). The four BESS-critical stations （德岭山， 河套， 谷山梁， 苏尼特） all match.
- **165 named 500 kV+ OIM stations not in the vision map** — mostly 蒙东/neighbour-province stations inside the extent, possibly a few vision-map misses worth a look.

**Coverage character:** strong on backbone **geometry + names**; near-absent on capacities (`output` populated on 20/5,139 generators; no transformer MVA); **BESS fleet absent entirely**. The dataset complements the vision map (which has MVA + connected plants but no coordinates); it does not replace it.

## 2. Integration shape

```
CHN.gpkg (snapshot, disk)                      vision map (knowledge/mengxi)
        │                                               │
        ▼                                               ▼
 services/openinfra/extract_gpkg.py          knowledge aliases (reviewed)
        │                                               │
        ▼                                               ▼
 staging.openinfra_substations / _lines / _generators   │
        │                                               │
        └──────► match_substations.py ◄─────────────────┘
                          │  (normalized names + reviewed aliases + match log)
                          ▼
        nodal_node_registry  +=  oim_name, oim_fid, lat, lon (ADDITIVE ONLY)
                          │
        ┌─────────────────┼──────────────────────────────┐
        ▼                 ▼                              ▼
  Nodal Maps tab    Nodal Trading S4            network graph (future):
  geographic        node table gains            lines adjacency → congestion
  underlay          coordinates                 model / corridor analysis
```

**Principle (matches the BESS-fleet principle):** OpenInfraMap is the geometry+topology source, ingested in full once per edition; the vision map stays authoritative for capacities and connected plants; the asset registry stays authoritative for BESS. No field OIM lacks gets overwritten by OIM.

## 3. Tables (staging, one schema per entity)

| table | key columns |
|---|---|
| `staging.openinfra_substations` | oim_fid (PK), name, name_en, voltages, max_voltage, operator, substation_type, lon, lat, geom_type (point/polygon), extent_tag |
| `staging.openinfra_lines` | oim_fid (PK), name, voltages, max_voltage, circuits, operator, path_json (downsampled ≤120 pts) |
| `staging.openinfra_generators` | oim_fid (PK), name, source, output, method, operator, lon, lat |
| `staging.openinfra_match_log` | id, registry_name, oim_fid, match_type (exact/alias/substring/manual), confidence, matched_by, matched_at |
| `staging.openinfra_aliases` | alias (PK), oim_name, reviewed_by, created_at — hand-reviewed name bridges （包北↔包头北 pattern) |

Geometry stored as plain floats + JSON paths (the app consumes JSON; the gpkg on disk remains the full-fidelity source). No PostGIS dependency.

## 4. Services (`services/openinfra/`)

- **`extract_gpkg.py`** — gpkg → staging upsert. Region param (`mengxi` extent today, `china` full later). GPB/WKB decoder already proven in `scripts/openinfra_preview.py`; move it into the service. Idempotent per edition (delete-by-extent + insert, or oim_fid upsert).
- **`match_substations.py`** — normalized matching (prefix/suffix/variant stripping, the ALIASES table, substring fallback) → match_log → **additive backfill** of `nodal_node_registry` (oim_name, oim_fid, lat, lon — never touches vision fields). Unmatched → review list.
- Tests: name normalization table (the 6 mismatch forms found in the preview), WKB decode (LS + MLS), extent filter, additive-only backfill (a vision field is never modified), idempotent re-run.

## 5. App surfaces

1. **Nodal Maps tab** (mengxi-dashboard): geographic underlay — 500/750 kV lines + station markers behind the existing per-node price/PF layers. Nodes with `lat/lon` (from §4 backfill) render on the real map; unmatched stay in the current schematic.
2. **Nodal Trading S4**: node-map table gains lon/lat columns; a small "coverage" line (n/m substations geo-located).
3. **Future network graph**: line→station adjacency from shared names/endpoints → congestion corridors （谷山梁 cluster export path), the spec's "PTDF if a network model arrives" hook.
4. **Cross-app note (not built here):** the interconnector tab (spot-market) could later use line geometry for channel-corridor display; generator layers as context. Flagged, out of scope.

## 6. Refresh policy

Purchased snapshots, not a feed: each new gpkg edition → re-run extract → re-run match → new match_log rows; aliases accumulate (never auto-deleted). Edition noted in staging (`edition_tag`).

## 7. Open questions (user)

1. **License terms** — `License.pdf` unread. ODbL-style share-alike/attribution would shape whether staging tables can be queried from the app UI (attribution line) and whether anything derived can leave the building. **Read before DB integration starts.**
2. **The 16 unmatched vision stations** — visible in the preview's missing table （乌后旗， 沙井， 耳字壕， 芒哈图， 昆都仑， 开林河， 德义， 苏敦， 敖瑞， 浩雅， 城川， 双井， 马兰， 鹰骏， 黄旗海， 杜尔伯特）. Worth an eyeball pass: genuine OIM gaps vs. name forms the aliases table should cover.
3. **蒙西 extent boundary** — preview uses 100–117.5°E/37–44.8°N and leaks 蒙东/邻省 stations (the 165 extras). Integration should use an Inner Mongolia boundary polygon (available in OIM? else hand-set) or keep the extent and accept extras with an `extent_tag`.
4. **蒙东 coverage** — the vision map's 蒙东 edition （兴安/呼伦贝尔/通辽/赤峰） is still unextracted; OIM likely covers those stations already — cheaper than vision extraction?

## 8. Sequencing preview (if approved)

1. T1: `services/openinfra/extract_gpkg.py` + staging tables + tests (mengxi extent)
2. T2: `match_substations.py` + registry additive backfill + aliases seed （包北 etc.) + review list of unmatched
3. T3: Nodal Maps geographic underlay (biggest visible win)
4. T4: License/edges review with user; then 蒙东 + full-China extract decision
