"""Single registry of every platform web app — read by the portal (tiles, links, status, start/stop)
and by scripts/local_apps.sh (local ports + entry points). Stdlib only, Python 3.9-compatible, so the
launcher can read it with the system python3.

Fields
  key      local app name (scripts/local_apps.sh up <key>)
  label    display name
  icon     emoji
  section  "agents" | "asset_risk" | "intl" | "tools" | "personal"
  blurb    one line for the tile
  slug     ALB path segment online (https://www.pjh-etrm.ai/<slug>/)
  port     local port, or None when the app cannot run from this repo
  entry    local Streamlit entry point, or None
  service  shared.service_control.SERVICES key (online status + start/stop), or None
  url      absolute online URL when the app is not under the platform domain
  admin_only  only Admin sees the tile
"""
from __future__ import annotations

APPS: list[dict] = [
    # Intelligence team (rendered as agent cards with Quick Ask)
    {"key": "spot-market", "label": "Strategist", "icon": "📊", "section": "agents",
     "blurb": "China spot markets, flows, fundamentals", "slug": "spot-markets", "port": 8505,
     "entry": "apps/spot-market/app.py", "service": "spot"},
    {"key": "bess-map", "label": "Quant", "icon": "📐", "section": "agents",
     "blurb": "BESS investment economics, province ranking, IRR", "slug": "bess-map", "port": 8503,
     "entry": "apps/bess-map/app.py", "service": "bess-map"},
    {"key": "mengxi-dashboard", "label": "Trader", "icon": "⚡", "section": "agents",
     "blurb": "Mengxi BESS trading ops and P&L attribution", "slug": "mengxi-dashboard", "port": 8511,
     "entry": "apps/mengxi-dashboard/app.py", "service": "mengxi"},
    {"key": "deal-structurer", "label": "Deal Structurer", "icon": "🏦", "section": "agents",
     "blurb": "Deal pricing, option-value engine, project finance", "slug": "deal-structurer", "port": 8522,
     "entry": "apps/deal_structurer/app.py", "service": "deal-structurer"},
    {"key": "market-maker", "label": "Market Maker", "icon": "📒", "section": "agents",
     "blurb": "Central risk book: positions, MtM, VaR, Greeks", "slug": "market-maker", "port": 8530,
     "entry": "apps/market_maker/app.py", "service": "market-maker"},
    # Asset & risk
    {"key": "asset-risk", "label": "Asset Risk", "icon": "🛡️", "section": "asset_risk",
     "blurb": "Settlements, realised P&L, waterfall, 复盘", "slug": "asset-risk", "port": 8512,
     "entry": "apps/asset_risk/app.py", "service": "asset-risk"},
    {"key": "retail-risk", "label": "Retail Risk", "icon": "🏪", "section": "asset_risk",
     "blurb": "Retail books: load, coverage, settlement", "slug": "retail-risk", "port": 8513,
     "entry": "apps/retail_risk/app.py", "service": "retail-risk"},
    # International markets
    {"key": "gb-market", "label": "GB", "icon": "🇬🇧", "section": "intl",
     "blurb": "GB BESS: leaderboard, BM, ancillary, pricing", "slug": "gb-market", "port": 8508,
     "entry": "apps/gb-market/app.py", "service": "gb"},
    {"key": "au-market", "label": "Australia (NEM)", "icon": "🇦🇺", "section": "intl",
     "blurb": "NEM spot, FCAS, BESS index", "slug": "au-market", "port": 8509,
     "entry": "apps/au-market/app.py", "service": "au"},
    {"key": "ph-market", "label": "Philippines", "icon": "🇵🇭", "section": "intl",
     "blurb": "WESM prices, reserves, policy", "slug": "ph-market", "port": 8510,
     "entry": "apps/ph-market/app.py", "service": "ph"},
    {"key": "po-market", "label": "Poland", "icon": "🇵🇱", "section": "intl",
     "blurb": "TGE prices, balancing, capacity market", "slug": "po-market", "port": 8514,
     "entry": "apps/po-market/app.py", "service": "po"},
    {"key": "ercot-market", "label": "ERCOT", "icon": "🤠", "section": "intl",
     "blurb": "Texas LMP and ancillary services", "slug": "ercot-market", "port": 8515,
     "entry": "apps/ercot-market/app.py", "service": "ercot"},
    {"key": "pjm-market", "label": "PJM", "icon": "🇺🇸", "section": "intl",
     "blurb": "US East LMP, regulation, capacity", "slug": "pjm-market", "port": 8516,
     "entry": "apps/pjm-market/app.py", "service": "pjm"},
    {"key": "caiso-market", "label": "CAISO", "icon": "🌴", "section": "intl",
     "blurb": "California LMP, AS, duck curve", "slug": "caiso-market", "port": 8517,
     "entry": "apps/caiso-market/app.py", "service": "caiso"},
    # Tools
    {"key": "options-cockpit", "label": "Options Cockpit", "icon": "🎛️", "section": "tools",
     "blurb": "Options pricing, structures, smile, Greeks", "slug": "options-cockpit", "port": 8507,
     "entry": "apps/options-cockpit/app.py", "service": "options"},
    {"key": "inner-mongolia", "label": "Inner Mongolia", "icon": "🗺️", "section": "tools",
     "blurb": "Mengxi profitability and ranking (legacy)", "slug": "inner-mongolia", "port": 8504,
     "entry": "apps/bess-inner-mongolia/im/app.py", "service": "im"},
    {"key": "model-catalogue", "label": "Model Catalogue", "icon": "📚", "section": "tools",
     "blurb": "Decision-model registry and lineage", "slug": "model-catalogue", "port": None,
     "entry": None, "service": None},
    # Personal (Admin only)
    {"key": "crystal-ball", "label": "Crystal Ball", "icon": "🔮", "section": "personal",
     "blurb": "Fortune-teller app", "slug": "crystal-ball", "port": None, "entry": None,
     "service": "crystal-ball", "admin_only": True},
    {"key": "crystal-ball-client", "label": "Crystal Ball Client", "icon": "🪄", "section": "personal",
     "blurb": "Client-facing Crystal Ball", "slug": "crystal-ball-client", "port": None, "entry": None,
     "service": "crystal-ball-client", "admin_only": True},
    {"key": "deeptutor", "label": "DeepTutor", "icon": "🎓", "section": "personal",
     "blurb": "Kids' tutor", "slug": None, "port": None, "entry": None, "service": "deeptutor",
     "url": "https://tutor.pjh-etrm.ai", "admin_only": True},
]

SECTIONS = [
    ("agents", "Your Intelligence Team"),
    ("asset_risk", "Asset & Risk"),
    ("intl", "International Markets"),
    ("tools", "Tools"),
    ("personal", "Personal"),
]

BY_KEY = {a["key"]: a for a in APPS}


def online_url(app: dict) -> str | None:
    if app.get("url"):
        return app["url"]
    return f"/{app['slug']}/" if app.get("slug") else None


def local_table() -> str:
    """'key|entry|port' lines for scripts/local_apps.sh (portal first, then every locally runnable app)."""
    rows = ["portal|apps/portal/app.py|8500"]
    rows += [f"{a['key']}|{a['entry']}|{a['port']}" for a in APPS if a.get("entry") and a.get("port")]
    return "\n".join(rows)


if __name__ == "__main__":
    print(local_table())
