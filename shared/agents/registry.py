import os

APP_CATALOG = [
    {
        "name": "Portal",
        "path": "/",
        "description": "Main control tower for the investment-trading intelligence system.",
        "roles": ["Admin", "Trader", "Quant", "Analyst", "Viewer"],
        "category": "Applications",
    },
    {
        "name": "BESS Map",
        "path": "/bess-map/",
        "description": "Asset mapping and dispatch analytics.",
        "roles": ["Admin", "Trader", "Quant", "Analyst"],
        "category": "Applications",
    },
    {
        "name": "Inner Mongolia Intelligence",
        "path": "/inner-mongolia/",
        "description": "Mengxi profitability, ranking, and spread analytics.",
        "roles": ["Admin", "Trader", "Quant", "Analyst"],
        "category": "Applications",
    },
    {
        "name": "Market Data Uploader",
        "path": "/uploader/",
        "description": "Market and operational data ingestion.",
        "roles": ["Admin", "Quant"],
        "category": "Applications",
    },
    {
        "name": "Model Catalogue",
        "path": "/model-catalogue/",
        "description": "Registry of all decision models — metadata, assumptions, and data lineage.",
        "roles": ["Admin", "Trader", "Quant", "Analyst"],
        "category": "Applications",
    },
    {
        "name": "Strategy Agent",
        "path": "/strategy-agent/",
        "description": "Opportunity screening, market structure, and deployment ranking.",
        "roles": ["Admin", "Trader", "Quant", "Analyst"],
        "category": "Agents",
        "task_definition": "bess-platform-strategy-agent",
    },
    {
        "name": "Portfolio & Risk Agent",
        "path": "/portfolio-risk-agent/",
        "description": "Allocation, concentration, stress testing, and risk framing.",
        "roles": ["Admin", "Trader", "Quant"],
        "category": "Agents",
        "task_definition": "bess-platform-portfolio-agent",
    },
    {
        "name": "Execution Agent",
        "path": "/execution-agent/",
        "description": "Action queue, data refresh workflow, and trader execution support.",
        "roles": ["Admin", "Trader", "Quant"],
        "category": "Agents",
        "task_definition": "bess-platform-execution-agent",
    },
    {
        "name": "IT Developer Agent",
        "path": "/it-dev-agent/",
        "description": "App revision planning, bug-fix triage, and code change proposals.",
        "roles": ["Admin", "Quant"],
        "category": "Agents",
        "task_definition": "bess-platform-dev-agent",
    },
    {
        "name": "Trading Performance Agent",
        "path": "/trading-performance-agent/",
        "description": (
            "Daily strategy performance monitoring for the 4 Inner Mongolia BESS assets. "
            "Claude-powered: strategy ranking, discrepancy attribution, realization & fragility "
            "status, operator narrative, and email reports."
        ),
        "roles": ["Admin", "Trader", "Quant"],
        "category": "Agents",
        "task_definition": "bess-trading-performance-agent",
    },
]


def _url_overrides() -> dict:
    """
    Parse APP_URL_MAP env var (comma-separated slug=url pairs).

    APP_URL_MAP format:  <path-slug>=<url>[,<path-slug>=<url>...]
    The slug is the path component without leading/trailing slashes.

    Example (local dev):
        APP_URL_MAP=inner-mongolia=http://localhost:8504,bess-map=http://localhost:8503

    In AWS mode, leave APP_URL_MAP unset so catalog paths (/inner-mongolia/, etc.) are used.
    """
    raw = os.getenv("APP_URL_MAP", "")
    result = {}
    for item in raw.split(","):
        item = item.strip()
        if not item or "=" not in item:
            continue
        slug, url = item.split("=", 1)
        result[slug.strip()] = url.strip()
    return result


def get_visible_apps(role: str):
    role_normalized = (role or "").strip()
    items = [x for x in APP_CATALOG if role_normalized in x.get("roles", [])]
    overrides = _url_overrides()
    if not overrides:
        return items
    result = []
    for item in items:
        slug = item.get("path", "").strip("/")
        if slug in overrides:
            item = dict(item)  # don't mutate the shared catalog
            item["path"] = overrides[slug]
        result.append(item)
    return result


def get_visible_by_category(role: str, category: str):
    return [x for x in get_visible_apps(role) if x.get("category") == category]


def get_catalog_item(name: str):
    for item in APP_CATALOG:
        if item.get("name") == name:
            return item
    return None