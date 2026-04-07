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
        "name": "China Spot Market",
        "path": "/spot-markets/",
        "description": "Daily DA/RT spot prices for the last 3 report dates, all provinces. Highlights for most recent day.",
        "roles": ["Admin", "Trader", "Quant", "Analyst", "Viewer"],
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
]


def get_visible_apps(role: str):
    role_normalized = (role or "").strip()
    return [x for x in APP_CATALOG if role_normalized in x.get("roles", [])]


def get_visible_by_category(role: str, category: str):
    return [x for x in get_visible_apps(role) if x.get("category") == category]


def get_catalog_item(name: str):
    for item in APP_CATALOG:
        if item.get("name") == name:
            return item
    return None