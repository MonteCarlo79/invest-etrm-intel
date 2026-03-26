from pathlib import Path
import pandas as pd


def scan_repo_overview(root: Path) -> pd.DataFrame:
    folders = []
    for name in ["apps", "services", "auth", "shared", "infra", "db", "config"]:
        p = root / name
        folders.append(
            {
                "folder": name,
                "exists": p.exists(),
                "kind": "directory" if p.exists() and p.is_dir() else "missing",
            }
        )
    return pd.DataFrame(folders)


def detect_candidate_targets(request_text: str):
    text = request_text.lower()
    targets = []

    if "portal" in text or "navigator" in text:
        targets.append("apps/portal/app.py")
    if "uploader" in text or "upload" in text:
        targets.append("apps/uploader/app.py")
    if "inner mongolia" in text or "inner-mongolia" in text or "mengxi" in text:
        targets.append("apps/bess-inner-mongolia/im/app.py")
    if "map" in text or "bess_map" in text or "bess-map" in text:
        targets.append("services/bess_map/streamlit_bess_profit_dashboard_v14.1_consistent_full2.py")
    if "rbac" in text or "auth" in text or "login" in text:
        targets.append("auth/rbac.py")

    return targets


def propose_change(request_text: str, targets: list[str]) -> str:
    target_text = "\n".join(f"- {t}" for t in targets) if targets else "- No obvious target detected"

    return f"""
IT Developer Agent v2

Incoming request:
{request_text}

Likely target files:
{target_text}

Suggested workflow:
1. Open target file(s)
2. Check shared import impact
3. Validate routing / ALB path assumptions
4. Prepare minimal patch
5. Test locally
6. Deploy through ECS after approval
""".strip()