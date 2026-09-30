# tests/apps/test_ranking_sort_and_dockerfile.py
"""Ranking chart: deterministic total sort + Dockerfile completeness."""
import re
from pathlib import Path

_APP = Path(__file__).resolve().parents[2] / "apps" / "bess-map" / "app.py"


def test_sort_uses_unstacked_totals_not_drop_duplicates():
    src = _APP.read_text(encoding="utf-8")
    # deterministic net-key sort (v73: explicit categoryarray + largest at top)
    assert "_order = sorted({p for p, _ in _per}, key=_key_for, reverse=True)" in src
    assert 'categoryorder="array"' in src and 'autorange="reversed"' in src
    # the buggy v72 patterns must be gone
    assert 'drop_duplicates("province")' not in src


def test_dockerfiles_copy_ancillary_revenue():
    root = _APP.parents[2]
    for name in ("Dockerfile", "Dockerfile.mirror"):
        content = (root / "apps" / "bess-map" / name).read_text()
        assert "COPY services/ancillary_revenue/" in content, f"{name} missing ancillary_revenue COPY"


def test_dockerfile_copy_list_covers_imports():
    """Every services.* package imported by app.py must be COPYed in the image."""
    src = _APP.read_text(encoding="utf-8")
    pkgs = sorted(set(re.findall(r"from (services\.[a-z_]+)", src)))
    assert pkgs, "no services imports found — regex broken?"
    content = (Path(__file__).resolve().parents[2] / "apps" / "bess-map" / "Dockerfile").read_text()
    missing = [p for p in pkgs if f"COPY {p.replace('.', '/')}/" not in content]
    assert not missing, f"Dockerfile missing COPY for: {missing}"
