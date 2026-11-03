"""WeCom group-file collector for 蒙西 nomination files.

Source of truth for received files: the WeCom desktop client's local cache
(~/Library/Containers/com.tencent.WeWorkMac/Data/Documents/Profiles/<id>/Caches/Files/YYYY-MM/),
which mirrors group-chat attachments with original filenames — no UI driving
needed (CDP/UI probes proved WeCom's CEF exposes neither a debug port nor a
download context menu).

Conventions (verified against the existing folders 2026-11):
- root: the LAST-VERSION monthly nomination file per month
  (调度计划表X月 complete month; mid-month versions X月(N) → archived/)
- archived/: mid-month nomination versions + 汇总 summary files
- client-reports/: date-named daily files (电价日报2026-XX.XX, 【X月X日】运营统计)
- Station folder mapping by filename (verified against rm_assets plants);
  unmatched → _unclassified/ with a log line (never silently dropped).
"""
from __future__ import annotations

import logging
import re
import shutil
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)

CACHE_ROOT = Path.home() / ("Library/Containers/com.tencent.WeWorkMac/Data/Documents/"
                            "Profiles/A33BD293713FD025106CC5D8F9DFA9B9/Caches/Files")
NOMINATION_ROOT = Path("data/raw/nomination")
STATE_FILE = Path("services/wecom_nominations/.checkpoint")

# Filename → station folder (place name). Verified against rm_assets plants
# and the existing folders.
PLANT_TO_FOLDER: list[tuple[str, str]] = [
    ("裕昭沙子坝", "谷山梁"),
    ("景蓝乌尔图", "苏右"),
    ("悦杭独贵", "杭锦旗"),
    ("景怡查干哈达", "巴盟"),
    ("四子王旗", "四子王旗"),       # WeCom filenames use place names (not registry plant names)
    ("景通四益堂", "四子王旗"),
    ("远景乌拉特", "乌拉特中旗"),
    ("乌拉特中旗", "乌拉特中旗"),
    ("乌海康富", "乌海"),
    ("乌海", "乌海"),
    ("武川储能", "武川"),
    ("武川", "武川"),
    ("阿拉善储能", "阿拉善"),
    ("阿拉善", "阿拉善"),
    ("景敖乌珠日", "阿拉善"),       # per existing client-report convention there
    ("阿拉腾敖包", "阿拉善"),
    ("阿拉善左旗", "阿拉善"),
    ("旭纯哈乐", "武川"),          # user-confirmed: 旭纯哈乐 station is in 武川
]

_MONTHLY_RE = re.compile(r"调度计划表(?P<mm>\d{1,2})月(?P<suffix>\(\d+\))?")
_DAILY_DATE_RES = [
    re.compile(r"【\d{1,2}月\d{1,2}日】"),
    re.compile(r"\d{4}[.-]\d{2}[.-]\d{2}"),          # 电价日报2026-09.25 / 2026.10.01
    re.compile(r"\d{2}-\d{2}"),                       # 09-28...
    re.compile(r"调度计划表\d{2}\.\d{2}"),              # 调度计划表09.29 / 10.01 (daily variant)
]


@dataclass(frozen=True)
class Classification:
    station: str | None          # station folder name, or None → _unclassified
    kind: str                    # 'monthly' | 'daily' | 'summary' | 'other'
    month: int | None = None     # for monthly: which month (1-12)


def classify(filename: str) -> Classification:
    """Map a cached filename to (station folder, kind, month)."""
    plant = next((folder for key, folder in PLANT_TO_FOLDER if key in filename), None)
    if "汇总" in filename:
        return Classification(plant, "summary")
    m = _MONTHLY_RE.search(filename)
    if m and not any(r.search(filename) for r in _DAILY_DATE_RES[:3]):
        # monthly nomination (调度计划表X月) — the daily variant 调度计划表09.29 is excluded
        if not re.search(r"调度计划表\d{2}\.\d{2}", filename):
            return Classification(plant, "monthly", int(m.group("mm")))
    if any(r.search(filename) for r in _DAILY_DATE_RES):
        return Classification(plant, "daily")
    return Classification(plant, "other")


def _checkpoint_mtime() -> float:
    try:
        return float(STATE_FILE.read_text().strip())
    except (OSError, ValueError):
        return 0.0


def _write_checkpoint(ts: float) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(str(ts))


def _monthly_target(station_dir: Path, filename: str, month: int) -> Path:
    """Place the newest monthly version in root; demote older same-month versions."""
    root_file = station_dir / filename
    for old in station_dir.glob(f"*调度计划表{month}月*.xlsx"):
        if old.name == filename:
            continue
        arch = station_dir / "archived"
        arch.mkdir(exist_ok=True)
        shutil.move(str(old), arch / old.name)
        logger.info("demoted %s → archived/", old.name)
    return root_file


def collect(since: float | None = None, dry_run: bool = False) -> dict:
    """Scan the cache, classify, copy to station folders per conventions.

    Returns stats {scanned, copied, skipped, unclassified, demoted}.
    """
    cutoff = since if since is not None else _checkpoint_mtime()
    stats = {"scanned": 0, "copied": 0, "skipped": 0, "unclassified": 0, "demoted": 0}
    newest = cutoff
    if not CACHE_ROOT.is_dir():
        logger.warning("WeCom cache root missing: %s", CACHE_ROOT)
        return stats
    for f in sorted(CACHE_ROOT.rglob("*")):
        if not f.is_file() or f.name.startswith("."):
            continue
        stats["scanned"] += 1
        mt = f.stat().st_mtime
        if mt <= cutoff:
            stats["skipped"] += 1
            continue
        newest = max(newest, mt)
        cls = classify(f.name)
        folder = cls.station or "_unclassified"
        if cls.station is None:
            stats["unclassified"] += 1
            logger.warning("unclassified file: %s", f.name)
        station_dir = NOMINATION_ROOT / folder
        if dry_run:
            logger.info("DRY %s → %s [%s]", f.name, station_dir, cls.kind)
            continue
        station_dir.mkdir(parents=True, exist_ok=True)
        if cls.kind == "monthly":
            dest = _monthly_target(station_dir, f.name, cls.month or 0)
            stats["demoted"] += 0  # demotions logged inside _monthly_target
        elif cls.kind == "daily":
            dest = station_dir / "client-reports"
            dest.mkdir(exist_ok=True)
            dest = dest / f.name
        elif cls.kind == "summary":
            dest = station_dir / "archived"
            dest.mkdir(exist_ok=True)
            dest = dest / f.name
        else:
            dest = station_dir / f.name
        if not dest.exists():
            shutil.copy2(f, dest)
            stats["copied"] += 1
            logger.info("copied %s → %s", f.name, dest)
    if not dry_run:
        _write_checkpoint(newest)
    return stats
