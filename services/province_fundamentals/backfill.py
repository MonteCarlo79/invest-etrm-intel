# services/province_fundamentals/backfill.py
"""回填 runner — 遍历月报目录, registry 派发, write_rows 写库.

CLI:
    python -m services.province_fundamentals.backfill \\
        --dir data/exchange-monthly-reports [--province 安徽] [--dry-run] [--pg-url DSN]

规则 (task-5 brief + spec 2026-11-02):
  - 每文件打印判定: 格式 / 行数 / 跳过原因; 未知格式计入 unknown, 不崩;
  - --dry-run 只打印判定, 绝不调用 write_rows;
  - 写库经 write_rows(..., default_status="confirmed") (spec: 回填直接 confirmed,
    来源为原始文件, 解析器已经人工校验);
  - --province 只保留该省行 (交接表全国册单文件 31 省, 按省过滤后写库);
  - pg_url 来源: --pg-url 参数, 缺省 PGURL 环境变量; 非 dry-run 均无 → 报错退出.

RDS 从 Mac 被墙时的 Fargate run-task 包装属部署任务, 不在此模块.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

from services.province_fundamentals import registry
from services.province_fundamentals.store import write_rows

log = logging.getLogger(__name__)

_SKIP_PREFIXES = ("~$", ".")        # Excel 锁文件 / 点文件 (.DS_Store 等)


def _iter_files(root: Path):
    """目录下全部候选文件 (递归, 排序 — 判定输出可复现)."""
    for p in sorted(root.rglob("*")):
        if p.is_file() and not p.name.startswith(_SKIP_PREFIXES):
            yield p


# ── Cross-source component-sum check lives in store.py (single write path) ───
from services.province_fundamentals.store import _crosscheck_components


def run(dir, *, province: str | None = None, dry_run: bool = False,
        pg_url: str | None = None, out=print) -> dict:
    """遍历 → 派发 → (可选) 写库 → 汇总计数. 任何单文件异常不中断整批."""
    root = Path(dir)
    if not root.is_dir():
        raise NotADirectoryError(f"--dir not a directory: {root}")
    summary = {"files": 0, "parsed": 0, "unknown": 0, "empty": 0,
               "rows": 0, "filtered": 0,
               "written": 0, "conflicts": 0, "skipped": 0,
               "formats": {}}
    for f in _iter_files(root):
        summary["files"] += 1
        rel = f.relative_to(root)
        if f.suffix.lower() != ".xlsx":
            summary["unknown"] += 1
            out(f"[unknown] {rel} — 非 xlsx 工作簿, 跳过")
            continue
        hits = registry.dispatch_all(f)
        if not hits:
            summary["unknown"] += 1
            out(f"[unknown] {rel} — 省份未注册且全国册嗅探不命中, 跳过")
            continue
        # 一个工作簿可命中多类解析器 (聚合册的披露 + 省间); 合并所有命中的行.
        rows = [r for _, rs in hits for r in rs]
        fmt = "+".join(h for h, _ in hits)
        if not rows:
            summary["empty"] += 1
            out(f"[{fmt}] {rel} — 格式命中但解析 0 行 (失败细节见 log), 跳过")
            continue
        summary["parsed"] += 1
        summary["formats"][fmt] = summary["formats"].get(fmt, 0) + 1
        kept = [r for r in rows if province is None or r.province == province]
        clean, suspect, warns = _crosscheck_components(kept)
        for w in warns:
            out(f"  ⚠ {w}")
        summary["suspect"] = summary.get("suspect", 0) + len(suspect)
        summary["rows"] += len(kept)
        summary["filtered"] += len(rows) - len(kept)
        if dry_run:
            out(f"[{fmt}] {rel} — 解析 {len(rows)} 行"
                + (f", --province {province} 过滤后 {len(kept)} 行"
                   if province else "")
                + (f", 其中 {len(suspect)} 行存疑(不写 confirmed)" if suspect else "")
                + " (dry-run, 不写库)")
            continue
        if not kept:
            out(f"[{fmt}] {rel} — 解析 {len(rows)} 行, "
                f"--province {province} 过滤后 0 行, 不写库")
            continue
        stats = write_rows(clean + suspect, pg_url=pg_url, default_status="confirmed")
        summary["written"] += stats["written"]
        summary["conflicts"] += stats["conflicts"]
        summary["skipped"] += stats["skipped"]
        out(f"[{fmt}] {rel} — 解析 {len(rows)} 行, 写 {len(kept)} 行: "
            f"written={stats['written']} conflicts={stats['conflicts']} "
            f"skipped={stats['skipped']} suspect={stats.get('suspect', 0)}"
            + (f" ⚠{len(stats.get('warnings', []))}组" if stats.get("warnings") else ""))
    out(f"汇总: files={summary['files']} parsed={summary['parsed']} "
        f"unknown={summary['unknown']} empty={summary['empty']} "
        f"rows={summary['rows']} filtered={summary['filtered']} "
        f"written={summary['written']} conflicts={summary['conflicts']} "
        f"skipped={summary['skipped']} suspect={summary.get('suspect', 0)} "
        f"formats={summary['formats']}")
    return summary


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        prog="province_fundamentals.backfill",
        description="省月度基本面月报批量回填 (registry 派发 → write_rows)")
    ap.add_argument("--dir", required=True, help="月报根目录 (递归遍历 *.xlsx)")
    ap.add_argument("--province", help="只回填该省行 (交接表全国册按省过滤)")
    ap.add_argument("--dry-run", action="store_true",
                    help="只打印每文件判定 (格式/行数/跳过原因), 不写库")
    ap.add_argument("--pg-url", default=os.environ.get("PGURL"),
                    help="PG DSN (缺省取 PGURL 环境变量)")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO,
                        format="%(levelname)s %(name)s: %(message)s")
    if not args.dry_run and not args.pg_url:
        ap.error("非 --dry-run 须供 --pg-url 或设 PGURL 环境变量")
    run(args.dir, province=args.province, dry_run=args.dry_run,
        pg_url=args.pg_url)
    return 0


if __name__ == "__main__":
    sys.exit(main())
