"""
services/ops/run_trading_agent.py

Daily BESS trading performance agent — CLI entry point.

Runs the Claude-powered trading performance review for the 4 Inner Mongolia
BESS assets, generates per-asset PDF reports, and optionally sends an email
summary.

Usage
-----
    # Dry run — analyse and print narrative, no email, no PDF write
    py services/ops/run_trading_agent.py --date 2026-04-17 --dry-run

    # Full run with email
    py services/ops/run_trading_agent.py --date 2026-04-17 --send-email

    # Single asset
    py services/ops/run_trading_agent.py --date 2026-04-17 --asset-code suyou

    # Yesterday (default) with email
    py services/ops/run_trading_agent.py --send-email

Required environment variables
-------------------------------
    DB_DSN or PGURL           — PostgreSQL connection string
    ANTHROPIC_API_KEY         — Claude API key

For --send-email:
    SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASSWORD, SMTP_FROM, REPORT_EMAIL_TO

Optional:
    REPORT_OUTPUT_DIR         — directory for PDF output (default: reports/)

ECS / webhook trigger
---------------------
This script is designed to be invoked as an ECS Fargate task after the Inner
Mongolia ops ingestion pipeline completes.  Example boto3 trigger:

    import boto3, datetime
    ecs = boto3.client("ecs", region_name="ap-southeast-1")
    ecs.run_task(
        cluster="bess-platform",
        taskDefinition="bess-trading-performance-agent",
        overrides={"containerOverrides": [{
            "name": "trading-performance-agent",
            "command": [
                "python", "services/ops/run_trading_agent.py",
                "--date", datetime.date.today().isoformat(),
                "--send-email",
            ],
        }]},
        launchType="FARGATE",
        ...
    )
"""
from __future__ import annotations

import argparse
import datetime
import io
import os
import sys

# Ensure project root is on sys.path when run as a script
_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, "..", ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

# Set DB_DSN from PGURL if only the latter is provided
_url = os.environ.get("PGURL") or os.environ.get("DB_DSN")
if _url:
    os.environ.setdefault("DB_DSN", _url)
    os.environ.setdefault("PGURL", _url)


def _yesterday_utc() -> str:
    return (datetime.datetime.now(datetime.timezone.utc).date()
            - datetime.timedelta(days=1)).isoformat()


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="BESS Trading Performance Agent — daily ops review"
    )
    p.add_argument(
        "--date",
        default=_yesterday_utc(),
        metavar="YYYY-MM-DD",
        help="Date to analyse (default: yesterday UTC)",
    )
    p.add_argument(
        "--asset-code",
        choices=["suyou", "hangjinqi", "siziwangqi", "gushanliang"],
        default=None,
        help="Run for a single asset only (default: all 4)",
    )
    p.add_argument(
        "--send-email",
        action="store_true",
        help="Send email report via SMTP (requires SMTP_* env vars and REPORT_EMAIL_TO)",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print narrative to stdout; skip email and PDF write",
    )
    return p.parse_args()


def _register_cjk_font() -> str:
    """
    Register a CJK-compatible font with reportlab and return the font name.

    Tries Windows system fonts (SimHei, SimSun) then falls back to reportlab's
    built-in STSong-Light CID font which covers GB2312 Chinese characters.
    """
    from reportlab.pdfbase import pdfmetrics

    for font_name, path in [
        ("SimHei", "C:/Windows/Fonts/simhei.ttf"),
        ("SimSun", "C:/Windows/Fonts/simsun.ttc"),
    ]:
        if os.path.exists(path):
            try:
                from reportlab.pdfbase.ttfonts import TTFont
                pdfmetrics.registerFont(TTFont(font_name, path))
                return font_name
            except Exception:
                continue

    # Built-in CID font — no file needed, covers GB2312 Chinese
    from reportlab.pdfbase.cidfonts import UnicodeCIDFont
    pdfmetrics.registerFont(UnicodeCIDFont("STSong-Light"))
    return "STSong-Light"


def _build_portfolio_pdf(narrative: str, date: str) -> bytes:
    """
    Build a PDF containing the Claude narrative using reportlab.
    Uses a CJK-compatible font so Chinese characters render correctly.
    Falls back to encoded UTF-8 bytes if reportlab is absent.
    """
    try:
        import io as _io
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.units import mm
        from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer

        cjk_font = _register_cjk_font()

        buf = _io.BytesIO()
        doc = SimpleDocTemplate(
            buf,
            pagesize=A4,
            leftMargin=20 * mm,
            rightMargin=20 * mm,
            topMargin=20 * mm,
            bottomMargin=20 * mm,
        )
        styles = getSampleStyleSheet()
        h1 = ParagraphStyle(
            "H1Cjk", parent=styles["Heading1"],
            fontName=cjk_font, fontSize=14, leading=18,
        )
        normal = ParagraphStyle(
            "NormalCjk", parent=styles["Normal"],
            fontName=cjk_font, fontSize=10, leading=14,
        )
        small = ParagraphStyle(
            "SmallCjk", parent=styles["Normal"],
            fontName=cjk_font, fontSize=9, leading=12,
        )
        h2 = ParagraphStyle(
            "H2Cjk", parent=styles["Heading2"],
            fontName=cjk_font, fontSize=12, leading=16,
        )

        story = []
        story.append(Paragraph(f"BESS储能每日交易绩效报告 — {date}", h1))
        story.append(Spacer(1, 4 * mm))

        for line in narrative.splitlines():
            stripped = line.strip()
            if not stripped:
                story.append(Spacer(1, 2 * mm))
                continue
            if stripped.startswith("## "):
                story.append(Paragraph(stripped[3:], h2))
            elif stripped.startswith("# "):
                story.append(Paragraph(stripped[2:], h1))
            elif stripped.startswith("- ") or stripped.startswith("* "):
                story.append(Paragraph(f"\u2022 {stripped[2:]}", small))
            elif stripped[0].isdigit() and ". " in stripped[:4]:
                story.append(Paragraph(stripped, small))
            else:
                story.append(Paragraph(stripped, normal))

        doc.build(story)
        return buf.getvalue()

    except ImportError:
        header = (
            "# PDF requires reportlab — pip install reportlab\n"
            f"# Falling back to plain text for {date}\n\n"
        )
        return (header + narrative).encode("utf-8")


def _save_pdf(pdf_bytes: bytes, filename: str) -> str:
    """Save PDF bytes to REPORT_OUTPUT_DIR and return the full path."""
    output_dir = os.environ.get("REPORT_OUTPUT_DIR", "reports")
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, filename)
    with open(path, "wb") as f:
        f.write(pdf_bytes)
    return path


def _upload_to_s3(pdf_bytes: bytes, filename: str) -> str | None:
    """Upload PDF to S3 under trading-performance/ prefix for historical access.

    Returns the S3 URI on success, or None if bucket not configured or upload fails.
    Requires UPLOADS_BUCKET_NAME env var and an IAM role with s3:PutObject on the bucket.
    """
    bucket = os.environ.get("UPLOADS_BUCKET_NAME")
    if not bucket:
        return None
    try:
        import boto3

        s3 = boto3.client("s3")
        key = f"trading-performance/{filename}"
        s3.put_object(Bucket=bucket, Key=key, Body=pdf_bytes, ContentType="application/pdf")
        uri = f"s3://{bucket}/{key}"
        print(f"Report uploaded: {uri}")
        return uri
    except Exception as exc:
        print(f"WARNING: S3 upload failed — {exc}", file=sys.stderr)
        return None


def main() -> None:
    args = _parse_args()
    date = args.date

    print(f"\n{'=' * 70}")
    print(f"  BESS Trading Performance Agent — {date}")
    print(f"{'=' * 70}")

    # ------------------------------------------------------------------
    # Run the agent
    # ------------------------------------------------------------------
    from libs.decision_models.adapters.agent.trading_performance_agent import (
        TradingPerformanceAgent,
    )

    agent = TradingPerformanceAgent()

    if args.asset_code:
        # Single-asset mode: use direct tool call via answer_query
        print(f"\nSingle-asset mode: {args.asset_code}")
        question = (
            f"Run a full strategy performance analysis for {args.asset_code} on {date}. "
            f"Include strategy ranking, ops dispatch data availability, attribution if available, "
            f"realization and fragility status, and recommendations."
        )
        response_text, _ = agent.answer_query(question, date)
        result_narrative = response_text
        n_alerts = 0
        tool_calls: list = []
    else:
        # Full 4-asset daily review
        print("\nRunning full 4-asset daily review via Claude agent loop...")
        result = agent.run_daily_review(date)
        result_narrative = result.narrative
        n_alerts = result.n_alerts
        tool_calls = result.tool_calls
        print(f"\nComplete — turns: {len(tool_calls)} tool calls, {result.n_alerts} alert(s)")

    # ------------------------------------------------------------------
    # Print narrative
    # ------------------------------------------------------------------
    print(f"\n{'─' * 70}")
    print(result_narrative)
    print(f"{'─' * 70}")

    if args.dry_run:
        print("\nDry run — skipping PDF write and email.")
        return

    # ------------------------------------------------------------------
    # PDF generation
    # ------------------------------------------------------------------
    filename = f"trading_performance_{date.replace('-', '')}.pdf"
    pdf_bytes = _build_portfolio_pdf(result_narrative, date)

    pdf_path = _save_pdf(pdf_bytes, filename)
    if pdf_bytes[:4] == b"%PDF":
        print(f"\nPDF written: {pdf_path} ({len(pdf_bytes):,} bytes)")
    else:
        print(f"\nPDF fallback (reportlab not installed): {pdf_path}")

    _upload_to_s3(pdf_bytes, filename)

    # ------------------------------------------------------------------
    # Email report
    # ------------------------------------------------------------------
    if args.send_email:
        try:
            from shared.agents.execution_agent import send_email_report

            subject = f"BESS Trading Performance — {date} — {n_alerts} alert(s)"
            send_email_report(
                subject=subject,
                body=result_narrative,
                pdf_bytes=pdf_bytes,
                filename=filename,
            )
            print(f"Email sent: {subject}")
        except Exception as exc:
            print(f"ERROR: email send failed — {exc}", file=sys.stderr)
            sys.exit(1)
    else:
        print("\n(Use --send-email to send the report by email)")

    print(f"\n{'=' * 70}")
    print("  DONE")
    print(f"{'=' * 70}\n")


if __name__ == "__main__":
    main()
