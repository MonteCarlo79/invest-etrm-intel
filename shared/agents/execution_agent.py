import os
from io import BytesIO
from pathlib import Path
from datetime import datetime

import pandas as pd
import requests
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from email.message import EmailMessage
import smtplib

from shared.agents.db import run_query


def build_execution_queue() -> pd.DataFrame:
    rows = [
        {"priority": 1, "task_type": "Dispatch Review", "scope": "Mengxi", "status": "Pending", "owner": "Trader Desk"},
        {"priority": 2, "task_type": "Province Spread Refresh", "scope": "All Provinces", "status": "Pending", "owner": "Quant"},
        {"priority": 3, "task_type": "Uploader Validation", "scope": "New File Intake", "status": "Pending", "owner": "Data Ops"},
    ]
    return pd.DataFrame(rows)


def build_execution_plan(region: str, objective: str) -> str:
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    return f"""
Execution Agent v2

Timestamp:
{now}

Region:
{region}

Objective:
{objective}

Recommended sequence:
1. Refresh latest market and spread tables
2. Validate new uploader intake and data freshness
3. Review top-ranked assets / sites in target region
4. Prepare trader checklist and manual approval gate
5. Log execution outcome and exceptions
""".strip()


def build_daily_operations_report() -> dict:
    top_provinces = run_query(
        """
        select province, irr_total, payback_years_total
        from bess_province_return_snapshot
        where as_of_date = (select max(as_of_date) from bess_province_return_snapshot)
        order by irr_total desc nulls last
        limit 10
        """
    )

    spread_monitor = run_query(
        """
        select province,
               avg(spread_cny_per_mwh) as avg_spread,
               stddev_samp(spread_cny_per_mwh) as spread_volatility
        from bess_theoretical_spread_ts
        where date >= current_date - interval '30 days'
        group by province
        order by avg_spread desc nulls last
        """
    )

    mengxi = run_query(
        """
        select date, site, profit_cny, rank
        from mengxi_profitability_daily
        where date >= current_date - interval '7 days'
        order by date desc, rank asc
        """
    )

    latest_spread_date = run_query(
        """
        select max(date) as latest_spread_date
        from bess_theoretical_spread_ts
        """
    )

    latest_mengxi_date = run_query(
        """
        select max(date) as latest_mengxi_date
        from mengxi_profitability_daily
        """
    )

    return {
        "generated_at": datetime.utcnow(),
        "top_provinces": top_provinces,
        "spread_monitor": spread_monitor,
        "mengxi": mengxi,
        "latest_spread_date": latest_spread_date,
        "latest_mengxi_date": latest_mengxi_date,
    }


def build_report_summary_text(report: dict) -> str:
    tp = report["top_provinces"]
    sm = report["spread_monitor"]
    mx = report["mengxi"]

    top_line = "No province data"
    if not tp.empty:
        top_line = ", ".join(tp["province"].head(3).astype(str).tolist())

    spread_line = "No spread data"
    if not sm.empty:
        best = sm.iloc[0]
        spread_line = f"{best['province']} leads 30d avg spread at {best['avg_spread']:.2f} CNY/MWh"

    mengxi_line = "No Mengxi data"
    if not mx.empty:
        latest_date = pd.to_datetime(mx["date"]).max()
        latest_slice = mx[pd.to_datetime(mx["date"]) == latest_date].sort_values("rank").head(3)
        mengxi_line = ", ".join(latest_slice["site"].astype(str).tolist())

    return (
        f"Daily Execution Report\n"
        f"Generated: {report['generated_at']:%Y-%m-%d %H:%M:%S UTC}\n\n"
        f"Top province shortlist: {top_line}\n"
        f"Spread monitor: {spread_line}\n"
        f"Mengxi top sites: {mengxi_line}\n"
    )


def render_report_pdf_bytes(report: dict, summary_text: str) -> bytes:
    buffer = BytesIO()
    c = canvas.Canvas(buffer, pagesize=A4)
    width, height = A4

    y = height - 50
    c.setFont("Helvetica-Bold", 14)
    c.drawString(50, y, "BESS Platform - Daily Execution Report")

    y -= 25
    c.setFont("Helvetica", 10)
    c.drawString(50, y, f"Generated: {report['generated_at']:%Y-%m-%d %H:%M:%S UTC}")

    y -= 30
    c.setFont("Helvetica-Bold", 11)
    c.drawString(50, y, "Summary")

    y -= 18
    c.setFont("Helvetica", 10)
    for line in summary_text.splitlines():
        c.drawString(50, y, line[:110])
        y -= 14
        if y < 80:
            c.showPage()
            y = height - 50
            c.setFont("Helvetica", 10)

    y -= 10
    c.setFont("Helvetica-Bold", 11)
    c.drawString(50, y, "Top Provinces")
    y -= 18
    c.setFont("Helvetica", 10)

    tp = report["top_provinces"].head(10)
    for _, row in tp.iterrows():
        line = f"{row['province']} | IRR={row['irr_total']:.2%} | Payback={row['payback_years_total']:.2f}y"
        c.drawString(50, y, line[:110])
        y -= 14
        if y < 80:
            c.showPage()
            y = height - 50
            c.setFont("Helvetica", 10)

    c.save()
    pdf_bytes = buffer.getvalue()
    buffer.close()
    return pdf_bytes


def save_pdf_to_disk(pdf_bytes: bytes, filename: str) -> str:
    out_dir = Path(os.getenv("REPORT_OUTPUT_DIR", "reports"))
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / filename
    path.write_bytes(pdf_bytes)
    return str(path)


def send_slack_report(summary_text: str, pdf_path: str = None) -> None:
    webhook = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook:
        raise ValueError("SLACK_WEBHOOK_URL is not set")

    text = summary_text
    if pdf_path:
        text += f"\nPDF saved on server: {pdf_path}"

    response = requests.post(
        webhook,
        json={"text": text},
        timeout=20,
    )
    response.raise_for_status()


def send_email_report(subject: str, body: str, pdf_bytes: bytes, filename: str) -> None:
    host = os.getenv("SMTP_HOST")
    port = int(os.getenv("SMTP_PORT", "587"))
    user = os.getenv("SMTP_USER")
    password = os.getenv("SMTP_PASSWORD")
    sender = os.getenv("SMTP_FROM", user)
    recipients = [x.strip() for x in os.getenv("REPORT_EMAIL_TO", "").split(",") if x.strip()]

    if not all([host, port, user, password, sender]) or not recipients:
        raise ValueError("SMTP or REPORT_EMAIL_TO settings are incomplete")

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)
    msg.set_content(body)

    msg.add_attachment(
        pdf_bytes,
        maintype="application",
        subtype="pdf",
        filename=filename,
    )

    with smtplib.SMTP(host, port) as smtp:
        smtp.starttls()
        smtp.login(user, password)
        smtp.send_message(msg)


def generate_and_send_daily_report(send_slack: bool = True, send_email: bool = True) -> dict:
    report = build_daily_operations_report()
    summary = build_report_summary_text(report)
    filename = f"execution_report_{report['generated_at']:%Y%m%d}.pdf"
    pdf_bytes = render_report_pdf_bytes(report, summary)
    pdf_path = save_pdf_to_disk(pdf_bytes, filename)

    if send_slack:
        send_slack_report(summary, pdf_path=pdf_path)

    if send_email:
        send_email_report(
            subject=f"BESS Daily Execution Report - {report['generated_at']:%Y-%m-%d}",
            body=summary,
            pdf_bytes=pdf_bytes,
            filename=filename,
        )

    return {
        "summary": summary,
        "pdf_path": pdf_path,
        "filename": filename,
    }