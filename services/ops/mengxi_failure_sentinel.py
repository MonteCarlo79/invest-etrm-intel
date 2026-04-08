import hashlib
import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.request import Request, urlopen

import boto3
from sqlalchemy import create_engine, text


PIPELINE_NAME = os.getenv("PIPELINE_NAME", "bess-mengxi-ingestion")
PROVINCE = os.getenv("PROVINCE", "mengxi")
DB_SCHEMA = os.getenv("DB_SCHEMA", "marketdata")
OPS_SCHEMA = os.getenv("OPS_SCHEMA", "ops")
AWS_REGION = os.getenv("AWS_REGION", "ap-southeast-1")
LOG_GROUP_NAME = os.getenv("MENGXI_LOG_GROUP", "/ecs/bess-mengxi-ingestion")
MARKET_LAG_DAYS = int(os.getenv("MARKET_LAG_DAYS", "1"))
LOOKBACK_HOURS = int(os.getenv("AGENT4_LOOKBACK_HOURS", "48"))
STREAM_SCAN_LIMIT = int(os.getenv("AGENT4_STREAM_SCAN_LIMIT", "10"))
EVENT_SCAN_LIMIT = int(os.getenv("AGENT4_EVENT_SCAN_LIMIT", "200"))
ALERT_DEDUP_HOURS = int(os.getenv("ALERT_DEDUP_HOURS", "6"))
ALERT_WEBHOOK_URL = os.getenv("ALERT_WEBHOOK_URL", "").strip()
ALERT_CONTEXT = os.getenv("ALERT_CONTEXT", "agent4-mengxi")
PGURL = os.getenv("PGURL") or os.getenv("DB_DSN") or os.getenv("DATABASE_URL")

FAILURE_PATTERNS = {
    "db_connect_timeout": [
        "RuntimeError: Database not reachable",
        "DB connection failed:",
        "timeout expired",
    ],
    "source_download_failure": [
        "HTTP ",
        "request timeout",
        "batch_downloader.py",
    ],
    "parse_or_extract_failure": [
        "[SHEET FAIL]",
        "openpyxl",
        "Excel",
        "No recognized sheets",
        "Bad filename:",
    ],
    "db_load_failure": [
        "constraint",
        "UndefinedColumn",
        "UndefinedTable",
        "duplicate key value",
        "psycopg2.errors",
        "sqlalchemy.exc",
    ],
}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def get_engine():
    if not PGURL:
        raise RuntimeError("PGURL/DB_DSN/DATABASE_URL not set for Agent 4 sentinel")
    return create_engine(PGURL, pool_pre_ping=True)


def ensure_tables(engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {OPS_SCHEMA};"))
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {OPS_SCHEMA}.mengxi_agent4_status (
                pipeline_name TEXT PRIMARY KEY,
                province TEXT NOT NULL,
                trust_state TEXT NOT NULL,
                last_run_status TEXT,
                failure_class TEXT,
                evidence_summary TEXT,
                heuristic_summary TEXT,
                recommended_action TEXT,
                latest_success_file_date DATE,
                latest_quality_date DATE,
                expected_file_date DATE,
                freshness_lag_days INTEGER,
                status_payload JSONB,
                source_log_group TEXT,
                source_log_stream TEXT,
                source_event_time TIMESTAMPTZ,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
        """))
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {OPS_SCHEMA}.mengxi_agent4_alert_state (
                incident_key TEXT PRIMARY KEY,
                pipeline_name TEXT NOT NULL,
                failure_class TEXT NOT NULL,
                trust_state TEXT NOT NULL,
                first_observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                last_observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                last_alert_sent_at TIMESTAMPTZ,
                resolved_at TIMESTAMPTZ,
                occurrence_count INTEGER NOT NULL DEFAULT 1,
                evidence_summary TEXT,
                recommended_action TEXT,
                status_payload JSONB
            );
        """))
        conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS idx_mengxi_agent4_alert_state_active
            ON {OPS_SCHEMA}.mengxi_agent4_alert_state (pipeline_name, failure_class, resolved_at);
        """))


def classify_failure(message: str) -> str:
    message_lower = (message or "").lower()
    for failure_class, patterns in FAILURE_PATTERNS.items():
        if any(pattern.lower() in message_lower for pattern in patterns):
            return failure_class
    return "unknown_terminal_failure"


def describe_failure(failure_class: str) -> tuple[str | None, str | None]:
    if failure_class == "db_connect_timeout":
        return (
            "Observed repeated Postgres timeout to the RDS endpoint from the ECS task.",
            "First operational fix: rerun terraform apply in bess-platform/infra/terraform/mengxi-ingestion/. "
            "If it still fails, inspect live ECS task SG versus the RDS-attached SG to confirm SG drift has been cleared.",
        )
    if failure_class == "source_download_failure":
        return (
            "Observed download-stage failure from the source retrieval path.",
            "Inspect source availability, throttling, and downloader HTTP behavior before rerunning blindly.",
        )
    if failure_class == "parse_or_extract_failure":
        return (
            "Observed parser or Excel extraction failure during ingestion.",
            "Inspect recent file format drift and the loader/parser assumptions for the affected date window.",
        )
    if failure_class == "db_load_failure":
        return (
            "Observed database load failure after file extraction.",
            "Inspect loader SQL compatibility, target table shape, and any new constraint or schema drift.",
        )
    return (
        "Observed terminal Mengxi ingestion failure without a matched known signature.",
        "Capture the failing log stream and classify it before retrying repeatedly.",
    )


def describe_non_failure_state(trust_state: str) -> tuple[str, str]:
    if trust_state == "healthy":
        return (
            "Observed recent successful Mengxi ingestion with fresh enough downstream data.",
            "Downstream agents can use Mengxi outputs normally.",
        )
    if trust_state == "degraded":
        return (
            "No terminal failure signature is active, but freshness or completeness is behind the ideal window.",
            "Downstream agents should qualify Mengxi conclusions and inspect missing or partial dates before relying on them.",
        )
    return (
        "No terminal failure signature is active, but Mengxi output freshness is beyond the safe trust window.",
        "Do not trust downstream Mengxi outputs until the missing period is backfilled or the failing run is understood.",
    )


def fetch_recent_streams(logs_client) -> list[dict[str, Any]]:
    response = logs_client.describe_log_streams(
        logGroupName=LOG_GROUP_NAME,
        orderBy="LastEventTime",
        descending=True,
        limit=STREAM_SCAN_LIMIT,
    )
    return response.get("logStreams", [])


def inspect_stream(logs_client, stream_name: str, lookback_start_ms: int) -> dict[str, Any] | None:
    response = logs_client.get_log_events(
        logGroupName=LOG_GROUP_NAME,
        logStreamName=stream_name,
        limit=EVENT_SCAN_LIMIT,
        startFromHead=False,
    )
    events = response.get("events", [])
    recent_events = [e for e in events if e.get("timestamp", 0) >= lookback_start_ms]
    if not recent_events:
        return None

    joined = "\n".join(e.get("message", "") for e in recent_events)
    if "Pipeline completed successfully" in joined:
        return {
            "run_status": "success",
            "failure_class": None,
            "source_log_stream": stream_name,
            "source_event_time": max(e["timestamp"] for e in recent_events),
            "evidence_summary": "Observed successful Mengxi pipeline completion in CloudWatch logs.",
            "raw_summary": joined[-4000:],
        }

    terminal_markers = [
        "RuntimeError: Database not reachable",
        "[FILE FAILED]",
        "Traceback (most recent call last):",
        "Command '['python'",
        "CalledProcessError",
        "SystemExit: 2",
    ]
    if any(marker in joined for marker in terminal_markers):
        failure_class = classify_failure(joined)
        evidence_summary, _ = describe_failure(failure_class)
        return {
            "run_status": "failed",
            "failure_class": failure_class,
            "source_log_stream": stream_name,
            "source_event_time": max(e["timestamp"] for e in recent_events),
            "evidence_summary": evidence_summary,
            "raw_summary": joined[-4000:],
        }

    return {
        "run_status": "unknown",
        "failure_class": None,
        "source_log_stream": stream_name,
        "source_event_time": max(e["timestamp"] for e in recent_events),
        "evidence_summary": "Observed recent Mengxi log activity without a terminal success/failure marker.",
        "raw_summary": joined[-4000:],
    }


def get_latest_run_signal(logs_client) -> dict[str, Any]:
    lookback_start = utc_now() - timedelta(hours=LOOKBACK_HOURS)
    lookback_start_ms = int(lookback_start.timestamp() * 1000)
    streams = fetch_recent_streams(logs_client)

    inspected = []
    for stream in streams:
        stream_name = stream["logStreamName"]
        result = inspect_stream(logs_client, stream_name, lookback_start_ms)
        if result:
            inspected.append(result)

    if not inspected:
        return {
            "run_status": "unknown",
            "failure_class": None,
            "source_log_stream": None,
            "source_event_time": None,
            "evidence_summary": "No recent Mengxi ECS log activity found within the configured lookback window.",
            "raw_summary": "",
        }

    inspected.sort(key=lambda item: item["source_event_time"] or 0, reverse=True)
    return inspected[0]


def get_latest_success_and_quality(engine) -> tuple[Any, Any, bool | None]:
    with engine.begin() as conn:
        latest_success = conn.execute(text(f"""
            SELECT max(file_date)
            FROM {DB_SCHEMA}.md_load_log
            WHERE status = 'success'
        """)).scalar()

        quality_row = conn.execute(text(f"""
            SELECT data_date, is_complete
            FROM {DB_SCHEMA}.data_quality_status
            WHERE province = :province
            ORDER BY data_date DESC
            LIMIT 1
        """), {"province": PROVINCE}).fetchone()

    if quality_row:
        return latest_success, quality_row[0], quality_row[1]
    return latest_success, None, None


def compute_trust_state(run_signal: dict[str, Any], latest_success_file_date, latest_quality_date, latest_is_complete):
    expected_file_date = utc_now().date() - timedelta(days=MARKET_LAG_DAYS)
    freshness_lag_days = None
    if latest_success_file_date:
        freshness_lag_days = (expected_file_date - latest_success_file_date).days

    if run_signal["run_status"] == "failed":
        return "unsafe_to_trust", expected_file_date, freshness_lag_days

    if latest_success_file_date is None:
        return "unsafe_to_trust", expected_file_date, freshness_lag_days

    if freshness_lag_days is not None and freshness_lag_days <= 0 and latest_is_complete:
        return "healthy", expected_file_date, freshness_lag_days

    if freshness_lag_days is not None and freshness_lag_days <= 1:
        return "degraded", expected_file_date, freshness_lag_days

    if latest_quality_date and latest_is_complete is False and freshness_lag_days is not None and freshness_lag_days <= 1:
        return "degraded", expected_file_date, freshness_lag_days

    return "unsafe_to_trust", expected_file_date, freshness_lag_days


def build_incident_key(run_signal: dict[str, Any]) -> str:
    basis = "|".join([
        PIPELINE_NAME,
        run_signal.get("failure_class") or "none",
    ])
    return hashlib.sha256(basis.encode("utf-8")).hexdigest()


def should_send_alert(engine, incident_key: str) -> bool:
    cooldown_start = utc_now() - timedelta(hours=ALERT_DEDUP_HOURS)
    with engine.begin() as conn:
        row = conn.execute(text(f"""
            SELECT last_alert_sent_at, resolved_at
            FROM {OPS_SCHEMA}.mengxi_agent4_alert_state
            WHERE incident_key = :incident_key
        """), {"incident_key": incident_key}).fetchone()

    if not row:
        return True

    last_alert_sent_at, resolved_at = row
    if resolved_at is not None:
        return True
    if last_alert_sent_at is None:
        return True
    return last_alert_sent_at <= cooldown_start


def send_alert(payload: dict[str, Any]) -> None:
    if not ALERT_WEBHOOK_URL:
        return

    body = json.dumps(payload).encode("utf-8")
    request = Request(
        ALERT_WEBHOOK_URL,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    with urlopen(request, timeout=10) as response:
        print("Agent 4 alert sent with status:", response.status)


def persist_status(engine, payload: dict[str, Any]) -> None:
    with engine.begin() as conn:
        conn.execute(text(f"""
            INSERT INTO {OPS_SCHEMA}.mengxi_agent4_status (
                pipeline_name,
                province,
                trust_state,
                last_run_status,
                failure_class,
                evidence_summary,
                heuristic_summary,
                recommended_action,
                latest_success_file_date,
                latest_quality_date,
                expected_file_date,
                freshness_lag_days,
                status_payload,
                source_log_group,
                source_log_stream,
                source_event_time,
                updated_at
            ) VALUES (
                :pipeline_name,
                :province,
                :trust_state,
                :last_run_status,
                :failure_class,
                :evidence_summary,
                :heuristic_summary,
                :recommended_action,
                :latest_success_file_date,
                :latest_quality_date,
                :expected_file_date,
                :freshness_lag_days,
                CAST(:status_payload AS JSONB),
                :source_log_group,
                :source_log_stream,
                :source_event_time,
                now()
            )
            ON CONFLICT (pipeline_name) DO UPDATE SET
                province = EXCLUDED.province,
                trust_state = EXCLUDED.trust_state,
                last_run_status = EXCLUDED.last_run_status,
                failure_class = EXCLUDED.failure_class,
                evidence_summary = EXCLUDED.evidence_summary,
                heuristic_summary = EXCLUDED.heuristic_summary,
                recommended_action = EXCLUDED.recommended_action,
                latest_success_file_date = EXCLUDED.latest_success_file_date,
                latest_quality_date = EXCLUDED.latest_quality_date,
                expected_file_date = EXCLUDED.expected_file_date,
                freshness_lag_days = EXCLUDED.freshness_lag_days,
                status_payload = EXCLUDED.status_payload,
                source_log_group = EXCLUDED.source_log_group,
                source_log_stream = EXCLUDED.source_log_stream,
                source_event_time = EXCLUDED.source_event_time,
                updated_at = now();
        """), {
            **payload,
            "status_payload": json.dumps(payload, default=str),
        })


def persist_alert_state(engine, incident_key: str, payload: dict[str, Any], alert_sent: bool) -> None:
    with engine.begin() as conn:
        conn.execute(text(f"""
            INSERT INTO {OPS_SCHEMA}.mengxi_agent4_alert_state (
                incident_key,
                pipeline_name,
                failure_class,
                trust_state,
                first_observed_at,
                last_observed_at,
                last_alert_sent_at,
                resolved_at,
                occurrence_count,
                evidence_summary,
                recommended_action,
                status_payload
            ) VALUES (
                :incident_key,
                :pipeline_name,
                :failure_class,
                :trust_state,
                now(),
                now(),
                :last_alert_sent_at,
                NULL,
                1,
                :evidence_summary,
                :recommended_action,
                CAST(:status_payload AS JSONB)
            )
            ON CONFLICT (incident_key) DO UPDATE SET
                last_observed_at = now(),
                last_alert_sent_at = CASE
                    WHEN :last_alert_sent_at IS NULL THEN {OPS_SCHEMA}.mengxi_agent4_alert_state.last_alert_sent_at
                    ELSE :last_alert_sent_at
                END,
                resolved_at = NULL,
                occurrence_count = {OPS_SCHEMA}.mengxi_agent4_alert_state.occurrence_count + 1,
                evidence_summary = EXCLUDED.evidence_summary,
                recommended_action = EXCLUDED.recommended_action,
                status_payload = EXCLUDED.status_payload,
                trust_state = EXCLUDED.trust_state;
        """), {
            "incident_key": incident_key,
            **payload,
            "last_alert_sent_at": utc_now() if alert_sent else None,
            "status_payload": json.dumps(payload, default=str),
        })


def resolve_active_alerts(engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(f"""
            UPDATE {OPS_SCHEMA}.mengxi_agent4_alert_state
            SET resolved_at = now()
            WHERE pipeline_name = :pipeline_name
              AND resolved_at IS NULL
        """), {"pipeline_name": PIPELINE_NAME})


def main() -> None:
    logs_client = boto3.client("logs", region_name=AWS_REGION)
    engine = get_engine()
    ensure_tables(engine)

    run_signal = get_latest_run_signal(logs_client)
    latest_success_file_date, latest_quality_date, latest_is_complete = get_latest_success_and_quality(engine)
    trust_state, expected_file_date, freshness_lag_days = compute_trust_state(
        run_signal,
        latest_success_file_date,
        latest_quality_date,
        latest_is_complete,
    )

    if run_signal.get("failure_class"):
        heuristic_summary, recommended_action = describe_failure(run_signal.get("failure_class"))
    else:
        heuristic_summary, recommended_action = describe_non_failure_state(trust_state)
    payload = {
        "pipeline_name": PIPELINE_NAME,
        "province": PROVINCE,
        "trust_state": trust_state,
        "last_run_status": run_signal.get("run_status"),
        "failure_class": run_signal.get("failure_class"),
        "evidence_summary": run_signal.get("evidence_summary"),
        "heuristic_summary": heuristic_summary,
        "recommended_action": recommended_action,
        "latest_success_file_date": latest_success_file_date,
        "latest_quality_date": latest_quality_date,
        "expected_file_date": expected_file_date,
        "freshness_lag_days": freshness_lag_days,
        "source_log_group": LOG_GROUP_NAME,
        "source_log_stream": run_signal.get("source_log_stream"),
        "source_event_time": datetime.fromtimestamp(
            (run_signal.get("source_event_time") or 0) / 1000,
            tz=timezone.utc,
        ) if run_signal.get("source_event_time") else None,
        "alert_context": ALERT_CONTEXT,
    }

    persist_status(engine, payload)

    if trust_state == "healthy":
        resolve_active_alerts(engine)
        print(json.dumps(payload, default=str))
        return

    incident_key = build_incident_key(run_signal)
    alert_sent = False
    if run_signal.get("failure_class") and should_send_alert(engine, incident_key):
        alert_payload = {
            "text": (
                f"Mengxi Agent 4 status: {trust_state}. "
                f"Failure class={run_signal.get('failure_class')}. "
                f"{run_signal.get('evidence_summary')} "
                f"Recommended action: {recommended_action}"
            ),
            **payload,
        }
        try:
            send_alert(alert_payload)
            alert_sent = bool(ALERT_WEBHOOK_URL)
        except Exception as alert_error:
            print(f"[WARN] Agent 4 alert delivery failed: {alert_error}")

    if run_signal.get("failure_class"):
        persist_alert_state(engine, incident_key, payload, alert_sent)

    print(json.dumps(payload, default=str))


if __name__ == "__main__":
    main()
