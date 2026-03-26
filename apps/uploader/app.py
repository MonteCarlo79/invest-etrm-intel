# -*- coding: utf-8 -*-
import os
import time
import subprocess
from pathlib import Path
import datetime as dt
import re
import sys
import traceback
import logging

import streamlit as st
import boto3
import botocore

from auth.rbac import get_user, get_groups, get_role, get_email, require_role

require_role(["Admin", "Trader", "Quant", "Analyst"])

st.set_page_config(page_title="BESS Pipeline", layout="wide")


def resolve_role() -> str | None:
    email = (get_email() or "").strip().lower()
    groups = [g.strip().lower() for g in (get_groups() or [])]

    group_role_map = {
        "admin": "Admin",
        "trader": "Trader",
        "quant": "Quant",
        "analyst": "Analyst",
    }

    for g in groups:
        if g in group_role_map:
            return group_role_map[g]

    raw_map = os.getenv("EMAIL_ROLE_MAP", "")
    mapping = {}

    for item in raw_map.split(","):
        item = item.strip()
        if not item or "=" not in item:
            continue
        k, v = item.split("=", 1)
        mapping[k.strip().lower()] = v.strip()

    if email in mapping:
        return mapping[email]

    if email == "chen_dpeng@hotmail.com":
        return "Admin"

    return "Analyst"


allowed_roles = ["Admin", "Quant"]

user = get_user()
if not user:
    st.warning("Please log in via SSO.")
    st.stop()

role = get_role() or resolve_role()

if not role:
    st.error(f"Access denied. No valid role found. Email: {get_email()}")
    st.stop()

if role not in allowed_roles:
    st.error(f"Access denied. Your role: {role}. Allowed roles: {allowed_roles}")
    st.stop()

user_email = user.get("email", "unknown")
st.caption(f"User: {user_email} | Role: {role}")

sys.path.append(str(Path(__file__).resolve().parent))

logging.basicConfig(level=logging.INFO)

S3_BUCKET = os.environ.get("S3_BUCKET")

try:
    session = boto3.session.Session()
    region = session.region_name

    s3_test = boto3.client("s3", region_name=region)
    response = s3_test.list_buckets()

    logging.info("===== S3 ACCESS SUCCESS =====")
    logging.info(f"Detected region: {region}")
    logging.info(f"Total buckets visible: {len(response.get('Buckets', []))}")

except botocore.exceptions.ClientError as e:
    logging.error("===== S3 ACCESS FAILED (ClientError) =====")
    logging.error(str(e))

except Exception as e:
    logging.error("===== S3 ACCESS FAILED (General) =====")
    logging.error(str(e))

s3 = boto3.client("s3") if S3_BUCKET else None

BASE_DIR = Path(__file__).resolve().parent
MODELS_DIR = BASE_DIR / "models"


def clean_province_from_filename(fname: str) -> str:
    stem = Path(fname).stem.strip()
    prov = re.sub(r"[^\u4e00-\u9fa5]", "", stem)
    return prov


UPLOAD_DIR = os.environ.get("UPLOAD_DIR", "/tmp/uploads")
LOG_DIR = os.environ.get("LOG_DIR", "/tmp/logs")

st.title("🚀 BESS End-to-End Pipeline")

if not S3_BUCKET:
    st.error("Missing S3_BUCKET env var in ECS task definition.")
    st.stop()

Path(UPLOAD_DIR).mkdir(parents=True, exist_ok=True)
Path(LOG_DIR).mkdir(parents=True, exist_ok=True)

# -----------------------------------------------------------------------------
# Upload
# -----------------------------------------------------------------------------
st.subheader("1) Upload Province Excel Files")

if "uploaded_names" not in st.session_state:
    st.session_state["uploaded_names"] = []

uploaded_files = st.file_uploader(
    "Upload province Excel files",
    type="xlsx",
    accept_multiple_files=True
)

if uploaded_files:
    saved_names = []

    for f in uploaded_files:
        s3.upload_fileobj(
            f,
            S3_BUCKET,
            f"uploads/{f.name}"
        )
        saved_names.append(f.name)

    st.session_state["uploaded_names"] = saved_names
    st.success(f"{len(saved_names)} file(s) uploaded to S3 bucket {S3_BUCKET}")


# -----------------------------------------------------------------------------
# Run plan
# -----------------------------------------------------------------------------
st.subheader("2) Run Plan")

colA, colB, colC = st.columns([1.2, 1.2, 1.6])

with colA:
    st.markdown("### A) Ingest")
    do_ingest = st.checkbox("Ingest (Excel → spot_prices_hourly)", value=True, key="ingest_checkbox")

with colB:
    st.markdown("### B) Theoretical dispatch")
    do_theo_2h = st.checkbox("Theoretical 2h", value=False, key="theoretical_2h_checkbox")
    force_theo_2h = st.checkbox("Force 2h recompute", value=False, key="force_theo_2h_checkbox")
    do_theo_4h = st.checkbox("Theoretical 4h", value=False, key="theoretical_4h_checkbox")
    force_theo_4h = st.checkbox("Force 4h recompute", value=False, key="force_theo_4h_checkbox")

with colC:
    st.markdown("### C) Forecast capture")
    model = st.selectbox("Forecast model", ["ols_da_time_v1", "naive_da"], index=0, key="forecast_model_selectbox")
    do_cap_2h = st.checkbox("Capture 2h (forecast)", value=False, key="capture_2h_checkbox")
    force_cap_2h = st.checkbox("Force 2h recompute", value=False, key="force_cap_2h_checkbox")
    do_cap_4h = st.checkbox("Capture 4h (forecast)", value=False, key="capture_4h_checkbox")
    force_cap_4h = st.checkbox("Force 4h recompute", value=False, key="force_cap_4h_checkbox")

st.caption("Note: If you tick nothing in B/C, the pipeline will only run ingestion (A).")

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
progress = st.progress(0)
run_status = st.empty()
log_box = st.empty()


def _now_stamp():
    return dt.datetime.now().strftime("%Y%m%d_%H%M%S")


def run_and_stream(cmd: list[str], title: str, log_path: Path, start_pct: int, end_pct: int):
    run_status.markdown(f"### {title}")
    st.code(" ".join(cmd))

    with open(log_path, "a", encoding="utf-8") as lf:
        lf.write("\n" + "=" * 80 + "\n")
        lf.write(f"[{dt.datetime.now().isoformat()}] START: {title}\n")
        lf.write("CMD: " + " ".join(cmd) + "\n\n")

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    buffer = ""
    progress.progress(start_pct)

    for line in process.stdout:
        print(line, end="")
        buffer += line
        log_box.code(buffer[-12000:])
        with open(log_path, "a", encoding="utf-8") as lf:
            lf.write(line)

        progress.progress(min(end_pct, int(start_pct + (end_pct - start_pct) * 0.02)))

    rc = process.wait()

    with open(log_path, "a", encoding="utf-8") as lf:
        lf.write(f"\n[{dt.datetime.now().isoformat()}] END: {title} rc={rc}\n")

    if rc != 0:
        raise RuntimeError(f"{title} failed (rc={rc}). See log: {log_path}")

    progress.progress(end_pct)


# -----------------------------------------------------------------------------
# Execute
# -----------------------------------------------------------------------------
st.subheader("3) Execute Pipeline")

if st.button("🚀 Run Pipeline"):
    uploaded_names = st.session_state.get("uploaded_names", [])
    log_path = Path(LOG_DIR) / f"pipeline_{_now_stamp()}.log"
    st.info(f"Persistent log file: {log_path}")

    try:
        plan = []

        # A) ingest
        if do_ingest:
            if not uploaded_names:
                st.warning("No files uploaded in this session.")
                st.stop()

            uploaded_csv = ",".join(uploaded_names)

            st.write("Files to ingest:")

            local_dir = Path("/tmp/uploads")
            local_dir.mkdir(exist_ok=True)

            for name in uploaded_names:
                st.write(f"• {name}")
                s3.download_file(
                    S3_BUCKET,
                    f"uploads/{name}",
                    str(local_dir / name)
                )

            plan.append(("Ingest", [
                "python", str(MODELS_DIR / "run_all_provinces.py"),
                "--indir", str(local_dir),
                "--only-files", uploaded_csv,
                "--auto-cols",
                "--upload-db",
                "--continue-on-error",
            ]))

        # Build province list from uploaded files
        provinces = [clean_province_from_filename(n) for n in uploaded_names]
        provinces = [p for p in provinces if p]
        province_csv = ",".join(sorted(set(provinces)))

        # B) THEORETICAL DISPATCH
        if do_theo_2h:
            if not province_csv:
                st.warning("No uploaded provinces selected.")
                st.stop()

            cmd = [
                "python", str(MODELS_DIR / "run_capture_pipeline.py"),
                "--duration-h", "2",
                "--model", model,
                "--province-list", province_csv,
            ]

            if force_theo_2h:
                cmd.append("--force-theoretical")

            plan.append(("Theoretical 2h", cmd))

        if do_theo_4h:
            if not province_csv:
                st.warning("No uploaded provinces selected.")
                st.stop()

            cmd = [
                "python", str(MODELS_DIR / "run_capture_pipeline.py"),
                "--duration-h", "4",
                "--model", model,
                "--province-list", province_csv,
            ]

            if force_theo_4h:
                cmd.append("--force-theoretical")

            plan.append(("Theoretical 4h", cmd))

        # C) CAPTURE (FORECAST)
        if do_cap_2h:
            if not province_csv:
                st.warning("No uploaded provinces selected.")
                st.stop()

            cmd = [
                "python", str(MODELS_DIR / "run_capture_pipeline.py"),
                "--duration-h", "2",
                "--model", model,
                "--province-list", province_csv,
            ]

            if force_cap_2h:
                cmd.append("--force")

            plan.append(("Capture 2h", cmd))

        if do_cap_4h:
            if not province_csv:
                st.warning("No uploaded provinces selected.")
                st.stop()

            cmd = [
                "python", str(MODELS_DIR / "run_capture_pipeline.py"),
                "--duration-h", "4",
                "--model", model,
                "--province-list", province_csv,
            ]

            if force_cap_4h:
                cmd.append("--force")

            plan.append(("Capture 4h", cmd))

        if not plan:
            st.warning("Nothing selected. Enable at least one step.")
            st.stop()

        st.markdown("### Execution Plan")
        for i, (name, cmd) in enumerate(plan, 1):
            st.write(f"{i}. {name}")
            st.code(" ".join(cmd))

        n = len(plan)

        for i, (name, cmd) in enumerate(plan, 1):
            start = int((i - 1) / n * 100)
            end = int(i / n * 100)
            run_and_stream(cmd, f"Step {i}/{n}: {name}", log_path, start, end)

        progress.progress(100)
        st.success("✅ Pipeline completed successfully")

        with open(Path(UPLOAD_DIR).parent / "cache_bust.txt", "w") as f:
            f.write(str(dt.datetime.now()))

        for name in uploaded_names:
            s3.copy_object(
                Bucket=S3_BUCKET,
                CopySource={"Bucket": S3_BUCKET, "Key": f"uploads/{name}"},
                Key=f"archived/{name}"
            )
            s3.delete_object(Bucket=S3_BUCKET, Key=f"uploads/{name}")

    except Exception as e:
        st.error(f"❌ Pipeline failed: {e}")
        print(traceback.format_exc())

    finally:
        try:
            if s3 and log_path.exists():
                s3.upload_file(str(log_path), S3_BUCKET, f"logs/{log_path.name}")
        except Exception:
            pass