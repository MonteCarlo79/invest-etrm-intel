FROM python:3.11-slim

WORKDIR /app

# Copy shared utilities and collectors
COPY services/data_ingestion/ ./services/data_ingestion/
COPY services/loader/province_misc_to_db_v2.py ./services/loader/province_misc_to_db_v2.py
COPY services/bess_map/run_all_provinces.py ./services/bess_map/run_all_provinces.py
COPY services/bess_map/run_one_province.py  ./services/bess_map/run_one_province.py
COPY services/bess_map/db.py               ./services/bess_map/db.py
COPY bess-marketdata-ingestion/providers/mengxi/ ./bess-marketdata-ingestion/providers/mengxi/

# Requirements
COPY services/data_ingestion/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONPATH=/app
