FROM python:3.11-slim

WORKDIR /app

COPY apps/spot-agent /app/apps/spot-agent

RUN pip install --no-cache-dir \
    boto3 \
    streamlit \
    pandas \
    psycopg2-binary \
    python-dotenv \
    pyyaml \
    pdfplumber \
    matplotlib

ENV PYTHONPATH=/app

EXPOSE 8505

CMD ["streamlit", "run", "apps/spot-agent/ui/spot_dashboard.py", \
     "--server.port=8505", \
     "--server.address=0.0.0.0", \
     "--server.baseUrlPath=spot-markets", \
     "--server.enableCORS=false", \
     "--server.enableXsrfProtection=false"]
