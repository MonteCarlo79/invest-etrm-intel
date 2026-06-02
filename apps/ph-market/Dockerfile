FROM python:3.11-slim
WORKDIR /app

RUN pip install --no-cache-dir --timeout 300 --retries 10 \
    "streamlit>=1.32" \
    "plotly==6.0.1" \
    pandas \
    "psycopg2-binary==2.9.10" \
    "sqlalchemy>=2.0" \
    numpy \
    "python-dotenv==1.0.1" \
    "anthropic>=0.40" \
    "requests==2.32.3" \
    openpyxl \
    "apscheduler==3.10.4" \
    "beautifulsoup4>=4.12" \
    lxml \
    "pypdf>=4.0" \
    "python-docx>=1.1" \
    "python-pptx>=0.6" \
    "reportlab>=4.0" \
    pypsa

COPY apps/ph-market/               ./apps/ph-market/
COPY services/common/               ./services/common/
COPY services/ph_knowledge/         ./services/ph_knowledge/
COPY services/intl_market_common/   ./services/intl_market_common/

ENV PYTHONPATH=/app

EXPOSE 8510

CMD ["streamlit", "run", "apps/ph-market/app.py", \
     "--server.port=8510", \
     "--server.address=0.0.0.0", \
     "--server.baseUrlPath=ph-market", \
     "--server.enableCORS=false", \
     "--server.enableXsrfProtection=false", \
     "--server.headless=true", \
     "--server.fileWatcherType=none"]
