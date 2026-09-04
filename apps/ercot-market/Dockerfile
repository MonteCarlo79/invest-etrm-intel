FROM python:3.11-slim
WORKDIR /app

RUN pip install --no-cache-dir --timeout 300 --retries 10 \
    "streamlit==1.58.0" \
    "plotly==6.0.1" \
    pandas \
    "psycopg2-binary==2.9.10" \
    "sqlalchemy>=2.0" \
    numpy \
    "python-dotenv==1.0.1" \
    "anthropic[bedrock]>=0.40" \
    "requests==2.32.3" \
    openpyxl \
    "apscheduler==3.10.4" \
    "beautifulsoup4>=4.12" \
    lxml \
    "pypdf>=4.0" \
    "python-docx>=1.1" \
    "reportlab>=4.0"

RUN pip install --no-cache-dir --timeout 600 --retries 10 \
    "playwright>=1.44" \
    "playwright-stealth>=1.0"

RUN apt-get update && apt-get install -y --no-install-recommends fonts-noto-cjk \
    && rm -rf /var/lib/apt/lists/*

RUN playwright install --with-deps chromium

COPY apps/ercot-market/            ./apps/ercot-market/
COPY shared/                       ./shared/
COPY services/modo_energy/         ./services/modo_energy/
COPY services/common/              ./services/common/
COPY services/ercot_knowledge/     ./services/ercot_knowledge/
COPY services/intl_market_common/  ./services/intl_market_common/

ENV PYTHONPATH=/app

EXPOSE 8510

RUN chmod +x ./apps/ercot-market/run.sh
CMD ["/bin/bash", "apps/ercot-market/run.sh"]
