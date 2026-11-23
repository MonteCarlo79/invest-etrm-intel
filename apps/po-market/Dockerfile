FROM python:3.11-slim
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg antiword && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir --timeout 300 --retries 10 \
    "streamlit>=1.32" \
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
    "python-pptx>=0.6" \
    "reportlab>=4.0" \
    pypsa \
    "openai>=1.0" \
    "pydub>=0.25" \
    pulp

COPY apps/po-market/               ./apps/po-market/
COPY shared/                        ./shared/
COPY services/common/               ./services/common/
COPY services/po_knowledge/         ./services/po_knowledge/
COPY services/intl_market_common/   ./services/intl_market_common/
COPY services/bess_map/             ./services/bess_map/
COPY libs/                          ./libs/

ENV PYTHONPATH=/app

EXPOSE 8511

CMD ["streamlit", "run", "apps/po-market/app.py", \
     "--server.port=8511", \
     "--server.address=0.0.0.0", \
     "--server.baseUrlPath=po-market", \
     "--server.enableCORS=false", \
     "--server.enableXsrfProtection=false", \
     "--server.headless=true", \
     "--server.fileWatcherType=none"]
