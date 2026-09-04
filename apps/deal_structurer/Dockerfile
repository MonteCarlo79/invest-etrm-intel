FROM python:3.11-slim
WORKDIR /app

# Tolerate slow/flaky PyPI links (large scientific wheels)
ENV PIP_DEFAULT_TIMEOUT=120 \
    PIP_RETRIES=10

RUN pip install --no-cache-dir \
    "streamlit==1.58.0" \
    "plotly==6.0.1" \
    pandas \
    numpy \
    scipy \
    "psycopg2-binary==2.9.10" \
    "sqlalchemy>=2.0" \
    "python-dotenv==1.0.1" \
    "anthropic[bedrock]>=0.40" \
    boto3 \
    pydantic \
    "statsmodels>=0.14" \
    reportlab \
    matplotlib \
    python-docx \
    python-pptx \
    openpyxl \
    xlrd \
    pdfplumber

# CJK font for reportlab DAF PDFs (export_utils falls back to STSong-Light if absent;
# tolerate apt failure so the build never breaks on font install)
RUN apt-get update && apt-get install -y --no-install-recommends fonts-wqy-microhei \
    && rm -rf /var/lib/apt/lists/* || true

COPY apps/deal_structurer/    ./apps/deal_structurer/
COPY libs/deal_models/        ./libs/deal_models/
COPY libs/risk/               ./libs/risk/
COPY services/deal_engine/    ./services/deal_engine/
COPY services/common/         ./services/common/
COPY shared/                  ./shared/
# deal committee layer
COPY services/deal_committee/  ./services/deal_committee/
COPY db/ddl/marketdata/deal_committee.sql ./db/ddl/marketdata/deal_committee.sql
COPY services/knowledge_pool/  ./services/knowledge_pool/
# headless agents dispatched via services.hermes.market_agent_bridge
COPY services/spot_mcp/        ./services/spot_mcp/
COPY services/market_fundamentals/ ./services/market_fundamentals/
COPY services/bess_map/        ./services/bess_map/
COPY services/mengxi_trading/  ./services/mengxi_trading/
COPY services/asset_risk/      ./services/asset_risk/
COPY services/retail_risk/     ./services/retail_risk/
COPY services/hermes/__init__.py            ./services/hermes/__init__.py
COPY services/hermes/market_agent_bridge.py ./services/hermes/market_agent_bridge.py
COPY services/hermes/export_utils.py        ./services/hermes/export_utils.py
# tool modules lazily imported by the asset_risk / retail_risk headless agents
COPY apps/asset_risk/__init__.py   ./apps/asset_risk/__init__.py
COPY apps/asset_risk/tab_agent.py  ./apps/asset_risk/tab_agent.py
COPY apps/retail_risk/__init__.py  ./apps/retail_risk/__init__.py
COPY apps/retail_risk/tab_agent.py ./apps/retail_risk/tab_agent.py

ENV PYTHONPATH=/app
ENV OPENBLAS_NUM_THREADS=1
ENV OMP_NUM_THREADS=1

EXPOSE 8522

CMD ["streamlit", "run", "apps/deal_structurer/app.py", \
     "--server.port=8522", \
     "--server.address=0.0.0.0", \
     "--server.baseUrlPath=deal-structurer", \
     "--server.enableCORS=false", \
     "--server.enableXsrfProtection=false", \
     "--server.headless=true", \
     "--server.fileWatcherType=none"]
