FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    "streamlit>=1.32" \
    "plotly==6.0.1" \
    pandas \
    "psycopg2-binary==2.9.10" \
    matplotlib \
    numpy \
    "python-dotenv==1.0.1" \
    "requests==2.32.3" \
    "anthropic[bedrock]>=0.40" \
    boto3 \
    pdfplumber \
    openpyxl \
    "statsmodels>=0.14"

# Install CJK font AFTER matplotlib so the font cache is rebuilt with it present.
# fonts-noto-cjk provides "Noto Sans CJK SC" used by chart_geo_map for Chinese titles.
RUN apt-get update && apt-get install -y --no-install-recommends \
        fonts-noto-cjk \
    && rm -rf /var/lib/apt/lists/* \
    && python -c "import matplotlib.font_manager as _fm; _fm.fontManager = _fm.FontManager(); print('matplotlib font cache rebuilt')"

# App + all service dependencies
COPY apps/spot-market/                  ./apps/spot-market/
COPY services/spot_ingest/              ./services/spot_ingest/
COPY services/spot_mcp/                 ./services/spot_mcp/
COPY services/knowledge_pool/           ./services/knowledge_pool/
COPY services/market_fundamentals/      ./services/market_fundamentals/
COPY apps/spot-watcher/                 ./apps/spot-watcher/
COPY data/market-fundamentals/          ./data/market-fundamentals/

ENV PYTHONPATH=/app

EXPOSE 8505

CMD ["streamlit", "run", "apps/spot-market/app.py", \
     "--server.port=8505", \
     "--server.address=0.0.0.0", \
     "--server.baseUrlPath=spot-markets", \
     "--server.enableCORS=false", \
     "--server.enableXsrfProtection=false", \
     "--server.headless=true", \
     "--server.fileWatcherType=none"]
