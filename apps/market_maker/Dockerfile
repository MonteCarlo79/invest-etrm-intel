FROM python:3.11-slim

WORKDIR /app

COPY apps/market_maker/requirements.txt ./apps/market_maker/requirements.txt
RUN pip install --no-cache-dir -i https://pypi.tuna.tsinghua.edu.cn/simple \
    --trusted-host pypi.tuna.tsinghua.edu.cn --timeout 300 --retries 5 \
    -r apps/market_maker/requirements.txt

COPY libs/ ./libs/
COPY services/ ./services/
COPY shared/ ./shared/
COPY auth/ ./auth/
COPY apps/market_maker/ ./apps/market_maker/
# migration runner: scripts/market_maker/migrate.py resolves the .sql as <workdir>/db/ddl/marketdata/
COPY scripts/market_maker/ ./scripts/market_maker/
COPY db/ddl/marketdata/market_maker_migration.sql ./db/ddl/marketdata/market_maker_migration.sql

ENV PYTHONPATH=/app

EXPOSE 8530

CMD ["streamlit", "run", "apps/market_maker/app.py", \
     "--server.port=8530", "--server.address=0.0.0.0", \
     "--server.baseUrlPath=market-maker", \
     "--server.fileWatcherType=none", \
     "--server.enableCORS=false", "--server.enableXsrfProtection=false"]
