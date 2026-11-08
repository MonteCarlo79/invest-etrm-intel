FROM python:3.11-slim

WORKDIR /app

# Install pip packages first (before system fonts / apt installs)
COPY apps/mengxi-dashboard/requirements.txt ./apps/mengxi-dashboard/requirements.txt
RUN pip install --no-cache-dir \
    -i https://pypi.tuna.tsinghua.edu.cn/simple \
    --trusted-host pypi.tuna.tsinghua.edu.cn \
    --timeout 300 \
    --retries 5 \
    -r apps/mengxi-dashboard/requirements.txt

# Copy source
COPY libs/ ./libs/
COPY services/ ./services/
COPY shared/ ./shared/
COPY apps/mengxi-dashboard/ ./apps/mengxi-dashboard/
# bess_context imports apps.trading.bess.mengxi.pnl_attribution.calc (ASSET_ALIAS_MAP)
COPY apps/trading/ ./apps/trading/

ENV PYTHONPATH=/app

EXPOSE 8505

CMD ["streamlit", "run", "apps/mengxi-dashboard/app.py", \
     "--server.port=8505", \
     "--server.address=0.0.0.0", \
     "--server.baseUrlPath=mengxi-dashboard", \
     "--server.fileWatcherType=none", \
     "--server.enableCORS=false", \
     "--server.enableXsrfProtection=false"]
