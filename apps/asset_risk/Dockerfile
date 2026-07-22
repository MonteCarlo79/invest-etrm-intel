FROM python:3.11-slim

WORKDIR /app

COPY apps/asset_risk/requirements.txt ./apps/asset_risk/requirements.txt
RUN pip install --no-cache-dir -i https://pypi.tuna.tsinghua.edu.cn/simple \
    --trusted-host pypi.tuna.tsinghua.edu.cn --timeout 300 --retries 5 \
    -r apps/asset_risk/requirements.txt

COPY libs/ ./libs/
COPY services/ ./services/
COPY shared/ ./shared/
COPY auth/ ./auth/
COPY apps/asset_risk/ ./apps/asset_risk/

ENV PYTHONPATH=/app

EXPOSE 8512

CMD ["streamlit", "run", "apps/asset_risk/app.py", \
     "--server.port=8512", "--server.address=0.0.0.0", \
     "--server.baseUrlPath=asset-risk", \
     "--server.fileWatcherType=none", \
     "--server.enableCORS=false", "--server.enableXsrfProtection=false"]
