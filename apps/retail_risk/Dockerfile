FROM python:3.11-slim

WORKDIR /app

COPY apps/retail_risk/requirements.txt ./apps/retail_risk/requirements.txt
RUN pip install --no-cache-dir -i https://pypi.tuna.tsinghua.edu.cn/simple \
    --trusted-host pypi.tuna.tsinghua.edu.cn --timeout 300 --retries 5 \
    -r apps/retail_risk/requirements.txt

COPY libs/ ./libs/
COPY services/ ./services/
COPY shared/ ./shared/
COPY auth/ ./auth/
COPY apps/retail_risk/ ./apps/retail_risk/

ENV PYTHONPATH=/app

EXPOSE 8513

CMD ["streamlit", "run", "apps/retail_risk/app.py", \
     "--server.port=8513", "--server.address=0.0.0.0", \
     "--server.baseUrlPath=retail-risk", \
     "--server.fileWatcherType=none", \
     "--server.enableCORS=false", "--server.enableXsrfProtection=false"]
