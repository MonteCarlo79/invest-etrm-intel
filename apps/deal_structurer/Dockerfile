FROM python:3.11-slim
WORKDIR /app

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
    "statsmodels>=0.14"

COPY apps/deal_structurer/    ./apps/deal_structurer/
COPY libs/deal_models/        ./libs/deal_models/
COPY services/deal_engine/    ./services/deal_engine/
COPY services/common/         ./services/common/
COPY shared/                  ./shared/

ENV PYTHONPATH=/app

EXPOSE 8522

CMD ["streamlit", "run", "apps/deal_structurer/app.py", \
     "--server.port=8522", \
     "--server.address=0.0.0.0", \
     "--server.baseUrlPath=deal-structurer", \
     "--server.enableCORS=false", \
     "--server.enableXsrfProtection=false", \
     "--server.headless=true", \
     "--server.fileWatcherType=none"]
