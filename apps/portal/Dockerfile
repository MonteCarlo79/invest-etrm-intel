FROM python:3.11-slim

WORKDIR /app

COPY apps/portal /app/apps/portal
COPY shared /app/shared
COPY auth /app/auth

RUN pip install --no-cache-dir \
    streamlit \
    pandas \
    sqlalchemy \
    psycopg2-binary \
    boto3

ENV PYTHONPATH=/app

EXPOSE 8500

CMD ["streamlit","run","apps/portal/app.py","--server.port=8500","--server.address=0.0.0.0","--server.baseUrlPath=portal"]