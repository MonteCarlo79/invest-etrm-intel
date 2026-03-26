FROM python:3.11-slim

WORKDIR /apps

RUN apt-get update && apt-get install -y libpq-dev gcc && rm -rf /var/lib/apt/lists/*

COPY apps/uploader/requirements.txt /tmp/requirements.txt

RUN pip install --upgrade pip && pip install --default-timeout=300 --no-cache-dir -r /tmp/requirements.txt

COPY apps/uploader /apps
COPY auth /apps/auth
COPY shared /apps/shared
COPY services/bess_map /apps/models

EXPOSE 8501

# CMD ["streamlit","run","app.py","--server.port=8501","--server.address=0.0.0.0","--server.baseUrlPath=upload"]

CMD ["streamlit","run","app.py","--server.port=8501","--server.address=0.0.0.0","--server.baseUrlPath=uploader"]