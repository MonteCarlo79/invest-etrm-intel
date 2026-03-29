FROM python:3.11-slim

WORKDIR /apps

RUN apt-get update && apt-get install -y libpq-dev gcc && rm -rf /var/lib/apt/lists/*

COPY apps/bess-inner-mongolia/im/requirements.txt /tmp/requirements.txt
RUN pip install --upgrade pip && pip install --no-cache-dir -r /tmp/requirements.txt

COPY apps/bess-inner-mongolia/im /apps
COPY auth /apps/auth
COPY apps/bess-inner-mongolia/shared /apps/shared
COPY apps/trading/bess/mengxi/pnl_attribution /apps/apps/trading/bess/mengxi/pnl_attribution
COPY services/bess_inner_mongolia /apps/services/bess_inner_mongolia

ENV PYTHONPATH=/apps

EXPOSE 8504

CMD ["streamlit","run","app.py","--server.port=8504","--server.address=0.0.0.0","--server.baseUrlPath=inner-mongolia","--server.enableCORS=false","--server.enableXsrfProtection=false"]
