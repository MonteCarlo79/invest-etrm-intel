FROM python:3.11-slim

WORKDIR /apps

RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY apps/trading/bess/mengxi/pnl_attribution/requirements.txt /tmp/requirements.txt
RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r /tmp/requirements.txt

COPY apps/trading/bess/mengxi/pnl_attribution /apps

ENV PYTHONPATH=/apps

EXPOSE 8502

CMD ["streamlit","run","app.py","--server.port=8502","--server.address=0.0.0.0","--server.baseUrlPath=pnl-attribution","--server.enableCORS=false","--server.enableXsrfProtection=false"]
