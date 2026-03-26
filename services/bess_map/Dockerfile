FROM python:3.11-slim

WORKDIR /apps

RUN apt-get update && apt-get install -y build-essential libpq-dev coinor-cbc && rm -rf /var/lib/apt/lists/*

COPY services/bess_map/requirements.txt /tmp/requirements.txt
RUN pip install --upgrade pip && pip install --no-cache-dir --default-timeout=100 -r /tmp/requirements.txt

COPY services/bess_map /apps

# shared platform modules (required for RBAC + agents)
COPY auth /apps/auth
COPY shared /apps/shared

# EXPOSE 8501

# CMD ["streamlit","run","streamlit_bess_profit_dashboard_v14.1_consistent_full2.py","--server.port=8501","--server.address=0.0.0.0","--server.baseUrlPath=bess-map","--server.enableCORS=false","--server.enableXsrfProtection=false","--","--env","/apps/.env","--schema","marketdata"]

EXPOSE 8503

CMD ["streamlit","run","streamlit_bess_profit_dashboard_v14.1_consistent_full2.py","--server.port=8503","--server.address=0.0.0.0","--server.baseUrlPath=bess-map","--server.enableCORS=false","--server.enableXsrfProtection=false","--","--env","/apps/.env","--schema","marketdata"]