FROM python:3.11-slim

WORKDIR /app

COPY apps/mengxi-dashboard/app.py ./apps/mengxi-dashboard/app.py

RUN pip install --no-cache-dir \
    streamlit>=1.30 \
    plotly>=5.18 \
    pandas>=2.0 \
    psycopg2-binary

ENV PYTHONPATH=/app

EXPOSE 8505

CMD ["streamlit", "run", "apps/mengxi-dashboard/app.py", \
     "--server.port=8505", \
     "--server.address=0.0.0.0", \
     "--server.baseUrlPath=mengxi-dashboard", \
     "--server.enableCORS=false", \
     "--server.enableXsrfProtection=false"]
