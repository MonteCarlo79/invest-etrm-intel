FROM python:3.11-slim
WORKDIR /app

# Only the decision model library is needed.
# Model modules use lazy imports for services/ so pulp, psycopg2, etc.
# are NOT required at catalogue load time.
COPY libs/ /app/libs/

RUN pip install --no-cache-dir \
    streamlit>=1.30 \
    numpy>=1.24 \
    pandas>=2.0

ENV PYTHONPATH=/app

EXPOSE 8506

CMD ["streamlit", "run", "libs/decision_models/adapters/app/catalogue_app.py", \
     "--server.port=8506", \
     "--server.address=0.0.0.0", \
     "--server.baseUrlPath=model-catalogue", \
     "--server.enableCORS=false", \
     "--server.enableXsrfProtection=false"]
