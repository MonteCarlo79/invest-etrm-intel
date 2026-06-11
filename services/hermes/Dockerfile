FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY services/hermes/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/hermes/ ./services/hermes/

ENV PYTHONPATH=/app

CMD ["uvicorn", "services.hermes.app:app", "--host", "0.0.0.0", "--port", "8000"]
