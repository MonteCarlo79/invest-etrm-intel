FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY batch_downloader.py .
COPY load_excel_to_marketdata.py .
COPY run_pipeline.py .

ENV PYTHONUNBUFFERED=1

CMD ["python", "run_pipeline.py"]