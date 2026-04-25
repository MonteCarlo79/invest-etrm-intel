FROM python:3.11-slim

WORKDIR /app

COPY apps/options-cockpit/requirements.txt ./apps/options-cockpit/requirements.txt
RUN pip install --no-cache-dir -r apps/options-cockpit/requirements.txt

COPY libs/options/ ./libs/options/
COPY apps/options-cockpit/app.py ./apps/options-cockpit/app.py

ENV PYTHONPATH=/app

EXPOSE 8507

CMD ["streamlit", "run", "apps/options-cockpit/app.py", \
     "--server.port=8507", \
     "--server.address=0.0.0.0", \
     "--server.baseUrlPath=options-cockpit", \
     "--server.enableCORS=false", \
     "--server.enableXsrfProtection=false"]
