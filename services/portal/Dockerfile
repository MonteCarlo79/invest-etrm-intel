FROM python:3.11-slim

WORKDIR /app

# copy requirements from this folder
COPY services/portal/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# copy portal app from apps/portal
COPY apps/portal/app.py ./app.py

EXPOSE 8500

CMD ["streamlit", "run", "app.py", "--server.port=8500", "--server.address=0.0.0.0", "--server.headless=true", "--server.enableCORS=false", "--server.enableXsrfProtection=false"]