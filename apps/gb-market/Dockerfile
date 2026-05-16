FROM python:3.11-slim
WORKDIR /app

RUN pip install --no-cache-dir \
    "streamlit>=1.32" \
    "plotly==6.0.1" \
    pandas \
    "psycopg2-binary==2.9.10" \
    "sqlalchemy>=2.0" \
    numpy \
    "python-dotenv==1.0.1" \
    "anthropic>=0.40" \
    "requests==2.32.3" \
    openpyxl \
    "apscheduler==3.10.4" \
    "beautifulsoup4>=4.12" \
    lxml \
    "pypdf>=4.0" \
    "python-docx>=1.1" \
    "python-pptx>=0.6" \
    "reportlab>=4.0" \
    "scikit-learn>=1.4" \
    "scipy>=1.12"

RUN apt-get update && apt-get install -y --no-install-recommends fonts-noto-cjk \
    && rm -rf /var/lib/apt/lists/*

COPY apps/gb-market/        ./apps/gb-market/
COPY services/modo_energy/  ./services/modo_energy/
COPY services/common/       ./services/common/
COPY services/gb_knowledge/ ./services/gb_knowledge/

ENV PYTHONPATH=/app

EXPOSE 8508

CMD ["streamlit", "run", "apps/gb-market/app.py", \
     "--server.port=8508", \
     "--server.address=0.0.0.0", \
     "--server.baseUrlPath=gb-market", \
     "--server.enableCORS=false", \
     "--server.enableXsrfProtection=false", \
     "--server.headless=true", \
     "--server.fileWatcherType=none"]
