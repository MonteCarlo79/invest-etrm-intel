# Playwright Python image — includes Python 3.11 + Chromium + all browser deps
FROM mcr.microsoft.com/playwright/python:v1.50.0-jammy

WORKDIR /app

# Install Python dependencies
COPY services/bess_map/requirements.txt /tmp/bess_map_req.txt
RUN pip install --no-cache-dir \
        schedule \
        python-dotenv \
        psycopg2-binary \
        playwright \
        requests \
        openpyxl \
        pandas \
        numpy \
    && pip install --no-cache-dir -r /tmp/bess_map_req.txt \
    && playwright install chromium --with-deps

# Copy application code
COPY services/lingfeng/     services/lingfeng/
COPY services/bess_map/     services/bess_map/
COPY services/market_fundamentals/  services/market_fundamentals/
COPY services/hermes/__init__.py        services/hermes/__init__.py
COPY services/hermes/feishu_client.py   services/hermes/feishu_client.py
COPY services/hermes/telegram_client.py services/hermes/telegram_client.py

# Blank __init__ files so Python treats directories as packages
RUN touch services/__init__.py services/bess_map/__init__.py services/market_fundamentals/__init__.py

CMD ["python", "services/lingfeng/entrypoint.py"]
