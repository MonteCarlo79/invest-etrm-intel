FROM python:3.11-slim
WORKDIR /app
COPY apps/trading-performance-agent/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
COPY . /app
WORKDIR /app/apps/trading-performance-agent
EXPOSE 8501
CMD ["python", "/app/services/ops/run_trading_agent.py", "--send-email"]
