FROM python:3.11

WORKDIR /app

COPY agents /app/agents
COPY shared /app/shared

RUN pip install --no-cache-dir -r agents/rl/requirements.txt

ENV PYTHONPATH=/app

CMD ["python", "-m", "shared.agents.run_rl_training"]