FROM python:3.12-slim

WORKDIR /app

# power-academy package (concepts, syllabus, academy lib) + the job + hermes feishu client
COPY power-academy /opt/power-academy
COPY services/professor /app/services/professor
COPY services/hermes/feishu_client.py /opt/services/hermes/feishu_client.py

RUN pip install --no-cache-dir pyyaml sqlalchemy psycopg2-binary anthropic pandas requests

CMD ["python", "services/professor/topic_card.py", "--send"]
