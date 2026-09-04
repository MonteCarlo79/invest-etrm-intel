FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    "anthropic>=0.40" \
    "psycopg2-binary==2.9.10" \
    "python-dotenv==1.0.1" \
    pdfplumber \
    python-pptx \
    python-docx \
    xlrd \
    openpyxl

# Only copy what the synthesis pipeline needs
COPY services/knowledge_pool/ ./services/knowledge_pool/
COPY scripts/run_synthesis_pipeline.py ./scripts/
COPY scripts/apply_kp_intelligence_ddl.py ./scripts/

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Default: shared docs, 1 worker.
# Override via ECS run-task --overrides containerOverrides.command
CMD ["python", "scripts/run_synthesis_pipeline.py", "--phase", "1", "--app", "shared", "--workers", "1"]
