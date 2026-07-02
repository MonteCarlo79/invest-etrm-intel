FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

WORKDIR /app

COPY apps/hermes-service/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# CJK font for matplotlib Chinese labels
RUN apt-get update && apt-get install -y --no-install-recommends fontconfig fonts-noto-cjk fonts-wqy-microhei \
    && fc-cache -fv && rm -rf /var/lib/apt/lists/*

# ── Agent Reach — internet capabilities ──────────────────────────────────────
# System deps: curl (Jina Reader), gh CLI (GitHub), Node.js+npm (mcporter/Exa)
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates gnupg \
    && mkdir -p /etc/apt/keyrings \
    && curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg \
       | dd of=/etc/apt/keyrings/githubcli-archive-keyring.gpg \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" \
       > /etc/apt/sources.list.d/github-cli.list \
    && curl -fsSL https://deb.nodesource.com/setup_22.x | bash - \
    && apt-get install -y --no-install-recommends gh nodejs \
    && rm -rf /var/lib/apt/lists/*

# Install agent-reach Python package (includes yt-dlp, feedparser)
RUN pip install --no-cache-dir "https://github.com/Panniantong/agent-reach/archive/main.zip"

# Install mcporter (Exa search MCP bridge) and bili-cli
RUN npm install -g mcporter bili-cli 2>/dev/null || true

# Bootstrap zero-config channels (web/YouTube/GitHub/RSS/Exa/V2EX/Bilibili)
# --env=server skips browser/desktop channel installs
RUN agent-reach install --env=server --yes 2>/dev/null || true

# Pre-download embedding model so it's baked into the image (avoids runtime download)
# Non-fatal: if model CDN is unreachable at build time, it will download on first use
RUN python -c "from fastembed import TextEmbedding; list(TextEmbedding('BAAI/bge-small-zh-v1.5').embed(['test']))" || true

# Copy hermes service code and shared modules
COPY services/ /app/services/
COPY shared/ /app/shared/

EXPOSE 8000

CMD ["uvicorn", "services.hermes.app:app", "--host", "0.0.0.0", "--port", "8000"]
