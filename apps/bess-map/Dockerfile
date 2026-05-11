FROM python:3.11-slim
WORKDIR /app

RUN pip install --no-cache-dir \
    "streamlit>=1.32" \
    "plotly==6.0.1" \
    pandas \
    "psycopg2-binary==2.9.10" \
    "sqlalchemy>=2.0" \
    numpy \
    "matplotlib>=3.8" \
    "python-dotenv==1.0.1" \
    "anthropic>=0.40" \
    "boto3==1.34.0" \
    openpyxl \
    pulp \
    "requests==2.32.3"

# Install CJK font so matplotlib can render Chinese titles on the geo map
RUN apt-get update && apt-get install -y --no-install-recommends \
        fonts-noto-cjk \
    && rm -rf /var/lib/apt/lists/* \
    && python -c "import matplotlib.font_manager as _fm; _fm.fontManager = _fm.FontManager(); print('matplotlib font cache rebuilt')"

COPY apps/bess-map/      ./apps/bess-map/
COPY services/bess_map/  ./services/bess_map/
COPY shared/             ./shared/
COPY auth/               ./auth/

ENV PYTHONPATH=/app
EXPOSE 8503

CMD ["streamlit", "run", "apps/bess-map/app.py", \
     "--server.port=8503", \
     "--server.address=0.0.0.0", \
     "--server.baseUrlPath=bess-map", \
     "--server.enableCORS=false", \
     "--server.enableXsrfProtection=false", \
     "--server.fileWatcherType=none", \
     "--server.headless=true"]
