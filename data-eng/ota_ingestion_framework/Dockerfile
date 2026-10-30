FROM python:3.12-slim
WORKDIR /app
# OpenSSL validates reviewed certificate chains. Poppler extracts PDF text.
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates openssl poppler-utils && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY src ./src
COPY config ./config
COPY ingestion_points/raw ./ingestion_points/raw
COPY ingestion_points/testing_rules.json ./ingestion_points/testing_rules.json
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 OTA_STORAGE_DIR=/storage
# Attach a Railway volume at /storage so data, profiles and progress survive.
CMD ["python", "-m", "src.run_pipeline", "--workers", "4", "--max-seconds", "300", "--timeout", "15"]
