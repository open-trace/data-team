# Image for running populate_local_db (schema + bronze sync + GIS) inside Docker.
# Used by the "setup" service in docker-compose; repo is mounted at runtime.
FROM python:3.11-slim

WORKDIR /app

# Install dependencies (repo is mounted when container runs)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Default: run the full populate script (overridden in compose)
CMD ["bash", "data/local/scripts/populate_local_db.sh"]
