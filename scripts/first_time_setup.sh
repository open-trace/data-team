#!/usr/bin/env bash
# First-time setup: build Docker, start Postgres, then sync dbt sources from BigQuery (OAuth).
# Run from repo root: bash scripts/first_time_setup.sh
#
# Prerequisites: Docker, Python 3, gcloud CLI (for OAuth). See docs/FIRST_TIME_SETUP.md.

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
ENV_FILE="$REPO_ROOT/data/local/.env"
ENV_EXAMPLE="$REPO_ROOT/data/local/.env.example"

cd "$REPO_ROOT"

echo "=== First-time setup (OpenTrace data-team) ==="
echo ""

# --- 1. Docker: build and start Postgres ---
echo "--- 1. Building Docker image and starting Postgres ---"
docker compose build
docker compose up -d
echo "Waiting for Postgres to be ready..."
sleep 3
echo "Postgres is up. Optional: run 'docker compose --profile setup up' to populate local DB from BigQuery (requires service-account key)."
echo ""

# --- 2. Env and OAuth ---
if [ ! -f "$ENV_FILE" ]; then
  if [ -f "$ENV_EXAMPLE" ]; then
    cp "$ENV_EXAMPLE" "$ENV_FILE"
    echo "--- 2. Created data/local/.env from .env.example ---"
    echo "   Edit data/local/.env and set BQ_PROJECT (and optionally BQ_DATASET_*)."
  else
    echo "--- 2. No data/local/.env found. Create it and set BQ_PROJECT (see data/local/.env.example). ---"
  fi
else
  echo "--- 2. Using existing data/local/.env ---"
fi
echo "   For dbt with OAuth (no key file), run once: gcloud auth application-default login"
echo ""

# Load .env for the next steps
if [ -f "$ENV_FILE" ]; then
  set -a
  source "$ENV_FILE"
  set +a
fi

# --- 3. Populate dbt sources from BigQuery (catalog + generate sources.yml) ---
echo "--- 3. Syncing dbt sources from BigQuery (OAuth) ---"
if [ -z "${BQ_PROJECT:-}" ]; then
  echo "   BQ_PROJECT is not set in data/local/.env. Set it, then run:"
  echo "   python data/local/scripts/generate_dbt_sources.py --refresh"
  echo "   Or re-run this script after editing .env."
else
  if ! python -c "import google.cloud.bigquery" 2>/dev/null; then
    echo "   Installing google-cloud-bigquery (required for catalog)..."
    pip install -q google-cloud-bigquery
  fi
  python data/local/scripts/generate_dbt_sources.py --refresh
  echo "   dbt/models/sources.yml is now in sync with BigQuery."
fi
echo ""

echo "=== Setup complete ==="
echo ""
echo "Next steps:"
echo "  1. Run dbt:  cd dbt && dbt deps && dbt run --target bronze"
echo "     (Use --select bronze.* to run only bronze models; see docs/FIRST_TIME_SETUP.md)"
echo "  2. Full guide: docs/FIRST_TIME_SETUP.md"
echo "  3. dbt details: dbt/README.md"
echo ""
