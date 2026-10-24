#!/usr/bin/env bash
# Load .env and run schema creation + data sync to populate local Postgres.
# Run from repo root: bash data/local/scripts/populate_local_db.sh

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
ENV_FILE="$REPO_ROOT/data/local/.env"

cd "$REPO_ROOT"
# When running in Docker, compose sets LOCAL_DB_URL to postgres:5432; don't overwrite with .env's localhost
SAVE_LOCAL_DB_URL="${LOCAL_DB_URL:-}"
SAVE_GOOGLE_APPLICATION_CREDENTIALS="${GOOGLE_APPLICATION_CREDENTIALS:-}"
if [ -f "$ENV_FILE" ]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
fi
[ -n "$SAVE_LOCAL_DB_URL" ] && export LOCAL_DB_URL="$SAVE_LOCAL_DB_URL"
[ -n "$SAVE_GOOGLE_APPLICATION_CREDENTIALS" ] && export GOOGLE_APPLICATION_CREDENTIALS="$SAVE_GOOGLE_APPLICATION_CREDENTIALS"

echo "=== 1. Creating tables from BigQuery schemas (landing, raw_dev, staging_dev, intermediate_dev, mart_dev) ==="
python data/local/scripts/bq_schema_to_local_pg.py

echo ""
echo "=== 2. Syncing data into local tables ==="
python data/local/scripts/sync_all_tables.py --dataset landing
python data/local/scripts/sync_all_tables.py --dataset raw_dev
python data/local/scripts/sync_all_tables.py --dataset staging_dev
python data/local/scripts/sync_all_tables.py --dataset intermediate_dev
python data/local/scripts/sync_all_tables.py --dataset mart_dev

echo ""
echo "=== 3. Loading GIS CSV into local DB (optional script if present) ==="
GIS_SCRIPT="data/ingestion/satellite/gis_data_ingestion.py"
if [ -f "$GIS_SCRIPT" ]; then
  python "$GIS_SCRIPT"
else
  echo "Skip: $GIS_SCRIPT not found"
fi

echo ""
echo "Done. Local schemas: landing, raw_dev, staging_dev, intermediate_dev, mart_dev."
echo "Note: ingest sandbox landing_dev is separate — dbt transforms use landing as SoT."
