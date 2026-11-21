#!/bin/bash
# Automated mining + ingestion entrypoint.
# Mine RSS articles → preprocess → upsert to Qdrant news collection.
#
# Required env:
#   MINING_COUNTRIES    — comma-separated, e.g. "Nigeria,Ghana,Kenya"
#   QDRANT_URL, QDRANT_API_KEY
#   QDRANT_COLLECTION_NEWS (default: news_data)
#
# Optional:
#   MINING_OUTPUT_DIR           — scratch dir for mined .txt files (default: /data/web_news_rss)
#   MINING_START_YEAR / MINING_START_DATE
#   MINING_END_YEAR   / MINING_END_DATE
#   MINING_MAX_URLS_PER_COUNTRY (default: 500)
#   MINING_BATCH_SIZE           (default: 100)
#   MINING_GOOGLE_DAILY_SLICE   — "1" to enable per-day Google News slicing
#   MINING_FEEDS_PATH           — path to feeds.json inside container
#   INGESTION_RESET             — "1" to delete+recreate collection before upsert
#   INGESTION_UPSERT_BATCH_SIZE (default: 200)
#   TAVILY_API_KEY              — if set, enables Tavily article enrichment
#   GOOGLE_APPLICATION_CREDENTIALS_BASE64 — GCP SA (not required for mining-only)

set -euo pipefail

GCP_SA_PATH="/tmp/gcp-sa-key.json"

echo "=== Automine + Ingest Entrypoint ==="

# --- GCP SA (optional, only for GCP-dependent paths) ---
if [ -n "${GOOGLE_APPLICATION_CREDENTIALS_BASE64:-}" ]; then
    printf '%s' "$GOOGLE_APPLICATION_CREDENTIALS_BASE64" | base64 -d > "$GCP_SA_PATH" 2>/dev/null || true
    if python -c "import json,sys; json.load(open(sys.argv[1]))" "$GCP_SA_PATH" 2>/dev/null; then
        export GOOGLE_APPLICATION_CREDENTIALS="$GCP_SA_PATH"
        echo "✓ GCP SA decoded"
    else
        rm -f "$GCP_SA_PATH"
        echo "⚠ GOOGLE_APPLICATION_CREDENTIALS_BASE64 did not decode to valid JSON — skipping"
    fi
fi

# --- Qdrant ---
if [ -z "${QDRANT_URL:-}" ]; then
    echo "✗ QDRANT_URL not set"
    exit 1
fi
echo "✓ QDRANT_URL=$QDRANT_URL"

COUNTRIES="${MINING_COUNTRIES:?Set MINING_COUNTRIES}"
OUTPUT_DIR="${MINING_OUTPUT_DIR:-/data/web_news_rss}"
MAX_URLS="${MINING_MAX_URLS_PER_COUNTRY:-500}"
BATCH="${MINING_BATCH_SIZE:-100}"

CURRENT_YEAR=$(date +%Y)
START_YEAR="${MINING_START_YEAR:-$CURRENT_YEAR}"
END_YEAR="${MINING_END_YEAR:-$CURRENT_YEAR}"

ARGS=(
    "mine-and-ingest"
    "--countries" "$COUNTRIES"
    "--output-dir" "$OUTPUT_DIR"
    "--max-urls-per-country" "$MAX_URLS"
    "--batch-size" "$BATCH"
    "--upsert-batch-size" "${INGESTION_UPSERT_BATCH_SIZE:-200}"
)

if [ -n "${MINING_START_DATE:-}" ] && [ -n "${MINING_END_DATE:-}" ]; then
    ARGS+=("--start-date" "$MINING_START_DATE" "--end-date" "$MINING_END_DATE")
else
    ARGS+=("--start-year" "$START_YEAR" "--end-year" "$END_YEAR")
fi

if [ -n "${MINING_FEEDS_PATH:-}" ]; then
    ARGS+=("--feeds" "$MINING_FEEDS_PATH")
fi

if [ "${MINING_GOOGLE_DAILY_SLICE:-0}" = "1" ]; then
    ARGS+=("--google-news-daily-slice")
fi

if [ "${INGESTION_RESET:-0}" = "1" ]; then
    ARGS+=("--reset")
fi

echo "Countries:  $COUNTRIES"
echo "Window:     ${MINING_START_DATE:-$START_YEAR} – ${MINING_END_DATE:-$END_YEAR}"
echo "Output dir: $OUTPUT_DIR"
echo "====================================="

exec python -m ml.rag.ingestion.cli "${ARGS[@]}"
