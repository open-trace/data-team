#!/bin/bash
# Ingestion worker entrypoint.
# Decodes GDrive OAuth token + GCP SA credentials, then runs the ingestion CLI.
#
# Required env:
#   GDRIVE_TOKEN_BASE64            — base64 of gdrive_token.json (pre-authorised OAuth token)
#   GDRIVE_OAUTH_CLIENT_SECRET_JSON — path inside container to client_secret.json
#                                     OR GDRIVE_CLIENT_SECRET_BASE64 (base64 of the file)
#   QDRANT_URL, QDRANT_API_KEY
#
# Optional:
#   GOOGLE_APPLICATION_CREDENTIALS_BASE64 — GCP SA for BQ (not needed for ingestion-only)
#   INGESTION_KIND    — which corpus to rebuild: news|research|ota|all  (default: all)
#   INGESTION_RESET   — set to "1" to delete+recreate collections before upserting

set -euo pipefail

GDRIVE_TOKEN_PATH="${GDRIVE_TOKEN_PATH:-/tmp/gdrive_token.json}"
GCP_SA_PATH="/tmp/gcp-sa-key.json"

echo "=== Ingestion Worker Entrypoint ==="

# --- GDrive OAuth token ---
if [ -n "${GDRIVE_TOKEN_BASE64:-}" ]; then
    echo "Decoding GDRIVE_TOKEN_BASE64..."
    if ! printf '%s' "$GDRIVE_TOKEN_BASE64" | base64 -d > "$GDRIVE_TOKEN_PATH" 2>/dev/null; then
        echo "✗ Failed to decode GDRIVE_TOKEN_BASE64"
        exit 1
    fi
    chmod 600 "$GDRIVE_TOKEN_PATH"
    if ! python -c "import json,sys; json.load(open(sys.argv[1]))" "$GDRIVE_TOKEN_PATH"; then
        echo "✗ GDRIVE_TOKEN_BASE64 did not decode to valid JSON"
        exit 1
    fi
    export GDRIVE_TOKEN_PATH
    echo "✓ GDrive token decoded to $GDRIVE_TOKEN_PATH"
else
    echo "✗ GDRIVE_TOKEN_BASE64 not set — cannot authenticate with Google Drive"
    exit 1
fi

# --- GDrive client secret (optional base64 form) ---
CLIENT_SECRET_PATH="${GDRIVE_OAUTH_CLIENT_SECRET_JSON:-}"
if [ -z "$CLIENT_SECRET_PATH" ] && [ -n "${GDRIVE_CLIENT_SECRET_BASE64:-}" ]; then
    CLIENT_SECRET_PATH="/tmp/gdrive_client_secret.json"
    echo "Decoding GDRIVE_CLIENT_SECRET_BASE64..."
    if ! printf '%s' "$GDRIVE_CLIENT_SECRET_BASE64" | base64 -d > "$CLIENT_SECRET_PATH" 2>/dev/null; then
        echo "✗ Failed to decode GDRIVE_CLIENT_SECRET_BASE64"
        exit 1
    fi
    chmod 600 "$CLIENT_SECRET_PATH"
fi
if [ -z "${CLIENT_SECRET_PATH:-}" ]; then
    echo "✗ Set GDRIVE_OAUTH_CLIENT_SECRET_JSON (path) or GDRIVE_CLIENT_SECRET_BASE64"
    exit 1
fi
export GDRIVE_OAUTH_CLIENT_SECRET_JSON="$CLIENT_SECRET_PATH"
echo "✓ GDrive client secret at $CLIENT_SECRET_PATH"

# --- GCP SA (optional — only needed if BQ_PROJECT is set) ---
if [ -n "${GOOGLE_APPLICATION_CREDENTIALS_BASE64:-}" ]; then
    echo "Decoding GOOGLE_APPLICATION_CREDENTIALS_BASE64..."
    if ! printf '%s' "$GOOGLE_APPLICATION_CREDENTIALS_BASE64" | base64 -d > "$GCP_SA_PATH" 2>/dev/null; then
        echo "✗ Failed to decode GOOGLE_APPLICATION_CREDENTIALS_BASE64"
        exit 1
    fi
    chmod 600 "$GCP_SA_PATH"
    export GOOGLE_APPLICATION_CREDENTIALS="$GCP_SA_PATH"
    echo "✓ GCP SA decoded to $GCP_SA_PATH"
fi

# --- Qdrant ---
if [ -z "${QDRANT_URL:-}" ]; then
    echo "✗ QDRANT_URL not set"
    exit 1
fi
echo "✓ QDRANT_URL=$QDRANT_URL"

KIND="${INGESTION_KIND:-all}"
RESET_FLAG=""
if [ "${INGESTION_RESET:-0}" = "1" ]; then
    RESET_FLAG="--reset"
fi

echo "==================================="
echo "Running: rebuild --kind $KIND $RESET_FLAG"
exec python -m ml.rag.ingestion.cli rebuild --kind "$KIND" $RESET_FLAG
