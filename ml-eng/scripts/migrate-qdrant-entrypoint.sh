#!/bin/bash
# Qdrant Cloud -> self-managed Qdrant snapshot migration entrypoint.
#
# Required env:
#   QDRANT_OLD_URL, QDRANT_OLD_API_KEY   — source (Qdrant Cloud, public)
#   QDRANT_URL, QDRANT_API_KEY           — target (self-managed Railway Qdrant,
#                                           reach it via QDRANT_URL=http://qdrant.railway.internal:6333
#                                           since this runs as a Railway service in the same environment)
#
# Optional:
#   MIGRATE_COLLECTIONS   — comma-separated list (default: all six production collections)
#   MIGRATE_VERIFY_ONLY   — "1" to only compare point counts, no transfer

set -euo pipefail

echo "=== Qdrant Migration Entrypoint ==="

for var in QDRANT_OLD_URL QDRANT_OLD_API_KEY QDRANT_URL QDRANT_API_KEY; do
    if [ -z "${!var:-}" ]; then
        echo "✗ $var not set"
        exit 1
    fi
done
echo "✓ source: $QDRANT_OLD_URL"
echo "✓ target: $QDRANT_URL"

ARGS=()
if [ -n "${MIGRATE_COLLECTIONS:-}" ]; then
    ARGS+=("--collections" "$MIGRATE_COLLECTIONS")
fi
if [ "${MIGRATE_VERIFY_ONLY:-0}" = "1" ]; then
    ARGS+=("--verify-only")
fi

echo "====================================="

exec python -m ml.rag.scripts.migrate_qdrant_cloud "${ARGS[@]}"
