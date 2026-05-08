#!/bin/sh
# Hugging Face Spaces / Docker: materialize GCP service account JSON from env secrets.
# Precedence: GCP_SA_JSON_B64, then GCP_SA_JSON. If neither is set, leave
# GOOGLE_APPLICATION_CREDENTIALS unchanged (e.g. Compose bind-mount to a key file).
set -e

if [ -n "${GCP_SA_JSON_B64:-}" ]; then
  printf '%s' "$GCP_SA_JSON_B64" | base64 -d > /tmp/gcp-sa.json
  chmod 600 /tmp/gcp-sa.json
  export GOOGLE_APPLICATION_CREDENTIALS=/tmp/gcp-sa.json
elif [ -n "${GCP_SA_JSON:-}" ]; then
  printf '%s' "$GCP_SA_JSON" > /tmp/gcp-sa.json
  chmod 600 /tmp/gcp-sa.json
  export GOOGLE_APPLICATION_CREDENTIALS=/tmp/gcp-sa.json
fi

exec "$@"
