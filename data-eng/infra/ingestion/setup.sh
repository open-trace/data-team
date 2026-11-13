#!/usr/bin/env bash
# Admin once: create ingest-dev SA, grant landing_dev write, create shared key OUTSIDE the repo.
# Run from anywhere; lives under data-eng/infra/ingestion/.
set -euo pipefail

PROJECT=opentrace-prod-5ga4
LOCATION=europe-west3
SA=ingest-dev
SA_EMAIL="$SA@$PROJECT.iam.gserviceaccount.com"
# Key must live outside the git repo. Override with KEY_OUT=/path/to/ingest-dev.json
KEY_OUT="${KEY_OUT:-${HOME}/.config/opentrace/ingest-dev.json}"

gcloud config set project "$PROJECT"
gcloud services enable iamcredentials.googleapis.com bigquery.googleapis.com

gcloud iam service-accounts describe "$SA_EMAIL" >/dev/null 2>&1 || \
  gcloud iam service-accounts create "$SA" --display-name="Ingestion (landing_dev only)"

bq --location="$LOCATION" show "$PROJECT:landing_dev" >/dev/null 2>&1 || \
  bq --location="$LOCATION" mk --dataset --description "Dev ingestion sandbox (expires)" \
     --default_partition_expiration 7776000 "$PROJECT:landing_dev"

gcloud projects add-iam-policy-binding "$PROJECT" \
  --member="serviceAccount:$SA_EMAIL" --role="roles/bigquery.jobUser" --condition=None

# dataset-level WRITER on landing_dev only (never grant write on landing here)
bq show --format=prettyjson "$PROJECT:landing_dev" > /tmp/ld.json
python3 - "$SA_EMAIL" <<'PY'
import json, sys
p = "/tmp/ld.json"
d = json.load(open(p))
e = sys.argv[1]
if not any(a.get("userByEmail") == e for a in d.get("access", [])):
    d.setdefault("access", []).append({"role": "WRITER", "userByEmail": e})
json.dump(d, open(p, "w"))
PY
bq update --source /tmp/ld.json "$PROJECT:landing_dev"

mkdir -p "$(dirname "$KEY_OUT")"
if [[ -f "$KEY_OUT" ]]; then
  echo "Key already exists at $KEY_OUT (not rotated). Delete it first to mint a new key."
else
  gcloud iam service-accounts keys create "$KEY_OUT" --iam-account="$SA_EMAIL"
  chmod 600 "$KEY_OUT" || true
  echo "Created key: $KEY_OUT"
fi

cat <<EOF

Done.
  SA:       $SA_EMAIL
  Dataset:  $PROJECT:landing_dev (WRITER) — ingest sandbox only
  Key:      $KEY_OUT
  Transform SoT remains dataset \`landing\` (dbt). Do not grant this SA write on landing.

Team: set GOOGLE_APPLICATION_CREDENTIALS=$KEY_OUT in data-eng/ingestion/.env
      then: cd data-eng && make ingest-auth

Rotate the shared key on departure or every ~90 days (admin-owned).
EOF
