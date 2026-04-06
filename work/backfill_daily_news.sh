#!/usr/bin/env bash
set -euo pipefail

# Backfill daily Google News slices month-by-month and save logs per month.
# Usage:
#   bash work/backfill_daily_news.sh
#   COUNTRY=Ghana START_YEAR=2020 END_YEAR=2024 bash work/backfill_daily_news.sh
#   RESUME_ONLY_FAILED=1 bash work/backfill_daily_news.sh

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

COUNTRY="${COUNTRY:-Nigeria}"
START_YEAR="${START_YEAR:-2015}"
END_YEAR="${END_YEAR:-2024}"
DELAY="${DELAY:-2.0}"

# If 1, skip months that already have a success marker in their log.
RESUME_ONLY_FAILED="${RESUME_ONLY_FAILED:-1}"
LOG_DIR="${LOG_DIR:-work/logs/news_backfill}"
mkdir -p "$LOG_DIR"

echo "Backfill settings:"
echo "  ROOT_DIR=$ROOT_DIR"
echo "  COUNTRY=$COUNTRY"
echo "  START_YEAR=$START_YEAR"
echo "  END_YEAR=$END_YEAR"
echo "  DELAY=$DELAY"
echo "  RESUME_ONLY_FAILED=$RESUME_ONLY_FAILED"
echo "  LOG_DIR=$LOG_DIR"
echo

run_month() {
  local y="$1"
  local m="$2"
  local start
  local end
  local next_m
  local next_y
  local end_epoch
  local log_path

  start=$(printf "%04d-%02d-01" "$y" "$m")

  # Compute last day of the month in a macOS-compatible way.
  if [ "$m" -eq 12 ]; then
    next_y=$((y + 1))
    next_m=1
  else
    next_y=$y
    next_m=$((m + 1))
  fi
  end_epoch=$(date -j -f "%Y-%m-%d" "$(printf "%04d-%02d-01" "$next_y" "$next_m")" "+%s")
  end_epoch=$((end_epoch - 86400))
  end=$(date -j -r "$end_epoch" "+%Y-%m-%d")

  log_path="$LOG_DIR/$(printf "%04d-%02d" "$y" "$m").log"
  if [ "$RESUME_ONLY_FAILED" = "1" ] && [ -f "$log_path" ] && rg -q "RUN_STATUS=SUCCESS" "$log_path"; then
    echo "SKIP $(printf "%04d-%02d" "$y" "$m") (already successful)"
    return 0
  fi

  echo "=== Running $start .. $end ==="
  {
    echo "RUN_START=$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
    echo "COUNTRY=$COUNTRY"
    echo "WINDOW_START=$start"
    echo "WINDOW_END=$end"
    PYTHONPATH=. python -m ml.web_data_mining.run_pipeline \
      --countries "$COUNTRY" \
      --start-date "$start" \
      --end-date "$end" \
      --google-news-daily-slice \
      --google-slice-delay "$DELAY" \
      --discovery-mode hybrid \
      --tavily-enrich \
      --pipeline-langgraph \
      --allow-tavily-search-snippets \
      --tavily-discovery-min-domain-score 0
    echo "RUN_STATUS=SUCCESS"
    echo "RUN_END=$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  } 2>&1 | tee "$log_path"
}

for year in $(seq "$START_YEAR" "$END_YEAR"); do
  for month in $(seq 1 12); do
    if ! run_month "$year" "$month"; then
      echo "Month failed: $(printf "%04d-%02d" "$year" "$month")"
      echo "See log: $LOG_DIR/$(printf "%04d-%02d" "$year" "$month").log"
      exit 1
    fi
  done
done

echo
echo "Backfill complete."
echo "Logs: $LOG_DIR"
