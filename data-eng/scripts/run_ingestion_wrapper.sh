#!/bin/bash
# Wrapper script for running ingestion scripts from cron
# Usage: run_ingestion_wrapper.sh <script_name> [args...]
# Example: run_ingestion_wrapper.sh fews_net_ingestion.py --countries KE NG

set -e

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
INGESTION_DIR="$PROJECT_ROOT/data-pipelines/ingestion"

# Check if script name is provided
if [ $# -lt 1 ]; then
    echo "Error: No script name provided"
    echo "Usage: $0 <script_name> [args...]"
    exit 1
fi

SCRIPT_NAME="$1"
shift  # Remove script name from arguments

# Full path to the ingestion script
SCRIPT_PATH="$INGESTION_DIR/$SCRIPT_NAME"

# Check if script exists
if [ ! -f "$SCRIPT_PATH" ]; then
    echo "Error: Script not found: $SCRIPT_PATH"
    exit 1
fi

# Log start time
echo "========================================="
echo "Starting ingestion: $SCRIPT_NAME"
echo "Time: $(date '+%Y-%m-%d %H:%M:%S')"
echo "Arguments: $@"
echo "========================================="

# Activate virtual environment if it exists
if [ -d "$PROJECT_ROOT/venv" ]; then
    echo "Activating virtual environment..."
    source "$PROJECT_ROOT/venv/bin/activate"
elif [ -d "$PROJECT_ROOT/.venv" ]; then
    echo "Activating virtual environment..."
    source "$PROJECT_ROOT/.venv/bin/activate"
fi

# Set PYTHONPATH
export PYTHONPATH="$PROJECT_ROOT:$PYTHONPATH"

# Set Google credentials if not already set
if [ -z "$GOOGLE_APPLICATION_CREDENTIALS" ]; then
    if [ -f "$PROJECT_ROOT/config/credentials.json" ]; then
        export GOOGLE_APPLICATION_CREDENTIALS="$PROJECT_ROOT/config/credentials.json"
    fi
fi

# Run the ingestion script
echo "Executing: python3 $SCRIPT_PATH $@"
echo ""

START_TIME=$(date +%s)

# Run script and capture exit code
python3 "$SCRIPT_PATH" "$@"
EXIT_CODE=$?

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

# Log completion
echo ""
echo "========================================="
if [ $EXIT_CODE -eq 0 ]; then
    echo "Status: SUCCESS"
else
    echo "Status: FAILED (exit code: $EXIT_CODE)"
fi
echo "Duration: ${DURATION}s"
echo "Completed: $(date '+%Y-%m-%d %H:%M:%S')"
echo "========================================="

exit $EXIT_CODE
