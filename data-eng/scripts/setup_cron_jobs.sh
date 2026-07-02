#!/bin/bash
# Setup script for installing data ingestion cron jobs
# Usage: sudo bash setup_cron_jobs.sh

set -e

echo "========================================="
echo "Data Ingestion Cron Jobs Setup"
echo "========================================="

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
CRONTAB_FILE="$PROJECT_ROOT/config/crontab.txt"

# Check if crontab file exists
if [ ! -f "$CRONTAB_FILE" ]; then
    echo "Error: crontab.txt not found at $CRONTAB_FILE"
    exit 1
fi

echo "Project root: $PROJECT_ROOT"
echo "Crontab file: $CRONTAB_FILE"

# Create log directory
LOG_DIR="/var/log/data-ingestion"
echo "Creating log directory: $LOG_DIR"
sudo mkdir -p "$LOG_DIR"
sudo chmod 755 "$LOG_DIR"

# Set ownership to current user
CURRENT_USER=$(whoami)
sudo chown -R "$CURRENT_USER:$CURRENT_USER" "$LOG_DIR"
echo "Log directory created and owned by $CURRENT_USER"

# Update paths in crontab file
echo "Updating paths in crontab configuration..."
TEMP_CRONTAB=$(mktemp)
sed "s|/path/to/data-team|$PROJECT_ROOT/..|g" "$CRONTAB_FILE" > "$TEMP_CRONTAB"

# Prompt for Google credentials path
echo ""
echo "Enter the full path to your Google Cloud credentials JSON file:"
read -r CREDENTIALS_PATH

if [ ! -f "$CREDENTIALS_PATH" ]; then
    echo "Warning: Credentials file not found at $CREDENTIALS_PATH"
    echo "You can update this later in your crontab"
fi

# Update credentials path
sed -i "s|/path/to/credentials.json|$CREDENTIALS_PATH|g" "$TEMP_CRONTAB"

# Show the crontab that will be installed
echo ""
echo "========================================="
echo "Crontab to be installed:"
echo "========================================="
cat "$TEMP_CRONTAB"
echo "========================================="
echo ""

# Ask for confirmation
read -p "Do you want to install these cron jobs? (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Installation cancelled"
    rm "$TEMP_CRONTAB"
    exit 0
fi

# Backup existing crontab
echo "Backing up existing crontab..."
crontab -l > "$HOME/crontab.backup.$(date +%Y%m%d_%H%M%S)" 2>/dev/null || true

# Install new crontab
echo "Installing cron jobs..."
crontab "$TEMP_CRONTAB"

# Clean up
rm "$TEMP_CRONTAB"

echo ""
echo "========================================="
echo "Installation Complete!"
echo "========================================="
echo "Cron jobs have been installed successfully."
echo ""
echo "To view installed cron jobs:"
echo "  crontab -l"
echo ""
echo "To edit cron jobs:"
echo "  crontab -e"
echo ""
echo "To remove all cron jobs:"
echo "  crontab -r"
echo ""
echo "Logs will be written to: $LOG_DIR"
echo ""
echo "Next steps:"
echo "1. Ensure Python dependencies are installed:"
echo "   pip install -r $PROJECT_ROOT/requirements.txt"
echo "2. Test ingestion scripts manually before relying on cron"
echo "3. Monitor logs in $LOG_DIR"
echo "========================================="
