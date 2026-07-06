# Data Ingestion Scripts

Python scripts for ingesting data from various sources into BigQuery. All scripts use direct API access with rate limiting, retry logic, and cron-based scheduling.

## Overview

This directory contains production-ready ingestion scripts that fetch data from external APIs and load it directly into BigQuery's `landing` dataset. No Airflow or Airbyte required - just Python + cron jobs.

## Architecture

```
External APIs → Python Scripts → BigQuery (landing dataset)
                     ↓
              Cron Jobs (scheduled)
```

## Available Scripts

### 1. **fews_net_ingestion.py** - Food Security Data
Fetches data from FEWS NET (Famine Early Warning Systems Network).

**Data Types:**
- IPC phase classifications
- Market prices
- Food security classifications

**Usage:**
```bash
# Fetch IPC phase data for specific countries
python fews_net_ingestion.py --countries KE NG ET --data-type ipc_phase

# Fetch market prices for East Africa
python fews_net_ingestion.py --region east_africa --data-type market_prices

# Fetch food security data for all African countries
python fews_net_ingestion.py --all-countries --data-type food_security
```

### 2. **wfp_food_prices_ingestion.py** - Market Prices
Fetches global food price data from WFP VAMPIRE Tool.

**Usage:**
```bash
# Fetch food prices for specific countries
python wfp_food_prices_ingestion.py --countries KE NG ET

# Fetch for West Africa
python wfp_food_prices_ingestion.py --region west_africa

# Fetch for all African countries
python wfp_food_prices_ingestion.py --all-countries
```

### 3. **isric_soil_ingestion.py** - Soil Properties
Fetches soil property data from ISRIC SoilGrids using coordinate-based sampling.

**Usage:**
```bash
# Fetch soil data for specific countries (0.5 degree grid)
python isric_soil_ingestion.py --countries KE NG ET --grid-resolution 0.5

# Fetch for Southern Africa (1.0 degree grid for faster processing)
python isric_soil_ingestion.py --region southern_africa --grid-resolution 1.0
```

**Note:** Uses bounding boxes and grid sampling. Only countries with defined bounds are supported.

### 4. **gbif_biodiversity_ingestion.py** - Species Occurrence Data
Fetches species occurrence records from GBIF (Global Biodiversity Information Facility).

**Usage:**
```bash
# Fetch 1000 records per country
python gbif_biodiversity_ingestion.py --countries KE NG ET --limit 1000

# Fetch 5000 records for East Africa
python gbif_biodiversity_ingestion.py --region east_africa --limit 5000
```

### 5. **fao_crop_ingestion.py** - Crop Production Statistics
Fetches crop production and agricultural statistics from FAO STAT.

**Domains:**
- `QC` - Crops and livestock products
- `QCL` - Crops and livestock products (legacy)
- `RL` - Land use
- `RFN` - Fertilizers
- `RP` - Pesticides
- `TI` - Trade indices
- `TCL` - Trade crops and livestock

**Usage:**
```bash
# Fetch crop production data (QC domain)
python fao_crop_ingestion.py --countries KE NG ET --domain QC

# Fetch land use data for West Africa
python fao_crop_ingestion.py --region west_africa --domain RL

# Fetch all domains for all countries (use with caution - large dataset)
python fao_crop_ingestion.py --all-countries --domain QC
```

## Common Options

All scripts support these common options:

### Country Selection (mutually exclusive)
- `--countries CODE [CODE ...]` - Specific country codes (e.g., KE NG ET)
- `--region REGION` - African region (east_africa, west_africa, central_africa, north_africa, southern_africa)
- `--all-countries` - All 54 African countries

### Logging
- `--log-level LEVEL` - Logging level (DEBUG, INFO, WARNING, ERROR)

## Configuration

### Environment Variables
```bash
# Required
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json
export BQ_PROJECT=opentrace-prod-5ga4

# Optional
export PYTHONPATH=/path/to/data-team/data-eng
```

### Configuration Files
- `config/africa_countries.yaml` - All 54 African countries with ISO codes
- `config/ingestion_sources.yaml` - API endpoints, rate limits, authentication
- `config/priority_sources_africa.yaml` - Priority sources and target countries

## Cron Job Setup

### Installation
```bash
# Make scripts executable
chmod +x ../scripts/setup_cron_jobs.sh
chmod +x ../scripts/run_ingestion_wrapper.sh

# Install cron jobs
cd ../scripts
sudo bash setup_cron_jobs.sh
```

### Schedule
- **Daily (6:00-7:30 AM):** FEWS NET, WFP food prices
- **Weekly (Sunday 2:00-6:00 AM):** ISRIC soil, GBIF biodiversity
- **Monthly (1st, 1:00-3:00 AM):** FAO crop production

### View Installed Jobs
```bash
crontab -l
```

### Logs
All logs are written to `/var/log/data-ingestion/`:
```bash
# View recent logs
tail -f /var/log/data-ingestion/fews_net_ipc.log

# Check for errors
grep ERROR /var/log/data-ingestion/*.log
```

## Dependencies

### Python Packages
```bash
pip install google-cloud-bigquery requests pyyaml urllib3
```

### System Requirements
- Python 3.8+
- Google Cloud credentials with BigQuery write access
- Internet access to external APIs

## Common Utilities

### `common/country_config.py`
Country code utilities:
- Load Africa countries configuration
- Validate country codes
- Get countries by region
- Convert between ISO alpha-2 and alpha-3

### `common/bigquery_client.py`
BigQuery operations:
- Table creation and management
- Row insertion
- DataFrame loading
- Query execution

### `common/api_helpers.py`
API utilities:
- Rate limiting (per-minute and per-hour)
- Automatic retry logic
- Pagination support
- Request/response handling

## Troubleshooting

### Authentication Errors
```bash
# Verify credentials
gcloud auth application-default login

# Or set service account key
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
```

### Rate Limiting
Scripts automatically handle rate limits. If you see "Rate limit: sleeping" messages, this is normal behavior.

### Missing Data
- Check API availability (some sources may have downtime)
- Verify country codes are valid
- Check logs for specific error messages

### BigQuery Errors
- Ensure dataset `landing` exists
- Verify write permissions
- Check table schemas match data structure

## Testing

### Manual Test
```bash
# Test with a single country
python fews_net_ingestion.py --countries KE --data-type ipc_phase --log-level DEBUG

# Dry run (check without loading)
# Add --dry-run flag if implemented
```

### Validate Output
```sql
-- Check recent ingestion
SELECT 
  source,
  country_code,
  COUNT(*) as record_count,
  MAX(ingestion_timestamp) as latest_ingestion
FROM `opentrace-prod-5ga4.landing.FEWS_NET_ipc_phase_data`
WHERE DATE(ingestion_timestamp) = CURRENT_DATE()
GROUP BY source, country_code
ORDER BY latest_ingestion DESC;
```

## Best Practices

1. **Start Small:** Test with `--countries` before using `--all-countries`
2. **Monitor Logs:** Check logs regularly for errors
3. **Rate Limits:** Respect API rate limits (scripts handle this automatically)
4. **Incremental:** Run scripts incrementally rather than all at once
5. **Backup:** Keep backups of BigQuery data before major changes

## Support

For issues or questions:
1. Check logs in `/var/log/data-ingestion/`
2. Review configuration files in `config/`
3. Test scripts manually with `--log-level DEBUG`
4. Check IMPLEMENTATION_TRACKER.md for known issues

## Future Enhancements

- [ ] Add `--dry-run` mode for testing
- [ ] Implement incremental ingestion (avoid full reloads)
- [ ] Add email/Slack notifications for failures
- [ ] Create monitoring dashboard
- [ ] Add data quality checks
- [ ] Implement deduplication logic
