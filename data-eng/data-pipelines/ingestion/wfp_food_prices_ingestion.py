#!/usr/bin/env python3
"""
WFP Food Prices Ingestion Script

Fetches global food price data from WFP VAMPIRE Tool and loads into BigQuery.
Supports multiple African countries and commodity types.

Usage:
    python wfp_food_prices_ingestion.py --countries KE NG ET
    python wfp_food_prices_ingestion.py --region west_africa
    python wfp_food_prices_ingestion.py --all-countries
"""
import argparse
import logging
from datetime import datetime
from typing import List, Dict, Any
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from common.api_helpers import APIClient, setup_logging
from common.bigquery_client import BigQueryClient
from common.country_config import (
    get_all_country_codes,
    get_countries_by_region,
    validate_country_code,
    get_country_name,
    get_country_by_alpha2
)


logger = logging.getLogger(__name__)


class WFPFoodPricesIngestion:
    """WFP Food Prices data ingestion handler."""
    
    def __init__(self):
        """Initialize WFP Food Prices ingestion."""
        self.api_client = APIClient('wfp_food_prices')
        self.bq_client = BigQueryClient()
        self.resource_id = self.api_client.config.get('resource_id')
    
    def fetch_food_prices(self, country_codes: List[str]) -> List[Dict[str, Any]]:
        """Fetch food price data for specified countries.
        
        Args:
            country_codes: List of ISO alpha-2 country codes
            
        Returns:
            List of data records
        """
        all_data = []
        
        for country_code in country_codes:
            country = get_country_by_alpha2(country_code)
            if not country:
                logger.warning(f"Country not found: {country_code}")
                continue
            
            country_name = country['name']
            logger.info(f"Fetching food prices for {country_name} ({country_code})")
            
            try:
                # WFP API uses country names, not codes
                params = {
                    'resource_id': self.resource_id,
                    'q': country_name,
                    'limit': 1000
                }
                
                endpoint = self.api_client.config['endpoints']['food_prices']
                
                # Fetch data with pagination
                data = self.api_client.paginate(
                    endpoint,
                    params=params,
                    page_param='offset',
                    max_pages=50
                )
                
                # Filter and add metadata
                filtered_data = []
                for record in data:
                    # Verify record is for the correct country
                    if record.get('adm0_name', '').lower() == country_name.lower():
                        record['ingestion_timestamp'] = datetime.utcnow().isoformat()
                        record['source'] = 'wfp_vampire'
                        record['country_code'] = country_code
                        filtered_data.append(record)
                
                all_data.extend(filtered_data)
                logger.info(f"Fetched {len(filtered_data)} food price records for {country_code}")
                
            except Exception as e:
                logger.error(f"Failed to fetch food prices for {country_code}: {e}")
                continue
        
        return all_data
    
    def load_to_bigquery(self, data: List[Dict[str, Any]]) -> None:
        """Load data to BigQuery.
        
        Args:
            data: List of data records
        """
        if not data:
            logger.warning("No data to load")
            return
        
        table_name = 'WFP_VAMPIRE_Tool_global_food_prices'
        
        try:
            self.bq_client.insert_rows(table_name, data)
            logger.info(f"Successfully loaded {len(data)} records to {table_name}")
        except Exception as e:
            logger.error(f"Failed to load data to BigQuery: {e}")
            raise
    
    def run(self, country_codes: List[str]) -> None:
        """Run the ingestion process.
        
        Args:
            country_codes: List of ISO alpha-2 country codes
        """
        logger.info(f"Starting WFP Food Prices ingestion for {len(country_codes)} countries")
        logger.info(f"Countries: {', '.join(country_codes)}")
        
        # Fetch data
        data = self.fetch_food_prices(country_codes)
        
        # Load to BigQuery
        if data:
            self.load_to_bigquery(data)
            logger.info(f"Ingestion complete: {len(data)} records loaded")
        else:
            logger.warning("No data fetched, nothing to load")


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='WFP Food Prices data ingestion')
    
    # Country selection (mutually exclusive)
    country_group = parser.add_mutually_exclusive_group(required=True)
    country_group.add_argument(
        '--countries',
        nargs='+',
        help='List of country codes (e.g., KE NG ET)'
    )
    country_group.add_argument(
        '--region',
        choices=['east_africa', 'west_africa', 'central_africa', 'north_africa', 'southern_africa'],
        help='African region'
    )
    country_group.add_argument(
        '--all-countries',
        action='store_true',
        help='Process all African countries'
    )
    
    # Logging
    parser.add_argument(
        '--log-level',
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        help='Logging level'
    )
    
    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_args()
    
    # Set up logging
    setup_logging(args.log_level)
    
    # Determine country codes
    if args.countries:
        country_codes = [c.upper() for c in args.countries]
        # Validate country codes
        invalid = [c for c in country_codes if not validate_country_code(c)]
        if invalid:
            logger.error(f"Invalid country codes: {', '.join(invalid)}")
            sys.exit(1)
    elif args.region:
        country_codes = get_countries_by_region(args.region)
    else:  # all-countries
        country_codes = get_all_country_codes('alpha2')
    
    logger.info(f"Processing {len(country_codes)} countries")
    
    # Run ingestion
    try:
        ingestion = WFPFoodPricesIngestion()
        ingestion.run(country_codes)
        logger.info("Ingestion completed successfully")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Ingestion failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
