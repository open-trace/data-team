#!/usr/bin/env python3
"""
FEWS NET Data Ingestion Script

Fetches food security data from FEWS NET API and loads into BigQuery.
Supports multiple African countries and data types (IPC phase, market prices, food security).

Usage:
    python fews_net_ingestion.py --countries KE NG ET --data-type ipc_phase
    python fews_net_ingestion.py --region east_africa --data-type market_prices
    python fews_net_ingestion.py --all-countries --data-type food_security
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
    get_country_name
)


logger = logging.getLogger(__name__)


class FEWSNETIngestion:
    """FEWS NET data ingestion handler."""
    
    def __init__(self):
        """Initialize FEWS NET ingestion."""
        self.api_client = APIClient('fews_net')
        self.bq_client = BigQueryClient()
        self.endpoints = self.api_client.config['endpoints']
    
    def fetch_ipc_phase_data(self, country_codes: List[str]) -> List[Dict[str, Any]]:
        """Fetch IPC phase classification data.
        
        Args:
            country_codes: List of ISO alpha-2 country codes
            
        Returns:
            List of data records
        """
        all_data = []
        
        for country_code in country_codes:
            country_name = get_country_name(country_code)
            logger.info(f"Fetching IPC phase data for {country_name} ({country_code})")
            
            try:
                params = {
                    'country_code': country_code,
                    'format': 'json'
                }
                
                data = self.api_client.paginate(
                    self.endpoints['ipc_phase'],
                    params=params,
                    page_param='page',
                    max_pages=100
                )
                
                # Add metadata
                for record in data:
                    record['ingestion_timestamp'] = datetime.utcnow().isoformat()
                    record['source'] = 'fews_net'
                    record['data_type'] = 'ipc_phase'
                
                all_data.extend(data)
                logger.info(f"Fetched {len(data)} IPC phase records for {country_code}")
                
            except Exception as e:
                logger.error(f"Failed to fetch IPC phase data for {country_code}: {e}")
                continue
        
        return all_data
    
    def fetch_market_prices(self, country_codes: List[str]) -> List[Dict[str, Any]]:
        """Fetch market price data.
        
        Args:
            country_codes: List of ISO alpha-2 country codes
            
        Returns:
            List of data records
        """
        all_data = []
        
        for country_code in country_codes:
            country_name = get_country_name(country_code)
            logger.info(f"Fetching market prices for {country_name} ({country_code})")
            
            try:
                params = {
                    'country_code': country_code,
                    'format': 'json'
                }
                
                data = self.api_client.paginate(
                    self.endpoints['market_prices'],
                    params=params,
                    page_param='page',
                    max_pages=100
                )
                
                # Add metadata
                for record in data:
                    record['ingestion_timestamp'] = datetime.utcnow().isoformat()
                    record['source'] = 'fews_net'
                    record['data_type'] = 'market_prices'
                
                all_data.extend(data)
                logger.info(f"Fetched {len(data)} market price records for {country_code}")
                
            except Exception as e:
                logger.error(f"Failed to fetch market prices for {country_code}: {e}")
                continue
        
        return all_data
    
    def fetch_food_security(self, country_codes: List[str]) -> List[Dict[str, Any]]:
        """Fetch food security classification data.
        
        Args:
            country_codes: List of ISO alpha-2 country codes
            
        Returns:
            List of data records
        """
        all_data = []
        
        for country_code in country_codes:
            country_name = get_country_name(country_code)
            logger.info(f"Fetching food security data for {country_name} ({country_code})")
            
            try:
                params = {
                    'country_code': country_code,
                    'format': 'json'
                }
                
                data = self.api_client.paginate(
                    self.endpoints['food_security'],
                    params=params,
                    page_param='page',
                    max_pages=100
                )
                
                # Add metadata
                for record in data:
                    record['ingestion_timestamp'] = datetime.utcnow().isoformat()
                    record['source'] = 'fews_net'
                    record['data_type'] = 'food_security'
                
                all_data.extend(data)
                logger.info(f"Fetched {len(data)} food security records for {country_code}")
                
            except Exception as e:
                logger.error(f"Failed to fetch food security data for {country_code}: {e}")
                continue
        
        return all_data
    
    def load_to_bigquery(self, data: List[Dict[str, Any]], table_name: str) -> None:
        """Load data to BigQuery.
        
        Args:
            data: List of data records
            table_name: Target table name
        """
        if not data:
            logger.warning("No data to load")
            return
        
        try:
            self.bq_client.insert_rows(table_name, data)
            logger.info(f"Successfully loaded {len(data)} records to {table_name}")
        except Exception as e:
            logger.error(f"Failed to load data to BigQuery: {e}")
            raise
    
    def run(self, country_codes: List[str], data_type: str) -> None:
        """Run the ingestion process.
        
        Args:
            country_codes: List of ISO alpha-2 country codes
            data_type: Type of data to fetch (ipc_phase, market_prices, food_security)
        """
        logger.info(f"Starting FEWS NET ingestion for {len(country_codes)} countries")
        logger.info(f"Data type: {data_type}")
        logger.info(f"Countries: {', '.join(country_codes)}")
        
        # Fetch data based on type
        if data_type == 'ipc_phase':
            data = self.fetch_ipc_phase_data(country_codes)
            table_name = 'FEWS_NET_ipc_phase_data'
        elif data_type == 'market_prices':
            data = self.fetch_market_prices(country_codes)
            table_name = 'FEWS_NET_market_prices_data'
        elif data_type == 'food_security':
            data = self.fetch_food_security(country_codes)
            table_name = 'FEWS_NET_food_security_data'
        else:
            raise ValueError(f"Invalid data type: {data_type}")
        
        # Load to BigQuery
        if data:
            self.load_to_bigquery(data, table_name)
            logger.info(f"Ingestion complete: {len(data)} records loaded")
        else:
            logger.warning("No data fetched, nothing to load")


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='FEWS NET data ingestion')
    
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
    
    # Data type
    parser.add_argument(
        '--data-type',
        required=True,
        choices=['ipc_phase', 'market_prices', 'food_security'],
        help='Type of data to fetch'
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
        ingestion = FEWSNETIngestion()
        ingestion.run(country_codes, args.data_type)
        logger.info("Ingestion completed successfully")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Ingestion failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
