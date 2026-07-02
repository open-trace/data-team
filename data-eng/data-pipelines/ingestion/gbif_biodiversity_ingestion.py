#!/usr/bin/env python3
"""
GBIF Biodiversity Ingestion Script

Fetches species occurrence data from GBIF API and loads into BigQuery.
Supports multiple African countries and taxonomic groups.

Usage:
    python gbif_biodiversity_ingestion.py --countries KE NG ET
    python gbif_biodiversity_ingestion.py --region east_africa
    python gbif_biodiversity_ingestion.py --all-countries --limit 1000
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


class GBIFBiodiversityIngestion:
    """GBIF Biodiversity data ingestion handler."""
    
    def __init__(self):
        """Initialize GBIF Biodiversity ingestion."""
        self.api_client = APIClient('gbif_biodiversity')
        self.bq_client = BigQueryClient()
        self.endpoints = self.api_client.config['endpoints']
    
    def fetch_occurrences(self, country_code: str, limit: int = 1000) -> List[Dict[str, Any]]:
        """Fetch species occurrence data for a country.
        
        Args:
            country_code: ISO alpha-2 country code
            limit: Maximum number of records to fetch
            
        Returns:
            List of data records
        """
        country_name = get_country_name(country_code)
        logger.info(f"Fetching biodiversity data for {country_name} ({country_code})")
        
        try:
            params = {
                'country': country_code,
                'hasCoordinate': 'true',
                'hasGeospatialIssue': 'false',
                'limit': min(limit, 300)  # GBIF max per page
            }
            
            # Fetch data with pagination
            data = self.api_client.paginate(
                self.endpoints['occurrence'],
                params=params,
                page_param='offset',
                max_pages=limit // 300 + 1
            )
            
            # Add metadata
            for record in data:
                record['ingestion_timestamp'] = datetime.utcnow().isoformat()
                record['source'] = 'gbif'
                record['country_code'] = country_code
            
            logger.info(f"Fetched {len(data)} occurrence records for {country_code}")
            return data[:limit]  # Ensure we don't exceed limit
            
        except Exception as e:
            logger.error(f"Failed to fetch biodiversity data for {country_code}: {e}")
            return []
    
    def load_to_bigquery(self, data: List[Dict[str, Any]]) -> None:
        """Load data to BigQuery.
        
        Args:
            data: List of data records
        """
        if not data:
            logger.warning("No data to load")
            return
        
        table_name = 'gbif_occurrence_search'
        
        try:
            self.bq_client.insert_rows(table_name, data)
            logger.info(f"Successfully loaded {len(data)} records to {table_name}")
        except Exception as e:
            logger.error(f"Failed to load data to BigQuery: {e}")
            raise
    
    def run(self, country_codes: List[str], limit: int = 1000) -> None:
        """Run the ingestion process.
        
        Args:
            country_codes: List of ISO alpha-2 country codes
            limit: Maximum records per country
        """
        logger.info(f"Starting GBIF Biodiversity ingestion for {len(country_codes)} countries")
        logger.info(f"Limit per country: {limit}")
        logger.info(f"Countries: {', '.join(country_codes)}")
        
        all_data = []
        for country_code in country_codes:
            data = self.fetch_occurrences(country_code, limit)
            all_data.extend(data)
        
        # Load to BigQuery
        if all_data:
            self.load_to_bigquery(all_data)
            logger.info(f"Ingestion complete: {len(all_data)} records loaded")
        else:
            logger.warning("No data fetched, nothing to load")


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='GBIF Biodiversity data ingestion')
    
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
    
    # Limit
    parser.add_argument(
        '--limit',
        type=int,
        default=1000,
        help='Maximum records per country (default: 1000)'
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
        ingestion = GBIFBiodiversityIngestion()
        ingestion.run(country_codes, args.limit)
        logger.info("Ingestion completed successfully")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Ingestion failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
