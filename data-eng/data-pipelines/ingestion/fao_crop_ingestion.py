#!/usr/bin/env python3
"""
FAO Crop Production Ingestion Script

Fetches crop production statistics from FAO STAT API and loads into BigQuery.
Supports multiple African countries and crop types.

Usage:
    python fao_crop_ingestion.py --countries KE NG ET --domain QC
    python fao_crop_ingestion.py --region west_africa --domain QCL
    python fao_crop_ingestion.py --all-countries --domain RL
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


class FAOCropIngestion:
    """FAO Crop Production data ingestion handler."""
    
    def __init__(self):
        """Initialize FAO Crop ingestion."""
        self.api_client = APIClient('fao_crop_production')
        self.bq_client = BigQueryClient()
        self.endpoints = self.api_client.config['endpoints']
        self.domains = self.api_client.config.get('domains', [])
    
    def fetch_crop_data(self, country_codes: List[str], domain: str = 'QC') -> List[Dict[str, Any]]:
        """Fetch crop production data for specified countries.
        
        Args:
            country_codes: List of ISO alpha-2 country codes
            domain: FAO domain code (QC, QCL, RL, RFN, RP, TI, TCL)
            
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
            country_alpha3 = country['alpha3']
            
            logger.info(f"Fetching FAO {domain} data for {country_name} ({country_code})")
            
            try:
                params = {
                    'area': country_alpha3,  # FAO uses alpha-3 codes
                    'domain': domain,
                    'format': 'json'
                }
                
                # Fetch data
                response = self.api_client.get(
                    self.endpoints['data'],
                    params=params
                )
                
                # Extract data from response
                if isinstance(response, dict):
                    data = response.get('data', [])
                elif isinstance(response, list):
                    data = response
                else:
                    data = []
                
                # Add metadata
                for record in data:
                    record['ingestion_timestamp'] = datetime.utcnow().isoformat()
                    record['source'] = 'fao_stat'
                    record['country_code'] = country_code
                    record['domain'] = domain
                
                all_data.extend(data)
                logger.info(f"Fetched {len(data)} FAO records for {country_code}")
                
            except Exception as e:
                logger.error(f"Failed to fetch FAO data for {country_code}: {e}")
                continue
        
        return all_data
    
    def load_to_bigquery(self, data: List[Dict[str, Any]], domain: str) -> None:
        """Load data to BigQuery.
        
        Args:
            data: List of data records
            domain: FAO domain code
        """
        if not data:
            logger.warning("No data to load")
            return
        
        # Map domain to table name
        domain_tables = {
            'QC': 'fao_qc',
            'QCL': 'fao_qcl',
            'RL': 'fao_rl',
            'RFN': 'fao_rfn',
            'RP': 'fao_rp',
            'TI': 'fao_ti',
            'TCL': 'fao_tcl'
        }
        
        table_name = domain_tables.get(domain, f'fao_{domain.lower()}')
        
        try:
            self.bq_client.insert_rows(table_name, data)
            logger.info(f"Successfully loaded {len(data)} records to {table_name}")
        except Exception as e:
            logger.error(f"Failed to load data to BigQuery: {e}")
            raise
    
    def run(self, country_codes: List[str], domain: str = 'QC') -> None:
        """Run the ingestion process.
        
        Args:
            country_codes: List of ISO alpha-2 country codes
            domain: FAO domain code
        """
        logger.info(f"Starting FAO Crop ingestion for {len(country_codes)} countries")
        logger.info(f"Domain: {domain}")
        logger.info(f"Countries: {', '.join(country_codes)}")
        
        # Fetch data
        data = self.fetch_crop_data(country_codes, domain)
        
        # Load to BigQuery
        if data:
            self.load_to_bigquery(data, domain)
            logger.info(f"Ingestion complete: {len(data)} records loaded")
        else:
            logger.warning("No data fetched, nothing to load")


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='FAO Crop Production data ingestion')
    
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
    
    # Domain
    parser.add_argument(
        '--domain',
        default='QC',
        choices=['QC', 'QCL', 'RL', 'RFN', 'RP', 'TI', 'TCL'],
        help='FAO domain code (default: QC - Crops and livestock products)'
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
        ingestion = FAOCropIngestion()
        ingestion.run(country_codes, args.domain)
        logger.info("Ingestion completed successfully")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Ingestion failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
