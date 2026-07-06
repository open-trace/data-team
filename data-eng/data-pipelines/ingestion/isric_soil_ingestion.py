#!/usr/bin/env python3
"""
ISRIC Soil Data Ingestion Script

Fetches soil property data from ISRIC SoilGrids API and loads into BigQuery.
Uses coordinate-based queries for African countries.

Usage:
    python isric_soil_ingestion.py --countries KE NG ET
    python isric_soil_ingestion.py --region southern_africa
    python isric_soil_ingestion.py --all-countries --grid-resolution 0.5
"""
import argparse
import logging
from datetime import datetime
from typing import List, Dict, Any, Tuple
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


# Approximate bounding boxes for African countries (lat_min, lat_max, lon_min, lon_max)
COUNTRY_BOUNDS = {
    'KE': (-4.68, 5.03, 33.91, 41.91),  # Kenya
    'NG': (4.27, 13.89, 2.69, 14.68),   # Nigeria
    'ET': (3.40, 14.88, 32.99, 47.99),  # Ethiopia
    'GH': (4.74, 11.17, -3.26, 1.19),   # Ghana
    'TZ': (-11.74, -0.99, 29.34, 40.44), # Tanzania
    # Add more as needed
}


class ISRICSoilIngestion:
    """ISRIC Soil data ingestion handler."""
    
    def __init__(self):
        """Initialize ISRIC Soil ingestion."""
        self.api_client = APIClient('isric_soil')
        self.bq_client = BigQueryClient()
        self.endpoints = self.api_client.config['endpoints']
    
    def generate_grid_points(self, bounds: Tuple[float, float, float, float], 
                            resolution: float = 0.5) -> List[Tuple[float, float]]:
        """Generate grid of coordinate points within bounds.
        
        Args:
            bounds: (lat_min, lat_max, lon_min, lon_max)
            resolution: Grid resolution in degrees
            
        Returns:
            List of (latitude, longitude) tuples
        """
        lat_min, lat_max, lon_min, lon_max = bounds
        points = []
        
        lat = lat_min
        while lat <= lat_max:
            lon = lon_min
            while lon <= lon_max:
                points.append((lat, lon))
                lon += resolution
            lat += resolution
        
        return points
    
    def fetch_soil_properties(self, lat: float, lon: float) -> Dict[str, Any]:
        """Fetch soil properties for a specific coordinate.
        
        Args:
            lat: Latitude
            lon: Longitude
            
        Returns:
            Dict with soil properties
        """
        try:
            params = {
                'lat': lat,
                'lon': lon,
                'property': 'all',  # Get all soil properties
                'depth': '0-5cm,5-15cm,15-30cm,30-60cm,60-100cm'
            }
            
            data = self.api_client.get(
                self.endpoints['properties'],
                params=params
            )
            
            # Add coordinates and metadata
            data['latitude'] = lat
            data['longitude'] = lon
            data['ingestion_timestamp'] = datetime.utcnow().isoformat()
            data['source'] = 'isric_soilgrids'
            
            return data
            
        except Exception as e:
            logger.debug(f"Failed to fetch soil data for ({lat}, {lon}): {e}")
            return None
    
    def fetch_country_soil_data(self, country_code: str, grid_resolution: float = 0.5) -> List[Dict[str, Any]]:
        """Fetch soil data for a country using grid sampling.
        
        Args:
            country_code: ISO alpha-2 country code
            grid_resolution: Grid resolution in degrees
            
        Returns:
            List of data records
        """
        country_name = get_country_name(country_code)
        logger.info(f"Fetching soil data for {country_name} ({country_code})")
        
        # Get country bounds
        if country_code not in COUNTRY_BOUNDS:
            logger.warning(f"No bounding box defined for {country_code}, skipping")
            return []
        
        bounds = COUNTRY_BOUNDS[country_code]
        points = self.generate_grid_points(bounds, grid_resolution)
        
        logger.info(f"Generated {len(points)} grid points for {country_code}")
        
        all_data = []
        for i, (lat, lon) in enumerate(points):
            if i % 10 == 0:
                logger.info(f"Processing point {i+1}/{len(points)}")
            
            data = self.fetch_soil_properties(lat, lon)
            if data:
                data['country_code'] = country_code
                all_data.append(data)
        
        logger.info(f"Fetched {len(all_data)} soil records for {country_code}")
        return all_data
    
    def load_to_bigquery(self, data: List[Dict[str, Any]]) -> None:
        """Load data to BigQuery.
        
        Args:
            data: List of data records
        """
        if not data:
            logger.warning("No data to load")
            return
        
        table_name = 'isric_africa_soil_data'
        
        try:
            self.bq_client.insert_rows(table_name, data)
            logger.info(f"Successfully loaded {len(data)} records to {table_name}")
        except Exception as e:
            logger.error(f"Failed to load data to BigQuery: {e}")
            raise
    
    def run(self, country_codes: List[str], grid_resolution: float = 0.5) -> None:
        """Run the ingestion process.
        
        Args:
            country_codes: List of ISO alpha-2 country codes
            grid_resolution: Grid resolution in degrees
        """
        logger.info(f"Starting ISRIC Soil ingestion for {len(country_codes)} countries")
        logger.info(f"Grid resolution: {grid_resolution} degrees")
        logger.info(f"Countries: {', '.join(country_codes)}")
        
        all_data = []
        for country_code in country_codes:
            data = self.fetch_country_soil_data(country_code, grid_resolution)
            all_data.extend(data)
        
        # Load to BigQuery
        if all_data:
            self.load_to_bigquery(all_data)
            logger.info(f"Ingestion complete: {len(all_data)} records loaded")
        else:
            logger.warning("No data fetched, nothing to load")


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='ISRIC Soil data ingestion')
    
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
    
    # Grid resolution
    parser.add_argument(
        '--grid-resolution',
        type=float,
        default=0.5,
        help='Grid resolution in degrees (default: 0.5)'
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
    
    # Filter to only countries with defined bounds
    available_countries = [c for c in country_codes if c in COUNTRY_BOUNDS]
    if len(available_countries) < len(country_codes):
        missing = set(country_codes) - set(available_countries)
        logger.warning(f"Skipping countries without bounding boxes: {', '.join(missing)}")
    
    if not available_countries:
        logger.error("No countries with defined bounding boxes")
        sys.exit(1)
    
    logger.info(f"Processing {len(available_countries)} countries")
    
    # Run ingestion
    try:
        ingestion = ISRICSoilIngestion()
        ingestion.run(available_countries, args.grid_resolution)
        logger.info("Ingestion completed successfully")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Ingestion failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
