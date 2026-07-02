"""Country configuration utilities for Africa data ingestion."""
from pathlib import Path
from typing import Dict, List, Optional
import yaml


def load_africa_countries() -> Dict:
    """Load Africa countries configuration from YAML file.
    
    Returns:
        Dict containing countries list and regional groupings
    """
    config_path = Path(__file__).parents[3] / "config" / "africa_countries.yaml"
    with open(config_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def get_country_by_alpha2(alpha2_code: str) -> Optional[Dict]:
    """Get country details by ISO alpha-2 code.
    
    Args:
        alpha2_code: Two-letter ISO country code (e.g., 'KE')
        
    Returns:
        Dict with country details or None if not found
    """
    config = load_africa_countries()
    for country in config['countries']:
        if country['alpha2'] == alpha2_code.upper():
            return country
    return None


def get_country_by_alpha3(alpha3_code: str) -> Optional[Dict]:
    """Get country details by ISO alpha-3 code.
    
    Args:
        alpha3_code: Three-letter ISO country code (e.g., 'KEN')
        
    Returns:
        Dict with country details or None if not found
    """
    config = load_africa_countries()
    for country in config['countries']:
        if country['alpha3'] == alpha3_code.upper():
            return country
    return None


def get_countries_by_region(region: str) -> List[str]:
    """Get list of country codes for a specific region.
    
    Args:
        region: Region name (e.g., 'east_africa', 'west_africa')
        
    Returns:
        List of ISO alpha-2 country codes
    """
    config = load_africa_countries()
    return config['regions'].get(region, [])


def get_all_country_codes(format: str = 'alpha2') -> List[str]:
    """Get all African country codes.
    
    Args:
        format: 'alpha2' or 'alpha3' for code format
        
    Returns:
        List of country codes
    """
    config = load_africa_countries()
    if format == 'alpha2':
        return [c['alpha2'] for c in config['countries']]
    elif format == 'alpha3':
        return [c['alpha3'] for c in config['countries']]
    else:
        raise ValueError(f"Invalid format: {format}. Use 'alpha2' or 'alpha3'")


def validate_country_code(code: str) -> bool:
    """Validate if a country code is valid for Africa.
    
    Args:
        code: ISO country code (alpha-2 or alpha-3)
        
    Returns:
        True if valid, False otherwise
    """
    code_upper = code.upper()
    if len(code) == 2:
        return code_upper in get_all_country_codes('alpha2')
    elif len(code) == 3:
        return code_upper in get_all_country_codes('alpha3')
    return False


def get_country_name(code: str) -> Optional[str]:
    """Get country name from ISO code.
    
    Args:
        code: ISO country code (alpha-2 or alpha-3)
        
    Returns:
        Country name or None if not found
    """
    if len(code) == 2:
        country = get_country_by_alpha2(code)
    elif len(code) == 3:
        country = get_country_by_alpha3(code)
    else:
        return None
    
    return country['name'] if country else None


def get_region_for_country(code: str) -> Optional[str]:
    """Get region name for a country code.
    
    Args:
        code: ISO country code (alpha-2 or alpha-3)
        
    Returns:
        Region name or None if not found
    """
    if len(code) == 2:
        country = get_country_by_alpha2(code)
    elif len(code) == 3:
        country = get_country_by_alpha3(code)
    else:
        return None
    
    return country['region'] if country else None
