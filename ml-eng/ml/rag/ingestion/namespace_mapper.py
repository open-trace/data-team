"""
Namespace Mapper for RAG Vector Database

Implements hierarchical namespace structure for efficient filtering in Qdrant.
Format: <source_category>:<country_code>:<data_type>

Examples:
- market:KE:prices
- weather:NG:rainfall
- soil:ET:isric
- crop:TZ:maize
"""
from typing import Dict, Optional, List
import re


# Source category mappings
SOURCE_CATEGORIES = {
    'fews_net': 'food_security',
    'wfp_vampire': 'market',
    'isric': 'soil',
    'isda': 'soil',
    'gbif': 'biodiversity',
    'fao': 'crop',
    'nasa_power': 'weather',
    'copernicus': 'climate',
    'openaire': 'research',
    'undp': 'socioeconomic',
    'world_bank': 'socioeconomic'
}

# Data type mappings
DATA_TYPE_MAPPINGS = {
    'ipc_phase': 'ipc',
    'market_prices': 'prices',
    'food_security': 'food_sec',
    'soil_properties': 'properties',
    'occurrence': 'species',
    'crop_production': 'production',
    'rainfall': 'precip',
    'temperature': 'temp'
}


def extract_country_code(text: str) -> Optional[str]:
    """Extract ISO alpha-2 country code from text.
    
    Args:
        text: Text containing country information
        
    Returns:
        ISO alpha-2 country code or None
    """
    if not text:
        return None
    
    # Look for ISO alpha-2 codes (2 uppercase letters)
    match = re.search(r'\b([A-Z]{2})\b', text)
    if match:
        return match.group(1)
    
    # Common country name to code mappings
    country_mappings = {
        'kenya': 'KE',
        'nigeria': 'NG',
        'ethiopia': 'ET',
        'ghana': 'GH',
        'tanzania': 'TZ',
        'uganda': 'UG',
        'rwanda': 'RW',
        'malawi': 'MW',
        'zambia': 'ZM',
        'zimbabwe': 'ZW',
        'south africa': 'ZA',
        'egypt': 'EG',
        'morocco': 'MA',
        'algeria': 'DZ',
        'tunisia': 'TN'
    }
    
    text_lower = text.lower()
    for country_name, code in country_mappings.items():
        if country_name in text_lower:
            return code
    
    return None


def detect_source_category(source: str, metadata: Dict) -> str:
    """Detect source category from source name and metadata.
    
    Args:
        source: Source identifier
        metadata: Document metadata
        
    Returns:
        Source category
    """
    source_lower = source.lower()
    
    # Check direct mappings
    for key, category in SOURCE_CATEGORIES.items():
        if key in source_lower:
            return category
    
    # Check metadata for hints
    if 'category' in metadata:
        return metadata['category']
    
    # Check content type
    if 'market' in source_lower or 'price' in source_lower:
        return 'market'
    elif 'soil' in source_lower:
        return 'soil'
    elif 'weather' in source_lower or 'climate' in source_lower:
        return 'weather'
    elif 'crop' in source_lower or 'agriculture' in source_lower:
        return 'crop'
    elif 'food' in source_lower or 'security' in source_lower:
        return 'food_security'
    
    return 'general'


def detect_data_type(content: str, metadata: Dict) -> str:
    """Detect data type from content and metadata.
    
    Args:
        content: Document content
        metadata: Document metadata
        
    Returns:
        Data type identifier
    """
    # Check metadata first
    if 'data_type' in metadata:
        data_type = metadata['data_type']
        return DATA_TYPE_MAPPINGS.get(data_type, data_type)
    
    if 'type' in metadata:
        return metadata['type']
    
    # Analyze content
    content_lower = content.lower() if content else ''
    
    if 'price' in content_lower or 'market' in content_lower:
        return 'prices'
    elif 'rainfall' in content_lower or 'precipitation' in content_lower:
        return 'precip'
    elif 'temperature' in content_lower:
        return 'temp'
    elif 'soil' in content_lower:
        return 'properties'
    elif 'crop' in content_lower or 'production' in content_lower:
        return 'production'
    elif 'species' in content_lower or 'biodiversity' in content_lower:
        return 'species'
    
    return 'general'


def create_hierarchical_namespace(
    source: str,
    country_code: Optional[str],
    data_type: Optional[str],
    metadata: Optional[Dict] = None
) -> str:
    """Create hierarchical namespace string.
    
    Args:
        source: Source identifier
        country_code: ISO alpha-2 country code
        data_type: Data type identifier
        metadata: Optional metadata dict
        
    Returns:
        Hierarchical namespace string (e.g., "market:KE:prices")
    """
    metadata = metadata or {}
    
    # Detect source category
    source_category = detect_source_category(source, metadata)
    
    # Extract/validate country code
    if not country_code:
        country_code = extract_country_code(str(metadata.get('country', '')))
    
    # Detect data type
    if not data_type:
        content = metadata.get('content', '')
        data_type = detect_data_type(content, metadata)
    
    # Build namespace
    parts = [source_category]
    
    if country_code:
        parts.append(country_code.upper())
    
    if data_type:
        parts.append(data_type.lower())
    
    return ':'.join(parts)


def extract_namespace_components(namespace: str) -> Dict[str, Optional[str]]:
    """Extract components from hierarchical namespace.
    
    Args:
        namespace: Hierarchical namespace string
        
    Returns:
        Dict with source_category, country_code, data_type
    """
    parts = namespace.split(':')
    
    return {
        'source_category': parts[0] if len(parts) > 0 else None,
        'country_code': parts[1] if len(parts) > 1 else None,
        'data_type': parts[2] if len(parts) > 2 else None
    }


def validate_namespace(namespace: str) -> bool:
    """Validate namespace format.
    
    Args:
        namespace: Namespace string to validate
        
    Returns:
        True if valid, False otherwise
    """
    if not namespace:
        return False
    
    parts = namespace.split(':')
    
    # Must have at least source category
    if len(parts) < 1:
        return False
    
    # If country code present, must be 2 uppercase letters
    if len(parts) > 1 and parts[1]:
        if not re.match(r'^[A-Z]{2}$', parts[1]):
            return False
    
    return True


def generate_namespace_filters(
    source_category: Optional[str] = None,
    country_code: Optional[str] = None,
    data_type: Optional[str] = None
) -> Dict[str, str]:
    """Generate Qdrant filter dict from namespace components.
    
    Args:
        source_category: Source category filter
        country_code: Country code filter
        data_type: Data type filter
        
    Returns:
        Dict suitable for Qdrant filtering
    """
    filters = {}
    
    if source_category:
        filters['source_category'] = source_category
    
    if country_code:
        filters['country_code'] = country_code.upper()
    
    if data_type:
        filters['data_type'] = data_type.lower()
    
    return filters


def enrich_document_metadata(
    metadata: Dict,
    source: str,
    content: Optional[str] = None
) -> Dict:
    """Enrich document metadata with namespace fields.
    
    Args:
        metadata: Original metadata dict
        source: Source identifier
        content: Optional document content
        
    Returns:
        Enriched metadata with namespace fields
    """
    enriched = metadata.copy()
    
    # Extract country code if not present
    if 'country_code' not in enriched:
        country_code = extract_country_code(
            enriched.get('country', '') or enriched.get('location', '')
        )
        if country_code:
            enriched['country_code'] = country_code
    
    # Detect source category
    enriched['source_category'] = detect_source_category(source, enriched)
    
    # Detect data type
    if 'data_type' not in enriched:
        enriched['data_type'] = detect_data_type(content or '', enriched)
    
    # Create hierarchical namespace
    enriched['namespace'] = create_hierarchical_namespace(
        source=source,
        country_code=enriched.get('country_code'),
        data_type=enriched.get('data_type'),
        metadata=enriched
    )
    
    return enriched
