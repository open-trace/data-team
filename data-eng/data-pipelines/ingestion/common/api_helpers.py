"""API helper utilities for data ingestion with rate limiting and retry logic."""
import time
import logging
from typing import Dict, Any, Optional, Callable
from pathlib import Path
import yaml
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


logger = logging.getLogger(__name__)


def load_source_config(source_name: str) -> Dict:
    """Load configuration for a specific data source.
    
    Args:
        source_name: Name of the data source (e.g., 'fews_net', 'wfp_food_prices')
        
    Returns:
        Dict with source configuration
    """
    config_path = Path(__file__).parents[3] / "config" / "ingestion_sources.yaml"
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    source_config = config.get('sources', {}).get(source_name)
    if not source_config:
        raise ValueError(f"Source '{source_name}' not found in configuration")
    
    # Add retry config
    source_config['retry'] = config.get('retry', {})
    return source_config


class RateLimiter:
    """Simple rate limiter for API requests."""
    
    def __init__(self, requests_per_minute: int = 60, requests_per_hour: int = 1000):
        """Initialize rate limiter.
        
        Args:
            requests_per_minute: Maximum requests per minute
            requests_per_hour: Maximum requests per hour
        """
        self.requests_per_minute = requests_per_minute
        self.requests_per_hour = requests_per_hour
        self.minute_requests = []
        self.hour_requests = []
    
    def wait_if_needed(self) -> None:
        """Wait if rate limits would be exceeded."""
        now = time.time()
        
        # Clean old requests
        self.minute_requests = [t for t in self.minute_requests if now - t < 60]
        self.hour_requests = [t for t in self.hour_requests if now - t < 3600]
        
        # Check minute limit
        if len(self.minute_requests) >= self.requests_per_minute:
            sleep_time = 60 - (now - self.minute_requests[0])
            if sleep_time > 0:
                logger.info(f"Rate limit: sleeping {sleep_time:.2f}s (minute limit)")
                time.sleep(sleep_time)
                self.minute_requests = []
        
        # Check hour limit
        if len(self.hour_requests) >= self.requests_per_hour:
            sleep_time = 3600 - (now - self.hour_requests[0])
            if sleep_time > 0:
                logger.info(f"Rate limit: sleeping {sleep_time:.2f}s (hour limit)")
                time.sleep(sleep_time)
                self.hour_requests = []
        
        # Record this request
        now = time.time()
        self.minute_requests.append(now)
        self.hour_requests.append(now)


class APIClient:
    """Generic API client with rate limiting and retry logic."""
    
    def __init__(self, source_name: str):
        """Initialize API client for a specific source.
        
        Args:
            source_name: Name of the data source
        """
        self.config = load_source_config(source_name)
        self.source_name = source_name
        self.base_url = self.config['base_url']
        
        # Set up rate limiter
        rate_limit = self.config.get('rate_limit', {})
        self.rate_limiter = RateLimiter(
            requests_per_minute=rate_limit.get('requests_per_minute', 60),
            requests_per_hour=rate_limit.get('requests_per_hour', 1000)
        )
        
        # Set up session with retry logic
        self.session = self._create_session()
        
        logger.info(f"Initialized API client for {source_name}")
    
    def _create_session(self) -> requests.Session:
        """Create requests session with retry logic."""
        session = requests.Session()
        
        retry_config = self.config.get('retry', {})
        retry_strategy = Retry(
            total=retry_config.get('max_attempts', 3),
            backoff_factor=retry_config.get('backoff_factor', 2),
            status_forcelist=retry_config.get('retry_on_status_codes', [429, 500, 502, 503, 504]),
            allowed_methods=["GET", "POST"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        return session
    
    def get(self, endpoint: str, params: Optional[Dict[str, Any]] = None, 
            headers: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """Make a GET request with rate limiting and retry logic.
        
        Args:
            endpoint: API endpoint (relative to base_url)
            params: Query parameters
            headers: Request headers
            
        Returns:
            JSON response as dictionary
        """
        self.rate_limiter.wait_if_needed()
        
        url = f"{self.base_url}{endpoint}"
        
        try:
            response = self.session.get(url, params=params, headers=headers, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for {url}: {e}")
            raise
    
    def post(self, endpoint: str, data: Optional[Dict[str, Any]] = None,
             json: Optional[Dict[str, Any]] = None,
             headers: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """Make a POST request with rate limiting and retry logic.
        
        Args:
            endpoint: API endpoint (relative to base_url)
            data: Form data
            json: JSON data
            headers: Request headers
            
        Returns:
            JSON response as dictionary
        """
        self.rate_limiter.wait_if_needed()
        
        url = f"{self.base_url}{endpoint}"
        
        try:
            response = self.session.post(url, data=data, json=json, headers=headers, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for {url}: {e}")
            raise
    
    def paginate(self, endpoint: str, params: Optional[Dict[str, Any]] = None,
                 page_param: str = 'page', max_pages: Optional[int] = None) -> list:
        """Paginate through API results.
        
        Args:
            endpoint: API endpoint
            params: Query parameters
            page_param: Name of the page parameter
            max_pages: Maximum number of pages to fetch
            
        Returns:
            List of all results
        """
        all_results = []
        page = 1
        params = params or {}
        
        while True:
            if max_pages and page > max_pages:
                break
            
            params[page_param] = page
            response = self.get(endpoint, params=params)
            
            # Handle different pagination response formats
            if isinstance(response, dict):
                results = response.get('results') or response.get('data') or response.get('records') or []
                if not results:
                    break
                all_results.extend(results)
                
                # Check if there are more pages
                if not response.get('next') and not response.get('has_more'):
                    break
            elif isinstance(response, list):
                if not response:
                    break
                all_results.extend(response)
            else:
                break
            
            page += 1
            logger.info(f"Fetched page {page-1}, total results: {len(all_results)}")
        
        return all_results


def setup_logging(log_level: str = "INFO") -> None:
    """Set up logging configuration.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
    """
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
