"""
API Extractor - Extract data from REST API endpoints
"""
import time
import requests
import pandas as pd
from typing import Optional, Dict, Any, List
from src.utils.logger import logger


class APIExtractor:
    """Extract data from API endpoints with retry logic and rate limiting"""
    
    def __init__(
        self,
        timeout: int = 30,
        max_retries: int = 3,
        retry_delay: int = 1,
        rate_limit_delay: float = 0.5
    ):
        """
        Initialize API Extractor
        
        Args:
            timeout: Request timeout in seconds (default: 30)
            max_retries: Maximum number of retry attempts (default: 3)
            retry_delay: Delay between retries in seconds (default: 1)
            rate_limit_delay: Delay between requests to respect rate limits (default: 0.5)
        """
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.rate_limit_delay = rate_limit_delay
        self.session = requests.Session()
    
    def extract_api(
        self,
        url: str,
        method: str = "GET",
        params: Optional[Dict] = None,
        headers: Optional[Dict] = None,
        data: Optional[Dict] = None,
        json_data: Optional[Dict] = None
    ) -> requests.Response:
        """
        Extract data from API endpoint with retry logic
        
        Args:
            url: API endpoint URL
            method: HTTP method (GET, POST, etc.)
            params: URL parameters
            headers: Request headers
            data: Form data (for POST)
            json_data: JSON data (for POST)
        
        Returns:
            Response object
        
        Raises:
            requests.RequestException: If request fails after retries
        """
        method = method.upper()
        
        logger.info(f"Extracting data from API: {method} {url}")
        
        for attempt in range(1, self.max_retries + 1):
            try:
                # Rate limiting
                if attempt > 1:
                    time.sleep(self.rate_limit_delay)
                
                # Make request
                response = self.session.request(
                    method=method,
                    url=url,
                    params=params,
                    headers=headers,
                    data=data,
                    json=json_data,
                    timeout=self.timeout
                )
                
                # Check for rate limiting
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', self.retry_delay * 2))
                    logger.warning(f"Rate limited. Waiting {retry_after} seconds...")
                    time.sleep(retry_after)
                    continue
                
                # Raise exception for HTTP errors
                response.raise_for_status()
                
                logger.info(f"Successfully fetched data from {url} (attempt {attempt})")
                return response
            
            except requests.exceptions.Timeout:
                if attempt < self.max_retries:
                    logger.warning(f"Request timeout (attempt {attempt}/{self.max_retries}). Retrying...")
                    time.sleep(self.retry_delay * attempt)
                else:
                    error_msg = f"Request timeout after {self.max_retries} attempts: {url}"
                    logger.error(error_msg)
                    raise requests.exceptions.Timeout(error_msg)
            
            except requests.exceptions.ConnectionError:
                if attempt < self.max_retries:
                    logger.warning(f"Connection error (attempt {attempt}/{self.max_retries}). Retrying...")
                    time.sleep(self.retry_delay * attempt)
                else:
                    error_msg = f"Connection error after {self.max_retries} attempts: {url}"
                    logger.error(error_msg)
                    raise requests.exceptions.ConnectionError(error_msg)
            
            except requests.exceptions.HTTPError as e:
                if e.response.status_code in [429, 500, 502, 503, 504] and attempt < self.max_retries:
                    logger.warning(f"HTTP error {e.response.status_code} (attempt {attempt}/{self.max_retries}). Retrying...")
                    time.sleep(self.retry_delay * attempt)
                else:
                    error_msg = f"HTTP error {e.response.status_code}: {url}"
                    logger.error(error_msg)
                    raise
    
    def extract_api_to_dataframe(
        self,
        url: str,
        method: str = "GET",
        params: Optional[Dict] = None,
        headers: Optional[Dict] = None,
        json_path: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Extract data from API and convert to DataFrame
        
        Args:
            url: API endpoint URL
            method: HTTP method
            params: URL parameters
            headers: Request headers
            json_path: JSON path to extract (e.g., 'data.results' for nested JSON)
        
        Returns:
            DataFrame containing API data
        """
        response = self.extract_api(
            url=url,
            method=method,
            params=params,
            headers=headers
        )
        
        try:
            # Parse JSON response
            data = response.json()
            
            # Handle nested JSON paths
            if json_path:
                keys = json_path.split('.')
                for key in keys:
                    if isinstance(data, dict) and key in data:
                        data = data[key]
                    else:
                        logger.warning(f"JSON path '{json_path}' not found. Using full response.")
                        break
            
            # Convert to DataFrame
            if isinstance(data, list):
                if len(data) == 0:
                    logger.warning(f"API returned empty list: {url}")
                    return pd.DataFrame()
                df = pd.json_normalize(data) if isinstance(data[0], dict) else pd.DataFrame(data)
            elif isinstance(data, dict):
                df = pd.json_normalize(data)
            else:
                df = pd.DataFrame([data])
            
            logger.info(f"Successfully converted API response to DataFrame: {len(df)} rows")
            logger.debug(f"Columns: {list(df.columns)}")
            
            return df
        
        except ValueError as e:
            error_msg = f"Error parsing JSON response from {url}: {str(e)}"
            logger.error(error_msg)
            raise ValueError(error_msg)
    
    def close(self):
        """Close session"""
        self.session.close()


def extract_api(
    url: str,
    method: str = "GET",
    params: Optional[Dict] = None,
    headers: Optional[Dict] = None,
    timeout: int = 30,
    max_retries: int = 3
) -> pd.DataFrame:
    """
    Convenience function to extract data from API
    
    Args:
        url: API endpoint URL
        method: HTTP method
        params: URL parameters
        headers: Request headers
        timeout: Request timeout
        max_retries: Maximum retry attempts
    
    Returns:
        DataFrame containing API data
    """
    extractor = APIExtractor(timeout=timeout, max_retries=max_retries)
    try:
        return extractor.extract_api_to_dataframe(
            url=url,
            method=method,
            params=params,
            headers=headers
        )
    finally:
        extractor.close()

