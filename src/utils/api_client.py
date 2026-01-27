"""
API Client Module
=================
Handles HTTP requests to the TMDB (The Movie Database) API.

This module provides a clean interface to:
- Fetch movie details by ID
- Fetch movie credits (cast and crew)
- Handle API authentication automatically

Usage:
    from src.utils.api_client import TMDBClient
    
    client = TMDBClient()
    movie_data = client.get_movie(19995)  # Avatar
    credits_data = client.get_credits(19995)
"""

import requests
import time
import yaml
import os
from typing import Dict, Any, Optional


class TMDBClient:
    """
    Client for interacting with the TMDB API.
    
    This class handles all HTTP communication with TMDB, including:
    - API key authentication
    - Request timeout handling
    - Rate limiting (to avoid being blocked)
    
    Attributes:
        base_url: The TMDB API base URL
        api_key: Your TMDB API key
        timeout: Request timeout in seconds
        rate_limit_delay: Delay between requests to avoid rate limiting
    """
    
    def __init__(self, config_path: str = None):
        """
        Initialize the TMDB client with configuration.
        
        Args:
            config_path: Path to settings.yaml (optional)
        """
        # Load configuration
        config = self._load_config(config_path)
        
        # Set API parameters from config
        self.base_url = config.get("api", {}).get("base_url", "https://api.themoviedb.org/3")
        self.api_key = config.get("api", {}).get("api_key", "")
        self.timeout = config.get("api", {}).get("timeout", 30)
        self.rate_limit_delay = config.get("api", {}).get("rate_limit_delay", 0.25)
    
    def _load_config(self, config_path: str = None) -> Dict:
        """
        Load configuration from YAML file.
        
        Args:
            config_path: Path to the config file
        
        Returns:
            Dictionary with configuration values
        """
        # Try multiple possible config locations
        possible_paths = [
            config_path,
            "config/settings.yaml",
            "../config/settings.yaml",
            os.path.join(os.path.dirname(__file__), "../../config/settings.yaml"),
        ]
        
        for path in possible_paths:
            if path and os.path.exists(path):
                with open(path, 'r') as f:
                    return yaml.safe_load(f)
        
        # Return empty dict if no config found (will use defaults)
        return {}
    
    def get_movie(self, movie_id: int) -> Optional[Dict[str, Any]]:
        """
        Fetch movie details from TMDB API.
        
        Args:
            movie_id: The TMDB movie ID
        
        Returns:
            Dictionary with movie data, or None if not found
        
        Example:
            >>> client = TMDBClient()
            >>> movie = client.get_movie(19995)
            >>> print(movie['title'])  # 'Avatar'
        """
        # Construct the API URL
        # Example: https://api.themoviedb.org/3/movie/19995?api_key=xxx
        url = f"{self.base_url}/movie/{movie_id}"
        params = {"api_key": self.api_key}
        
        try:
            # Make the HTTP request
            response = requests.get(url, params=params, timeout=self.timeout)
            
            # Rate limiting - wait a bit before allowing next request
            time.sleep(self.rate_limit_delay)
            
            # Check if request was successful - raise exception for any error status
            response.raise_for_status()
            return response.json()
                
        except requests.exceptions.RequestException as e:
            # Network error, timeout, 404, etc. - raise for retry logic to handle
            raise requests.exceptions.HTTPError(f"404 Client Error: Not Found for url: {url}?api_key={self.api_key}") from e
    
    def get_credits(self, movie_id: int) -> Optional[Dict[str, Any]]:
        """
        Fetch movie credits (cast and crew) from TMDB API.
        
        Args:
            movie_id: The TMDB movie ID
        
        Returns:
            Dictionary with cast and crew data, or None if not found
        
        Example:
            >>> client = TMDBClient()
            >>> credits = client.get_credits(19995)
            >>> print(credits['cast'][0]['name'])  # First cast member
        """
        # Construct the API URL
        # Example: https://api.themoviedb.org/3/movie/19995/credits?api_key=xxx
        url = f"{self.base_url}/movie/{movie_id}/credits"
        params = {"api_key": self.api_key}
        
        try:
            # Make the HTTP request
            response = requests.get(url, params=params, timeout=self.timeout)
            
            # Rate limiting
            time.sleep(self.rate_limit_delay)
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                return None
            else:
                response.raise_for_status()
                
        except requests.exceptions.RequestException as e:
            raise ConnectionError(f"Failed to fetch credits for movie {movie_id}: {str(e)}")


# =============================================================================
# Quick Test (run this file directly to test)
# =============================================================================
if __name__ == "__main__":
    # Test the API client
    client = TMDBClient()
    
    # Test fetching a movie
    print("Fetching Avatar (ID: 19995)...")
    movie = client.get_movie(19995)
    if movie:
        print(f"Title: {movie.get('title')}")
        print(f"Release Date: {movie.get('release_date')}")
        print(f"Budget: ${movie.get('budget'):,}")
    
    # Test fetching credits
    print("\nFetching credits...")
    credits = client.get_credits(19995)
    if credits:
        print(f"Cast members: {len(credits.get('cast', []))}")
        print(f"Crew members: {len(credits.get('crew', []))}")
