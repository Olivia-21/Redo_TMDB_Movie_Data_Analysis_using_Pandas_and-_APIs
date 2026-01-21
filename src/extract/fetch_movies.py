"""
Fetch Movies Module
===================
Handles extraction of movie data from the TMDB API.

This module is responsible for:
- Fetching movie details for each movie ID
- Fetching cast and crew information
- Combining all data into a pandas DataFrame

Usage:
    from src.extract.fetch_movies import fetch_movies
    from src.utils.constants import MOVIE_IDS
    
    raw_df = fetch_movies(MOVIE_IDS)
"""

import pandas as pd
from typing import List, Dict, Any
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from src.utils.api_client import TMDBClient
from orchestrator.logger import setup_logger


def fetch_movies(movie_ids: List[int], logger=None) -> pd.DataFrame:
    """
    Fetch movie data from TMDB API for a list of movie IDs.
    
    This function:
    1. Creates a TMDB API client
    2. Fetches movie details for each ID
    3. Fetches credits (cast/crew) for each movie
    4. Combines everything into a DataFrame
    
    Args:
        movie_ids: List of TMDB movie IDs to fetch
        logger: Optional logger for tracking progress
    
    Returns:
        pandas DataFrame with raw movie data
    
    Example:
        >>> movie_ids = [19995, 299534, 597]
        >>> df = fetch_movies(movie_ids)
        >>> print(df['title'].tolist())
        ['Avatar', 'Avengers: Endgame', 'Titanic']
    """
    # Set up logging if not provided
    if logger is None:
        logger = setup_logger("extract", "logs/extract.log")
    
    logger.info(f"Starting extraction for {len(movie_ids)} movies...")
    
    # Initialize the API client
    client = TMDBClient()
    
    # List to store all movie data
    movies_data = []
    
    # Track success and failures
    success_count = 0
    failed_ids = []
    
    # -------------------------------------------------------------------------
    # Fetch each movie
    # -------------------------------------------------------------------------
    for i, movie_id in enumerate(movie_ids, 1):
        try:
            logger.info(f"Fetching movie {i}/{len(movie_ids)} (ID: {movie_id})...")
            
            # Fetch movie details
            movie = client.get_movie(movie_id)
            
            if movie is None:
                logger.warning(f"Movie ID {movie_id} not found, skipping...")
                failed_ids.append(movie_id)
                continue
            
            # Fetch movie credits (cast and crew)
            credits = client.get_credits(movie_id)
            
            # Extract cast information
            if credits and 'cast' in credits:
                # Get top 10 cast members' names
                cast_list = credits['cast'][:10]
                cast_names = [c['name'] for c in cast_list]
                movie['cast'] = "|".join(cast_names)
                movie['cast_size'] = len(credits['cast'])
            else:
                movie['cast'] = None
                movie['cast_size'] = 0
            
            # Extract director information
            if credits and 'crew' in credits:
                # Find the director(s)
                directors = [c['name'] for c in credits['crew'] if c['job'] == 'Director']
                movie['director'] = "|".join(directors) if directors else None
                movie['crew_size'] = len(credits['crew'])
            else:
                movie['director'] = None
                movie['crew_size'] = 0
            
            # Add to our list
            movies_data.append(movie)
            success_count += 1
            
        except Exception as e:
            logger.error(f"Error fetching movie {movie_id}: {str(e)}")
            failed_ids.append(movie_id)
    
    # -------------------------------------------------------------------------
    # Create DataFrame
    # -------------------------------------------------------------------------
    logger.info(f"Extraction complete: {success_count} succeeded, {len(failed_ids)} failed")
    
    if failed_ids:
        logger.warning(f"Failed movie IDs: {failed_ids}")
    
    # Convert list of dictionaries to DataFrame
    df = pd.DataFrame(movies_data)
    
    logger.info(f"Created DataFrame with {len(df)} rows and {len(df.columns)} columns")
    
    return df


def save_raw_data(df: pd.DataFrame, output_path: str, logger=None) -> str:
    """
    Save the raw movie data to a CSV file.
    
    Args:
        df: The raw movie DataFrame
        output_path: Path to save the CSV file
        logger: Optional logger
    
    Returns:
        The path where the file was saved
    """
    if logger is None:
        logger = setup_logger("extract")
    
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Save to CSV
    df.to_csv(output_path, index=False)
    logger.info(f"Raw data saved to: {output_path}")
    
    return output_path


# =============================================================================
# Quick Test (run this file directly to test)
# =============================================================================
if __name__ == "__main__":
    # Test with a few movie IDs
    test_ids = [19995, 299534, 597]  # Avatar, Endgame, Titanic
    
    print("Testing fetch_movies function...")
    df = fetch_movies(test_ids)
    
    print(f"\nFetched {len(df)} movies:")
    print(df[['id', 'title', 'release_date', 'budget']].to_string())
