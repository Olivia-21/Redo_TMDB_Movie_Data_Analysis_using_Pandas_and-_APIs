"""
Advanced Filters Module
=======================
Provides advanced search and filter queries for the movie dataset.

This module implements the specific search queries from the task:
- Search 1: Best-rated Sci-Fi Action movies with Bruce Willis
- Search 2: Uma Thurman movies directed by Quentin Tarantino

Usage:
    from src.analysis.advanced_filters import (
        search_scifi_action_bruce_willis,
        search_uma_thurman_tarantino,
        advanced_movie_search
    )
    
    results = search_scifi_action_bruce_willis(df)
"""

import pandas as pd
from typing import List, Optional
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from orchestrator.logger import setup_logger


def check_genres(genres_str: str, required_genres: List[str]) -> bool:
    """
    Check if a movie has all required genres.
    
    Args:
        genres_str: Pipe-separated genre string (e.g., "Action|Adventure|Sci-Fi")
        required_genres: List of genres that must be present
    
    Returns:
        True if all required genres are found
    
    Example:
        >>> check_genres("Action|Adventure|Science Fiction", ["Action", "Science Fiction"])
        True
    """
    if pd.isna(genres_str) or not genres_str:
        return False
    
    # Split genres and normalize
    movie_genres = [g.strip().lower() for g in str(genres_str).split('|')]
    
    # Check if all required genres are present
    for genre in required_genres:
        genre_lower = genre.lower()
        # Check for partial matches (e.g., "sci-fi" matches "science fiction")
        if not any(genre_lower in mg or mg in genre_lower for mg in movie_genres):
            return False
    
    return True


def check_cast(cast_str: str, actor_name: str) -> bool:
    """
    Check if a specific actor is in the cast.
    
    Args:
        cast_str: Pipe-separated cast string
        actor_name: Name of actor to search for
    
    Returns:
        True if actor is found in cast
    
    Example:
        >>> check_cast("Bruce Willis|Samuel L. Jackson", "Bruce Willis")
        True
    """
    if pd.isna(cast_str) or not cast_str:
        return False
    
    # Split cast and normalize
    cast_list = [c.strip().lower() for c in str(cast_str).split('|')]
    actor_lower = actor_name.lower()
    
    # Check for match
    return any(actor_lower in c for c in cast_list)


def check_director(director_str: str, director_name: str) -> bool:
    """
    Check if a specific director directed the movie.
    
    Args:
        director_str: Pipe-separated director string
        director_name: Name of director to search for
    
    Returns:
        True if director is found
    """
    if pd.isna(director_str) or not director_str:
        return False
    
    directors = [d.strip().lower() for d in str(director_str).split('|')]
    director_lower = director_name.lower()
    
    return any(director_lower in d for d in directors)


def search_scifi_action_bruce_willis(
    df: pd.DataFrame,
    logger=None
) -> pd.DataFrame:
    """
    Search 1: Find best-rated Science Fiction Action movies starring Bruce Willis.
    
    Filters:
    - Genres include both 'Science Fiction' and 'Action'
    - Cast includes 'Bruce Willis'
    - Sorted by rating (highest to lowest)
    
    Args:
        df: Movie DataFrame
        logger: Optional logger
    
    Returns:
        Filtered and sorted DataFrame
    """
    if logger is None:
        logger = setup_logger("analysis")
    
    logger.info("Search 1: Finding Sci-Fi Action movies with Bruce Willis...")
    
    # Apply filters
    mask = df.apply(
        lambda row: (
            check_genres(row.get('genres', ''), ['Science Fiction', 'Action']) and
            check_cast(row.get('cast', ''), 'Bruce Willis')
        ),
        axis=1
    )
    
    results = df[mask].copy()
    
    # Sort by rating (highest first)
    if 'vote_average' in results.columns:
        results = results.sort_values('vote_average', ascending=False)
    
    # Select display columns
    display_cols = ['title', 'genres', 'vote_average', 'release_year', 'cast']
    available_cols = [c for c in display_cols if c in results.columns]
    
    logger.info(f"  Found {len(results)} matching movies")
    
    return results[available_cols].reset_index(drop=True)


def search_uma_thurman_tarantino(
    df: pd.DataFrame,
    logger=None
) -> pd.DataFrame:
    """
    Search 2: Find movies starring Uma Thurman directed by Quentin Tarantino.
    
    Filters:
    - Cast includes 'Uma Thurman'
    - Director includes 'Quentin Tarantino'
    - Sorted by runtime (shortest to longest)
    
    Args:
        df: Movie DataFrame
        logger: Optional logger
    
    Returns:
        Filtered and sorted DataFrame
    """
    if logger is None:
        logger = setup_logger("analysis")
    
    logger.info("Search 2: Finding Uma Thurman movies directed by Tarantino...")
    
    # Apply filters
    mask = df.apply(
        lambda row: (
            check_cast(row.get('cast', ''), 'Uma Thurman') and
            check_director(row.get('director', ''), 'Quentin Tarantino')
        ),
        axis=1
    )
    
    results = df[mask].copy()
    
    # Sort by runtime (shortest first)
    if 'runtime' in results.columns:
        results = results.sort_values('runtime', ascending=True)
    
    # Select display columns
    display_cols = ['title', 'runtime', 'director', 'release_year', 'vote_average']
    available_cols = [c for c in display_cols if c in results.columns]
    
    logger.info(f"  Found {len(results)} matching movies")
    
    return results[available_cols].reset_index(drop=True)


def advanced_movie_search(
    df: pd.DataFrame,
    genres: List[str] = None,
    actor: str = None,
    director: str = None,
    min_rating: float = None,
    max_rating: float = None,
    min_year: int = None,
    max_year: int = None,
    sort_by: str = 'vote_average',
    ascending: bool = False,
    logger=None
) -> pd.DataFrame:
    """
    Flexible advanced search with multiple filter criteria.
    
    This is a general-purpose search function that can be used
    for custom queries.
    
    Args:
        df: Movie DataFrame
        genres: List of genres to filter by (all must match)
        actor: Actor name to search for in cast
        director: Director name to search for
        min_rating: Minimum vote_average
        max_rating: Maximum vote_average
        min_year: Minimum release year
        max_year: Maximum release year
        sort_by: Column to sort by
        ascending: Sort order
        logger: Optional logger
    
    Returns:
        Filtered and sorted DataFrame
    
    Example:
        >>> results = advanced_movie_search(
        ...     df,
        ...     genres=['Action'],
        ...     min_rating=7.0,
        ...     min_year=2010,
        ...     sort_by='revenue_musd'
        ... )
    """
    if logger is None:
        logger = setup_logger("analysis")
    
    logger.info("Running advanced movie search...")
    
    # Start with all movies
    results = df.copy()
    
    # Apply genre filter
    if genres:
        genre_mask = results.apply(
            lambda row: check_genres(row.get('genres', ''), genres),
            axis=1
        )
        results = results[genre_mask]
        logger.info(f"  After genre filter: {len(results)} movies")
    
    # Apply actor filter
    if actor:
        actor_mask = results.apply(
            lambda row: check_cast(row.get('cast', ''), actor),
            axis=1
        )
        results = results[actor_mask]
        logger.info(f"  After actor filter: {len(results)} movies")
    
    # Apply director filter
    if director:
        director_mask = results.apply(
            lambda row: check_director(row.get('director', ''), director),
            axis=1
        )
        results = results[director_mask]
        logger.info(f"  After director filter: {len(results)} movies")
    
    # Apply rating filters
    if min_rating is not None and 'vote_average' in results.columns:
        results = results[results['vote_average'] >= min_rating]
    if max_rating is not None and 'vote_average' in results.columns:
        results = results[results['vote_average'] <= max_rating]
    
    # Apply year filters
    if min_year is not None and 'release_year' in results.columns:
        results = results[results['release_year'] >= min_year]
    if max_year is not None and 'release_year' in results.columns:
        results = results[results['release_year'] <= max_year]
    
    # Sort results
    if sort_by in results.columns:
        results = results.sort_values(sort_by, ascending=ascending)
    
    logger.info(f"  Final results: {len(results)} movies")
    
    return results.reset_index(drop=True)


# =============================================================================
# Quick Test (run this file directly to test)
# =============================================================================
if __name__ == "__main__":
    # Create sample test data
    test_data = pd.DataFrame({
        'title': ['Die Hard', 'Pulp Fiction', 'Kill Bill', 'Avatar', 'Armageddon'],
        'genres': [
            'Action|Thriller', 
            'Crime|Drama', 
            'Action|Thriller',
            'Action|Adventure|Science Fiction',
            'Action|Adventure|Science Fiction'
        ],
        'cast': [
            'Bruce Willis|Alan Rickman',
            'John Travolta|Uma Thurman|Samuel L. Jackson',
            'Uma Thurman|David Carradine',
            'Sam Worthington|Zoe Saldana',
            'Bruce Willis|Ben Affleck'
        ],
        'director': [
            'John McTiernan',
            'Quentin Tarantino',
            'Quentin Tarantino',
            'James Cameron',
            'Michael Bay'
        ],
        'vote_average': [7.8, 8.5, 8.0, 7.6, 6.8],
        'runtime': [132, 154, 111, 162, 151],
        'release_year': [1988, 1994, 2003, 2009, 1998],
    })
    
    print("Testing Advanced Filters...\n")
    
    # Test Search 1
    print("Search 1: Sci-Fi Action with Bruce Willis")
    result1 = search_scifi_action_bruce_willis(test_data)
    print(result1.to_string() if not result1.empty else "No results found")
    
    # Test Search 2
    print("\nSearch 2: Uma Thurman + Tarantino")
    result2 = search_uma_thurman_tarantino(test_data)
    print(result2.to_string() if not result2.empty else "No results found")
    
    # Test general search
    print("\nGeneral Search: Action movies, rating >= 7.0")
    result3 = advanced_movie_search(test_data, genres=['Action'], min_rating=7.0)
    print(result3[['title', 'genres', 'vote_average']].to_string())
