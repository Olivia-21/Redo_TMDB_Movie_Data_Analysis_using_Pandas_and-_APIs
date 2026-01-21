"""
Director Analysis Module
========================
Analyzes director performance across movies.

This module provides:
- Most successful directors by movie count
- Directors ranked by total revenue
- Directors ranked by average rating

Usage:
    from src.analysis.director_analysis import get_top_directors
    
    top_directors = get_top_directors(df)
"""

import pandas as pd
import numpy as np
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from orchestrator.logger import setup_logger


def get_top_directors(
    df: pd.DataFrame,
    n: int = 10,
    min_movies: int = 1,
    logger=None
) -> pd.DataFrame:
    """
    Find the most successful directors.
    
    Rankings based on:
    - Total number of movies directed
    - Total revenue
    - Mean rating
    
    Args:
        df: Movie DataFrame with 'director' column
        n: Number of top directors to return
        min_movies: Minimum number of movies to be included
        logger: Optional logger
    
    Returns:
        DataFrame with director statistics
    
    Example:
        >>> top_directors = get_top_directors(df, n=10)
        >>> print(top_directors)
    """
    if logger is None:
        logger = setup_logger("analysis")
    
    logger.info("Finding top directors...")
    
    # Check if we have the required column
    if 'director' not in df.columns:
        logger.warning("No 'director' column found")
        return pd.DataFrame()
    
    # Filter out movies without director info
    df_with_director = df[df['director'].notna()].copy()
    
    if df_with_director.empty:
        logger.warning("No movies with director information found")
        return pd.DataFrame()
    
    # =========================================================================
    # Handle multiple directors per movie
    # =========================================================================
    # Some movies have multiple directors (separated by |)
    # We need to "explode" these into separate rows
    
    # Create a list of records, one per director
    director_records = []
    
    for _, row in df_with_director.iterrows():
        directors = str(row['director']).split('|')
        for director in directors:
            director = director.strip()
            if director:
                record = {
                    'director': director,
                    'revenue_musd': row.get('revenue_musd', np.nan),
                    'budget_musd': row.get('budget_musd', np.nan),
                    'vote_average': row.get('vote_average', np.nan),
                    'profit_musd': row.get('profit_musd', np.nan),
                }
                director_records.append(record)
    
    # Create DataFrame from records
    director_df = pd.DataFrame(director_records)
    
    # =========================================================================
    # Aggregate by director
    # =========================================================================
    director_stats = director_df.groupby('director').agg({
        'revenue_musd': ['count', 'sum', 'mean'],
        'vote_average': 'mean',
        'profit_musd': 'sum',
    }).round(2)
    
    # Flatten column names
    director_stats.columns = [
        'movie_count',
        'total_revenue_musd', 'mean_revenue_musd',
        'mean_rating',
        'total_profit_musd'
    ]
    
    # =========================================================================
    # Filter and sort
    # =========================================================================
    
    # Apply minimum movies filter
    director_stats = director_stats[director_stats['movie_count'] >= min_movies]
    
    # Sort by total revenue (you could change this to other metrics)
    director_stats = director_stats.sort_values('total_revenue_musd', ascending=False)
    
    # Reset index to make director name a column
    director_stats = director_stats.reset_index()
    
    # Return top N
    result = director_stats.head(n)
    
    logger.info(f"Found {len(result)} top directors (with >= {min_movies} movies)")
    
    return result


def get_directors_by_movie_count(df: pd.DataFrame, n: int = 10, logger=None) -> pd.DataFrame:
    """
    Get directors ranked by number of movies directed.
    
    Args:
        df: Movie DataFrame
        n: Number of results
        logger: Optional logger
    
    Returns:
        DataFrame with director and movie count
    """
    top = get_top_directors(df, n=n*2, logger=logger)  # Get more to sort
    if top.empty:
        return top
    
    return top.sort_values('movie_count', ascending=False).head(n)[
        ['director', 'movie_count', 'mean_rating']
    ].reset_index(drop=True)


def get_directors_by_revenue(df: pd.DataFrame, n: int = 10, logger=None) -> pd.DataFrame:
    """
    Get directors ranked by total revenue.
    
    Args:
        df: Movie DataFrame
        n: Number of results
        logger: Optional logger
    
    Returns:
        DataFrame with director and revenue stats
    """
    top = get_top_directors(df, n=n, logger=logger)
    if top.empty:
        return top
    
    return top[['director', 'total_revenue_musd', 'movie_count', 'mean_rating']].reset_index(drop=True)


def get_directors_by_rating(
    df: pd.DataFrame,
    n: int = 10,
    min_movies: int = 2,
    logger=None
) -> pd.DataFrame:
    """
    Get directors ranked by average rating.
    
    Args:
        df: Movie DataFrame
        n: Number of results
        min_movies: Minimum movies required (default 2 for meaningful average)
        logger: Optional logger
    
    Returns:
        DataFrame with director and rating stats
    """
    top = get_top_directors(df, n=n*2, min_movies=min_movies, logger=logger)
    if top.empty:
        return top
    
    return top.sort_values('mean_rating', ascending=False).head(n)[
        ['director', 'mean_rating', 'movie_count', 'total_revenue_musd']
    ].reset_index(drop=True)


def get_director_filmography(df: pd.DataFrame, director_name: str) -> pd.DataFrame:
    """
    Get all movies by a specific director.
    
    Args:
        df: Movie DataFrame
        director_name: Name of the director
    
    Returns:
        DataFrame with all movies by the director
    """
    if 'director' not in df.columns:
        return pd.DataFrame()
    
    # Filter for movies with this director
    mask = df['director'].apply(
        lambda x: director_name.lower() in str(x).lower() if pd.notna(x) else False
    )
    
    director_movies = df[mask].copy()
    
    # Select relevant columns
    display_cols = [
        'title', 'release_year', 'budget_musd', 'revenue_musd',
        'profit_musd', 'vote_average', 'genres'
    ]
    available_cols = [c for c in display_cols if c in director_movies.columns]
    
    # Sort by release year
    if 'release_year' in director_movies.columns:
        director_movies = director_movies.sort_values('release_year')
    
    return director_movies[available_cols].reset_index(drop=True)


# =============================================================================
# Quick Test (run this file directly to test)
# =============================================================================
if __name__ == "__main__":
    # Create sample test data
    test_data = pd.DataFrame({
        'id': [1, 2, 3, 4, 5, 6],
        'title': ['Movie A', 'Movie B', 'Movie C', 'Movie D', 'Movie E', 'Movie F'],
        'director': [
            'Christopher Nolan', 'Christopher Nolan', 'James Cameron',
            'James Cameron', 'Steven Spielberg', 'Christopher Nolan|David Fincher'
        ],
        'budget_musd': [150, 200, 240, 180, 100, 130],
        'revenue_musd': [500, 700, 800, 600, 400, 450],
        'profit_musd': [350, 500, 560, 420, 300, 320],
        'vote_average': [8.5, 9.0, 7.9, 8.3, 8.0, 7.5],
    })
    
    print("Testing Director Analysis...\n")
    
    # Test top directors
    print("Top Directors (by revenue):")
    top = get_top_directors(test_data, n=5)
    print(top.to_string(index=False))
    
    # Test by movie count
    print("\nDirectors by Movie Count:")
    by_count = get_directors_by_movie_count(test_data, n=5)
    print(by_count.to_string(index=False))
    
    # Test by rating
    print("\nDirectors by Rating:")
    by_rating = get_directors_by_rating(test_data, n=5, min_movies=1)
    print(by_rating.to_string(index=False))
    
    # Test filmography
    print("\nChristopher Nolan Filmography:")
    filmography = get_director_filmography(test_data, 'Christopher Nolan')
    print(filmography.to_string(index=False))
