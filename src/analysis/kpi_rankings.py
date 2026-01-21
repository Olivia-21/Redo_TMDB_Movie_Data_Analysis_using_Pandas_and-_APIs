"""
KPI Rankings Module
===================
Provides functions to identify best and worst performing movies.

This module implements KPIs for:
- Highest/Lowest Revenue
- Highest/Lowest Budget  
- Highest/Lowest Profit
- Best/Worst ROI (Return on Investment)
- Most Voted Movies
- Highest/Lowest Rated Movies
- Most Popular Movies

Usage:
    from src.analysis.kpi_rankings import get_top_movies, get_all_rankings
    
    # Get top 5 movies by revenue
    top_revenue = get_top_movies(df, 'revenue_musd', n=5, ascending=False)
    
    # Get all KPI rankings at once
    all_rankings = get_all_rankings(df)
"""

import pandas as pd
from typing import Dict, Optional
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from src.utils.constants import MIN_BUDGET_FOR_ROI, MIN_VOTES_FOR_RATING
from orchestrator.logger import setup_logger


def get_top_movies(
    df: pd.DataFrame,
    column: str,
    n: int = 10,
    ascending: bool = False,
    filter_condition: pd.Series = None,
    display_columns: list = None
) -> pd.DataFrame:
    """
    User-Defined Function (UDF) to get top/bottom N movies by a specific column.
    
    This is a reusable function that streamlines ranking operations across
    different metrics.
    
    Args:
        df: The movie DataFrame
        column: Column name to rank by
        n: Number of top/bottom results to return (default: 10)
        ascending: If True, returns lowest values (default: False = highest)
        filter_condition: Optional boolean Series to filter data first
        display_columns: Columns to include in output (default: ['title', column])
    
    Returns:
        DataFrame with top/bottom N movies
    
    Example:
        >>> # Get top 5 highest revenue movies
        >>> top_revenue = get_top_movies(df, 'revenue_musd', n=5)
        
        >>> # Get bottom 5 ROI for movies with budget >= 10M
        >>> budget_filter = df['budget_musd'] >= 10
        >>> worst_roi = get_top_movies(df, 'roi', n=5, ascending=True, filter_condition=budget_filter)
    """
    # Apply filter if provided
    if filter_condition is not None:
        filtered_df = df[filter_condition].copy()
    else:
        filtered_df = df.copy()
    
    # Remove rows where the ranking column is NaN
    filtered_df = filtered_df.dropna(subset=[column])
    
    # Sort and get top N
    sorted_df = filtered_df.sort_values(by=column, ascending=ascending).head(n)
    
    # Select display columns
    if display_columns is None:
        display_columns = ['title', column]
    
    # Only include columns that exist
    available_columns = [col for col in display_columns if col in sorted_df.columns]
    
    return sorted_df[available_columns].reset_index(drop=True)


def get_highest_revenue(df: pd.DataFrame, n: int = 10) -> pd.DataFrame:
    """Get movies with highest revenue."""
    return get_top_movies(
        df, 'revenue_musd', n=n, ascending=False,
        display_columns=['title', 'revenue_musd', 'release_year']
    )


def get_highest_budget(df: pd.DataFrame, n: int = 10) -> pd.DataFrame:
    """Get movies with highest budget."""
    return get_top_movies(
        df, 'budget_musd', n=n, ascending=False,
        display_columns=['title', 'budget_musd', 'release_year']
    )


def get_highest_profit(df: pd.DataFrame, n: int = 10) -> pd.DataFrame:
    """Get movies with highest profit (Revenue - Budget)."""
    return get_top_movies(
        df, 'profit_musd', n=n, ascending=False,
        display_columns=['title', 'profit_musd', 'budget_musd', 'revenue_musd']
    )


def get_lowest_profit(df: pd.DataFrame, n: int = 10) -> pd.DataFrame:
    """Get movies with lowest profit (biggest losses)."""
    return get_top_movies(
        df, 'profit_musd', n=n, ascending=True,
        display_columns=['title', 'profit_musd', 'budget_musd', 'revenue_musd']
    )


def get_highest_roi(df: pd.DataFrame, n: int = 10, min_budget: float = None) -> pd.DataFrame:
    """
    Get movies with highest ROI (Return on Investment).
    
    Args:
        df: Movie DataFrame
        n: Number of results
        min_budget: Minimum budget in millions (default: from constants)
    """
    if min_budget is None:
        min_budget = MIN_BUDGET_FOR_ROI / 1_000_000  # Convert to millions
    
    # Filter for movies with sufficient budget
    budget_filter = df['budget_musd'] >= min_budget
    
    return get_top_movies(
        df, 'roi', n=n, ascending=False,
        filter_condition=budget_filter,
        display_columns=['title', 'roi', 'budget_musd', 'revenue_musd']
    )


def get_lowest_roi(df: pd.DataFrame, n: int = 10, min_budget: float = None) -> pd.DataFrame:
    """
    Get movies with lowest ROI (worst return on investment).
    
    Args:
        df: Movie DataFrame
        n: Number of results  
        min_budget: Minimum budget in millions (default: from constants)
    """
    if min_budget is None:
        min_budget = MIN_BUDGET_FOR_ROI / 1_000_000
    
    budget_filter = df['budget_musd'] >= min_budget
    
    return get_top_movies(
        df, 'roi', n=n, ascending=True,
        filter_condition=budget_filter,
        display_columns=['title', 'roi', 'budget_musd', 'revenue_musd']
    )


def get_most_voted(df: pd.DataFrame, n: int = 10) -> pd.DataFrame:
    """Get movies with most votes."""
    return get_top_movies(
        df, 'vote_count', n=n, ascending=False,
        display_columns=['title', 'vote_count', 'vote_average']
    )


def get_highest_rated(df: pd.DataFrame, n: int = 10, min_votes: int = None) -> pd.DataFrame:
    """
    Get highest rated movies (only those with minimum votes).
    
    Args:
        df: Movie DataFrame
        n: Number of results
        min_votes: Minimum vote count (default: from constants)
    """
    if min_votes is None:
        min_votes = MIN_VOTES_FOR_RATING
    
    votes_filter = df['vote_count'] >= min_votes
    
    return get_top_movies(
        df, 'vote_average', n=n, ascending=False,
        filter_condition=votes_filter,
        display_columns=['title', 'vote_average', 'vote_count']
    )


def get_lowest_rated(df: pd.DataFrame, n: int = 10, min_votes: int = None) -> pd.DataFrame:
    """
    Get lowest rated movies (only those with minimum votes).
    
    Args:
        df: Movie DataFrame
        n: Number of results
        min_votes: Minimum vote count (default: from constants)
    """
    if min_votes is None:
        min_votes = MIN_VOTES_FOR_RATING
    
    votes_filter = df['vote_count'] >= min_votes
    
    return get_top_movies(
        df, 'vote_average', n=n, ascending=True,
        filter_condition=votes_filter,
        display_columns=['title', 'vote_average', 'vote_count']
    )


def get_most_popular(df: pd.DataFrame, n: int = 10) -> pd.DataFrame:
    """Get most popular movies."""
    return get_top_movies(
        df, 'popularity', n=n, ascending=False,
        display_columns=['title', 'popularity', 'vote_average']
    )


def get_all_rankings(df: pd.DataFrame, n: int = 5, logger=None) -> Dict[str, pd.DataFrame]:
    """
    Generate all KPI rankings at once.
    
    Args:
        df: Movie DataFrame
        n: Number of results per ranking
        logger: Optional logger
    
    Returns:
        Dictionary with all ranking DataFrames
    
    Example:
        >>> rankings = get_all_rankings(df, n=5)
        >>> print(rankings['highest_revenue'])
        >>> print(rankings['best_roi'])
    """
    if logger is None:
        logger = setup_logger("analysis")
    
    logger.info("Generating all KPI rankings...")
    
    rankings = {
        'highest_revenue': get_highest_revenue(df, n),
        'highest_budget': get_highest_budget(df, n),
        'highest_profit': get_highest_profit(df, n),
        'lowest_profit': get_lowest_profit(df, n),
        'highest_roi': get_highest_roi(df, n),
        'lowest_roi': get_lowest_roi(df, n),
        'most_voted': get_most_voted(df, n),
        'highest_rated': get_highest_rated(df, n),
        'lowest_rated': get_lowest_rated(df, n),
        'most_popular': get_most_popular(df, n),
    }
    
    logger.info(f"Generated {len(rankings)} ranking tables")
    
    return rankings


def print_all_rankings(rankings: Dict[str, pd.DataFrame]) -> None:
    """
    Print all rankings in a nicely formatted way.
    
    Args:
        rankings: Dictionary of ranking DataFrames from get_all_rankings()
    """
    titles = {
        'highest_revenue': '[$$] Highest Revenue Movies',
        'highest_budget': '[$$] Highest Budget Movies',
        'highest_profit': '[+] Highest Profit Movies',
        'lowest_profit': '[-] Lowest Profit Movies (Biggest Losses)',
        'highest_roi': '[^] Best ROI (Budget >= $10M)',
        'lowest_roi': '[v] Worst ROI (Budget >= $10M)',
        'most_voted': '[#] Most Voted Movies',
        'highest_rated': '[*] Highest Rated Movies (>=10 votes)',
        'lowest_rated': '[x] Lowest Rated Movies (>=10 votes)',
        'most_popular': '[!] Most Popular Movies',
    }
    
    for key, df in rankings.items():
        print(f"\n{titles.get(key, key)}")
        print("=" * 60)
        print(df.to_string(index=False))


# =============================================================================
# Quick Test (run this file directly to test)
# =============================================================================
if __name__ == "__main__":
    # Create sample test data
    test_data = pd.DataFrame({
        'title': ['Movie A', 'Movie B', 'Movie C', 'Movie D', 'Movie E'],
        'budget_musd': [100, 50, 200, 10, 150],
        'revenue_musd': [500, 30, 800, 100, 400],
        'profit_musd': [400, -20, 600, 90, 250],
        'roi': [4.0, -0.4, 3.0, 9.0, 1.67],
        'vote_count': [5000, 100, 15000, 50, 8000],
        'vote_average': [7.5, 6.0, 8.5, 9.0, 7.0],
        'popularity': [45.5, 12.3, 89.7, 5.2, 67.8],
        'release_year': [2020, 2019, 2021, 2018, 2022],
    })
    
    print("Testing KPI Rankings...")
    
    # Test individual rankings
    print("\nTop 3 Highest Profit:")
    print(get_highest_profit(test_data, n=3).to_string(index=False))
    
    print("\nTop 3 Best ROI:")
    print(get_highest_roi(test_data, n=3, min_budget=5).to_string(index=False))
    
    # Test all rankings
    print("\n\nGenerating all rankings...")
    rankings = get_all_rankings(test_data, n=3)
    print(f"Generated {len(rankings)} rankings")
