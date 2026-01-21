"""
Franchise Analysis Module
=========================
Analyzes franchise (collection) performance versus standalone movies.

This module provides:
- Comparison of franchise vs standalone movies
- Most successful franchise rankings
- Franchise-level aggregations

Usage:
    from src.analysis.franchise_analysis import (
        compare_franchise_vs_standalone,
        get_top_franchises
    )
    
    comparison = compare_franchise_vs_standalone(df)
    top_franchises = get_top_franchises(df)
"""

import pandas as pd
import numpy as np
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from orchestrator.logger import setup_logger


def compare_franchise_vs_standalone(df: pd.DataFrame, logger=None) -> pd.DataFrame:
    """
    Compare franchise movies vs standalone movies.
    
    Metrics compared:
    - Mean Revenue
    - Median ROI
    - Mean Budget
    - Mean Popularity
    - Mean Rating (vote_average)
    
    Args:
        df: Movie DataFrame with 'belongs_to_collection' or 'is_franchise' column
        logger: Optional logger
    
    Returns:
        DataFrame with comparison metrics
    
    Example:
        >>> comparison = compare_franchise_vs_standalone(df)
        >>> print(comparison)
                        Franchise  Standalone
        Mean Revenue       350.5       120.3
        Median ROI           2.5         1.8
        ...
    """
    if logger is None:
        logger = setup_logger("analysis")
    
    logger.info("Comparing franchise vs standalone movies...")
    
    # Determine franchise status
    if 'is_franchise' in df.columns:
        franchise_mask = df['is_franchise'] == True
    elif 'belongs_to_collection' in df.columns:
        franchise_mask = df['belongs_to_collection'].notna()
    else:
        logger.warning("No franchise information available")
        return pd.DataFrame()
    
    # Split into two groups
    franchise_movies = df[franchise_mask]
    standalone_movies = df[~franchise_mask]
    
    logger.info(f"  Franchise movies: {len(franchise_movies)}")
    logger.info(f"  Standalone movies: {len(standalone_movies)}")
    
    # Calculate metrics for each group
    metrics = {}
    
    # Mean Revenue
    if 'revenue_musd' in df.columns:
        metrics['Mean Revenue ($M)'] = {
            'Franchise': franchise_movies['revenue_musd'].mean(),
            'Standalone': standalone_movies['revenue_musd'].mean()
        }
    
    # Median ROI
    if 'roi' in df.columns:
        metrics['Median ROI'] = {
            'Franchise': franchise_movies['roi'].median(),
            'Standalone': standalone_movies['roi'].median()
        }
    
    # Mean Budget
    if 'budget_musd' in df.columns:
        metrics['Mean Budget ($M)'] = {
            'Franchise': franchise_movies['budget_musd'].mean(),
            'Standalone': standalone_movies['budget_musd'].mean()
        }
    
    # Mean Popularity
    if 'popularity' in df.columns:
        metrics['Mean Popularity'] = {
            'Franchise': franchise_movies['popularity'].mean(),
            'Standalone': standalone_movies['popularity'].mean()
        }
    
    # Mean Rating
    if 'vote_average' in df.columns:
        metrics['Mean Rating'] = {
            'Franchise': franchise_movies['vote_average'].mean(),
            'Standalone': standalone_movies['vote_average'].mean()
        }
    
    # Movie Count
    metrics['Movie Count'] = {
        'Franchise': len(franchise_movies),
        'Standalone': len(standalone_movies)
    }
    
    # Convert to DataFrame
    comparison_df = pd.DataFrame(metrics).T
    
    # Round numeric values for readability
    comparison_df = comparison_df.round(2)
    
    logger.info("Franchise vs standalone comparison complete")
    
    return comparison_df


def get_top_franchises(df: pd.DataFrame, n: int = 10, logger=None) -> pd.DataFrame:
    """
    Find the most successful movie franchises.
    
    Rankings based on:
    - Total number of movies in franchise
    - Total & Mean Budget
    - Total & Mean Revenue
    - Mean Rating
    
    Args:
        df: Movie DataFrame with 'belongs_to_collection' column
        n: Number of top franchises to return
        logger: Optional logger
    
    Returns:
        DataFrame with franchise statistics
    """
    if logger is None:
        logger = setup_logger("analysis")
    
    logger.info("Finding top franchises...")
    
    # Check if we have the required column
    if 'belongs_to_collection' not in df.columns:
        logger.warning("No 'belongs_to_collection' column found")
        return pd.DataFrame()
    
    # Filter to only franchise movies
    franchise_df = df[df['belongs_to_collection'].notna()].copy()
    
    if franchise_df.empty:
        logger.warning("No franchise movies found")
        return pd.DataFrame()
    
    # Group by collection/franchise
    franchise_stats = franchise_df.groupby('belongs_to_collection').agg({
        'id': 'count',  # Number of movies
        'budget_musd': ['sum', 'mean'],
        'revenue_musd': ['sum', 'mean'],
        'vote_average': 'mean',
        'profit_musd': 'sum',
    }).round(2)
    
    # Flatten column names
    franchise_stats.columns = [
        'movie_count',
        'total_budget_musd', 'mean_budget_musd',
        'total_revenue_musd', 'mean_revenue_musd',
        'mean_rating',
        'total_profit_musd'
    ]
    
    # Calculate total ROI for the franchise
    franchise_stats['franchise_roi'] = (
        franchise_stats['total_profit_musd'] / franchise_stats['total_budget_musd']
    ).round(2)
    
    # Sort by total revenue (or another metric)
    franchise_stats = franchise_stats.sort_values('total_revenue_musd', ascending=False)
    
    # Reset index to make collection name a column
    franchise_stats = franchise_stats.reset_index()
    franchise_stats = franchise_stats.rename(columns={'belongs_to_collection': 'franchise'})
    
    # Return top N
    result = franchise_stats.head(n)
    
    logger.info(f"Found {len(result)} top franchises")
    
    return result


def get_franchise_details(df: pd.DataFrame, franchise_name: str) -> pd.DataFrame:
    """
    Get detailed information about a specific franchise.
    
    Args:
        df: Movie DataFrame
        franchise_name: Name of the franchise/collection
    
    Returns:
        DataFrame with all movies in the franchise
    """
    if 'belongs_to_collection' not in df.columns:
        return pd.DataFrame()
    
    # Filter for this franchise
    mask = df['belongs_to_collection'] == franchise_name
    franchise_movies = df[mask].copy()
    
    # Select relevant columns
    display_cols = [
        'title', 'release_year', 'budget_musd', 'revenue_musd',
        'profit_musd', 'roi', 'vote_average'
    ]
    available_cols = [c for c in display_cols if c in franchise_movies.columns]
    
    # Sort by release year
    if 'release_year' in franchise_movies.columns:
        franchise_movies = franchise_movies.sort_values('release_year')
    
    return franchise_movies[available_cols].reset_index(drop=True)


# =============================================================================
# Quick Test (run this file directly to test)
# =============================================================================
if __name__ == "__main__":
    # Create sample test data
    test_data = pd.DataFrame({
        'id': [1, 2, 3, 4, 5, 6],
        'title': ['Avengers 1', 'Avengers 2', 'Spider-Man', 'Inception', 'Batman 1', 'Batman 2'],
        'belongs_to_collection': [
            'Avengers Collection', 'Avengers Collection', None,
            None, 'Batman Collection', 'Batman Collection'
        ],
        'budget_musd': [150, 250, 100, 160, 200, 300],
        'revenue_musd': [600, 800, 400, 500, 500, 900],
        'profit_musd': [450, 550, 300, 340, 300, 600],
        'roi': [3.0, 2.2, 3.0, 2.1, 1.5, 2.0],
        'vote_average': [8.0, 7.5, 7.8, 8.8, 7.2, 8.5],
        'popularity': [120, 150, 80, 90, 70, 130],
        'is_franchise': [True, True, False, False, True, True],
    })
    
    print("Testing Franchise Analysis...\n")
    
    # Test comparison
    print("Franchise vs Standalone Comparison:")
    comparison = compare_franchise_vs_standalone(test_data)
    print(comparison.to_string())
    
    # Test top franchises
    print("\nTop Franchises:")
    top = get_top_franchises(test_data, n=5)
    print(top.to_string(index=False))
    
    # Test franchise details
    print("\nAvengers Collection Details:")
    details = get_franchise_details(test_data, 'Avengers Collection')
    print(details.to_string(index=False))
