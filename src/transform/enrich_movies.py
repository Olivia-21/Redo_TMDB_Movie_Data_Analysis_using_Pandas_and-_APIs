"""
Enrich Movies Module
====================
Adds derived metrics and calculated fields to the movie dataset.

This module calculates:
- Profit (Revenue - Budget)
- ROI (Return on Investment)
- Year and month from release date
- Runtime categories

Usage:
    from src.transform.enrich_movies import enrich_movies
    
    enriched_df = enrich_movies(cleaned_df)
"""

import pandas as pd
import numpy as np
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from orchestrator.logger import setup_logger


def enrich_movies(df: pd.DataFrame, logger=None) -> pd.DataFrame:
    """
    Add derived metrics and calculated fields to the movie DataFrame.
    
    This function adds:
    - profit_musd: Revenue minus Budget (in millions)
    - roi: Return on Investment ((Revenue - Budget) / Budget)
    - release_year: Year extracted from release_date
    - release_month: Month extracted from release_date
    - runtime_category: Short/Medium/Long classification
    
    Args:
        df: Cleaned movie DataFrame
        logger: Optional logger for tracking progress
    
    Returns:
        Enriched pandas DataFrame with new calculated columns
    
    Example:
        >>> enriched_df = enrich_movies(cleaned_df)
        >>> print(enriched_df[['title', 'profit_musd', 'roi']].head())
    """
    if logger is None:
        logger = setup_logger("enrich", "logs/transform.log")
    
    logger.info(f"Starting data enrichment. Input: {len(df)} rows")
    
    # Make a copy to avoid modifying the original
    df = df.copy()
    
    # =========================================================================
    # Calculate Profit (in millions USD)
    # =========================================================================
    # Profit = Revenue - Budget
    # Only calculate if both values are available
    
    if 'revenue_musd' in df.columns and 'budget_musd' in df.columns:
        df['profit_musd'] = df['revenue_musd'] - df['budget_musd']
        
        # Count how many movies have valid profit
        valid_profit = df['profit_musd'].notna().sum()
        logger.info(f"  Calculated profit for {valid_profit} movies")
    else:
        logger.warning("  Could not calculate profit - missing revenue_musd or budget_musd")
    
    # =========================================================================
    # Calculate ROI (Return on Investment)
    # =========================================================================
    # ROI = (Revenue - Budget) / Budget = Profit / Budget
    # Only meaningful when budget > 0
    
    if 'profit_musd' in df.columns and 'budget_musd' in df.columns:
        # Avoid division by zero - only calculate ROI where budget > 0
        df['roi'] = np.where(
            df['budget_musd'] > 0,
            df['profit_musd'] / df['budget_musd'],
            np.nan
        )
        
        valid_roi = df['roi'].notna().sum()
        logger.info(f"  Calculated ROI for {valid_roi} movies")
    
    # =========================================================================
    # Extract Date Components
    # =========================================================================
    
    if 'release_date' in df.columns:
        # Ensure release_date is datetime
        df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce')
        
        # Extract year
        df['release_year'] = df['release_date'].dt.year
        
        # Extract month
        df['release_month'] = df['release_date'].dt.month
        
        logger.info("  Extracted release_year and release_month")
    
    # =========================================================================
    # Categorize Runtime
    # =========================================================================
    # Short: < 90 minutes
    # Medium: 90-150 minutes
    # Long: > 150 minutes
    
    if 'runtime' in df.columns:
        def categorize_runtime(minutes):
            """Categorize movie runtime into Short/Medium/Long."""
            if pd.isna(minutes):
                return None
            elif minutes < 90:
                return 'Short'
            elif minutes <= 150:
                return 'Medium'
            else:
                return 'Long'
        
        df['runtime_category'] = df['runtime'].apply(categorize_runtime)
        logger.info("  Created runtime_category column")
    
    # =========================================================================
    # Flag Franchise vs Standalone
    # =========================================================================
    
    if 'belongs_to_collection' in df.columns:
        df['is_franchise'] = df['belongs_to_collection'].notna()
        logger.info("  Created is_franchise flag")
    
    # =========================================================================
    # Calculate Vote Score (weighted average)
    # =========================================================================
    # A more meaningful score that considers both rating and number of votes
    # Using a simple formula: weighted_score = (vote_count / (vote_count + min_votes)) * vote_average
    
    if 'vote_average' in df.columns and 'vote_count' in df.columns:
        min_votes = 100  # Minimum votes for full weight
        
        df['vote_score'] = (
            df['vote_count'] / (df['vote_count'] + min_votes)
        ) * df['vote_average']
        
        logger.info("  Created vote_score (weighted rating) column")
    
    logger.info(f"Enrichment complete. Output: {len(df)} rows, {len(df.columns)} columns")
    
    return df


def save_enriched_data(df: pd.DataFrame, output_path: str, logger=None) -> str:
    """
    Save the enriched movie data to a CSV file.
    
    Args:
        df: The enriched movie DataFrame
        output_path: Path to save the CSV file
        logger: Optional logger
    
    Returns:
        The path where the file was saved
    """
    if logger is None:
        logger = setup_logger("enrich")
    
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Save to CSV
    df.to_csv(output_path, index=False)
    logger.info(f"Enriched data saved to: {output_path}")
    
    return output_path


# =============================================================================
# Quick Test (run this file directly to test)
# =============================================================================
if __name__ == "__main__":
    # Create sample test data
    test_data = pd.DataFrame({
        'id': [1, 2, 3],
        'title': ['Movie A', 'Movie B', 'Movie C'],
        'budget_musd': [100, 50, 200],
        'revenue_musd': [500, 30, 800],
        'release_date': ['2020-06-15', '2019-12-01', '2021-03-20'],
        'vote_average': [7.5, 6.0, 8.5],
        'vote_count': [5000, 100, 15000],
        'runtime': [120, 85, 180],
        'belongs_to_collection': ['Marvel Collection', None, 'DC Collection'],
    })
    
    print("Testing enrich_movies function...")
    print(f"Input data:\n{test_data}\n")
    
    enriched = enrich_movies(test_data)
    
    print("Output (new columns):")
    new_cols = ['title', 'profit_musd', 'roi', 'release_year', 'runtime_category', 'is_franchise']
    print(enriched[new_cols].to_string())
