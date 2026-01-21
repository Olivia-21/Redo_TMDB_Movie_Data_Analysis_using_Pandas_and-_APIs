"""
Clean Movies Module
===================
Handles data cleaning and preprocessing for the movie dataset.

This module performs all the data cleaning steps:
- Drop irrelevant columns
- Parse JSON-like columns (genres, companies, etc.)
- Handle missing values
- Convert data types
- Remove duplicates
- Filter and reorder columns

Usage:
    from src.transform.clean_movies import clean_movies
    
    cleaned_df = clean_movies(raw_df)
"""

import pandas as pd
import numpy as np
import ast
from typing import Any, List
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from src.utils.constants import COLUMNS_TO_DROP, FINAL_COLUMN_ORDER
from orchestrator.logger import setup_logger


def safe_eval(value: Any) -> Any:
    """
    Safely evaluate a string that looks like a Python literal.
    
    This is used to convert string representations of lists/dicts
    back into actual Python objects.
    
    Args:
        value: The value to evaluate (could be string, dict, list, or None)
    
    Returns:
        The evaluated Python object, or None if evaluation fails
    
    Example:
        >>> safe_eval("[{'id': 1, 'name': 'Action'}]")
        [{'id': 1, 'name': 'Action'}]
    """
    # Handle None
    if value is None:
        return None
    
    # Handle pandas NaN / numpy NaN
    try:
        if pd.isna(value):
            return None
    except (ValueError, TypeError):
        # pd.isna can fail on arrays, so handle that case
        pass
    
    # If already a dict or list, return as-is
    if isinstance(value, (dict, list)):
        return value
    
    # Try to parse string representation
    try:
        return ast.literal_eval(str(value))
    except (ValueError, SyntaxError):
        return None



def extract_collection_name(value: Any) -> str:
    """
    Extract the collection name from the belongs_to_collection field.
    
    Args:
        value: The belongs_to_collection value (dict or string)
    
    Returns:
        Collection name or None
    
    Example:
        >>> extract_collection_name({'id': 1, 'name': 'Marvel Cinematic Universe'})
        'Marvel Cinematic Universe'
    """
    parsed = safe_eval(value)
    if parsed and isinstance(parsed, dict):
        return parsed.get('name')
    return None


def extract_names_from_list(value: Any, key: str = 'name', separator: str = '|') -> str:
    """
    Extract names from a list of dictionaries and join with separator.
    
    This is used for fields like genres, production_companies, etc.
    
    Args:
        value: The value to parse (list of dicts or string representation)
        key: The dictionary key to extract (default: 'name')
        separator: String to join multiple values (default: '|')
    
    Returns:
        Joined string of names, or None if empty
    
    Example:
        >>> genres = [{'id': 1, 'name': 'Action'}, {'id': 2, 'name': 'Sci-Fi'}]
        >>> extract_names_from_list(genres)
        'Action|Sci-Fi'
    """
    parsed = safe_eval(value)
    if parsed and isinstance(parsed, list):
        names = [item.get(key, '') for item in parsed if isinstance(item, dict) and item.get(key)]
        return separator.join(names) if names else None
    return None


def extract_language_codes(value: Any, separator: str = '|') -> str:
    """
    Extract language codes from spoken_languages field.
    
    Args:
        value: The spoken_languages value
        separator: String to join multiple values
    
    Returns:
        Joined string of language names
    """
    parsed = safe_eval(value)
    if parsed and isinstance(parsed, list):
        # Try to get english_name first, fall back to name
        names = []
        for item in parsed:
            if isinstance(item, dict):
                name = item.get('english_name') or item.get('name') or item.get('iso_639_1')
                if name:
                    names.append(name)
        return separator.join(names) if names else None
    return None


def clean_movies(df: pd.DataFrame, logger=None) -> pd.DataFrame:
    """
    Clean and preprocess the raw movie DataFrame.
    
    This function performs all required cleaning steps from the task:
    1. Drop irrelevant columns
    2. Parse JSON-like columns
    3. Handle missing and incorrect data
    4. Convert data types
    5. Remove duplicates
    6. Filter and reorder columns
    
    Args:
        df: Raw movie DataFrame from extraction
        logger: Optional logger for tracking progress
    
    Returns:
        Cleaned pandas DataFrame
    
    Example:
        >>> cleaned_df = clean_movies(raw_df)
        >>> print(cleaned_df.columns.tolist())
    """
    if logger is None:
        logger = setup_logger("transform", "logs/transform.log")
    
    logger.info(f"Starting data cleaning. Input: {len(df)} rows, {len(df.columns)} columns")
    
    # Make a copy to avoid modifying the original
    df = df.copy()
    
    # =========================================================================
    # STEP 1: Drop Irrelevant Columns
    # =========================================================================
    logger.info("Step 1: Dropping irrelevant columns...")
    
    # Only drop columns that exist in the DataFrame
    cols_to_drop = [col for col in COLUMNS_TO_DROP if col in df.columns]
    df = df.drop(columns=cols_to_drop)
    logger.info(f"  Dropped {len(cols_to_drop)} columns: {cols_to_drop}")
    
    # =========================================================================
    # STEP 2: Parse JSON-like Columns
    # =========================================================================
    logger.info("Step 2: Parsing JSON-like columns...")
    
    # Extract collection name
    if 'belongs_to_collection' in df.columns:
        df['belongs_to_collection'] = df['belongs_to_collection'].apply(extract_collection_name)
        logger.info("  Parsed belongs_to_collection")
    
    # Extract genre names (separated by |)
    if 'genres' in df.columns:
        df['genres'] = df['genres'].apply(lambda x: extract_names_from_list(x, 'name'))
        logger.info("  Parsed genres")
    
    # Extract spoken languages
    if 'spoken_languages' in df.columns:
        df['spoken_languages'] = df['spoken_languages'].apply(extract_language_codes)
        logger.info("  Parsed spoken_languages")
    
    # Extract production countries
    if 'production_countries' in df.columns:
        df['production_countries'] = df['production_countries'].apply(
            lambda x: extract_names_from_list(x, 'name')
        )
        logger.info("  Parsed production_countries")
    
    # Extract production companies
    if 'production_companies' in df.columns:
        df['production_companies'] = df['production_companies'].apply(
            lambda x: extract_names_from_list(x, 'name')
        )
        logger.info("  Parsed production_companies")
    
    # =========================================================================
    # STEP 3: Convert Data Types
    # =========================================================================
    logger.info("Step 3: Converting data types...")
    
    # Convert numeric columns (set invalid to NaN)
    numeric_columns = ['budget', 'id', 'popularity', 'revenue', 'runtime', 'vote_count', 'vote_average']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    logger.info(f"  Converted {len(numeric_columns)} columns to numeric")
    
    # Convert release_date to datetime
    if 'release_date' in df.columns:
        df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce')
        logger.info("  Converted release_date to datetime")
    
    # =========================================================================
    # STEP 4: Handle Missing and Incorrect Data
    # =========================================================================
    logger.info("Step 4: Handling missing and incorrect data...")
    
    # Replace 0 budget/revenue/runtime with NaN (unrealistic values)
    for col in ['budget', 'revenue', 'runtime']:
        if col in df.columns:
            zeros_count = (df[col] == 0).sum()
            df[col] = df[col].replace(0, np.nan)
            logger.info(f"  Replaced {zeros_count} zero values in {col} with NaN")
    
    # Convert budget and revenue to millions USD
    if 'budget' in df.columns:
        df['budget_musd'] = df['budget'] / 1_000_000
    if 'revenue' in df.columns:
        df['revenue_musd'] = df['revenue'] / 1_000_000
    logger.info("  Created budget_musd and revenue_musd columns")
    
    # Handle overview and tagline placeholders
    for col in ['overview', 'tagline']:
        if col in df.columns:
            # Replace common placeholders with NaN
            placeholders = ['No Data', 'No data', 'N/A', 'n/a', '', ' ']
            df[col] = df[col].replace(placeholders, np.nan)
    
    # =========================================================================
    # STEP 5: Remove Duplicates and Invalid Rows
    # =========================================================================
    logger.info("Step 5: Removing duplicates and invalid rows...")
    
    initial_count = len(df)
    
    # Remove duplicates based on ID
    if 'id' in df.columns:
        df = df.drop_duplicates(subset=['id'], keep='first')
        logger.info(f"  Removed {initial_count - len(df)} duplicate rows")
    
    # Drop rows with unknown id or title
    if 'id' in df.columns and 'title' in df.columns:
        before_drop = len(df)
        df = df.dropna(subset=['id', 'title'])
        logger.info(f"  Removed {before_drop - len(df)} rows with missing id or title")
    
    # Keep only rows where at least 10 columns have non-NaN values
    before_drop = len(df)
    min_non_null = 10
    df = df.dropna(thresh=min_non_null)
    logger.info(f"  Removed {before_drop - len(df)} rows with less than {min_non_null} non-null values")
    
    # =========================================================================
    # STEP 6: Filter Released Movies Only
    # =========================================================================
    logger.info("Step 6: Filtering released movies...")
    
    if 'status' in df.columns:
        before_filter = len(df)
        df = df[df['status'] == 'Released']
        df = df.drop(columns=['status'])
        logger.info(f"  Kept {len(df)} released movies (removed {before_filter - len(df)})")
    
    # =========================================================================
    # STEP 7: Reorder Columns
    # =========================================================================
    logger.info("Step 7: Reordering columns...")
    
    # Get columns that exist in both FINAL_COLUMN_ORDER and our DataFrame
    available_columns = [col for col in FINAL_COLUMN_ORDER if col in df.columns]
    
    # Add any extra columns not in FINAL_COLUMN_ORDER at the end
    extra_columns = [col for col in df.columns if col not in FINAL_COLUMN_ORDER]
    
    # Reorder
    final_columns = available_columns + extra_columns
    df = df[final_columns]
    
    # =========================================================================
    # STEP 8: Reset Index
    # =========================================================================
    df = df.reset_index(drop=True)
    
    logger.info(f"Cleaning complete. Output: {len(df)} rows, {len(df.columns)} columns")
    
    return df


def save_cleaned_data(df: pd.DataFrame, output_path: str, logger=None) -> str:
    """
    Save the cleaned movie data to a CSV file.
    
    Args:
        df: The cleaned movie DataFrame
        output_path: Path to save the CSV file
        logger: Optional logger
    
    Returns:
        The path where the file was saved
    """
    if logger is None:
        logger = setup_logger("transform")
    
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Save to CSV
    df.to_csv(output_path, index=False)
    logger.info(f"Cleaned data saved to: {output_path}")
    
    return output_path


# =============================================================================
# Quick Test (run this file directly to test)
# =============================================================================
if __name__ == "__main__":
    # Create sample test data
    test_data = pd.DataFrame({
        'id': [1, 2],
        'title': ['Test Movie 1', 'Test Movie 2'],
        'budget': [100000000, 0],
        'revenue': [500000000, 200000000],
        'genres': ["[{'id': 28, 'name': 'Action'}, {'id': 12, 'name': 'Adventure'}]", None],
        'status': ['Released', 'Released'],
        'adult': [False, False],
        'release_date': ['2023-01-15', '2023-06-20'],
    })
    
    print("Testing clean_movies function...")
    print(f"Input shape: {test_data.shape}")
    
    cleaned = clean_movies(test_data)
    
    print(f"Output shape: {cleaned.shape}")
    print(f"Columns: {cleaned.columns.tolist()}")
    print(cleaned.to_string())
