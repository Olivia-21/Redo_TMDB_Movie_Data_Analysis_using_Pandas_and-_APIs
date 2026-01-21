"""
Validators Module
=================
Provides data validation helper functions for the pipeline.

These functions help ensure data quality by:
- Checking for required columns
- Validating data types
- Identifying missing or invalid values

Usage:
    from src.utils.validators import validate_movie_data, check_required_columns
"""

import pandas as pd
from typing import List, Tuple


def validate_movie_data(df: pd.DataFrame) -> Tuple[bool, List[str]]:
    """
    Validate the movie DataFrame for required fields and data quality.
    
    This function checks:
    - Required columns exist
    - Key columns have valid data
    - No completely empty rows
    
    Args:
        df: The movie DataFrame to validate
    
    Returns:
        Tuple of (is_valid, list_of_issues)
    
    Example:
        >>> is_valid, issues = validate_movie_data(movies_df)
        >>> if not is_valid:
        ...     print("Issues found:", issues)
    """
    issues = []
    
    # Check if DataFrame is empty
    if df.empty:
        issues.append("DataFrame is empty")
        return False, issues
    
    # Required columns that must exist
    required_columns = ['id', 'title']
    for col in required_columns:
        if col not in df.columns:
            issues.append(f"Missing required column: {col}")
    
    # Check for completely null rows
    null_rows = df.isnull().all(axis=1).sum()
    if null_rows > 0:
        issues.append(f"Found {null_rows} completely empty rows")
    
    # Check for duplicate IDs
    if 'id' in df.columns:
        duplicate_ids = df['id'].duplicated().sum()
        if duplicate_ids > 0:
            issues.append(f"Found {duplicate_ids} duplicate movie IDs")
    
    is_valid = len(issues) == 0
    return is_valid, issues


def check_required_columns(df: pd.DataFrame, columns: List[str]) -> List[str]:
    """
    Check which required columns are missing from the DataFrame.
    
    Args:
        df: The DataFrame to check
        columns: List of column names that should exist
    
    Returns:
        List of missing column names
    
    Example:
        >>> missing = check_required_columns(df, ['id', 'title', 'budget'])
        >>> print(f"Missing columns: {missing}")
    """
    return [col for col in columns if col not in df.columns]


def get_data_quality_report(df: pd.DataFrame) -> dict:
    """
    Generate a data quality report for the DataFrame.
    
    Args:
        df: The DataFrame to analyze
    
    Returns:
        Dictionary with quality metrics
    
    Example:
        >>> report = get_data_quality_report(movies_df)
        >>> print(f"Completeness: {report['completeness']:.2%}")
    """
    total_cells = df.size
    null_cells = df.isnull().sum().sum()
    
    report = {
        "total_rows": len(df),
        "total_columns": len(df.columns),
        "total_cells": total_cells,
        "null_cells": null_cells,
        "non_null_cells": total_cells - null_cells,
        "completeness": (total_cells - null_cells) / total_cells if total_cells > 0 else 0,
        "columns_with_nulls": df.columns[df.isnull().any()].tolist(),
        "null_counts_by_column": df.isnull().sum().to_dict(),
    }
    
    return report


def validate_numeric_range(
    df: pd.DataFrame, 
    column: str, 
    min_val: float = None, 
    max_val: float = None
) -> pd.Series:
    """
    Validate that numeric values fall within expected range.
    
    Args:
        df: The DataFrame
        column: Column name to validate
        min_val: Minimum allowed value (optional)
        max_val: Maximum allowed value (optional)
    
    Returns:
        Boolean Series (True = valid, False = invalid)
    
    Example:
        >>> valid_mask = validate_numeric_range(df, 'budget', min_val=0)
        >>> invalid_count = (~valid_mask).sum()
    """
    if column not in df.columns:
        raise ValueError(f"Column '{column}' not found in DataFrame")
    
    valid = pd.Series([True] * len(df), index=df.index)
    
    if min_val is not None:
        valid = valid & (df[column] >= min_val)
    
    if max_val is not None:
        valid = valid & (df[column] <= max_val)
    
    return valid


# =============================================================================
# Quick Test (run this file directly to test)
# =============================================================================
if __name__ == "__main__":
    # Create sample data for testing
    sample_data = pd.DataFrame({
        'id': [1, 2, 3],
        'title': ['Movie A', 'Movie B', None],
        'budget': [1000000, 0, 5000000],
    })
    
    # Test validation
    is_valid, issues = validate_movie_data(sample_data)
    print(f"Is valid: {is_valid}")
    print(f"Issues: {issues}")
    
    # Test quality report
    report = get_data_quality_report(sample_data)
    print(f"\nData Quality Report:")
    print(f"  Completeness: {report['completeness']:.2%}")
    print(f"  Columns with nulls: {report['columns_with_nulls']}")
