"""
Run Pipeline - Main Orchestrator
=================================
This is the main entry point for the TMDB Movie ETL Pipeline.

Running this script will:
1. Extract movie data from the TMDB API
2. Clean and preprocess the data
3. Enrich with derived metrics (ROI, profit, etc.)
4. Generate KPI rankings and analysis
5. Create visualizations

Usage:
    From the tmdb_movie_pipeline directory:
    
    python -m orchestrator.run_pipeline
    
    Or run directly:
    python orchestrator/run_pipeline.py
"""

import sys
import os
from datetime import datetime

# =============================================================================
# Path Setup
# =============================================================================
# Add the project root to the Python path so imports work correctly
# This allows us to run the script from any location

# Get the directory containing this script
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# Get the project root (one level up from orchestrator/)
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
# Add to Python path
sys.path.insert(0, PROJECT_ROOT)

# Change to project root for relative paths
os.chdir(PROJECT_ROOT)

# =============================================================================
# Imports
# =============================================================================
from orchestrator.logger import setup_logger, get_pipeline_logger, get_extract_logger, get_transform_logger
from orchestrator.retry import run_with_retry

from src.utils.constants import MOVIE_IDS
from src.extract.fetch_movies import fetch_movies, save_raw_data
from src.transform.clean_movies import clean_movies, save_cleaned_data
from src.transform.enrich_movies import enrich_movies, save_enriched_data
from src.analysis.kpi_rankings import get_all_rankings, print_all_rankings
from src.analysis.advanced_filters import search_scifi_action_bruce_willis, search_uma_thurman_tarantino
from src.analysis.franchise_analysis import compare_franchise_vs_standalone, get_top_franchises
from src.analysis.director_analysis import get_top_directors
from src.visualization.plots import create_all_visualizations


def main():
    """
    Main pipeline orchestration function.
    
    This function runs the complete ETL pipeline:
    1. Extract - Fetch data from TMDB API
    2. Transform - Clean and enrich the data
    3. Load - Save to CSV files
    4. Analyze - Generate KPIs and insights
    5. Visualize - Create charts and plots
    """
    # =========================================================================
    # Initialize
    # =========================================================================
    start_time = datetime.now()
    
    # Create the main pipeline logger
    pipeline_logger = get_pipeline_logger()
    
    pipeline_logger.info("=" * 60)
    pipeline_logger.info("TMDB Movie ETL Pipeline Started")
    pipeline_logger.info(f"Start Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    pipeline_logger.info("=" * 60)
    
    # Create data directories if they don't exist
    for directory in ["data/raw", "data/processed", "data/analytics", "data/visualizations", "logs"]:
        os.makedirs(directory, exist_ok=True)
    
    # =========================================================================
    # STEP 1: EXTRACT - Fetch Movie Data from TMDB API
    # =========================================================================
    pipeline_logger.info("\n" + "=" * 60)
    pipeline_logger.info("STEP 1: EXTRACTION")
    pipeline_logger.info("=" * 60)
    
    # Get the extract-specific logger for detailed extraction logging
    extract_logger = get_extract_logger()
    
    # Use retry logic for API extraction
    raw_df = run_with_retry(
        func=lambda: fetch_movies(MOVIE_IDS, extract_logger),
        retries=3,
        delay=2.0,
        logger=pipeline_logger,
        step_name="API Extraction"
    )
    
    # Save raw data
    save_raw_data(raw_df, "data/raw/movies_raw.csv", extract_logger)
    
    pipeline_logger.info(f"Extraction complete: {len(raw_df)} movies fetched")
    
    # =========================================================================
    # STEP 2: TRANSFORM - Clean the Data
    # =========================================================================
    pipeline_logger.info("\n" + "=" * 60)
    pipeline_logger.info("STEP 2: TRANSFORMATION - Cleaning")
    pipeline_logger.info("=" * 60)
    
    # Get the transform-specific logger for detailed transformation logging
    transform_logger = get_transform_logger()
    
    cleaned_df = clean_movies(raw_df, transform_logger)
    
    # Save cleaned data
    save_cleaned_data(cleaned_df, "data/processed/movies_cleaned.csv", transform_logger)
    
    pipeline_logger.info(f"Cleaning complete: {len(cleaned_df)} movies after cleaning")
    
    # =========================================================================
    # STEP 3: ENRICH - Add Derived Metrics
    # =========================================================================
    pipeline_logger.info("\n" + "=" * 60)
    pipeline_logger.info("STEP 3: TRANSFORMATION - Enrichment")
    pipeline_logger.info("=" * 60)
    
    enriched_df = enrich_movies(cleaned_df, transform_logger)
    
    # Save enriched/final data
    save_enriched_data(enriched_df, "data/analytics/movies_final.csv", transform_logger)
    
    pipeline_logger.info(f"Enrichment complete: {len(enriched_df.columns)} columns in final dataset")
    
    # =========================================================================
    # STEP 4: ANALYSIS - Generate KPIs and Insights
    # =========================================================================
    pipeline_logger.info("\n" + "=" * 60)
    pipeline_logger.info("STEP 4: ANALYSIS")
    pipeline_logger.info("=" * 60)
    
    # 4.1: KPI Rankings
    pipeline_logger.info("\n--- KPI Rankings ---")
    rankings = get_all_rankings(enriched_df, n=5, logger=pipeline_logger)
    
    # Print rankings to console
    print("\n" + "=" * 60)
    print("KPI RANKINGS")
    print("=" * 60)
    print_all_rankings(rankings)
    
    # 4.2: Advanced Searches
    pipeline_logger.info("\n--- Advanced Searches ---")
    
    print("\n" + "=" * 60)
    print("ADVANCED SEARCH RESULTS")
    print("=" * 60)
    
    # Search 1: Sci-Fi Action with Bruce Willis
    print("\n[*] Search 1: Best Sci-Fi Action Movies with Bruce Willis")
    print("-" * 50)
    search1_results = search_scifi_action_bruce_willis(enriched_df, pipeline_logger)
    if not search1_results.empty:
        print(search1_results.to_string(index=False))
    else:
        print("No matching movies found in dataset")
    
    # Search 2: Uma Thurman + Tarantino
    print("\n[*] Search 2: Uma Thurman Movies Directed by Tarantino")
    print("-" * 50)
    search2_results = search_uma_thurman_tarantino(enriched_df, pipeline_logger)
    if not search2_results.empty:
        print(search2_results.to_string(index=False))
    else:
        print("No matching movies found in dataset")
    
    # 4.3: Franchise Analysis
    pipeline_logger.info("\n--- Franchise Analysis ---")
    
    print("\n" + "=" * 60)
    print("FRANCHISE ANALYSIS")
    print("=" * 60)
    
    # Franchise vs Standalone comparison
    print("\n[>] Franchise vs Standalone Comparison")
    print("-" * 50)
    comparison = compare_franchise_vs_standalone(enriched_df, pipeline_logger)
    if not comparison.empty:
        print(comparison.to_string())
    
    # Top franchises
    print("\n[>] Top Franchises")
    print("-" * 50)
    top_franchises = get_top_franchises(enriched_df, n=10, logger=pipeline_logger)
    if not top_franchises.empty:
        print(top_franchises.to_string(index=False))
    
    # 4.4: Director Analysis
    pipeline_logger.info("\n--- Director Analysis ---")
    
    print("\n" + "=" * 60)
    print("DIRECTOR ANALYSIS")
    print("=" * 60)
    
    print("\n[>] Top Directors by Revenue")
    print("-" * 50)
    top_directors = get_top_directors(enriched_df, n=10, logger=pipeline_logger)
    if not top_directors.empty:
        print(top_directors.to_string(index=False))
    
    # =========================================================================
    # STEP 5: VISUALIZATION - Create Charts
    # =========================================================================
    pipeline_logger.info("\n" + "=" * 60)
    pipeline_logger.info("STEP 5: VISUALIZATION")
    pipeline_logger.info("=" * 60)
    
    viz_paths = create_all_visualizations(
        enriched_df,
        output_dir="data/visualizations",
        logger=pipeline_logger
    )
    
    print("\n" + "=" * 60)
    print("VISUALIZATIONS CREATED")
    print("=" * 60)
    for name, path in viz_paths.items():
        print(f"  [+] {name}: {path}")
    
    # =========================================================================
    # COMPLETE
    # =========================================================================
    end_time = datetime.now()
    duration = end_time - start_time
    
    pipeline_logger.info("\n" + "=" * 60)
    pipeline_logger.info("PIPELINE COMPLETED SUCCESSFULLY")
    pipeline_logger.info(f"End Time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    pipeline_logger.info(f"Duration: {duration}")
    pipeline_logger.info("=" * 60)
    
    print("\n" + "=" * 60)
    print("[OK] PIPELINE COMPLETED SUCCESSFULLY")
    print("=" * 60)
    print(f"\nSummary:")
    print(f"  - Movies processed: {len(enriched_df)}")
    print(f"  - Visualizations created: {len(viz_paths)}")
    print(f"  - Duration: {duration}")
    print(f"\nOutput files:")
    print(f"  - Raw data:     data/raw/movies_raw.csv")
    print(f"  - Cleaned data: data/processed/movies_cleaned.csv")
    print(f"  - Final data:   data/analytics/movies_final.csv")
    print(f"  - Charts:       data/visualizations/")
    print(f"  - Logs:         logs/pipeline.log, extract.log, transform.log")
    
    return enriched_df


# =============================================================================
# Entry Point
# =============================================================================
if __name__ == "__main__":
    try:
        result_df = main()
    except KeyboardInterrupt:
        print("\n\n[!] Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n[X] Pipeline failed with error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
