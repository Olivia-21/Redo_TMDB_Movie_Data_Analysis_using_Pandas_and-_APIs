# TMDB Movie Data Analysis Pipeline

A beginner-friendly ETL pipeline for analyzing movie data from The Movie Database (TMDB) API.

## Project Structure

```
tmdb_movie_pipeline/
├── orchestrator/           # Pipeline orchestration
│   ├── run_pipeline.py     # Main entry point
│   ├── logger.py           # Logging setup
│   └── retry.py            # Retry logic
├── src/                    # Source modules
│   ├── extract/            # Data extraction
│   ├── transform/          # Data cleaning & enrichment
│   ├── analysis/           # KPIs & analytics
│   ├── visualization/      # Charts & plots
│   └── utils/              # Helpers & utilities
├── data/                   # Data storage
│   ├── raw/                # Raw API data
│   ├── processed/          # Cleaned data
│   └── analytics/          # Final analysis data
├── logs/                   # Pipeline logs
└── config/                 # Configuration files
```

## Quick Start

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Run the Pipeline
```bash
cd tmdb_movie_pipeline
python -m orchestrator.run_pipeline
```

## Features

- **Extract**: Fetch movie data from TMDB API
- **Transform**: Clean, preprocess, and enrich data
- **Analyze**: Calculate KPIs, rankings, and insights
- **Visualize**: Generate charts and plots

## KPIs Implemented

- Highest/Lowest Revenue, Budget, Profit
- Best/Worst ROI (Return on Investment)
- Most Voted & Highest Rated Movies
- Franchise vs Standalone Analysis
- Director Performance Analysis

## Requirements

- Python 3.8+
- TMDB API Key (included in config)
