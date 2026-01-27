# Final Report: TMDB Movie Analysis

## 1. Introduction

This project analyzed movie data collected from the TMDB API. The goal was to clean the dataset, transform key fields, and extract insights about movie performance over the years.

---

## 2. Methodology

### Data Collection
- Data was gathered using the TMDB API.
- Results were loaded into a Pandas DataFrame for cleaning and analysis.

### Data Cleaning Steps
- Removed rows missing essential fields such as 'title', 'id', and 'release_date'.
- Filled missing values in fields like 'runtime' and 'overview'.
- Converted important columns such as 'budget', 'popularity', and 'release_date' to proper datatypes.
- Extracted the movie 'release_year' for trend analysis.

### Exploratory Data Analysis
- Identified trends in movie releases.
- Examined popular movies and revenue patterns.
- Investigated runtime, ratings, and production budgets.
- Looked for correlations among key metrics (budget, revenue, popularity, vote_average).

---

## 3. Key Performance Indicators (KPIs)

### Best/Worst Performing Movies
- **Highest Revenue**: Top-grossing movies ranked by total revenue.
- **Highest Budget**: Movies with the largest production budgets.
- **Highest Profit**: Movies with the greatest (Revenue - Budget).
- **Lowest Profit**: Movies with the smallest or negative profit margins.
- **Highest ROI**: Best return on investment (movies with Budget ≥ $10M).
- **Lowest ROI**: Worst return on investment (movies with Budget ≥ $10M).
- **Most Voted**: Movies with the highest vote counts.
- **Highest Rated**: Best-rated movies (with ≥ 10 votes).
- **Lowest Rated**: Worst-rated movies (with ≥ 10 votes).
- **Most Popular**: Movies ranked by popularity score.

### Franchise vs. Standalone Analysis
- Compared movies belonging to franchises vs. standalone films.
- Metrics analyzed: Mean Revenue, Median ROI, Mean Budget, Mean Popularity, Mean Rating.

### Director Performance Analysis
- Ranked directors by total number of movies directed.
- Analyzed total revenue and mean rating per director.

---

## 4. Data Visualization

The following visualizations were generated:
- **Revenue vs. Budget Trends**: Scatter plot showing relationship between production costs and earnings.
- **ROI Distribution by Genre**: Box plot comparing return on investment across genres.
- **Popularity vs. Rating**: Correlation analysis between audience engagement and critical reception.
- **Yearly Trends in Box Office Performance**: Time series of revenue trends over release years.
- **Franchise vs. Standalone Success**: Bar chart comparing performance metrics.

---

## 5. Project Structure

```
tmdb_movie_pipeline/
├── orchestrator/                     # Pipeline orchestration
│   ├── run_pipeline.py               # Main entry point
|   ├── pipeline_notebook.ipynb       # Main jupyter notebook entry point for demo
│   ├── logger.py                     # Logging setup
│   └── retry.py                      # Retry logic
├── src/                              # Source modules
│   ├── extract/                      # Data extraction from TMDB API
│   ├── transform/                    # Data cleaning & enrichment
│   ├── analysis/                     # KPIs & analytics
│   ├── visualization/                # Charts & plots
│   └── utils/                        # API client & helpers
├── data/                             # Data storage
│   ├── raw/                          # Raw API data
│   ├── processed/                    # Cleaned data
│   └── analytics/                    # Final analysis data
├── logs/                             # Pipeline logs
└── config/                           # Configuration files
```

---
