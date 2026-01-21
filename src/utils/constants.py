"""
Constants Module
================
Contains all constant values used throughout the pipeline.

These constants are defined in one place so that:
- They're easy to find and modify
- Changes only need to be made once
- Code is more readable and maintainable

Usage:
    from src.utils.constants import MOVIE_IDS, COLUMNS_TO_DROP
"""

# =============================================================================
# MOVIE IDs TO FETCH
# =============================================================================
# These are the movie IDs we'll fetch from the TMDB API
# ID 0 is invalid and will be handled gracefully

MOVIE_IDS = [
    0,        # Invalid ID (for testing error handling)
    299534,   # Avengers: Endgame
    19995,    # Avatar
    140607,   # Star Wars: The Force Awakens
    299536,   # Avengers: Infinity War
    597,      # Titanic
    135397,   # Jurassic World
    420818,   # The Lion King (2019)
    24428,    # The Avengers
    168259,   # Furious 7
    99861,    # Avengers: Age of Ultron
    284054,   # Black Panther
    12445,    # Harry Potter and the Deathly Hallows: Part 2
    181808,   # Star Wars: The Last Jedi
    330457,   # Frozen II
    351286,   # Jurassic World: Fallen Kingdom
    109445,   # Frozen
    321612,   # Beauty and the Beast (2017)
    260513,   # Incredibles 2
]


# =============================================================================
# COLUMNS TO DROP FROM RAW DATA
# =============================================================================
# These columns are not needed for our analysis

COLUMNS_TO_DROP = [
    'adult',           # Movies are all non-adult in our dataset
    'imdb_id',         # We use TMDB ID instead
    'original_title',  # We use 'title' instead
    'video',           # Not relevant for our analysis
    'homepage',        # Not needed
]


# =============================================================================
# JSON-LIKE COLUMNS THAT NEED PARSING
# =============================================================================
# These columns contain nested data that needs to be extracted

JSON_COLUMNS = [
    'belongs_to_collection',
    'genres',
    'production_countries',
    'production_companies',
    'spoken_languages',
]


# =============================================================================
# FINAL COLUMN ORDER
# =============================================================================
# The order in which columns should appear in the final DataFrame

FINAL_COLUMN_ORDER = [
    'id',
    'title',
    'tagline',
    'release_date',
    'genres',
    'belongs_to_collection',
    'original_language',
    'budget_musd',
    'revenue_musd',
    'production_companies',
    'production_countries',
    'vote_count',
    'vote_average',
    'popularity',
    'runtime',
    'overview',
    'spoken_languages',
    'poster_path',
    'cast',
    'cast_size',
    'director',
    'crew_size',
]


# =============================================================================
# ANALYSIS THRESHOLDS
# =============================================================================
# Minimum values for certain analyses

MIN_BUDGET_FOR_ROI = 10_000_000  # $10M minimum for ROI analysis
MIN_VOTES_FOR_RATING = 10        # Minimum votes for rating analysis


# =============================================================================
# Quick Test (run this file directly to test)
# =============================================================================
if __name__ == "__main__":
    print(f"Number of movies to fetch: {len(MOVIE_IDS)}")
    print(f"Columns to drop: {COLUMNS_TO_DROP}")
    print(f"Final column count: {len(FINAL_COLUMN_ORDER)}")
