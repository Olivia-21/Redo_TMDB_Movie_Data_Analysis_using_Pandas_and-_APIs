"""
Plots Module
============
Creates visualizations for movie data analysis.

This module provides functions to create:
- Revenue vs Budget scatter plot
- ROI Distribution by Genre
- Popularity vs Rating scatter plot
- Yearly Trends in Box Office Performance
- Franchise vs Standalone Comparison

Usage:
    from src.visualization.plots import create_all_visualizations
    
    create_all_visualizations(df, output_dir='data/visualizations')
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import os
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from orchestrator.logger import setup_logger


# =============================================================================
# Styling Configuration
# =============================================================================
# Set a clean, modern style for all plots
plt.style.use('seaborn-v0_8-whitegrid')

# Color palette (modern, vibrant colors)
COLORS = {
    'primary': '#2563EB',      # Blue
    'secondary': '#7C3AED',    # Purple
    'success': '#10B981',      # Green
    'warning': '#F59E0B',      # Amber
    'danger': '#EF4444',       # Red
    'info': '#06B6D4',         # Cyan
    'franchise': '#2563EB',    # Blue
    'standalone': '#F59E0B',   # Amber
}


def setup_plot_style():
    """Configure matplotlib for clean, modern plots."""
    plt.rcParams.update({
        'figure.figsize': (12, 7),
        'figure.dpi': 100,
        'axes.titlesize': 14,
        'axes.titleweight': 'bold',
        'axes.labelsize': 12,
        'xtick.labelsize': 10,
        'ytick.labelsize': 10,
        'legend.fontsize': 10,
        'figure.facecolor': 'white',
        'axes.facecolor': 'white',
        'axes.edgecolor': '#E5E7EB',
        'axes.linewidth': 1.5,
        'grid.color': '#E5E7EB',
        'grid.linestyle': '-',
        'grid.linewidth': 0.5,
    })


def plot_revenue_vs_budget(
    df: pd.DataFrame,
    output_path: str = None,
    logger=None
) -> plt.Figure:
    """
    Create a scatter plot of Revenue vs Budget.
    
    This visualization helps identify:
    - Which movies made money relative to their budget
    - Movies that over/under-performed expectations
    - The general relationship between spending and earnings
    
    Args:
        df: Movie DataFrame with 'budget_musd' and 'revenue_musd' columns
        output_path: Path to save the figure (optional)
        logger: Optional logger
    
    Returns:
        matplotlib Figure object
    """
    if logger is None:
        logger = setup_logger("visualization")
    
    logger.info("Creating Revenue vs Budget plot...")
    setup_plot_style()
    
    # Filter out rows with missing data
    plot_df = df.dropna(subset=['budget_musd', 'revenue_musd'])
    
    # Create figure
    fig, ax = plt.subplots(figsize=(12, 8))
    
    # Create scatter plot
    scatter = ax.scatter(
        plot_df['budget_musd'],
        plot_df['revenue_musd'],
        c=plot_df.get('vote_average', 7),  # Color by rating if available
        cmap='viridis',
        alpha=0.7,
        s=100,
        edgecolors='white',
        linewidth=0.5
    )
    
    # Add break-even line (Revenue = Budget)
    max_val = max(plot_df['budget_musd'].max(), plot_df['revenue_musd'].max())
    ax.plot([0, max_val], [0, max_val], 'r--', alpha=0.5, label='Break-even line')
    
    # Add colorbar for rating
    if 'vote_average' in plot_df.columns:
        cbar = plt.colorbar(scatter, ax=ax)
        cbar.set_label('Rating', fontsize=11)
    
    # Labels for top movies (optional - top 5 by revenue)
    top_movies = plot_df.nlargest(5, 'revenue_musd')
    for _, row in top_movies.iterrows():
        ax.annotate(
            row['title'][:20],  # Truncate long titles
            (row['budget_musd'], row['revenue_musd']),
            fontsize=8,
            alpha=0.8,
            xytext=(5, 5),
            textcoords='offset points'
        )
    
    # Formatting
    ax.set_xlabel('Budget (Million USD)', fontsize=12)
    ax.set_ylabel('Revenue (Million USD)', fontsize=12)
    ax.set_title('Movie Revenue vs Budget', fontsize=16, fontweight='bold', pad=20)
    ax.legend(loc='upper left')
    
    # Add profit zones
    ax.fill_between([0, max_val], [0, 0], [0, max_val], alpha=0.1, color='red', label='Loss Zone')
    ax.fill_between([0, max_val], [0, max_val], [max_val*2, max_val*2], alpha=0.1, color='green')
    
    plt.tight_layout()
    
    # Save if output path provided
    if output_path:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        fig.savefig(output_path, dpi=150, bbox_inches='tight')
        logger.info(f"  Saved to: {output_path}")
    
    return fig


def plot_roi_by_genre(
    df: pd.DataFrame,
    output_path: str = None,
    logger=None
) -> plt.Figure:
    """
    Create a bar chart showing ROI distribution by genre.
    
    Args:
        df: Movie DataFrame with 'genres' and 'roi' columns
        output_path: Path to save the figure (optional)
        logger: Optional logger
    
    Returns:
        matplotlib Figure object
    """
    if logger is None:
        logger = setup_logger("visualization")
    
    logger.info("Creating ROI by Genre plot...")
    setup_plot_style()
    
    # Expand genres (each movie can have multiple genres separated by |)
    genre_roi = []
    for _, row in df.iterrows():
        if pd.notna(row.get('genres')) and pd.notna(row.get('roi')):
            genres = str(row['genres']).split('|')
            for genre in genres:
                genre_roi.append({
                    'genre': genre.strip(),
                    'roi': row['roi']
                })
    
    genre_df = pd.DataFrame(genre_roi)
    
    if genre_df.empty:
        logger.warning("No genre/ROI data available for plotting")
        return None
    
    # Calculate mean ROI by genre
    genre_stats = genre_df.groupby('genre')['roi'].agg(['mean', 'count']).reset_index()
    genre_stats.columns = ['genre', 'mean_roi', 'count']
    
    # Filter to genres with at least 2 movies and sort
    genre_stats = genre_stats[genre_stats['count'] >= 1]
    genre_stats = genre_stats.sort_values('mean_roi', ascending=True)
    
    # Create figure
    fig, ax = plt.subplots(figsize=(12, 8))
    
    # Create horizontal bar chart
    bars = ax.barh(
        genre_stats['genre'],
        genre_stats['mean_roi'],
        color=[COLORS['success'] if x >= 0 else COLORS['danger'] for x in genre_stats['mean_roi']],
        edgecolor='white',
        linewidth=0.5
    )
    
    # Add value labels
    for bar, val in zip(bars, genre_stats['mean_roi']):
        width = bar.get_width()
        ax.text(
            width + 0.1 if width >= 0 else width - 0.1,
            bar.get_y() + bar.get_height()/2,
            f'{val:.2f}x',
            va='center',
            ha='left' if width >= 0 else 'right',
            fontsize=9
        )
    
    # Add vertical line at x=0 (break-even)
    ax.axvline(x=0, color='gray', linestyle='-', linewidth=1)
    
    # Formatting
    ax.set_xlabel('Average ROI (Return on Investment)', fontsize=12)
    ax.set_ylabel('Genre', fontsize=12)
    ax.set_title('ROI Distribution by Genre', fontsize=16, fontweight='bold', pad=20)
    
    plt.tight_layout()
    
    # Save if output path provided
    if output_path:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        fig.savefig(output_path, dpi=150, bbox_inches='tight')
        logger.info(f"  Saved to: {output_path}")
    
    return fig


def plot_popularity_vs_rating(
    df: pd.DataFrame,
    output_path: str = None,
    logger=None
) -> plt.Figure:
    """
    Create a scatter plot of Popularity vs Rating.
    
    This helps identify:
    - Highly rated but underappreciated movies
    - Popular movies with lower ratings (crowd-pleasers)
    - The correlation between quality and popularity
    
    Args:
        df: Movie DataFrame with 'popularity' and 'vote_average' columns
        output_path: Path to save the figure (optional)
        logger: Optional logger
    
    Returns:
        matplotlib Figure object
    """
    if logger is None:
        logger = setup_logger("visualization")
    
    logger.info("Creating Popularity vs Rating plot...")
    setup_plot_style()
    
    # Filter out rows with missing data
    plot_df = df.dropna(subset=['popularity', 'vote_average'])
    
    # Create figure
    fig, ax = plt.subplots(figsize=(12, 8))
    
    # Size by revenue if available
    sizes = plot_df.get('revenue_musd', pd.Series([100]*len(plot_df)))
    sizes = sizes.fillna(50)
    sizes = (sizes / sizes.max()) * 500 + 50  # Scale to reasonable size range
    
    # Create scatter plot
    scatter = ax.scatter(
        plot_df['popularity'],
        plot_df['vote_average'],
        c=plot_df.get('release_year', 2020),
        cmap='plasma',
        s=sizes,
        alpha=0.7,
        edgecolors='white',
        linewidth=0.5
    )
    
    # Add colorbar for year
    if 'release_year' in plot_df.columns:
        cbar = plt.colorbar(scatter, ax=ax)
        cbar.set_label('Release Year', fontsize=11)
    
    # Add labels for notable movies
    top_movies = plot_df.nlargest(5, 'popularity')
    for _, row in top_movies.iterrows():
        ax.annotate(
            row['title'][:20],
            (row['popularity'], row['vote_average']),
            fontsize=8,
            alpha=0.8,
            xytext=(5, 5),
            textcoords='offset points'
        )
    
    # Add quadrant lines at median values
    med_pop = plot_df['popularity'].median()
    med_rating = plot_df['vote_average'].median()
    ax.axvline(x=med_pop, color='gray', linestyle='--', alpha=0.3)
    ax.axhline(y=med_rating, color='gray', linestyle='--', alpha=0.3)
    
    # Formatting
    ax.set_xlabel('Popularity Score', fontsize=12)
    ax.set_ylabel('Average Rating', fontsize=12)
    ax.set_title('Popularity vs Rating', fontsize=16, fontweight='bold', pad=20)
    
    plt.tight_layout()
    
    # Save if output path provided
    if output_path:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        fig.savefig(output_path, dpi=150, bbox_inches='tight')
        logger.info(f"  Saved to: {output_path}")
    
    return fig


def plot_yearly_trends(
    df: pd.DataFrame,
    output_path: str = None,
    logger=None
) -> plt.Figure:
    """
    Create a line chart showing yearly trends in box office performance.
    
    Args:
        df: Movie DataFrame with 'release_year', 'budget_musd', 'revenue_musd'
        output_path: Path to save the figure (optional)
        logger: Optional logger
    
    Returns:
        matplotlib Figure object
    """
    if logger is None:
        logger = setup_logger("visualization")
    
    logger.info("Creating Yearly Trends plot...")
    setup_plot_style()
    
    # Filter and aggregate by year
    plot_df = df.dropna(subset=['release_year']).copy()
    plot_df['release_year'] = plot_df['release_year'].astype(int)
    
    yearly_stats = plot_df.groupby('release_year').agg({
        'budget_musd': 'mean',
        'revenue_musd': 'mean',
        'vote_average': 'mean',
        'id': 'count'  # Movie count
    }).reset_index()
    yearly_stats.columns = ['year', 'avg_budget', 'avg_revenue', 'avg_rating', 'movie_count']
    
    # Sort by year
    yearly_stats = yearly_stats.sort_values('year')
    
    # Create figure with dual y-axis
    fig, ax1 = plt.subplots(figsize=(14, 7))
    
    # Plot budget and revenue on primary axis
    line1 = ax1.plot(
        yearly_stats['year'],
        yearly_stats['avg_budget'],
        marker='o',
        linewidth=2,
        color=COLORS['warning'],
        label='Avg Budget ($M)'
    )
    line2 = ax1.plot(
        yearly_stats['year'],
        yearly_stats['avg_revenue'],
        marker='s',
        linewidth=2,
        color=COLORS['success'],
        label='Avg Revenue ($M)'
    )
    
    ax1.set_xlabel('Year', fontsize=12)
    ax1.set_ylabel('Amount (Million USD)', fontsize=12, color='black')
    ax1.tick_params(axis='y', labelcolor='black')
    
    # Create secondary y-axis for rating
    ax2 = ax1.twinx()
    line3 = ax2.plot(
        yearly_stats['year'],
        yearly_stats['avg_rating'],
        marker='^',
        linewidth=2,
        linestyle='--',
        color=COLORS['secondary'],
        label='Avg Rating'
    )
    ax2.set_ylabel('Average Rating', fontsize=12, color=COLORS['secondary'])
    ax2.tick_params(axis='y', labelcolor=COLORS['secondary'])
    ax2.set_ylim(0, 10)
    
    # Combine legends
    lines = line1 + line2 + line3
    labels = [l.get_label() for l in lines]
    ax1.legend(lines, labels, loc='upper left')
    
    # Title
    ax1.set_title('Yearly Trends in Box Office Performance', fontsize=16, fontweight='bold', pad=20)
    
    # Format x-axis
    ax1.xaxis.set_major_locator(ticker.MaxNLocator(integer=True))
    
    plt.tight_layout()
    
    # Save if output path provided
    if output_path:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        fig.savefig(output_path, dpi=150, bbox_inches='tight')
        logger.info(f"  Saved to: {output_path}")
    
    return fig


def plot_franchise_comparison(
    df: pd.DataFrame,
    output_path: str = None,
    logger=None
) -> plt.Figure:
    """
    Create a grouped bar chart comparing franchise vs standalone movies.
    
    Args:
        df: Movie DataFrame with 'belongs_to_collection' or 'is_franchise'
        output_path: Path to save the figure (optional)
        logger: Optional logger
    
    Returns:
        matplotlib Figure object
    """
    if logger is None:
        logger = setup_logger("visualization")
    
    logger.info("Creating Franchise Comparison plot...")
    setup_plot_style()
    
    # Determine franchise status
    if 'is_franchise' in df.columns:
        franchise_mask = df['is_franchise'] == True
    elif 'belongs_to_collection' in df.columns:
        franchise_mask = df['belongs_to_collection'].notna()
    else:
        logger.warning("No franchise information available")
        return None
    
    franchise_movies = df[franchise_mask]
    standalone_movies = df[~franchise_mask]
    
    # Calculate metrics
    metrics = {
        'Avg Revenue\n($M)': [
            franchise_movies['revenue_musd'].mean(),
            standalone_movies['revenue_musd'].mean()
        ],
        'Avg Budget\n($M)': [
            franchise_movies['budget_musd'].mean(),
            standalone_movies['budget_musd'].mean()
        ],
        'Avg Rating': [
            franchise_movies['vote_average'].mean() * 10,  # Scale for visibility
            standalone_movies['vote_average'].mean() * 10
        ],
        'Avg Popularity': [
            franchise_movies['popularity'].mean(),
            standalone_movies['popularity'].mean()
        ],
    }
    
    # Create figure
    fig, ax = plt.subplots(figsize=(12, 7))
    
    x = np.arange(len(metrics))
    width = 0.35
    
    # Create bars
    bars1 = ax.bar(
        x - width/2,
        [v[0] for v in metrics.values()],
        width,
        label='Franchise',
        color=COLORS['franchise'],
        edgecolor='white'
    )
    bars2 = ax.bar(
        x + width/2,
        [v[1] for v in metrics.values()],
        width,
        label='Standalone',
        color=COLORS['standalone'],
        edgecolor='white'
    )
    
    # Add value labels on bars
    def add_labels(bars):
        for bar in bars:
            height = bar.get_height()
            ax.annotate(
                f'{height:.1f}',
                xy=(bar.get_x() + bar.get_width() / 2, height),
                xytext=(0, 3),
                textcoords="offset points",
                ha='center', va='bottom',
                fontsize=9
            )
    
    add_labels(bars1)
    add_labels(bars2)
    
    # Formatting
    ax.set_ylabel('Value', fontsize=12)
    ax.set_title('Franchise vs Standalone Movies Comparison', fontsize=16, fontweight='bold', pad=20)
    ax.set_xticks(x)
    ax.set_xticklabels(metrics.keys())
    ax.legend()
    
    # Add note about rating scale
    ax.text(
        0.99, 0.01,
        'Note: Rating is scaled by 10x for visibility',
        transform=ax.transAxes,
        fontsize=8,
        ha='right',
        va='bottom',
        style='italic',
        color='gray'
    )
    
    plt.tight_layout()
    
    # Save if output path provided
    if output_path:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        fig.savefig(output_path, dpi=150, bbox_inches='tight')
        logger.info(f"  Saved to: {output_path}")
    
    return fig


def create_all_visualizations(
    df: pd.DataFrame,
    output_dir: str = "data/visualizations",
    logger=None
) -> dict:
    """
    Create all visualizations and save them to the output directory.
    
    Args:
        df: Movie DataFrame
        output_dir: Directory to save plots
        logger: Optional logger
    
    Returns:
        Dictionary mapping plot names to file paths
    """
    if logger is None:
        logger = setup_logger("visualization")
    
    logger.info(f"Creating all visualizations in {output_dir}...")
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Store paths
    paths = {}
    
    # 1. Revenue vs Budget
    try:
        path = os.path.join(output_dir, "revenue_vs_budget.png")
        plot_revenue_vs_budget(df, path, logger)
        paths['revenue_vs_budget'] = path
    except Exception as e:
        logger.error(f"Failed to create revenue_vs_budget plot: {e}")
    
    # 2. ROI by Genre
    try:
        path = os.path.join(output_dir, "roi_by_genre.png")
        plot_roi_by_genre(df, path, logger)
        paths['roi_by_genre'] = path
    except Exception as e:
        logger.error(f"Failed to create roi_by_genre plot: {e}")
    
    # 3. Popularity vs Rating
    try:
        path = os.path.join(output_dir, "popularity_vs_rating.png")
        plot_popularity_vs_rating(df, path, logger)
        paths['popularity_vs_rating'] = path
    except Exception as e:
        logger.error(f"Failed to create popularity_vs_rating plot: {e}")
    
    # 4. Yearly Trends
    try:
        path = os.path.join(output_dir, "yearly_trends.png")
        plot_yearly_trends(df, path, logger)
        paths['yearly_trends'] = path
    except Exception as e:
        logger.error(f"Failed to create yearly_trends plot: {e}")
    
    # 5. Franchise Comparison
    try:
        path = os.path.join(output_dir, "franchise_comparison.png")
        plot_franchise_comparison(df, path, logger)
        paths['franchise_comparison'] = path
    except Exception as e:
        logger.error(f"Failed to create franchise_comparison plot: {e}")
    
    logger.info(f"Created {len(paths)} visualizations")
    
    # Close all figures to free memory
    plt.close('all')
    
    return paths


# =============================================================================
# Quick Test (run this file directly to test)
# =============================================================================
if __name__ == "__main__":
    # Create sample test data
    np.random.seed(42)
    n = 20
    
    test_data = pd.DataFrame({
        'title': [f'Movie {i}' for i in range(n)],
        'budget_musd': np.random.uniform(50, 300, n),
        'revenue_musd': np.random.uniform(100, 1000, n),
        'roi': np.random.uniform(-0.5, 5, n),
        'vote_average': np.random.uniform(5, 9, n),
        'popularity': np.random.uniform(20, 150, n),
        'release_year': np.random.randint(2015, 2024, n),
        'genres': np.random.choice(['Action|Adventure', 'Drama|Thriller', 'Comedy|Romance', 'Science Fiction|Action'], n),
        'belongs_to_collection': [f'Collection {i%3}' if i % 2 == 0 else None for i in range(n)],
        'is_franchise': [i % 2 == 0 for i in range(n)],
    })
    
    print("Testing Visualization Module...")
    print(f"Test data shape: {test_data.shape}")
    
    # Test all visualizations
    paths = create_all_visualizations(test_data, "test_visualizations")
    
    print(f"\nCreated {len(paths)} visualizations:")
    for name, path in paths.items():
        print(f"  - {name}: {path}")
    
    print("\nVisualization test complete!")
