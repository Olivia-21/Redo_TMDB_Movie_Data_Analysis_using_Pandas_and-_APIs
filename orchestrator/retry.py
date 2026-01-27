"""
Retry module for TMDB Movie Pipeline.
Provides retry logic with exponential backoff for handling transient failures.
"""

import time
import logging


def run_with_retry(func, retries=3, delay=1, backoff=2, logger=None, step_name="Operation", movie_id=None):
    """
    Execute a function with retry logic and exponential backoff.
    
    Args:
        func: Callable to execute. Should be a zero-argument function (use lambda for args).
        retries: Maximum number of retry attempts (default: 3).
        delay: Initial delay between retries in seconds (default: 1).
        backoff: Multiplier for delay after each retry (default: 2).
        logger: Logger instance for logging attempts and failures.
        step_name: Name of the step for logging purposes.
        movie_id: Optional movie ID for more specific logging.
    
    Returns:
        Result of the function call if successful, or None if all retries fail.
    """
    if logger is None:
        logger = logging.getLogger(__name__)
    
    current_delay = delay
    last_exception = None
    
    # Create context string for logging
    context = f" for movie ID {movie_id}" if movie_id is not None else ""
    
    for attempt in range(1, retries + 1):
        try:
            logger.info(f"Attempt {attempt} of {retries}{context}")
            result = func()
            return result
        
        except Exception as e:
            last_exception = e
            logger.warning(
                f"Attempt {attempt} failed{context}: {str(e)}"
            )
            
            if attempt < retries:
                logger.info(f"Retrying in {current_delay} seconds...")
                time.sleep(current_delay)
                current_delay *= backoff
            else:
                logger.warning(
                    f"Failed to fetch movie ID {movie_id} after all retries" if movie_id is not None 
                    else f"{step_name}: All {retries} attempts failed"
                )
    
    # Return None instead of raising exception to allow pipeline to continue
    return None
