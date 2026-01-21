"""
Retry Module
============
Provides retry logic with exponential backoff for handling transient failures.

This module provides a way to automatically retry failed operations,
waiting a bit longer between each attempt (exponential backoff).

    )
"""

import time
import logging
from typing import Callable, TypeVar, Any

# TypeVar for generic return type
T = TypeVar('T')


def run_with_retry(
    func: Callable[[], T],
    retries: int = 3,
    delay: float = 2.0,
    logger: logging.Logger = None,
    step_name: str = "Operation"
) -> T:
    """
    Execute a function with retry logic and exponential backoff.
    
    If the function fails, it will wait and try again. Each retry
    waits longer than the previous one (exponential backoff).
    
    Args:
        func: The function to execute (should take no arguments)
        retries: Maximum number of retry attempts (default: 3)
        delay: Initial delay between retries in seconds (default: 2.0)
        logger: Logger for recording attempts and failures
        step_name: Name of the step for logging purposes
    
    Returns:
        The result of the function if successful
    
    Raises:
        Exception: The last exception if all retries fail
    
    Example:
        >>> def risky_operation():
        ...     # This might fail sometimes
        ...     return fetch_from_api()
        >>> 
        >>> result = run_with_retry(
        ...     func=risky_operation,
        ...     retries=3,
        ...     logger=my_logger,
        ...     step_name="Fetch Movies"
        ... )
    """
    last_exception = None
    
    for attempt in range(1, retries + 1):
        try:
            # Log the attempt
            if logger:
                logger.info(f"{step_name}: Attempt {attempt}/{retries}")
            
            # Try to execute the function
            result = func()
            
            # Success! Log and return
            if logger:
                logger.info(f"{step_name}: Completed successfully")
            return result
            
        except Exception as e:
            last_exception = e
            
            # Log the failure
            if logger:
                logger.warning(
                    f"{step_name}: Attempt {attempt}/{retries} failed - {str(e)}"
                )
            
            # If we have more retries, wait before trying again
            if attempt < retries:
                # Calculate wait time with exponential backoff
                # Attempt 1: delay * 1 = 2 seconds
                # Attempt 2: delay * 2 = 4 seconds
                # Attempt 3: delay * 4 = 8 seconds
                wait_time = delay * (2 ** (attempt - 1))
                
                if logger:
                    logger.info(f"Waiting {wait_time:.1f} seconds before retry...")
                
                time.sleep(wait_time)
    
    # All retries exhausted - raise the last exception
    if logger:
        logger.error(f"{step_name}: All {retries} attempts failed!")
    
    raise last_exception


# =============================================================================
# Quick Test (run this file directly to test)
# =============================================================================
if __name__ == "__main__":
    from orchestrator.logger import setup_logger
    
    # Create a test logger
    test_logger = setup_logger("retry_test")
    
    # Counter to simulate failures
    attempt_counter = {"count": 0}
    
    def flaky_function():
        """A function that fails the first 2 times, then succeeds."""
        attempt_counter["count"] += 1
        if attempt_counter["count"] < 3:
            raise ConnectionError("Simulated network error")
        return "Success!"
    
    # Test the retry logic
    try:
        result = run_with_retry(
            func=flaky_function,
            retries=3,
            delay=1,
            logger=test_logger,
            step_name="Test Operation"
        )
        print(f"Result: {result}")
    except Exception as e:
        print(f"Final error: {e}")
