"""
Logger Module
=============
Sets up logging for the ETL pipeline with both file and console output.

This module provides a simple way to create loggers that:
- Write logs to a file for later review
- Display logs in the console for real-time monitoring
- Include timestamps and log levels for easy debugging

Usage:
    from orchestrator.logger import setup_logger
    logger = setup_logger("my_module", "logs/my_module.log")
    logger.info("This is an info message")
"""

import logging
import os
from datetime import datetime


def setup_logger(name: str, log_file: str = None) -> logging.Logger:
    """
    Create and configure a logger with file and console handlers.
    
    Args:
        name: Name of the logger (usually the module name)
        log_file: Path to the log file (optional, will create if provided)
    
    Returns:
        logging.Logger: Configured logger instance
    
    Example:
        >>> logger = setup_logger("extract", "logs/extract.log")
        >>> logger.info("Starting extraction...")
    """
    # Create a logger with the given name
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)  # Capture all levels
    
    # Prevent duplicate handlers if logger already exists
    if logger.handlers:
        return logger
    
    # Define the log message format
    # Format: 2024-01-15 10:30:45 | extract | INFO | Starting extraction...
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    # -------------------------------------------------------------------------
    # Console Handler - Displays logs in terminal
    # -------------------------------------------------------------------------
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)  # Show INFO and above in console
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # -------------------------------------------------------------------------
    # File Handler - Saves logs to file (if path provided)
    # -------------------------------------------------------------------------
    if log_file:
        # Create the logs directory if it doesn't exist
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)
        
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)  # Save all levels to file
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger


def get_pipeline_logger() -> logging.Logger:
    """
    Get the main pipeline logger.
    
    This is a convenience function to get a pre-configured logger
    for the main pipeline orchestration.
    
    Returns:
        logging.Logger: The pipeline logger
    """
    return setup_logger("pipeline", "logs/pipeline.log")


def get_extract_logger() -> logging.Logger:
    """
    Get the extract phase logger.
    
    This logger writes to logs/extract.log for extraction-specific logging.
    
    Returns:
        logging.Logger: The extract logger
    """
    return setup_logger("extract", "logs/extract.log")


def get_transform_logger() -> logging.Logger:
    """
    Get the transform phase logger.
    
    This logger writes to logs/transform.log for transformation-specific logging.
    
    Returns:
        logging.Logger: The transform logger
    """
    return setup_logger("transform", "logs/transform.log")


# =============================================================================
# Quick Test (run this file directly to test)
# =============================================================================
if __name__ == "__main__":
    # Test the logger
    test_logger = setup_logger("test", "logs/test.log")
    test_logger.debug("This is a debug message")
    test_logger.info("This is an info message")
    test_logger.warning("This is a warning message")
    test_logger.error("This is an error message")
    print("Logger test complete! Check logs/test.log")
