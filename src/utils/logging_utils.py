# File: src/utils/logging_utils.py

# -*- coding: utf-8 -*-

"""
Logging utilities for the Comtrade Data Pipeline.
Provides centralized logging configuration with rotation, formatting,
and multiple output handlers.
"""

import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional

from loguru import logger


def setup_logger(
    config: Dict[str, Any],
    log_level: str = "INFO",
    log_file: Optional[str] = None,
    console: bool = True
) -> None:
    """
    Setup the logger with the specified configuration.
    
    Args:
        config: Configuration dictionary containing logging settings.
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
        log_file: Optional log file path. If None, a default path will be used.
        console: Whether to log to console.
    """
    # Remove default handlers
    logger.remove()
    
    # Setup log directory
    log_dir = config.get('logging', {}).get('log_dir', 'logs')
    os.makedirs(log_dir, exist_ok=True)
    
    # Default log file path if not provided
    if not log_file:
        current_date = datetime.now().strftime('%Y-%m-%d')
        log_file = os.path.join(log_dir, f"comtrade_pipeline_{current_date}.log")
    
    # Ensure the log directory exists
    log_path = Path(log_file)
    os.makedirs(log_path.parent, exist_ok=True)
    
    # Format for logs
    log_format = (
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
        "<level>{message}</level>"
    )
    
    # Add console handler if requested
    if console:
        logger.add(
            sys.stderr,
            format=log_format,
            level=log_level,
            colorize=True
        )
    
    # Add file handler with rotation
    rotation_size = config.get('logging', {}).get('rotation_size', '100 MB')
    retention_days = config.get('logging', {}).get('retention_days', '30 days')
    compression = config.get('logging', {}).get('compression', 'zip')
    
    logger.add(
        log_file,
        format=log_format,
        level=log_level,
        rotation=rotation_size,
        retention=retention_days,
        compression=compression,
        backtrace=True,
        diagnose=True
    )
    
    logger.info(f"Logging initialized at {log_level} level")
    logger.info(f"Log file: {log_file}")


def get_module_logger(name: str) -> "logger":
    """
    Get a logger for a specific module.
    
    Args:
        name: Module name for the logger.
        
    Returns:
        logger: Configured logger for the module.
    """
    return logger.bind(name=name)


def log_api_call(endpoint: str, params: Dict[str, Any], success: bool, response_size: int = 0) -> None:
    """
    Log an API call with detailed information.
    
    Args:
        endpoint: API endpoint called.
        params: Parameters sent to the API.
        success: Whether the call was successful.
        response_size: Size of the response in bytes.
    """
    status = "SUCCESS" if success else "FAILED"
    
    # Sanitize API keys from log
    if 'subscription-key' in params:
        params = params.copy()
        key = params['subscription-key']
        if key:
            params['subscription-key'] = f"{key[:4]}...{key[-4:]}" if len(key) > 8 else "****"
    
    logger.bind(api_call=True).info(
        f"API Call [{status}] | Endpoint: {endpoint} | "
        f"Params: {params} | Response size: {response_size} bytes"
    )


def log_pipeline_stats(stats: Dict[str, Any]) -> None:
    """
    Log pipeline statistics.
    
    Args:
        stats: Dictionary of pipeline statistics.
    """
    logger.bind(pipeline_stats=True).info(
        f"Pipeline Stats | "
        f"Total calls: {stats.get('total_calls', 0)} | "
        f"API calls: {stats.get('api_calls', 0)} | "
        f"Cache hits: {stats.get('cache_hits', 0)} | "
        f"Processed: {stats.get('processed_records', 0)} | "
        f"Stored: {stats.get('stored_records', 0)} | "
        f"Failed: {stats.get('failed_calls', 0)}"
    )
