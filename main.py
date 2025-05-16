# File: main.py (aggiornamento completo)

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Main entry point for the Comtrade Data Pipeline.
This script downloads and stores UN Comtrade tariffline data for EU countries.
"""

import sys
import argparse
from datetime import datetime
from loguru import logger

from src.pipeline import ComtradePipeline
from src.utils.config_loader import load_config
from src.utils.date_utils import parse_date


def setup_argparse():
    """Set up command line arguments."""
    parser = argparse.ArgumentParser(description='Download and store UN Comtrade data for EU countries.')
    
    # Countries parameter
    parser.add_argument(
        '--countries', 
        type=str, 
        help='Comma-separated list of EU country codes or "all" for all 27 EU countries', 
        default='all'
    )
    
    # Date parameters
    parser.add_argument(
        '--start-date', 
        type=str, 
        required=True, 
        help='Start date in YYYY-MM format'
    )
    
    parser.add_argument(
        '--end-date', 
        type=str, 
        required=True, 
        help='End date in YYYY-MM format'
    )
    
    # Logging parameters
    parser.add_argument(
        '--log-level', 
        type=str, 
        default='INFO', 
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help='Set the logging level'
    )
    
    parser.add_argument(
        '--log-file', 
        type=str, 
        help='Optional log file path (default: logs/comtrade_pipeline_YYYY-MM-DD.log)'
    )
    
    # Cache parameters
    parser.add_argument(
        '--clear-cache', 
        action='store_true', 
        help='Clear the cache before running'
    )
    
    parser.add_argument(
        '--cache-days', 
        type=int, 
        default=None,
        help='Clear cache entries older than this many days'
    )
    
    # Database parameters
    parser.add_argument(
        '--db-init-only', 
        action='store_true', 
        help='Only initialize the database schema and exit'
    )

    return parser.parse_args()


def validate_args(args):
    """Validate command line arguments."""
    # Validate dates
    try:
        start_date = parse_date(args.start_date)
        end_date = parse_date(args.end_date)
        
        if end_date < start_date:
            logger.error("End date must be after start date")
            return False
            
    except ValueError as e:
        logger.error(str(e))
        return False
    
    # Validate country codes
    if args.countries != 'all':
        country_codes = args.countries.split(',')
        if not country_codes:
            logger.error("No country codes provided")
            return False
    
    return True


def setup_logging(args):
    """Configure logging."""
    logger.remove()  # Remove default handler
    
    # Add stderr handler
    logger.add(sys.stderr, level=args.log_level)
    
    # Add file handler with rotation
    log_file = args.log_file
    if not log_file:
        current_date = datetime.now().strftime('%Y-%m-%d')
        log_file = f"logs/comtrade_pipeline_{current_date}.log"
    
    logger.add(
        log_file, 
        rotation='100 MB', 
        retention='30 days', 
        level=args.log_level
    )
    
    logger.info(f"Logging initialized at {args.log_level} level")
    logger.info(f"Log file: {log_file}")


def handle_cache(pipeline, args):
    """Handle cache operations."""
    if args.clear_cache:
        if args.cache_days:
            count = pipeline.cache_manager.clear(days_old=args.cache_days)
            logger.info(f"Cleared {count} cache entries older than {args.cache_days} days")
        else:
            count = pipeline.cache_manager.clear()
            logger.info(f"Cleared {count} cache entries")


def main():
    """Main function to run the pipeline."""
    # Parse and validate arguments
    args = setup_argparse()
    if not validate_args(args):
        sys.exit(1)
    
    # Setup logging
    setup_logging(args)
    
    # Load configuration
    config = load_config()
    logger.info('Configuration loaded')
    
    try:
        # Initialize pipeline
        pipeline = ComtradePipeline(config)
        logger.info('Pipeline initialized')
        
        # Handle cache operations
        handle_cache(pipeline, args)
        
        # If only initializing database, do that and exit
        if args.db_init_only:
            if pipeline.db_manager.initialize_schema():
                logger.info('Database schema initialized successfully')
                sys.exit(0)
            else:
                logger.error('Failed to initialize database schema')
                sys.exit(1)
        
        # Run the pipeline
        logger.info(f"Starting Comtrade Data Pipeline for period {args.start_date} to {args.end_date}")
        
        countries = args.countries.split(',') if args.countries != 'all' else 'all'
        success = pipeline.run(
            countries=countries,
            start_date=args.start_date,
            end_date=args.end_date
        )
        
        if success:
            logger.info('Pipeline completed successfully')
            sys.exit(0)
        else:
            logger.error('Pipeline completed with errors')
            sys.exit(1)
            
    except Exception as e:
        logger.exception(f'Pipeline failed with error: {e}')
        sys.exit(1)


if __name__ == '__main__':
    main()