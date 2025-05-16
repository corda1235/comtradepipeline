# File: main.py

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Main entry point for the Comtrade Data Pipeline.
This script downloads and stores UN Comtrade tariffline data for EU countries.
"""

import sys
import time
import argparse
from datetime import datetime
from typing import List, Union, Optional

from src.pipeline import ComtradePipeline
from src.utils.config_loader import load_config
from src.utils.date_utils import parse_date
from src.utils.logging_utils import setup_logger, get_module_logger
from src.monitoring.monitor import PipelineMonitor

# Module logger
logger = get_module_logger("main")


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
        help='Start date in YYYY-MM format'
    )
    
    parser.add_argument(
        '--end-date', 
        type=str, 
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
    
    # Monitoring parameters
    parser.add_argument(
        '--daily-report',
        action='store_true',
        help='Generate a daily report after execution'
    )
    
    parser.add_argument(
        '--no-alerts',
        action='store_true',
        help='Disable failure alerts'
    )

    return parser.parse_args()


def validate_args(args):
    """
    Validate command line arguments.
    
    Args:
        args: Command line arguments.
        
    Returns:
        bool: True if arguments are valid, False otherwise.
    """
    # Skip validation for database initialization only
    if args.db_init_only:
        return True
    
    # Validate dates
    if not args.start_date or not args.end_date:
        logger.error("Start date and end date are required")
        return False
    
    try:
        start_date = parse_date(args.start_date)
        end_date = parse_date(args.end_date)
        
        if end_date < start_date:
            logger.error("End date must be after start date")
            return False
            
    except ValueError as e:
        logger.error(f"Date validation error: {str(e)}")
        return False
    
    # Validate country codes
    if args.countries != 'all':
        country_codes = args.countries.split(',')
        if not country_codes:
            logger.error("No country codes provided")
            return False
    
    return True


def get_countries_list(countries_arg: str, eu_countries: List[str]) -> List[str]:
    """
    Get list of countries to process.
    
    Args:
        countries_arg: Countries argument from command line.
        eu_countries: List of all EU countries.
        
    Returns:
        List of country codes to process.
    """
    if countries_arg == 'all':
        return eu_countries
    
    country_codes = countries_arg.split(',')
    
    # Filter only valid EU countries
    valid_countries = [c for c in country_codes if c in eu_countries]
    
    if len(valid_countries) != len(country_codes):
        invalid_countries = set(country_codes) - set(valid_countries)
        logger.warning(f"Ignored invalid country codes: {', '.join(invalid_countries)}")
    
    if not valid_countries:
        logger.warning("No valid country codes provided, using all EU countries")
        return eu_countries
    
    return valid_countries


def handle_cache(pipeline, args):
    """
    Handle cache operations.
    
    Args:
        pipeline: Pipeline instance.
        args: Command line arguments.
    """
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
    
    # Load configuration
    config = load_config()
    
    # Setup logging
    setup_logger(
        config=config,
        log_level=args.log_level,
        log_file=args.log_file
    )
    
    # Validate arguments after logging is set up
    if not validate_args(args):
        sys.exit(1)
    
    logger.info('Configuration loaded')
    
    # Set up monitoring
    monitor = PipelineMonitor(config)
    
    # Update monitoring configuration from command line args
    if args.no_alerts and 'monitoring' in config:
        config['monitoring']['alerts']['enabled'] = False
    
    start_time = time.time()
    success = False
    pipeline_stats = {}
    countries_list = []
    
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
        
        # Get list of countries to process
        countries_list = get_countries_list(args.countries, config['eu_countries'])
        
        # Run the pipeline
        logger.info(f"Starting Comtrade Data Pipeline for period {args.start_date} to {args.end_date}")
        logger.info(f"Processing {len(countries_list)} countries: {', '.join(countries_list)}")
        
        success = pipeline.run(
            countries=countries_list,
            start_date=args.start_date,
            end_date=args.end_date
        )
        
        # Get pipeline statistics
        pipeline_stats = pipeline.stats
        
        if success:
            logger.info('Pipeline completed successfully')
        else:
            logger.error('Pipeline completed with errors')
            
    except Exception as e:
        logger.exception(f'Pipeline failed with error: {e}')
        success = False
    finally:
        # Calculate execution time
        execution_time = time.time() - start_time
        logger.info(f"Total execution time: {execution_time:.2f} seconds")
        
        # Save execution statistics
        monitor.save_execution_stats(
            stats=pipeline_stats,
            countries=countries_list,
            start_date=args.start_date if hasattr(args, 'start_date') else None,
            end_date=args.end_date if hasattr(args, 'end_date') else None,
            execution_time=execution_time,
            success=success
        )
        
        # Generate daily report if requested
        if args.daily_report:
            report_file = monitor.generate_daily_report()
            if report_file:
                logger.info(f"Daily report generated: {report_file}")
    
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
