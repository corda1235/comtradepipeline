# File: src/pipeline.py

# -*- coding: utf-8 -*-

"""
Main pipeline for the Comtrade Data Pipeline.
Orchestrates the entire process of downloading and storing data.
"""

import time
import os
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple, Union

from src.api import ComtradeAPIClient
from src.cache import CacheManager
from src.database import DatabaseManager
from src.processing import DataProcessor
from src.utils.date_utils import generate_date_ranges
from src.utils.logging_utils import get_module_logger, log_pipeline_stats

# Module logger
logger = get_module_logger("pipeline")


class ComtradePipeline:
    """Main pipeline for the Comtrade Data Pipeline."""

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the pipeline.
        
        Args:
            config: Configuration dictionary.
        """
        self.config = config
        
        # Initialize components
        logger.info("Initializing pipeline components...")
        
        self.api_client = ComtradeAPIClient(config)
        logger.debug("API client initialized")
        
        self.cache_manager = CacheManager(config)
        logger.debug("Cache manager initialized")
        
        self.db_manager = DatabaseManager(config)
        logger.debug("Database manager initialized")
        
        self.data_processor = DataProcessor(self.db_manager)
        logger.debug("Data processor initialized")
        
        # EU countries list
        self.eu_countries = config['eu_countries']
        logger.debug(f"EU countries loaded: {len(self.eu_countries)} countries")
        
        # Pipeline statistics
        self.stats = {
            'total_calls': 0,
            'cache_hits': 0,
            'api_calls': 0,
            'processed_records': 0,
            'stored_records': 0,
            'failed_calls': 0,
            'skipped_records': 0
        }
        
        logger.info("ComtradePipeline initialized")
    
    def _initialize_database(self) -> bool:
        """
        Initialize the database schema.
        
        Returns:
            bool: True if initialization successful, False otherwise.
        """
        logger.info("Initializing database schema...")
        result = self.db_manager.initialize_schema()
        
        if result:
            logger.info("Database schema initialized successfully")
        else:
            logger.error("Failed to initialize database schema")
            
        return result
    
    def _get_api_params(self, reporter_code: str, start_date: str, end_date: str) -> Dict[str, Any]:
        """
        Generate parameters for API call.
        
        Args:
            reporter_code: Country code of the reporting country.
            start_date: Start date in YYYY-MM format.
            end_date: End date in YYYY-MM format.
            
        Returns:
            dict: API call parameters.
        """
        return {
            'reporterCode': reporter_code,
            'partnerCode': 'ALL',  # Get all partner countries
            'period': f"{start_date.replace('-', '')}:{end_date.replace('-', '')}",
            'flowCode': self.config['comtrade']['flow_code'],  # M for imports
            'typeCode': self.config['comtrade']['type_code'],  # C for commodities
            'freqCode': self.config['comtrade']['frequency'],  # M for monthly
            'classCode': self.config['comtrade']['classification'],  # HS classification
            'cmdCode': 'TOTAL',  # Will retrieve all commodity codes
            'subscription-key': None  # Will be added by the API client
        }
    
    def _fetch_data(self, reporter_code: str, start_date: str, end_date: str) -> Tuple[Optional[Dict[str, Any]], bool]:
        """
        Fetch data from cache or API.
        
        Args:
            reporter_code: Country code of the reporting country.
            start_date: Start date in YYYY-MM format.
            end_date: End date in YYYY-MM format.
            
        Returns:
            Tuple containing:
                - The data (or None on failure)
                - Boolean indicating if the call was successful
        """
        # Generate API parameters
        params = self._get_api_params(reporter_code, start_date, end_date)
        
        # Try to get from cache first
        data, is_cache_hit = self.cache_manager.get(params)
        
        if is_cache_hit:
            logger.info(f"Cache hit for reporter {reporter_code}, period {start_date} to {end_date}")
            self.stats['cache_hits'] += 1
            self.stats['total_calls'] += 1
            return data, True
        
        # Not in cache, fetch from API
        logger.info(f"Cache miss for reporter {reporter_code}, period {start_date} to {end_date}. Fetching from API...")
        
        data, success = self.api_client.get_tariffline_data(
            reporter_code=reporter_code,
            period_start=start_date.replace('-', ''),
            period_end=end_date.replace('-', '')
        )
        
        self.stats['api_calls'] += 1
        self.stats['total_calls'] += 1
        
        if not success:
            self.stats['failed_calls'] += 1
            logger.error(f"Failed to fetch data for reporter {reporter_code}, period {start_date} to {end_date}")
            return None, False
        
        # Save to cache
        logger.debug(f"Saving API response to cache for reporter {reporter_code}, period {start_date} to {end_date}")
        self.cache_manager.save(params, data)
        
        return data, True
    
    def _process_and_store(self, data: Dict[str, Any], reporter_code: str, start_date: str, end_date: str) -> bool:
        """
        Process and store fetched data.
        
        Args:
            data: The API response or cached data.
            reporter_code: Country code of the reporting country.
            start_date: Start date in YYYY-MM format.
            end_date: End date in YYYY-MM format.
            
        Returns:
            bool: True if processing and storage successful, False otherwise.
        """
        if not data or 'data' not in data or not data['data']:
            logger.warning(f"No data available for reporter {reporter_code}, period {start_date} to {end_date}")
            return False
        
        # Generate source identifier
        source_id = f"{reporter_code}_{start_date.replace('-', '')}_{end_date.replace('-', '')}"
        
        # Process the data
        logger.info(f"Processing data for reporter {reporter_code}, period {start_date} to {end_date}")
        processed_records = self.data_processor.process_api_response(data, source_id)
        
        if not processed_records:
            logger.warning(f"No valid records for reporter {reporter_code}, period {start_date} to {end_date}")
            return False
        
        self.stats['processed_records'] += len(processed_records)
        logger.info(f"Processed {len(processed_records)} records for reporter {reporter_code}")
        
        # Store in database
        logger.info(f"Storing {len(processed_records)} records for reporter {reporter_code}, period {start_date} to {end_date}")
        inserted, skipped = self.db_manager.bulk_insert_tariffline_data(processed_records, source_id)
        
        self.stats['stored_records'] += inserted
        self.stats['skipped_records'] += skipped
        
        # Log the import operation in the database
        operation_status = "SUCCESS" if inserted > 0 else "PARTIAL" if skipped > 0 else "FAILED"
        self.db_manager.log_import_operation(
            reporter_code=reporter_code,
            start_period=start_date.replace('-', ''),
            end_period=end_date.replace('-', ''),
            stats=self.stats,
            duration=0.0,  # Will be calculated later
            status=operation_status
        )
        
        return inserted > 0
    
    def _process_reporter(self, reporter_code: str, start_date: str, end_date: str) -> bool:
        """
        Process a single reporter country for the given period.
        
        Args:
            reporter_code: Country code of the reporting country.
            start_date: Start date in YYYY-MM format.
            end_date: End date in YYYY-MM format.
            
        Returns:
            bool: True if all processing successful, False otherwise.
        """
        logger.info(f"Processing reporter {reporter_code} for period {start_date} to {end_date}")
        
        # Create smaller date ranges to avoid hitting API record limits
        date_ranges = generate_date_ranges(start_date, end_date, months_per_call=3)
        logger.debug(f"Generated {len(date_ranges)} date ranges for API calls")
        
        success = True
        for i, (range_start, range_end) in enumerate(date_ranges):
            logger.info(f"Processing date range {i+1}/{len(date_ranges)}: {range_start} to {range_end}")
            
            # Fetch data
            data, fetch_success = self._fetch_data(reporter_code, range_start, range_end)
            
            if not fetch_success:
                success = False
                logger.error(f"Failed to fetch data for range {range_start} to {range_end}")
                continue
            
            # Process and store data
            store_success = self._process_and_store(data, reporter_code, range_start, range_end)
            
            if not store_success:
                success = False
                logger.warning(f"No data stored for range {range_start} to {range_end}")
            
            # Sleep a bit to avoid overwhelming the API
            time.sleep(1)
        
        if success:
            logger.info(f"Successfully processed reporter {reporter_code} for period {start_date} to {end_date}")
        else:
            logger.warning(f"Partially processed reporter {reporter_code} for period {start_date} to {end_date}")
            
        return success
    
    def run(self, countries: Union[List[str], str], start_date: str, end_date: str) -> bool:
        """
        Run the pipeline for the specified countries and period.
        
        Args:
            countries: List of country codes or 'all' for all EU countries.
            start_date: Start date in YYYY-MM format.
            end_date: End date in YYYY-MM format.
            
        Returns:
            bool: True if pipeline completed successfully, False otherwise.
        """
        start_time = time.time()
        logger.info(f"Starting pipeline run for period {start_date} to {end_date}")
        
        # Reset statistics
        self.stats = {
            'total_calls': 0,
            'cache_hits': 0,
            'api_calls': 0,
            'processed_records': 0,
            'stored_records': 0,
            'failed_calls': 0,
            'skipped_records': 0
        }
        
        # Initialize database
        if not self._initialize_database():
            logger.error("Failed to initialize database")
            return False
        
        # Determine countries to process
        if countries == 'all':
            target_countries = self.eu_countries
        else:
            # Filter only valid EU countries
            target_countries = [c for c in countries if c in self.eu_countries]
            
            # Log warning if invalid countries were provided
            if isinstance(countries, list) and len(countries) != len(target_countries):
                invalid_countries = set(countries) - set(target_countries)
                logger.warning(f"Ignoring invalid country codes: {', '.join(invalid_countries)}")
        
        logger.info(f"Processing {len(target_countries)} countries: {', '.join(target_countries)}")
        
        # Process each country
        success = True
        for i, country in enumerate(target_countries):
            logger.info(f"Processing country {i+1}/{len(target_countries)}: {country}")
            country_success = self._process_reporter(country, start_date, end_date)
            if not country_success:
                success = False
                logger.warning(f"Problems encountered while processing country {country}")
        
        # Log statistics
        elapsed_time = time.time() - start_time
        logger.info(f"Pipeline completed in {elapsed_time:.2f} seconds")
        
        # Log detailed statistics
        log_pipeline_stats(self.stats)
        
        if success:
            logger.info("Pipeline completed successfully")
        else:
            logger.warning("Pipeline completed with some issues")
        
        return success
