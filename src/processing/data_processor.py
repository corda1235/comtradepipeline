# File: src/processing/data_processor.py

# -*- coding: utf-8 -*-

"""
Data processor for the Comtrade Data Pipeline.
Handles transformations and normalization of API data for database storage.
"""

import re
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple

from src.utils.logging_utils import get_module_logger

# Module logger
logger = get_module_logger("data_processor")


class DataProcessor:
    """Processor for transforming and normalizing API data."""

    def __init__(self, db_manager):
        """
        Initialize the data processor.
        
        Args:
            db_manager: Database manager instance for ID lookups.
        """
        self.db_manager = db_manager
        
        # Cache for dimension IDs
        self.reporter_cache = {}
        self.partner_cache = {}
        self.commodity_cache = {}
        self.flow_cache = {}
        
        logger.info("DataProcessor initialized")
    
    def extract_metadata(self, api_response: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Extract metadata entities from API response.
        
        Args:
            api_response: The API response dictionary.
            
        Returns:
            Tuple containing lists of:
                - Reporters
                - Partners
                - Commodities
                - Flows
        """
        reporters = []
        partners = []
        commodities = []
        flows = []
        
        # Extract reporters
        if 'reporterAreas' in api_response:
            for area in api_response['reporterAreas']:
                if 'id' in area and 'text' in area:
                    reporters.append({
                        'reporter_code': area['id'],
                        'reporter_name': area['text']
                    })
        
        # Extract partners
        if 'partnerAreas' in api_response:
            for area in api_response['partnerAreas']:
                if 'id' in area and 'text' in area:
                    partners.append({
                        'partner_code': area['id'],
                        'partner_name': area['text']
                    })
        
        # Extract commodities
        if 'cmdCodes' in api_response:
            for cmd in api_response['cmdCodes']:
                if 'id' in cmd and 'text' in cmd:
                    commodities.append({
                        'commodity_code': cmd['id'],
                        'commodity_description': cmd['text']
                    })
        
        # Extract flows
        if 'flowCodes' in api_response:
            for flow in api_response['flowCodes']:
                if 'id' in flow and 'text' in flow:
                    flows.append({
                        'flow_code': flow['id'],
                        'flow_desc': flow['text']
                    })
        
        logger.info(f"Extracted metadata: {len(reporters)} reporters, {len(partners)} partners, "
                    f"{len(commodities)} commodities, {len(flows)} flows")
        
        return reporters, partners, commodities, flows
    
    def store_metadata(self, api_response: Dict[str, Any]) -> bool:
        """
        Extract and store metadata from the API response.
        
        Args:
            api_response: The API response dictionary.
            
        Returns:
            bool: True if successful, False otherwise.
        """
        reporters, partners, commodities, flows = self.extract_metadata(api_response)
        
        # Store each type of metadata
        success = True
        if reporters:
            if not self.db_manager.upsert_reporters(reporters):
                success = False
                logger.error("Failed to store reporter metadata")
        
        if partners:
            if not self.db_manager.upsert_partners(partners):
                success = False
                logger.error("Failed to store partner metadata")
        
        if commodities:
            if not self.db_manager.upsert_commodities(commodities):
                success = False
                logger.error("Failed to store commodity metadata")
        
        if flows:
            if not self.db_manager.upsert_flows(flows):
                success = False
                logger.error("Failed to store flow metadata")
        
        return success
    
    def get_reporter_id(self, reporter_code: str) -> Optional[int]:
        """
        Get reporter ID, using cache when possible.
        
        Args:
            reporter_code: Reporter country code.
            
        Returns:
            int: Reporter ID if found, None otherwise.
        """
        if reporter_code in self.reporter_cache:
            return self.reporter_cache[reporter_code]
        
        reporter_id = self.db_manager.get_reporter_id(reporter_code)
        if reporter_id:
            self.reporter_cache[reporter_code] = reporter_id
        else:
            logger.warning(f"Reporter ID not found for code: {reporter_code}")
            
        return reporter_id
    
    def get_partner_id(self, partner_code: str) -> Optional[int]:
        """
        Get partner ID, using cache when possible.
        
        Args:
            partner_code: Partner country code.
            
        Returns:
            int: Partner ID if found, None otherwise.
        """
        if partner_code in self.partner_cache:
            return self.partner_cache[partner_code]
        
        partner_id = self.db_manager.get_partner_id(partner_code)
        if partner_id:
            self.partner_cache[partner_code] = partner_id
        else:
            logger.warning(f"Partner ID not found for code: {partner_code}")
            
        return partner_id
    
    def get_commodity_id(self, commodity_code: str) -> Optional[int]:
        """
        Get commodity ID, using cache when possible.
        
        Args:
            commodity_code: Commodity code.
            
        Returns:
            int: Commodity ID if found, None otherwise.
        """
        if commodity_code in self.commodity_cache:
            return self.commodity_cache[commodity_code]
        
        commodity_id = self.db_manager.get_commodity_id(commodity_code)
        if commodity_id:
            self.commodity_cache[commodity_code] = commodity_id
        else:
            logger.warning(f"Commodity ID not found for code: {commodity_code}")
            
        return commodity_id
    
    def get_flow_id(self, flow_code: str) -> Optional[int]:
        """
        Get flow ID, using cache when possible.
        
        Args:
            flow_code: Flow code.
            
        Returns:
            int: Flow ID if found, None otherwise.
        """
        if flow_code in self.flow_cache:
            return self.flow_cache[flow_code]
        
        flow_id = self.db_manager.get_flow_id(flow_code)
        if flow_id:
            self.flow_cache[flow_code] = flow_id
        else:
            logger.warning(f"Flow ID not found for code: {flow_code}")
            
        return flow_id
    
    def parse_period(self, period: str) -> Tuple[Optional[int], Optional[int]]:
        """
        Parse period string into year and month.
        
        Args:
            period: Period string (e.g., '202201' for January 2022).
            
        Returns:
            Tuple containing:
                - Year (int)
                - Month (int)
        """
        if not period or len(period) != 6:
            logger.warning(f"Invalid period format: {period}")
            return None, None
        
        try:
            year = int(period[:4])
            month = int(period[4:])
            
            if year < 1900 or year > 2100 or month < 1 or month > 12:
                logger.warning(f"Period out of range: {period}")
                return None, None
                
            return year, month
            
        except ValueError:
            logger.warning(f"Could not parse period: {period}")
            return None, None
    
    def safe_float(self, value: Any) -> Optional[float]:
        """
        Safely convert value to float.
        
        Args:
            value: Value to convert.
            
        Returns:
            float: Converted value, or None if conversion fails.
        """
        if value is None:
            return None
        
        try:
            return float(value)
        except (ValueError, TypeError):
            logger.debug(f"Could not convert to float: {value}")
            return None
    
    def safe_int(self, value: Any) -> Optional[int]:
        """
        Safely convert value to int.
        
        Args:
            value: Value to convert.
            
        Returns:
            int: Converted value, or None if conversion fails.
        """
        if value is None:
            return None
        
        try:
            return int(value)
        except (ValueError, TypeError):
            logger.debug(f"Could not convert to int: {value}")
            return None
    
    def safe_bool(self, value: Any) -> Optional[bool]:
        """
        Safely convert value to boolean.
        
        Args:
            value: Value to convert.
            
        Returns:
            bool: Converted value, or None if conversion fails.
        """
        if value is None:
            return None
        
        if isinstance(value, bool):
            return value
            
        if isinstance(value, str):
            value = value.lower()
            if value in ('true', 'yes', '1', 't', 'y'):
                return True
            elif value in ('false', 'no', '0', 'f', 'n'):
                return False
        
        try:
            return bool(int(value))
        except (ValueError, TypeError):
            logger.debug(f"Could not convert to bool: {value}")
            return None
    
    def process_tariffline_record(self, record: Dict[str, Any], source_file: str = None) -> Optional[Dict[str, Any]]:
        """
        Process a single tariffline record.
        
        Args:
            record: Raw record from API.
            source_file: Optional source file identifier.
            
        Returns:
            dict: Processed record ready for database insertion, or None if invalid.
        """
        if not record:
            return None
        
        try:
            # Get dimensional IDs
            reporter_id = self.get_reporter_id(str(record.get('reporterCode')))
            partner_id = self.get_partner_id(str(record.get('partnerCode')))
            commodity_id = self.get_commodity_id(str(record.get('cmdCode')))
            flow_id = self.get_flow_id(str(record.get('flowCode')))
            
            # Check if all required dimensional IDs are available
            if not all([reporter_id, partner_id, commodity_id, flow_id]):
                missing = []
                if not reporter_id:
                    missing.append(f"reporter_id for {record.get('reporterCode')}")
                if not partner_id:
                    missing.append(f"partner_id for {record.get('partnerCode')}")
                if not commodity_id:
                    missing.append(f"commodity_id for {record.get('cmdCode')}")
                if not flow_id:
                    missing.append(f"flow_id for {record.get('flowCode')}")
                
                logger.warning(f"Missing dimensional IDs: {', '.join(missing)}")
                return None
            
            # Get period information
            period = str(record.get('period'))
            year, month = self.parse_period(period)
            
            if year is None or month is None:
                logger.warning(f"Invalid period format: {period}")
                return None
            
            # Prepare processed record
            processed_record = {
                'reporter_id': reporter_id,
                'partner_id': partner_id,
                'commodity_id': commodity_id,
                'flow_id': flow_id,
                'period': period,
                'year': year,
                'month': month,
                'net_weight': self.safe_float(record.get('netWgt')),
                'quantity': self.safe_float(record.get('qty')),
                'quantity_unit': record.get('qtyUnit'),
                'trade_value': self.safe_float(record.get('primaryValue')),
                'flag': self.safe_int(record.get('flag')),
                'is_reporter_estimate': self.safe_bool(record.get('isReporterEstimate')),
                'customs': self.safe_float(record.get('customs')),
                'qty_unit_code': record.get('qtyUnitCode'),
                'qty_unit': record.get('qtyUnit'),
                'alt_qty': self.safe_float(record.get('altQty')),
                'alt_qty_unit_code': record.get('altQtyUnitCode'),
                'gross_weight': self.safe_float(record.get('grossWgt')),
                'cif_value': self.safe_float(record.get('cifvalue')),
                'fob_value': self.safe_float(record.get('fobvalue')),
                'source_file': source_file
            }
            
            return processed_record
            
        except Exception as e:
            logger.exception(f"Error processing record: {str(e)}")
            return None
    
    def process_api_response(self, api_response: Dict[str, Any], source_identifier: str = None) -> List[Dict[str, Any]]:
        """
        Process the entire API response.
        
        Args:
            api_response: The complete API response.
            source_identifier: Optional identifier for the data source.
            
        Returns:
            list: List of processed records ready for database insertion.
        """
        processed_records = []
        
        if not api_response or 'data' not in api_response or not api_response['data']:
            logger.warning("API response contains no data")
            return processed_records
        
        # Store metadata first
        logger.info("Storing metadata from API response")
        metadata_success = self.store_metadata(api_response)
        
        if not metadata_success:
            logger.warning("Some metadata could not be stored")
        
        # Process each record
        raw_records = api_response['data']
        logger.info(f"Processing {len(raw_records)} raw records")
        
        # Track processed and failed records
        processed_count = 0
        failed_count = 0
        
        for raw_record in raw_records:
            processed_record = self.process_tariffline_record(raw_record, source_identifier)
            if processed_record:
                processed_records.append(processed_record)
                processed_count += 1
            else:
                failed_count += 1
        
        if failed_count > 0:
            logger.warning(f"Failed to process {failed_count} records")
            
        logger.info(f"Successfully processed {processed_count} records, failed {failed_count} records")
        return processed_records
