# File: src/api/client.py

# -*- coding: utf-8 -*-

"""
Comtrade API client for making API calls to UN Comtrade.
This handles rate limiting, switching between API keys, and retries.
"""

import time
import random
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, Tuple

import comtradeapicall
import requests
from loguru import logger


class ComtradeAPIClient:
    """Client for interacting with the UN Comtrade API."""

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the Comtrade API client.
        
        Args:
            config: Configuration dictionary containing API keys and limits.
        """
        self.primary_key = config['api']['primary_key']
        self.secondary_key = config['api']['secondary_key']
        self.daily_limit = config['api']['daily_limit']
        self.record_limit = config['api']['record_limit']
        
        # API call tracking
        self.call_count = 0
        self.reset_date = datetime.now().date() + timedelta(days=1)
        
        # Current active key
        self.current_key = self.primary_key
        self.use_primary = True
        
        # Retry configuration
        self.max_retries = 5
        self.base_retry_delay = 2  # seconds
        
        logger.info("ComtradeAPIClient initialized")
        
    def _should_switch_key(self) -> bool:
        """
        Determine if we should switch to the secondary key.
        
        Returns:
            bool: True if we should switch keys, False otherwise.
        """
        # If half of daily limit is reached, switch to secondary key
        return self.use_primary and self.call_count >= (self.daily_limit // 2) and self.secondary_key
    
    def _switch_key(self) -> None:
        """Switch between primary and secondary API keys."""
        if self.use_primary and self.secondary_key:
            self.current_key = self.secondary_key
            self.use_primary = False
            logger.info("Switched to secondary API key")
        elif not self.use_primary and self.primary_key:
            self.current_key = self.primary_key
            self.use_primary = True
            logger.info("Switched to primary API key")
        else:
            logger.warning("Cannot switch keys - only one key is available")
    
    def _reset_call_count(self) -> None:
        """Reset the API call count if a new day has started."""
        today = datetime.now().date()
        if today >= self.reset_date:
            logger.info(f"Resetting API call count (was {self.call_count})")
            self.call_count = 0
            self.reset_date = today + timedelta(days=1)
    
    def _increment_call_count(self) -> None:
        """Increment the API call count."""
        self.call_count += 1
        logger.debug(f"API call count: {self.call_count}/{self.daily_limit}")
        
        # Switch key if necessary
        if self._should_switch_key():
            self._switch_key()
    
    def get_tariffline_data(
        self,
        reporter_code: str,
        partner_code: str = 'ALL',
        period_start: str = None,
        period_end: str = None,
        hs_code: str = 'TOTAL',
        flow_code: str = 'M',
        type_code: str = 'C',
        frequency: str = 'M',
        partner2_code: str = None,
        custom_args: Dict[str, Any] = None
    ) -> Tuple[Optional[Dict[str, Any]], bool]:
        """
        Fetch tariffline data from UN Comtrade API.
        
        Args:
            reporter_code: Country code of the reporting country
            partner_code: Country code of the partner country, default 'ALL'
            period_start: Start period in YYYY-MM format
            period_end: End period in YYYY-MM format
            hs_code: HS code, default 'TOTAL'
            flow_code: Flow code (M=import, X=export), default 'M'
            type_code: Type code (C=commodities), default 'C'
            frequency: Frequency (M=monthly), default 'M'
            partner2_code: Second partner code, default None
            custom_args: Additional arguments to pass to the API
            
        Returns:
            Tuple containing:
                - The API response as a dictionary (or None on failure)
                - Boolean indicating if the call was successful
        """
        self._reset_call_count()
        
        # Check if we've reached the daily limit
        if self.call_count >= self.daily_limit:
            logger.error(f"Daily API call limit reached ({self.daily_limit})")
            return None, False
        
        # Prepare the API call parameters
        params = {
            'subscription-key': self.current_key,
            'typeCode': type_code,
            'freqCode': frequency,
            'reporterCode': reporter_code,
            'partnerCode': partner_code,
            'flowCode': flow_code,
            'period': f"{period_start}:{period_end}",
            'cmdCode': hs_code
        }
        
        # Add partner2Code if provided
        if partner2_code:
            params['partner2Code'] = partner2_code
            
        # Add any custom parameters
        if custom_args:
            params.update(custom_args)
            
        # Perform the API call with retries
        for attempt in range(1, self.max_retries + 1):
            try:
                logger.debug(f"Making tariffline API call (attempt {attempt}): {params}")
                data = comtradeapicall.getTarifflineData(params)
                
                # Check for valid response
                if isinstance(data, dict) and 'data' in data:
                    self._increment_call_count()
                    
                    # Check if number of records exceeds limit
                    count = len(data.get('data', []))
                    if count >= self.record_limit:
                        logger.warning(
                            f"API response contains {count} records, which is at or near the limit "
                            f"({self.record_limit}). Some data may be missing."
                        )
                    
                    logger.debug(f"API call successful, retrieved {count} records")
                    return data, True
                
                error_msg = data.get('error', {}).get('message', str(data)) if isinstance(data, dict) else str(data)
                logger.error(f"API call failed: {error_msg}")
                
                # Handle specific error cases
                if isinstance(data, dict) and 'error' in data:
                    if 'rate limit' in str(data['error']).lower():
                        # Switch keys on rate limit errors
                        if self.secondary_key and self.use_primary:
                            self._switch_key()
                            # Wait a bit before retrying
                            time.sleep(5)
                            continue
                    
                # For other errors, use exponential backoff
                retry_delay = self.base_retry_delay * (2 ** (attempt - 1)) + random.uniform(0, 1)
                logger.info(f"Retrying in {retry_delay:.2f} seconds...")
                time.sleep(retry_delay)
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Request exception: {str(e)}")
                retry_delay = self.base_retry_delay * (2 ** (attempt - 1)) + random.uniform(0, 1)
                logger.info(f"Retrying in {retry_delay:.2f} seconds...")
                time.sleep(retry_delay)
                
            except Exception as e:
                logger.exception(f"Unexpected error during API call: {str(e)}")
                retry_delay = self.base_retry_delay * (2 ** (attempt - 1)) + random.uniform(0, 1)
                logger.info(f"Retrying in {retry_delay:.2f} seconds...")
                time.sleep(retry_delay)
        
        # All retries failed
        logger.error(f"All {self.max_retries} attempts failed. Giving up.")
        return None, False