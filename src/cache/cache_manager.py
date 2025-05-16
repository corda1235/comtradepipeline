# File: src/cache/cache_manager.py

# -*- coding: utf-8 -*-

"""
Cache manager for the Comtrade Data Pipeline.
Handles caching of API responses to reduce API calls.
"""

import os
import json
import hashlib
import time
from datetime import datetime, timedelta
from typing import Dict, Any, Tuple, Optional, List

from src.utils.logging_utils import get_module_logger

# Module logger
logger = get_module_logger("cache")


class CacheManager:
    """Manager for API response caching."""

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the cache manager.
        
        Args:
            config: Configuration dictionary containing cache settings.
        """
        self.config = config
        self.cache_dir = config['cache'].get('cache_dir', 'cache')
        self.enabled = config['cache'].get('enabled', True)
        self.ttl_days = config['cache'].get('ttl_days', 30)
        
        # Create cache directory if it doesn't exist
        if self.enabled:
            os.makedirs(self.cache_dir, exist_ok=True)
            logger.info(f"Cache initialized at {os.path.abspath(self.cache_dir)}")
        else:
            logger.info("Cache is disabled")
    
    def _generate_cache_key(self, params: Dict[str, Any]) -> str:
        """
        Generate a cache key from API parameters.
        
        Args:
            params: API call parameters.
            
        Returns:
            str: Cache key.
        """
        # Clone and sort parameters to ensure consistent keys
        sorted_params = dict(sorted(params.items()))
        
        # Remove subscription key from parameters for caching
        if 'subscription-key' in sorted_params:
            sorted_params.pop('subscription-key')
        
        # Generate key using MD5 hash of serialized parameters
        params_str = json.dumps(sorted_params, sort_keys=True)
        return hashlib.md5(params_str.encode()).hexdigest()
    
    def _get_cache_path(self, cache_key: str) -> str:
        """
        Get the file path for a cache key.
        
        Args:
            cache_key: Cache key.
            
        Returns:
            str: Cache file path.
        """
        return os.path.join(self.cache_dir, f"{cache_key}.json")
    
    def get(self, params: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], bool]:
        """
        Get data from cache if available.
        
        Args:
            params: API call parameters.
            
        Returns:
            Tuple containing:
                - The cached data (or None if not in cache)
                - Boolean indicating if the data was found in cache
        """
        if not self.enabled:
            logger.debug("Cache is disabled, skipping cache lookup")
            return None, False
        
        # Generate cache key
        cache_key = self._generate_cache_key(params)
        cache_path = self._get_cache_path(cache_key)
        
        # Check if cache file exists
        if not os.path.exists(cache_path):
            logger.debug(f"Cache miss for key: {cache_key}")
            return None, False
        
        try:
            # Check cache file age
            file_age_days = (datetime.now() - datetime.fromtimestamp(os.path.getmtime(cache_path))).days
            
            if file_age_days > self.ttl_days:
                logger.debug(f"Cache expired for key: {cache_key} (age: {file_age_days} days, TTL: {self.ttl_days} days)")
                return None, False
            
            # Load cached data
            with open(cache_path, 'r') as f:
                data = json.load(f)
            
            logger.debug(f"Cache hit for key: {cache_key} (age: {file_age_days} days)")
            return data, True
            
        except (json.JSONDecodeError, IOError) as e:
            logger.error(f"Error reading cache file {cache_path}: {str(e)}")
            return None, False
    
    def save(self, params: Dict[str, Any], data: Dict[str, Any]) -> bool:
        """
        Save data to cache.
        
        Args:
            params: API call parameters.
            data: Data to cache.
            
        Returns:
            bool: True if save successful, False otherwise.
        """
        if not self.enabled:
            logger.debug("Cache is disabled, skipping cache save")
            return False
        
        # Generate cache key
        cache_key = self._generate_cache_key(params)
        cache_path = self._get_cache_path(cache_key)
        
        try:
            # Save data to cache
            with open(cache_path, 'w') as f:
                json.dump(data, f)
            
            logger.debug(f"Saved data to cache: {cache_key}")
            return True
            
        except IOError as e:
            logger.error(f"Error saving to cache file {cache_path}: {str(e)}")
            return False
    
    def clear(self, days_old: Optional[int] = None) -> int:
        """
        Clear cache files.
        
        Args:
            days_old: If provided, only clear files older than this many days.
            
        Returns:
            int: Number of files cleared.
        """
        if not self.enabled:
            logger.debug("Cache is disabled, skipping cache clear")
            return 0
        
        if not os.path.exists(self.cache_dir):
            logger.warning(f"Cache directory {self.cache_dir} does not exist")
            return 0
        
        count = 0
        now = datetime.now()
        
        for filename in os.listdir(self.cache_dir):
            if not filename.endswith('.json'):
                continue
            
            file_path = os.path.join(self.cache_dir, filename)
            
            # Check file age if days_old is provided
            if days_old is not None:
                file_time = datetime.fromtimestamp(os.path.getmtime(file_path))
                file_age_days = (now - file_time).days
                
                if file_age_days < days_old:
                    continue
            
            try:
                os.remove(file_path)
                count += 1
            except OSError as e:
                logger.error(f"Error removing cache file {file_path}: {str(e)}")
        
        if days_old is not None:
            logger.info(f"Cleared {count} cache files older than {days_old} days")
        else:
            logger.info(f"Cleared {count} cache files")
        
        return count
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the cache.
        
        Returns:
            dict: Cache statistics.
        """
        if not self.enabled or not os.path.exists(self.cache_dir):
            return {
                'enabled': self.enabled,
                'count': 0,
                'size_bytes': 0,
                'oldest_file_days': 0,
                'newest_file_days': 0
            }
        
        files = [os.path.join(self.cache_dir, f) for f in os.listdir(self.cache_dir) if f.endswith('.json')]
        
        if not files:
            return {
                'enabled': self.enabled,
                'count': 0,
                'size_bytes': 0,
                'oldest_file_days': 0,
                'newest_file_days': 0
            }
        
        # Get file stats
        now = datetime.now()
        file_stats = []
        
        for file_path in files:
            try:
                mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
                age_days = (now - mtime).days
                size = os.path.getsize(file_path)
                
                file_stats.append({
                    'path': file_path,
                    'age_days': age_days,
                    'size': size
                })
            except OSError:
                continue
        
        if not file_stats:
            return {
                'enabled': self.enabled,
                'count': 0,
                'size_bytes': 0,
                'oldest_file_days': 0,
                'newest_file_days': 0
            }
        
        # Calculate statistics
        oldest_file = max(file_stats, key=lambda x: x['age_days'])
        newest_file = min(file_stats, key=lambda x: x['age_days'])
        total_size = sum(f['size'] for f in file_stats)
        
        return {
            'enabled': self.enabled,
            'count': len(file_stats),
            'size_bytes': total_size,
            'size_mb': total_size / (1024 * 1024),
            'oldest_file_days': oldest_file['age_days'],
            'newest_file_days': newest_file['age_days']
        }