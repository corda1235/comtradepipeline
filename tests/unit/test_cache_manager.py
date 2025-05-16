# File: tests/test_cache_manager.py
# -*- coding: utf-8 -*-

"""
Unit tests for the cache manager.
"""

import os
import json
import pytest
from datetime import datetime, timedelta

from src.cache.cache_manager import CacheManager


class TestCacheManager:
    """Tests for CacheManager class."""
    
    def test_initialization(self, test_config, setup_test_environment):
        """Test initialization of cache manager."""
        cache_manager = CacheManager(test_config)
        assert cache_manager.enabled == test_config['cache']['enabled']
        assert cache_manager.cache_dir == test_config['cache']['cache_dir']
        assert cache_manager.ttl_days == test_config['cache']['ttl_days']
        assert os.path.exists(cache_manager.cache_dir)
    
    def test_cache_key_generation(self, test_config, setup_test_environment):
        """Test generation of cache keys."""
        cache_manager = CacheManager(test_config)
        
        # Test with basic parameters
        params1 = {
            'reporterCode': 'DE',
            'partnerCode': 'ALL',
            'period': '202201:202203',
            'flowCode': 'M',
            'subscription-key': 'test_key'  # Should be removed from key generation
        }
        
        key1 = cache_manager._generate_cache_key(params1)
        assert isinstance(key1, str)
        assert len(key1) > 0
        
        # Test that subscription-key is ignored
        params2 = params1.copy()
        params2['subscription-key'] = 'different_key'
        key2 = cache_manager._generate_cache_key(params2)
        assert key1 == key2
        
        # Test with different parameters
        params3 = params1.copy()
        params3['reporterCode'] = 'FR'
        key3 = cache_manager._generate_cache_key(params3)
        assert key1 != key3
    
    def test_cache_save_and_get(self, test_config, setup_test_environment, mock_api_response):
        """Test saving and retrieving from cache."""
        cache_manager = CacheManager(test_config)
        
        # Parameters for testing
        params = {
            'reporterCode': 'DE',
            'partnerCode': 'ALL',
            'period': '202201:202203',
            'flowCode': 'M',
            'subscription-key': 'test_key'
        }
        
        # Save to cache
        result = cache_manager.save(params, mock_api_response)
        assert result is True
        
        # Check that file exists
        cache_key = cache_manager._generate_cache_key(params)
        cache_path = cache_manager._get_cache_path(cache_key)
        assert os.path.exists(cache_path)
        
        # Get from cache
        data, is_cache_hit = cache_manager.get(params)
        assert is_cache_hit is True
        assert data == mock_api_response
        
        # Test with nonexistent data
        params['reporterCode'] = 'FR'
        data, is_cache_hit = cache_manager.get(params)
        assert is_cache_hit is False
        assert data is None
    
    def test_cache_ttl(self, test_config, setup_test_environment, mock_api_response, monkeypatch):
        """Test cache TTL (Time To Live)."""
        cache_manager = CacheManager(test_config)
        
        # Parameters for testing
        params = {
            'reporterCode': 'DE',
            'partnerCode': 'ALL',
            'period': '202201:202203',
            'flowCode': 'M'
        }
        
        # Save to cache
        cache_manager.save(params, mock_api_response)
        
        # Mock file age to be older than TTL
        cache_key = cache_manager._generate_cache_key(params)
        cache_path = cache_manager._get_cache_path(cache_key)
        
        # Use monkeypatch to mock the file modification time
        old_time = datetime.now() - timedelta(days=cache_manager.ttl_days + 1)
        os.utime(cache_path, (old_time.timestamp(), old_time.timestamp()))
        
        # Get from cache - should be a miss due to TTL
        data, is_cache_hit = cache_manager.get(params)
        assert is_cache_hit is False
        assert data is None
    
    def test_cache_clear(self, test_config, setup_test_environment, mock_api_response):
        """Test clearing the cache."""
        cache_manager = CacheManager(test_config)
        
        # Save multiple items to cache
        for i in range(3):
            params = {
                'reporterCode': f'DE{i}',
                'partnerCode': 'ALL',
                'period': '202201:202203',
                'flowCode': 'M'
            }
            cache_manager.save(params, mock_api_response)
        
        # Clear cache
        count = cache_manager.clear()
        assert count == 3
        
        # Verify cache directory is empty
        assert len([f for f in os.listdir(cache_manager.cache_dir) if f.endswith('.json')]) == 0
    
    def test_cache_clear_with_age(self, test_config, setup_test_environment, mock_api_response, monkeypatch):
        """Test clearing cache with age filter."""
        cache_manager = CacheManager(test_config)
        
        # Save multiple items to cache
        for i in range(5):
            params = {
                'reporterCode': f'DE{i}',
                'partnerCode': 'ALL',
                'period': '202201:202203',
                'flowCode': 'M'
            }
            cache_manager.save(params, mock_api_response)
            
            # Set different ages for cache files
            cache_key = cache_manager._generate_cache_key(params)
            cache_path = cache_manager._get_cache_path(cache_key)
            
            if i < 3:  # Make 3 files older
                old_time = datetime.now() - timedelta(days=5)
                os.utime(cache_path, (old_time.timestamp(), old_time.timestamp()))
        
        # Clear cache older than 3 days
        count = cache_manager.clear(days_old=3)
        assert count == 3
        
        # Verify only newer files remain
        assert len([f for f in os.listdir(cache_manager.cache_dir) if f.endswith('.json')]) == 2
    
    def test_cache_disabled(self, test_config, setup_test_environment, mock_api_response):
        """Test behavior when cache is disabled."""
        # Create config with disabled cache
        disabled_config = test_config.copy()
        disabled_config['cache'] = test_config['cache'].copy()
        disabled_config['cache']['enabled'] = False
        
        cache_manager = CacheManager(disabled_config)
        
        # Attempt operations with disabled cache
        params = {
            'reporterCode': 'DE',
            'partnerCode': 'ALL',
            'period': '202201:202203',
            'flowCode': 'M'
        }
        
        # Save should return False
        result = cache_manager.save(params, mock_api_response)
        assert result is False
        
        # Get should return cache miss
        data, is_cache_hit = cache_manager.get(params)
        assert is_cache_hit is False
        assert data is None
        
        # Clear should return 0
        count = cache_manager.clear()
        assert count == 0