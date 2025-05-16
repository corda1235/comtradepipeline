# File: src/utils/config_loader.py

# -*- coding: utf-8 -*-

"""
Configuration loader for the Comtrade Data Pipeline.
Loads configuration from environment variables and applies defaults.
"""

import os
from typing import Dict, Any
from dotenv import load_dotenv

from .config_defaults import (
    DEFAULT_LOGGING_CONFIG,
    DEFAULT_API_CONFIG,
    DEFAULT_CACHE_CONFIG,
    DEFAULT_DB_CONFIG,
    DEFAULT_COMTRADE_CONFIG
)


def load_config() -> Dict[str, Any]:
    """
    Load configuration from environment variables and apply defaults.
    
    Returns:
        dict: Configuration dictionary.
    """
    # Load environment variables from .env file
    load_dotenv()
    
    # API configurations
    api_config = DEFAULT_API_CONFIG.copy()
    api_config.update({
        'primary_key': os.getenv('COMTRADE_API_KEY_PRIMARY'),
        'secondary_key': os.getenv('COMTRADE_API_KEY_SECONDARY'),
        'daily_limit': int(os.getenv('API_DAILY_LIMIT', api_config['daily_limit'])),
        'record_limit': int(os.getenv('API_RECORD_LIMIT', api_config['record_limit'])),
    })
    
    # Database configurations
    db_config = DEFAULT_DB_CONFIG.copy()
    db_config.update({
        'host': os.getenv('DB_HOST', db_config['host']),
        'port': int(os.getenv('DB_PORT', db_config['port'])),
        'dbname': os.getenv('DB_NAME', db_config['dbname']),
        'user': os.getenv('DB_USER', db_config['user']),
        'password': os.getenv('DB_PASSWORD', db_config['password']),
    })
    
    # EU countries (27 members)
    eu_countries = [
        'AT', 'BE', 'BG', 'HR', 'CY', 'CZ', 'DK', 'EE', 'FI', 'FR', 
        'DE', 'GR', 'HU', 'IE', 'IT', 'LV', 'LT', 'LU', 'MT', 'NL', 
        'PL', 'PT', 'RO', 'SK', 'SI', 'ES', 'SE'
    ]
    
    # Cache configuration
    cache_config = DEFAULT_CACHE_CONFIG.copy()
    cache_config.update({
        'cache_dir': os.getenv('CACHE_DIR', cache_config['cache_dir']),
        'enabled': os.getenv('CACHE_ENABLED', 'true').lower() == 'true',
        'ttl_days': int(os.getenv('CACHE_TTL_DAYS', cache_config['ttl_days'])),
    })
    
    # Logging configuration
    logging_config = DEFAULT_LOGGING_CONFIG.copy()
    logging_config.update({
        'log_dir': os.getenv('LOG_DIR', logging_config['log_dir']),
        'rotation_size': os.getenv('LOG_ROTATION_SIZE', logging_config['rotation_size']),
        'retention_days': os.getenv('LOG_RETENTION_DAYS', logging_config['retention_days']),
        'compression': os.getenv('LOG_COMPRESSION', logging_config['compression']),
        'default_level': os.getenv('LOG_LEVEL', logging_config['default_level']),
        'console': os.getenv('LOG_CONSOLE', 'true').lower() == 'true',
    })
    
    # Comtrade API specific configurations
    comtrade_config = DEFAULT_COMTRADE_CONFIG.copy()
    
    config = {
        'api': api_config,
        'db': db_config,
        'eu_countries': eu_countries,
        'cache': cache_config,
        'comtrade': comtrade_config,
        'logging': logging_config,
    }
    
    # Validate configuration
    _validate_config(config)
    
    return config


def _validate_config(config: Dict[str, Any]) -> None:
    """
    Validate the configuration.
    
    Args:
        config (dict): Configuration dictionary.
        
    Raises:
        ValueError: If any required configuration is missing.
    """
    if not config['api']['primary_key'] and not config['api']['secondary_key']:
        raise ValueError('Both primary and secondary API keys are missing. At least one is required.')
    
    if not config['api']['primary_key']:
        print('Warning: Primary API key is missing')
    
    if not config['api']['secondary_key']:
        print('Warning: Secondary API key is missing')
        
    if not config['db']['password']:
        print('Warning: Database password is not set')
