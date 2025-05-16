# File: src/utils/config_defaults.py

# -*- coding: utf-8 -*-

"""
Default configurations for the Comtrade Data Pipeline.
"""

# Default logging configuration
DEFAULT_LOGGING_CONFIG = {
    'log_dir': 'logs',
    'rotation_size': '100 MB',
    'retention_days': '30 days',
    'compression': 'zip',
    'default_level': 'INFO',
    'console': True
}

# Default API configuration
DEFAULT_API_CONFIG = {
    'daily_limit': 500,
    'record_limit': 100000,
    'retry_attempts': 5,
    'base_retry_delay': 2
}

# Default cache configuration
DEFAULT_CACHE_CONFIG = {
    'cache_dir': 'cache',
    'enabled': True,
    'ttl_days': 30  # Time to live in days
}

# Default database configuration
DEFAULT_DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'dbname': 'comtrade_data',
    'user': 'postgres',
    'password': '',
    'max_retries': 3,
    'retry_delay': 2
}

# Default Comtrade API configuration
DEFAULT_COMTRADE_CONFIG = {
    'flow_code': 'M',  # Imports
    'type_code': 'C',  # Commodities
    'frequency': 'M',  # Monthly
    'classification': 'HS',  # Harmonized System
    'hs_level': 6  # 6-digit HS code
}
