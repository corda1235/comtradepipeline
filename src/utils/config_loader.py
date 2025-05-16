import os
from dotenv import load_dotenv
from loguru import logger


def load_config():
    \"\"\"
    Load configuration from environment variables.
    
    Returns:
        dict: Configuration dictionary.
    \"\"\"
    # Load environment variables from .env file
    load_dotenv()
    
    # API configurations
    api_config = {
        'primary_key': os.getenv('COMTRADE_API_KEY_PRIMARY'),
        'secondary_key': os.getenv('COMTRADE_API_KEY_SECONDARY'),
        'daily_limit': int(os.getenv('API_DAILY_LIMIT', 500)),
        'record_limit': int(os.getenv('API_RECORD_LIMIT', 100000)),
    }
    
    # Database configurations
    db_config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', 5432)),
        'dbname': os.getenv('DB_NAME', 'comtrade_data'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', ''),
    }
    
    # EU countries (27 members)
    eu_countries = [
        'AT', 'BE', 'BG', 'HR', 'CY', 'CZ', 'DK', 'EE', 'FI', 'FR', 
        'DE', 'GR', 'HU', 'IE', 'IT', 'LV', 'LT', 'LU', 'MT', 'NL', 
        'PL', 'PT', 'RO', 'SK', 'SI', 'ES', 'SE'
    ]
    
    # Cache configuration
    cache_config = {
        'cache_dir': 'cache',
        'enabled': True,
    }
    
    # Comtrade API specific configurations
    comtrade_config = {
        'flow_code': 'M',  # Imports
        'type_code': 'C',  # Commodities
        'frequency': 'M',  # Monthly
        'classification': 'HS',  # Harmonized System
        'hs_level': 6,  # 6-digit HS code
    }
    
    config = {
        'api': api_config,
        'db': db_config,
        'eu_countries': eu_countries,
        'cache': cache_config,
        'comtrade': comtrade_config,
    }
    
    # Validate configuration
    _validate_config(config)
    
    return config


def _validate_config(config):
    \"\"\"
    Validate the configuration.
    
    Args:
        config (dict): Configuration dictionary.
        
    Raises:
        ValueError: If any required configuration is missing.
    \"\"\"
    if not config['api']['primary_key']:
        logger.warning('Primary API key is missing')
    
    if not config['api']['secondary_key']:
        logger.warning('Secondary API key is missing')
        
    if not config['api']['primary_key'] and not config['api']['secondary_key']:
        raise ValueError('Both primary and secondary API keys are missing. At least one is required.')
    
    if not config['db']['password']:
        logger.warning('Database password is not set')