# File: tests/test_main.py
# -*- coding: utf-8 -*-

"""
Unit tests for the main script.
"""

import pytest
import sys
from unittest.mock import patch, MagicMock
from datetime import datetime

import main
from src.pipeline import ComtradePipeline


class TestMain:
   """Tests for the main script."""
   
   def test_setup_argparse(self):
       """Test command line argument setup."""
       args = main.setup_argparse()
       
       # Check default values
       assert hasattr(args, 'countries')
       assert args.countries == 'all'
       assert hasattr(args, 'start_date')
       assert hasattr(args, 'end_date')
       assert hasattr(args, 'log_level')
       assert args.log_level == 'INFO'
       assert hasattr(args, 'clear_cache')
       assert args.clear_cache is False
       assert hasattr(args, 'db_init_only')
       assert args.db_init_only is False
   
   def test_validate_args_valid(self):
       """Test validation of valid arguments."""
       args = MagicMock()
       args.db_init_only = False
       args.start_date = '2022-01'
       args.end_date = '2022-03'
       args.countries = 'DE,FR'
       
       result = main.validate_args(args)
       assert result is True
   
   def test_validate_args_invalid_dates(self):
       """Test validation of invalid date arguments."""
       args = MagicMock()
       args.db_init_only = False
       
       # Missing dates
       args.start_date = None
       args.end_date = '2022-03'
       result = main.validate_args(args)
       assert result is False
       
       # End date before start date
       args.start_date = '2022-03'
       args.end_date = '2022-01'
       result = main.validate_args(args)
       assert result is False
       
       # Invalid date format
       args.start_date = '2022/01'
       args.end_date = '2022/03'
       result = main.validate_args(args)
       assert result is False
   
   def test_get_countries_list_all(self, test_config):
       """Test getting all countries."""
       eu_countries = test_config['eu_countries']
       countries = main.get_countries_list('all', eu_countries)
       assert countries == eu_countries
   
   def test_get_countries_list_specific(self, test_config):
       """Test getting specific countries."""
       eu_countries = test_config['eu_countries']
       countries = main.get_countries_list('DE,FR,IT', eu_countries)
       assert len(countries) == 3
       assert 'DE' in countries
       assert 'FR' in countries
       assert 'IT' in countries
   
   def test_get_countries_list_invalid(self, test_config):
       """Test getting countries with some invalid codes."""
       eu_countries = test_config['eu_countries']
       countries = main.get_countries_list('DE,XX,IT', eu_countries)
       assert len(countries) == 2
       assert 'DE' in countries
       assert 'IT' in countries
       assert 'XX' not in countries
   
   def test_handle_cache(self):
       """Test cache handling."""
       pipeline = MagicMock()
       pipeline.cache_manager = MagicMock()
       
       # Test no cache clearing
       args = MagicMock()
       args.clear_cache = False
       main.handle_cache(pipeline, args)
       pipeline.cache_manager.clear.assert_not_called()
       
       # Test cache clearing
       args.clear_cache = True
       args.cache_days = None
       main.handle_cache(pipeline, args)
       pipeline.cache_manager.clear.assert_called_once_with()
       
       # Test cache clearing with days
       pipeline.cache_manager.clear.reset_mock()
       args.cache_days = 30
       main.handle_cache(pipeline, args)
       pipeline.cache_manager.clear.assert_called_once_with(days_old=30)
   
   @patch('main.load_config')
   @patch('main.setup_logger')
   @patch('main.ComtradePipeline')
   @patch('main.PipelineMonitor')
   @patch('sys.exit')
   def test_main_db_init_only(self, mock_exit, mock_monitor, mock_pipeline, mock_logger, mock_config):
       """Test main function with db_init_only flag."""
       # Mock args
       with patch('main.setup_argparse') as mock_args:
           args = MagicMock()
           args.db_init_only = True
           args.log_level = 'INFO'
           args.log_file = None
           args.countries = 'all'
           args.clear_cache = False
           args.cache_days = None
           args.daily_report = False
           args.no_alerts = False
           mock_args.return_value = args
           
           # Mock validate_args
           with patch('main.validate_args') as mock_validate:
               mock_validate.return_value = True
               
               # Mock pipeline
               pipeline_instance = mock_pipeline.return_value
               pipeline_instance.db_manager.initialize_schema.return_value = True
               
               # Run main
               main.main()
               
               # Verify pipeline initialization
               mock_pipeline.assert_called_once()
               
               # Verify database initialization
               pipeline_instance.db_manager.initialize_schema.assert_called_once()
               
               # Verify exit code
               mock_exit.assert_called_once_with(0)
   
   @patch('main.load_config')
   @patch('main.setup_logger')
   @patch('main.ComtradePipeline')
   @patch('main.PipelineMonitor')
   @patch('sys.exit')
   def test_main_success(self, mock_exit, mock_monitor, mock_pipeline, mock_logger, mock_config):
       """Test main function with successful pipeline run."""
       # Mock args
       with patch('main.setup_argparse') as mock_args:
           args = MagicMock()
           args.db_init_only = False
           args.log_level = 'INFO'
           args.log_file = None
           args.countries = 'DE,FR'
           args.start_date = '2022-01'
           args.end_date = '2022-03'
           args.clear_cache = False
           args.cache_days = None
           args.daily_report = False
           args.no_alerts = False
           mock_args.return_value = args
           
           # Mock validate_args
           with patch('main.validate_args') as mock_validate:
               mock_validate.return_value = True
               
               # Mock pipeline
               pipeline_instance = mock_pipeline.return_value
               pipeline_instance.run.return_value = True
               pipeline_instance.stats = {
                   'total_calls': 5,
                   'cache_hits': 2,
                   'api_calls': 3,
                   'processed_records': 100,
                   'stored_records': 95,
                   'failed_calls': 0,
                   'skipped_records': 5
               }
               
               # Mock monitor
               monitor_instance = mock_monitor.return_value
               
               # Run main
               main.main()
               
               # Verify countries were determined
               with patch('main.get_countries_list') as mock_get_countries:
                   mock_get_countries.return_value = ['DE', 'FR']
               
               # Verify pipeline run
               pipeline_instance.run.assert_called_once()
               
               # Verify monitor was used
               monitor_instance.save_execution_stats.assert_called_once()
               
               # Verify exit code
               mock_exit.assert_called_once_with(0)
   
   @patch('main.load_config')
   @patch('main.setup_logger')
   @patch('main.ComtradePipeline')
   @patch('main.PipelineMonitor')
   @patch('sys.exit')
   def test_main_failure(self, mock_exit, mock_monitor, mock_pipeline, mock_logger, mock_config):
       """Test main function with failed pipeline run."""
       # Mock args
       with patch('main.setup_argparse') as mock_args:
           args = MagicMock()
           args.db_init_only = False
           args.log_level = 'INFO'
           args.log_file = None
           args.countries = 'DE,FR'
           args.start_date = '2022-01'
           args.end_date = '2022-03'
           args.clear_cache = False
           args.cache_days = None
           args.daily_report = False
           args.no_alerts = False
           mock_args.return_value = args
           
           # Mock validate_args
           with patch('main.validate_args') as mock_validate:
               mock_validate.return_value = True
               
               # Mock pipeline to fail
               pipeline_instance = mock_pipeline.return_value
               pipeline_instance.run.return_value = False
               pipeline_instance.stats = {
                   'total_calls': 5,
                   'cache_hits': 2,
                   'api_calls': 3,
                   'processed_records': 50,
                   'stored_records': 45,
                   'failed_calls': 2,
                   'skipped_records': 5
               }
               
               # Mock monitor
               monitor_instance = mock_monitor.return_value
               
               # Run main
               main.main()
               
               # Verify pipeline run
               pipeline_instance.run.assert_called_once()
               
               # Verify monitor was used
               monitor_instance.save_execution_stats.assert_called_once()
               
               # Verify exit code
               mock_exit.assert_called_once_with(1)
   
   @patch('main.load_config')
   @patch('main.setup_logger')
   @patch('main.ComtradePipeline')
   @patch('main.PipelineMonitor')
   @patch('sys.exit')
   def test_main_daily_report(self, mock_exit, mock_monitor, mock_pipeline, mock_logger, mock_config):
       """Test main function with daily report generation."""
       # Mock args
       with patch('main.setup_argparse') as mock_args:
           args = MagicMock()
           args.db_init_only = False
           args.log_level = 'INFO'
           args.log_file = None
           args.countries = 'DE,FR'
           args.start_date = '2022-01'
           args.end_date = '2022-03'
           args.clear_cache = False
           args.cache_days = None
           args.daily_report = True
           args.no_alerts = False
           mock_args.return_value = args
           
           # Mock validate_args
           with patch('main.validate_args') as mock_validate:
               mock_validate.return_value = True
               
               # Mock pipeline
               pipeline_instance = mock_pipeline.return_value
               pipeline_instance.run.return_value = True
               pipeline_instance.stats = {
                   'total_calls': 5,
                   'cache_hits': 2,
                   'api_calls': 3,
                   'processed_records': 100,
                   'stored_records': 95,
                   'failed_calls': 0,
                   'skipped_records': 5
               }
               
               # Mock monitor
               monitor_instance = mock_monitor.return_value
               monitor_instance.generate_daily_report.return_value = 'report.txt'
               
               # Run main
               main.main()
               
               # Verify daily report was generated
               monitor_instance.generate_daily_report.assert_called_once()
               
               # Verify exit code
               mock_exit.assert_called_once_with(0)
