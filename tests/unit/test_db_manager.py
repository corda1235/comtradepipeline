# File: tests/test_db_manager.py
# -*- coding: utf-8 -*-

"""
Unit tests for the Database Manager.
"""

import pytest
from unittest.mock import patch, MagicMock, call
import psycopg2
from psycopg2 import sql

from src.database.db_manager import DatabaseManager


class TestDatabaseManager:
    """Tests for DatabaseManager class."""
    
    @patch('psycopg2.connect')
    def test_initialization(self, mock_connect, test_config):
        """Test initialization of database manager."""
        db_manager = DatabaseManager(test_config)
        
        # Check that configuration is stored correctly
        assert db_manager.db_config == test_config['db']
        assert db_manager.connection is None
        assert db_manager.max_retries == test_config['db']['max_retries']
        assert db_manager.retry_delay == test_config['db']['retry_delay']
        
        # Verify that no connection was attempted during initialization
        mock_connect.assert_not_called()
    
    @patch('psycopg2.connect')
    def test_connect_success(self, mock_connect, test_config):
        """Test successful database connection."""
        db_manager = DatabaseManager(test_config)
        
        # Mock the connection
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        
        # Test connection
        result = db_manager.connect()
        
        # Verify results
        assert result is True
        assert db_manager.connection == mock_connection
        
        # Verify connect was called with correct parameters
        mock_connect.assert_called_once_with(
            host=test_config['db']['host'],
            port=test_config['db']['port'],
            dbname=test_config['db']['dbname'],
            user=test_config['db']['user'],
            password=test_config['db']['password']
        )
        
        # Verify autocommit was set to False
        assert mock_connection.autocommit is False
    
    @patch('psycopg2.connect')
    @patch('time.sleep')
    def test_connect_retry(self, mock_sleep, mock_connect, test_config):
        """Test connection retry on failure."""
        db_manager = DatabaseManager(test_config)
        db_manager.retry_delay = 0.01  # Make tests faster
        
        # Mock first connection attempt to fail, second to succeed
        mock_connect.side_effect = [
            psycopg2.Error("Test connection error"),
            MagicMock()
        ]
        
        # Test connection
        result = db_manager.connect()
        
        # Verify results
        assert result is True
        assert mock_connect.call_count == 2
        mock_sleep.assert_called_once()
    
    @patch('psycopg2.connect')
    @patch('time.sleep')
    def test_connect_all_retries_fail(self, mock_sleep, mock_connect, test_config):
        """Test behavior when all connection retries fail."""
        db_manager = DatabaseManager(test_config)
        db_manager.retry_delay = 0.01  # Make tests faster
        
        # Mock connection to always fail
        mock_connect.side_effect = psycopg2.Error("Test connection error")
        
        # Test connection
        result = db_manager.connect()
        
        # Verify results
        assert result is False
        assert mock_connect.call_count == db_manager.max_retries
    
    @patch('psycopg2.connect')
    def test_disconnect(self, mock_connect, test_config):
        """Test disconnecting from database."""
        db_manager = DatabaseManager(test_config)
        
        # Create a mock connection
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        
        # Connect
        db_manager.connect()
        
        # Disconnect
        db_manager.disconnect()
        
        # Verify connection was closed
        mock_connection.close.assert_called_once()
        assert db_manager.connection is None
    
    @patch('psycopg2.connect')
    def test_initialize_schema(self, mock_connect, test_config):
        """Test database schema initialization."""
        db_manager = DatabaseManager(test_config)
        
        # Create mock connection and cursor
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        
        # Test schema initialization
        result = db_manager.initialize_schema()
        
        # Verify results
        assert result is True
        
        # Verify correct SQL statements were executed in order
        # This is a simplified check - we just verify that important schema elements were created
        cursor_calls = [call[0][0].lower() for call in mock_cursor.execute.call_args_list]
        
        # Check for key schema creation statements
        assert any('create schema' in call for call in cursor_calls)
        assert any('create table if not exists comtrade.reporters' in call for call in cursor_calls)
        assert any('create table if not exists comtrade.partners' in call for call in cursor_calls)
        assert any('create table if not exists comtrade.commodities' in call for call in cursor_calls)
        assert any('create table if not exists comtrade.flows' in call for call in cursor_calls)
        assert any('create table if not exists comtrade.tariffline_data' in call for call in cursor_calls)
        assert any('create table if not exists comtrade.import_logs' in call for call in cursor_calls)
        
        # Verify commit was called
        mock_connection.commit.assert_called_once()
    
    @patch('psycopg2.connect')
    def test_initialize_schema_failure(self, mock_connect, test_config):
        """Test schema initialization failure."""
        db_manager = DatabaseManager(test_config)
        
        # Create mock connection and cursor
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = psycopg2.Error("Test error")
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        
        # Test schema initialization
        result = db_manager.initialize_schema()
        
        # Verify results
        assert result is False
        
        # Verify rollback was called
        mock_connection.rollback.assert_called_once()
    
    @patch('psycopg2.connect')
    def test_upsert_reporters(self, mock_connect, test_config):
        """Test upserting reporters."""
        db_manager = DatabaseManager(test_config)
        
        # Create mock connection and cursor
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        
        # Test data
        reporters = [
            {'reporter_code': 'DE', 'reporter_name': 'Germany'},
            {'reporter_code': 'FR', 'reporter_name': 'France'}
        ]
        
        # Test upsert
        result = db_manager.upsert_reporters(reporters)
        
        # Verify results
        assert result is True
        
        # Verify execute calls
        assert mock_cursor.execute.call_count == len(reporters)
        
        # Verify first execute call
        args, kwargs = mock_cursor.execute.call_args_list[0]
        assert 'insert into comtrade.reporters' in args[0].lower()
        assert args[1] == ('DE', 'Germany')
        
        # Verify commit was called
        mock_connection.commit.assert_called_once()
    
    @patch('psycopg2.connect')
    def test_bulk_insert_tariffline_data(self, mock_connect, test_config):
        """Test bulk inserting tariffline data."""
        db_manager = DatabaseManager(test_config)
        
        # Create mock connection and cursor
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 5  # 5 rows inserted
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        
        # Mock execute_values
        with patch('src.database.db_manager.execute_values') as mock_execute_values:
            # Test data
            data_records = [
                {
                    'reporter_id': 1,
                    'partner_id': 2,
                    'commodity_id': 3,
                    'flow_id': 1,
                    'period': '202201',
                    'year': 2022,
                    'month': 1,
                    'trade_value': 1000.50
                }
            ] * 5  # 5 identical records for simplicity
            
            # Test bulk insert
            inserted, skipped = db_manager.bulk_insert_tariffline_data(data_records, 'test_source')
            
            # Verify results
            assert inserted == 5
            assert skipped == 0
            
            # Verify execute_values was called
            mock_execute_values.assert_called_once()
            
            # Verify commit was called
            mock_connection.commit.assert_called_once()
    
    @patch('psycopg2.connect')
    def test_log_import_operation(self, mock_connect, test_config):
        """Test logging import operation."""
        db_manager = DatabaseManager(test_config)
        
        # Create mock connection and cursor
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        
        # Test data
        stats = {
            'processed_records': 100,
            'stored_records': 95,
            'skipped_records': 5,
            'api_calls': 10,
            'cache_hits': 5
        }
        
        # Test logging
        result = db_manager.log_import_operation(
            reporter_code='DE',
            start_period='202201',
            end_period='202203',
            stats=stats,
            duration=150.5,
            status='SUCCESS'
        )
        
        # Verify results
        assert result is True
        
        # Verify execute was called
        mock_cursor.execute.assert_called_once()
        assert 'insert into comtrade.import_logs' in mock_cursor.execute.call_args[0][0].lower()
        
        # Verify commit was called
        mock_connection.commit.assert_called_once()
