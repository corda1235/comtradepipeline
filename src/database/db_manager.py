# File: src/database/db_manager.py

# -*- coding: utf-8 -*-

"""
Database manager for the Comtrade Data Pipeline.
Handles connections to PostgreSQL and database operations.
"""

import time
from typing import Dict, Any, List, Optional, Tuple
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

from src.utils.logging_utils import get_module_logger

# Module logger
logger = get_module_logger("database")


class DatabaseManager:
    """Manager for database operations."""

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the database manager.
        
        Args:
            config: Configuration dictionary containing database settings.
        """
        self.db_config = config['db']
        self.connection = None
        self.max_retries = self.db_config.get('max_retries', 3)
        self.retry_delay = self.db_config.get('retry_delay', 2)  # seconds
        
        logger.info("DatabaseManager initialized")
    
    def connect(self) -> bool:
        """
        Establish a connection to the PostgreSQL database.
        
        Returns:
            bool: True if connection successful, False otherwise.
        """
        if self.connection and not self.connection.closed:
            return True
        
        for attempt in range(1, self.max_retries + 1):
            try:
                logger.debug(f"Connecting to database (attempt {attempt}/{self.max_retries})...")
                
                self.connection = psycopg2.connect(
                    host=self.db_config['host'],
                    port=self.db_config['port'],
                    dbname=self.db_config['dbname'],
                    user=self.db_config['user'],
                    password=self.db_config['password']
                )
                self.connection.autocommit = False
                
                logger.info(
                    f"Database connection established to "
                    f"{self.db_config['host']}:{self.db_config['port']}/{self.db_config['dbname']}"
                )
                return True
                
            except psycopg2.Error as e:
                logger.error(f"Database connection error: {str(e)}")
                
                if attempt < self.max_retries:
                    retry_delay = self.retry_delay * (2 ** (attempt - 1))
                    logger.info(f"Retrying database connection in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    logger.error("All database connection attempts failed")
        
        return False
    
    def disconnect(self) -> None:
        """Close the database connection."""
        if self.connection and not self.connection.closed:
            self.connection.close()
            logger.info("Database connection closed")
        self.connection = None
    
    def initialize_schema(self) -> bool:
        """
        Initialize the database schema.
        
        Returns:
            bool: True if schema initialization successful, False otherwise.
        """
        if not self.connect():
            return False
        
        try:
            with self.connection.cursor() as cursor:
                # Create schema
                logger.info("Creating comtrade schema if not exists")
                cursor.execute("CREATE SCHEMA IF NOT EXISTS comtrade")
                
                # Create tables
                logger.info("Creating tables if not exist")
                
                # Create reporters table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS comtrade.reporters (
                        id SERIAL PRIMARY KEY,
                        reporter_code VARCHAR(5) UNIQUE NOT NULL,
                        reporter_name VARCHAR(100) NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # Create partners table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS comtrade.partners (
                        id SERIAL PRIMARY KEY,
                        partner_code VARCHAR(5) UNIQUE NOT NULL,
                        partner_name VARCHAR(100) NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # Create commodities table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS comtrade.commodities (
                        id SERIAL PRIMARY KEY,
                        commodity_code VARCHAR(10) UNIQUE NOT NULL,
                        commodity_description TEXT NOT NULL,
                        hs_level SMALLINT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # Create flows table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS comtrade.flows (
                        id SERIAL PRIMARY KEY,
                        flow_code VARCHAR(5) UNIQUE NOT NULL,
                        flow_desc VARCHAR(50) NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # Create import_logs table to track import operations
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS comtrade.import_logs (
                        id SERIAL PRIMARY KEY,
                        reporter_code VARCHAR(5) NOT NULL,
                        start_period VARCHAR(10) NOT NULL,
                        end_period VARCHAR(10) NOT NULL,
                        records_processed INTEGER NOT NULL DEFAULT 0,
                        records_inserted INTEGER NOT NULL DEFAULT 0,
                        records_skipped INTEGER NOT NULL DEFAULT 0,
                        duration_seconds DECIMAL(10, 2),
                        status VARCHAR(20) NOT NULL,
                        error_message TEXT,
                        started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        completed_at TIMESTAMP,
                        api_calls INTEGER NOT NULL DEFAULT 0,
                        cache_hits INTEGER NOT NULL DEFAULT 0,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # Create main tariffline data table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS comtrade.tariffline_data (
                        id SERIAL PRIMARY KEY,
                        
                        -- Dimensional fields
                        reporter_id INTEGER NOT NULL REFERENCES comtrade.reporters(id),
                        partner_id INTEGER NOT NULL REFERENCES comtrade.partners(id),
                        commodity_id INTEGER NOT NULL REFERENCES comtrade.commodities(id),
                        flow_id INTEGER NOT NULL REFERENCES comtrade.flows(id),
                        
                        -- Time fields
                        period VARCHAR(10) NOT NULL,
                        year SMALLINT NOT NULL,
                        month SMALLINT NOT NULL,
                        
                        -- Measurement fields
                        net_weight DECIMAL(18, 2),
                        quantity DECIMAL(18, 2),
                        quantity_unit VARCHAR(50),
                        trade_value DECIMAL(18, 2),
                        flag INTEGER,
                        
                        -- Additional fields
                        is_reporter_estimate BOOLEAN,
                        customs DECIMAL(18, 2),
                        qty_unit_code VARCHAR(10),
                        qty_unit VARCHAR(50),
                        alt_qty DECIMAL(18, 2),
                        alt_qty_unit_code VARCHAR(10),
                        gross_weight DECIMAL(18, 2),
                        cif_value DECIMAL(18, 2),
                        fob_value DECIMAL(18, 2),
                        
                        -- Metadata
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        source_file VARCHAR(255),
                        
                        -- Composite unique key
                        UNIQUE(reporter_id, partner_id, commodity_id, flow_id, period)
                    )
                """)
                
                # Add indexes for better query performance
                logger.info("Creating indexes if not exist")
                
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_tariffline_reporter 
                    ON comtrade.tariffline_data(reporter_id)
                """)
                
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_tariffline_partner 
                    ON comtrade.tariffline_data(partner_id)
                """)
                
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_tariffline_commodity 
                    ON comtrade.tariffline_data(commodity_id)
                """)
                
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_tariffline_period 
                    ON comtrade.tariffline_data(period)
                """)
                
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_tariffline_year_month 
                    ON comtrade.tariffline_data(year, month)
                """)
                
                # Create function to update timestamp
                logger.info("Creating updated_at trigger functions")
                
                cursor.execute("""
                    CREATE OR REPLACE FUNCTION update_modified_column()
                    RETURNS TRIGGER AS $$
                    BEGIN
                        NEW.updated_at = NOW();
                        RETURN NEW;
                    END;
                    $$ LANGUAGE plpgsql
                """)
                
                # Create triggers for updated_at
                logger.info("Creating updated_at triggers")
                
                for table in ['reporters', 'partners', 'commodities', 'flows', 'tariffline_data', 'import_logs']:
                    cursor.execute(f"""
                        DROP TRIGGER IF EXISTS update_{table}_timestamp 
                        ON comtrade.{table}
                    """)
                    
                    cursor.execute(f"""
                        CREATE TRIGGER update_{table}_timestamp
                        BEFORE UPDATE ON comtrade.{table}
                        FOR EACH ROW
                        EXECUTE FUNCTION update_modified_column()
                    """)
                
                # Insert default flow values
                logger.info("Inserting default flow values")
                
                cursor.execute("""
                    INSERT INTO comtrade.flows (flow_code, flow_desc)
                    VALUES ('M', 'Import')
                    ON CONFLICT (flow_code) DO NOTHING
                """)
                
                # Commit changes
                self.connection.commit()
                logger.info("Schema initialization completed successfully")
                return True
                
        except psycopg2.Error as e:
            self.connection.rollback()
            logger.error(f"Schema initialization error: {str(e)}")
            return False

    def upsert_reporters(self, reporters: List[Dict[str, Any]]) -> bool:
        """
        Insert or update reporter countries.
        
        Args:
            reporters: List of reporter dictionaries {reporter_code, reporter_name}.
            
        Returns:
            bool: True if operation successful, False otherwise.
        """
        if not self.connect():
            return False
        
        try:
            with self.connection.cursor() as cursor:
                for reporter in reporters:
                    cursor.execute("""
                        INSERT INTO comtrade.reporters (reporter_code, reporter_name)
                        VALUES (%s, %s)
                        ON CONFLICT (reporter_code) 
                        DO UPDATE SET reporter_name = EXCLUDED.reporter_name
                    """, (reporter['reporter_code'], reporter['reporter_name']))
                
                self.connection.commit()
                logger.info(f"Upserted {len(reporters)} reporters")
                return True
                
        except psycopg2.Error as e:
            self.connection.rollback()
            logger.error(f"Reporter upsert error: {str(e)}")
            return False
    
    # ... [Il resto del codice rimane invariato ma con l'aggiunta di più logging dettagliato] ...
    
    def bulk_insert_tariffline_data(self, data_records: List[Dict[str, Any]], source_file: str = None) -> Tuple[int, int]:
        """
        Bulk insert tariffline data records.
        
        Args:
            data_records: List of data dictionaries to insert.
            source_file: Optional identifier for the source of the data.
            
        Returns:
            Tuple containing:
                - Number of records inserted
                - Number of records skipped (already exist)
        """
        if not self.connect() or not data_records:
            return 0, 0
        
        inserted = 0
        skipped = 0
        
        try:
            with self.connection.cursor() as cursor:
                # Prepare the data for insertion
                columns = [
                    'reporter_id', 'partner_id', 'commodity_id', 'flow_id',
                    'period', 'year', 'month',
                    'net_weight', 'quantity', 'quantity_unit', 'trade_value', 'flag',
                    'is_reporter_estimate', 'customs', 'qty_unit_code', 'qty_unit',
                    'alt_qty', 'alt_qty_unit_code', 'gross_weight',
                    'cif_value', 'fob_value', 'source_file'
                ]
                
                # Create SQL query
                query = sql.SQL("""
                    INSERT INTO comtrade.tariffline_data ({})
                    VALUES %s
                    ON CONFLICT (reporter_id, partner_id, commodity_id, flow_id, period)
                    DO NOTHING
                """).format(sql.SQL(', ').join(map(sql.Identifier, columns)))
                
                # Extract values from records
                values = []
                for record in data_records:
                    # Extract values in the same order as columns
                    row = []
                    for col in columns:
                        if col == 'source_file':
                            row.append(source_file)
                        else:
                            row.append(record.get(col))
                    values.append(row)
                
                # Log the insertion
                logger.debug(f"Inserting {len(data_records)} records into tariffline_data")
                
                # Execute the query
                execute_values(cursor, query, values)
                
                # Get the number of rows inserted
                inserted = cursor.rowcount
                skipped = len(data_records) - inserted
                
                # Commit changes
                self.connection.commit()
                
                if inserted > 0:
                    logger.info(f"Successfully inserted {inserted} records into database")
                
                if skipped > 0:
                    logger.info(f"Skipped {skipped} records (already exist in database)")
                
                return inserted, skipped
                
        except psycopg2.Error as e:
            self.connection.rollback()
            logger.error(f"Bulk insert error: {str(e)}")
            return 0, 0
        except Exception as e:
            self.connection.rollback()
            logger.exception(f"Unexpected error during bulk insert: {str(e)}")
            return 0, 0
    
    def log_import_operation(
        self, 
        reporter_code: str, 
        start_period: str, 
        end_period: str,
        stats: Dict[str, Any],
        duration: float,
        status: str,
        error_message: Optional[str] = None
    ) -> bool:
        """
        Log an import operation to the import_logs table.
        
        Args:
            reporter_code: Country code of the reporting country.
            start_period: Start period in YYYYMM format.
            end_period: End period in YYYYMM format.
            stats: Dictionary of operation statistics.
            duration: Duration of the operation in seconds.
            status: Status of the operation ('SUCCESS', 'FAILED', 'PARTIAL').
            error_message: Optional error message if the operation failed.
            
        Returns:
            bool: True if logging successful, False otherwise.
        """
        if not self.connect():
            return False
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO comtrade.import_logs (
                        reporter_code, start_period, end_period,
                        records_processed, records_inserted, records_skipped,
                        duration_seconds, status, error_message,
                        started_at, completed_at, api_calls, cache_hits
                    ) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    reporter_code,
                    start_period,
                    end_period,
                    stats.get('processed_records', 0),
                    stats.get('stored_records', 0),
                    stats.get('skipped_records', 0),
                    duration,
                    status,
                    error_message,
                    datetime.now() - timedelta(seconds=duration),  # approximate start time
                    datetime.now(),
                    stats.get('api_calls', 0),
                    stats.get('cache_hits', 0)
                ))
                
                self.connection.commit()
                logger.debug(f"Import operation logged to database: {reporter_code}, {start_period}-{end_period}, status={status}")
                return True
                
        except psycopg2.Error as e:
            self.connection.rollback()
            logger.error(f"Error logging import operation: {str(e)}")
            return False
        except Exception as e:
            self.connection.rollback()
            logger.exception(f"Unexpected error logging import operation: {str(e)}")
            return False
