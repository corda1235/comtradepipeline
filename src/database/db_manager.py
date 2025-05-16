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
from loguru import logger


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
        self.max_retries = 3
        self.retry_delay = 2  # seconds
        
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
                logger.info(f"Connecting to database (attempt {attempt})...")
                self.connection = psycopg2.connect(
                    host=self.db_config['host'],
                    port=self.db_config['port'],
                    dbname=self.db_config['dbname'],
                    user=self.db_config['user'],
                    password=self.db_config['password']
                )
                self.connection.autocommit = False
                logger.info("Database connection established")
                return True
                
            except psycopg2.Error as e:
                logger.error(f"Database connection error: {str(e)}")
                
                if attempt < self.max_retries:
                    retry_delay = self.retry_delay * (2 ** (attempt - 1))
                    logger.info(f"Retrying in {retry_delay} seconds...")
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
                        reporter_name VARCHAR(100) NOT NULL
                    )
                """)
                
                # Create partners table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS comtrade.partners (
                        id SERIAL PRIMARY KEY,
                        partner_code VARCHAR(5) UNIQUE NOT NULL,
                        partner_name VARCHAR(100) NOT NULL
                    )
                """)
                
                # Create commodities table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS comtrade.commodities (
                        id SERIAL PRIMARY KEY,
                        commodity_code VARCHAR(10) UNIQUE NOT NULL,
                        commodity_description TEXT NOT NULL
                    )
                """)
                
                # Create flows table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS comtrade.flows (
                        id SERIAL PRIMARY KEY,
                        flow_code VARCHAR(5) UNIQUE NOT NULL,
                        flow_desc VARCHAR(50) NOT NULL
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
                cursor.execute("""
                    CREATE OR REPLACE FUNCTION update_modified_column()
                    RETURNS TRIGGER AS $$
                    BEGIN
                        NEW.updated_at = NOW();
                        RETURN NEW;
                    END;
                    $$ LANGUAGE plpgsql
                """)
                
                # Create trigger for updated_at
                cursor.execute("""
                    DROP TRIGGER IF EXISTS update_tariffline_data_timestamp 
                    ON comtrade.tariffline_data
                """)
                
                cursor.execute("""
                    CREATE TRIGGER update_tariffline_data_timestamp
                    BEFORE UPDATE ON comtrade.tariffline_data
                    FOR EACH ROW
                    EXECUTE FUNCTION update_modified_column()
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
    
    def upsert_partners(self, partners: List[Dict[str, Any]]) -> bool:
        """
        Insert or update partner countries.
        
        Args:
            partners: List of partner dictionaries {partner_code, partner_name}.
            
        Returns:
            bool: True if operation successful, False otherwise.
        """
        if not self.connect():
            return False
        
        try:
            with self.connection.cursor() as cursor:
                for partner in partners:
                    cursor.execute("""
                        INSERT INTO comtrade.partners (partner_code, partner_name)
                        VALUES (%s, %s)
                        ON CONFLICT (partner_code) 
                        DO UPDATE SET partner_name = EXCLUDED.partner_name
                    """, (partner['partner_code'], partner['partner_name']))
                
                self.connection.commit()
                logger.info(f"Upserted {len(partners)} partners")
                return True
                
        except psycopg2.Error as e:
            self.connection.rollback()
            logger.error(f"Partner upsert error: {str(e)}")
            return False
    
    def upsert_commodities(self, commodities: List[Dict[str, Any]]) -> bool:
        """
        Insert or update commodities.
        
        Args:
            commodities: List of commodity dictionaries {commodity_code, commodity_description}.
            
        Returns:
            bool: True if operation successful, False otherwise.
        """
        if not self.connect():
            return False
        
        try:
            with self.connection.cursor() as cursor:
                for commodity in commodities:
                    cursor.execute("""
                        INSERT INTO comtrade.commodities (commodity_code, commodity_description)
                        VALUES (%s, %s)
                        ON CONFLICT (commodity_code) 
                        DO UPDATE SET commodity_description = EXCLUDED.commodity_description
                    """, (commodity['commodity_code'], commodity['commodity_description']))
                
                self.connection.commit()
                logger.info(f"Upserted {len(commodities)} commodities")
                return True
                
        except psycopg2.Error as e:
            self.connection.rollback()
            logger.error(f"Commodity upsert error: {str(e)}")
            return False
    
    def upsert_flows(self, flows: List[Dict[str, Any]]) -> bool:
        """
        Insert or update flows.
        
        Args:
            flows: List of flow dictionaries {flow_code, flow_desc}.
            
        Returns:
            bool: True if operation successful, False otherwise.
        """
        if not self.connect():
            return False
        
        try:
            with self.connection.cursor() as cursor:
                for flow in flows:
                    cursor.execute("""
                        INSERT INTO comtrade.flows (flow_code, flow_desc)
                        VALUES (%s, %s)
                        ON CONFLICT (flow_code) 
                        DO UPDATE SET flow_desc = EXCLUDED.flow_desc
                    """, (flow['flow_code'], flow['flow_desc']))
                
                self.connection.commit()
                logger.info(f"Upserted {len(flows)} flows")
                return True
                
        except psycopg2.Error as e:
            self.connection.rollback()
            logger.error(f"Flow upsert error: {str(e)}")
            return False
    
    def get_reporter_id(self, reporter_code: str) -> Optional[int]:
        """
        Get reporter ID from reporter code.
        
        Args:
            reporter_code: Reporter country code.
            
        Returns:
            int: Reporter ID if found, None otherwise.
        """
        if not self.connect():
            return None
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    SELECT id FROM comtrade.reporters
                    WHERE reporter_code = %s
                """, (reporter_code,))
                
                result = cursor.fetchone()
                return result[0] if result else None
                
        except psycopg2.Error as e:
            logger.error(f"Error getting reporter ID: {str(e)}")
            return None
    
    def get_partner_id(self, partner_code: str) -> Optional[int]:
        """
        Get partner ID from partner code.
        
        Args:
            partner_code: Partner country code.
            
        Returns:
            int: Partner ID if found, None otherwise.
        """
        if not self.connect():
            return None
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    SELECT id FROM comtrade.partners
                    WHERE partner_code = %s
                """, (partner_code,))
                
                result = cursor.fetchone()
                return result[0] if result else None
                
        except psycopg2.Error as e:
            logger.error(f"Error getting partner ID: {str(e)}")
            return None
    
    def get_commodity_id(self, commodity_code: str) -> Optional[int]:
        """
        Get commodity ID from commodity code.
        
        Args:
            commodity_code: Commodity code.
            
        Returns:
            int: Commodity ID if found, None otherwise.
        """
        if not self.connect():
            return None
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    SELECT id FROM comtrade.commodities
                    WHERE commodity_code = %s
                """, (commodity_code,))
                
                result = cursor.fetchone()
                return result[0] if result else None
                
        except psycopg2.Error as e:
            logger.error(f"Error getting commodity ID: {str(e)}")
            return None
    
    def get_flow_id(self, flow_code: str) -> Optional[int]:
        """
        Get flow ID from flow code.
        
        Args:
            flow_code: Flow code.
            
        Returns:
            int: Flow ID if found, None otherwise.
        """
        if not self.connect():
            return None
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    SELECT id FROM comtrade.flows
                    WHERE flow_code = %s
                """, (flow_code,))
                
                result = cursor.fetchone()
                return result[0] if result else None
                
        except psycopg2.Error as e:
            logger.error(f"Error getting flow ID: {str(e)}")
            return None
    
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
                    'is_reporter_estimate', 'customs', 'qty_unit',
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
                    row = [
                        record.get('reporter_id'),
                        record.get('partner_id'),
                        record.get('commodity_id'),
                        record.get('flow_id'),
                        record.get('period'),
                        record.get('year'),
                        record.get('month'),
                        record.get('net_weight'),
                        record.get('quantity'),
                        record.get('quantity_unit'),
                        record.get('trade_value'),
                        record.get('flag'),
                        record.get('is_reporter_estimate'),
                        record.get('customs'),
                        record.get('qty_unit_code'),
                        record.get('qty_unit'),
                        record.get('alt_qty'),
                        record.get('alt_qty_unit_code'),
                        record.get('gross_weight'),
                        record.get('cif_value'),
                        record.get('fob_value'),
                        source_file
                    ]
                    values.append(row)
                
                # Execute the query
                execute_values(cursor, query, values)
                
                # Get the number of rows inserted
                inserted = cursor.rowcount
                skipped = len(data_records) - inserted
                
                # Commit changes
                self.connection.commit()
                logger.info(f"Inserted {inserted} records, skipped {skipped} (already exist)")
                
                return inserted, skipped
                
        except psycopg2.Error as e:
            self.connection.rollback()
            logger.error(f"Bulk insert error: {str(e)}")
            return 0, 0
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> Optional[List[tuple]]:
        """
        Execute a custom SQL query.
        
        Args:
            query: SQL query string.
            params: Optional parameters for the query.
            
        Returns:
            List of tuples containing the query results, or None on error.
        """
        if not self.connect():
            return None
        
        try:
            with self.connection.cursor() as cursor:
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                
                if cursor.description:  # Check if the query returns data
                    return cursor.fetchall()
                return []
                
        except psycopg2.Error as e:
            self.connection.rollback()
            logger.error(f"Query execution error: {str(e)}")
            return None
    
    def execute_transaction(self, queries: List[Tuple[str, Optional[tuple]]]) -> bool:
        """
        Execute multiple queries in a single transaction.
        
        Args:
            queries: List of tuples containing (query_string, params_tuple).
            
        Returns:
            bool: True if transaction successful, False otherwise.
        """
        if not self.connect():
            return False
        
        try:
            with self.connection.cursor() as cursor:
                for query, params in queries:
                    if params:
                        cursor.execute(query, params)
                    else:
                        cursor.execute(query)
                
                self.connection.commit()
                return True
                
        except psycopg2.Error as e:
            self.connection.rollback()
            logger.error(f"Transaction error: {str(e)}")
            return False