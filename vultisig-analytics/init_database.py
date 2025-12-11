#!/usr/bin/env python3
# init_database.py

import os
import sys
import logging
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from config import config

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_database_if_not_exists():
    """Create database if it doesn't exist"""
    try:
        # Parse connection string to get database details
        import urllib.parse
        parsed = urllib.parse.urlparse(config.DATABASE_URL)
        
        db_name = parsed.path[1:]  # Remove leading '/'
        host = parsed.hostname
        port = parsed.port or 5432
        username = parsed.username
        password = parsed.password
        
        # Connect to postgres database to create our target database
        postgres_url = f"postgresql://{username}:{password}@{host}:{port}/postgres"
        
        logger.info(f"Connecting to PostgreSQL server at {host}:{port}")
        conn = psycopg2.connect(postgres_url)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        
        with conn.cursor() as cursor:
            # Check if database exists
            cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
            if cursor.fetchone():
                logger.info(f"Database '{db_name}' already exists")
            else:
                logger.info(f"Creating database '{db_name}'")
                cursor.execute(f'CREATE DATABASE "{db_name}"')
                logger.info(f"Database '{db_name}' created successfully")
        
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"Failed to create database: {e}")
        return False

def initialize_schema():
    """Initialize database schema"""
    try:
        logger.info("Connecting to target database...")
        conn = psycopg2.connect(config.DATABASE_URL)
        
        # Read schema file
        schema_file = "ingestors/database_schema.sql"
        if not os.path.exists(schema_file):
            logger.error(f"Schema file '{schema_file}' not found")
            return False
        
        logger.info("Reading schema file...")
        with open(schema_file, 'r') as f:
            schema_sql = f.read()
        
        logger.info("Executing schema SQL...")
        with conn.cursor() as cursor:
            cursor.execute(schema_sql)
        
        conn.commit()
        conn.close()
        
        logger.info("✅ Database schema initialized successfully")
        return True
        
    except Exception as e:
        logger.error(f"❌ Schema initialization failed: {e}")
        if conn:
            conn.rollback()
            conn.close()
        return False

def test_database():
    """Test database connection and basic operations"""
    try:
        logger.info("Testing database connection...")
        from database.connection import db_manager
        
        if not db_manager.test_connection():
            logger.error("Database connection test failed")
            return False
        
        logger.info("Testing database queries...")
        stats = db_manager.get_database_stats()
        logger.info("Database statistics:")
        for stat in stats:
            logger.info(f"  {stat['table_name']}: {stat['row_count']} rows, {stat['size_pretty']}")
        
        # Test sync status
        sync_statuses = db_manager.execute_query("SELECT * FROM sync_status", fetch=True)
        logger.info(f"Sync status entries: {len(sync_statuses)}")
        
        logger.info("✅ Database test completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"❌ Database test failed: {e}")
        return False

def main():
    logger.info("🚀 Starting database initialization...")
    
    # Step 1: Create database if it doesn't exist
    if not create_database_if_not_exists():
        logger.error("Database creation failed. Exiting.")
        sys.exit(1)
    
    # Step 2: Initialize schema
    if not initialize_schema():
        logger.error("Schema initialization failed. Exiting.")
        sys.exit(1)
    
    # Step 3: Test database
    if not test_database():
        logger.error("Database test failed. Exiting.")
        sys.exit(1)
    
    logger.info("🎉 Database initialization completed successfully!")
    logger.info("")
    logger.info("Next steps:")
    logger.info("1. Run the sync service: python main.py")
    logger.info("2. Check logs for sync progress")
    logger.info("3. Query the database to verify data is being ingested")

if __name__ == "__main__":
    main()