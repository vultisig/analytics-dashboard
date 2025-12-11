#!/usr/bin/env python3
"""
Run database migration to add price fetching tables
"""
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')

if not DATABASE_URL:
    print("❌ DATABASE_URL not set")
    exit(1)

# Read migration SQL
with open('migrations/add_price_tables.sql', 'r') as f:
    migration_sql = f.read()

# Connect and execute
conn = psycopg2.connect(DATABASE_URL)
cursor = conn.cursor()

try:
    print("Running migration: add_price_tables.sql")
    cursor.execute(migration_sql)
    conn.commit()
    print("✅ Migration completed successfully")
    
    # Verify tables were created
    cursor.execute("""
        SELECT table_name FROM information_schema.tables 
        WHERE table_name IN ('historical_prices', 'ingestion_errors')
        ORDER BY table_name
    """)
    tables = cursor.fetchall()
    print(f"\nCreated tables: {[t[0] for t in tables]}")
    
except Exception as e:
    conn.rollback()
    print(f"❌ Migration failed: {e}")
    exit(1)
finally:
    cursor.close()
    conn.close()
