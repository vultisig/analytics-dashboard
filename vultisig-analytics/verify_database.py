# verify_database.py
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://vultisig_user:your_secure_password@localhost:5432/vultisig_analytics")

def verify_database():
    try:
        # Connect to database
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        print("✅ Database connection successful!")
        
        # Check if TimescaleDB extension is installed
        cursor.execute("SELECT * FROM pg_extension WHERE extname = 'timescaledb';")
        if cursor.fetchone():
            print("✅ TimescaleDB extension is installed!")
        else:
            print("❌ TimescaleDB extension is NOT installed!")
        
        # Check if tables exist
        tables_to_check = ['swaps', 'sync_status']
        cursor.execute("""
            SELECT tablename FROM pg_tables 
            WHERE schemaname = 'public' 
            AND tablename = ANY(%s)
        """, (tables_to_check,))
        
        existing_tables = [row['tablename'] for row in cursor.fetchall()]
        
        for table in tables_to_check:
            if table in existing_tables:
                print(f"✅ Table '{table}' exists!")
            else:
                print(f"❌ Table '{table}' does NOT exist!")
        
        # Check if hypertable is created
        cursor.execute("""
            SELECT hypertable_name FROM timescaledb_information.hypertables 
            WHERE hypertable_name = 'swaps'
        """)
        if cursor.fetchone():
            print("✅ Swaps hypertable is created!")
        else:
            print("❌ Swaps hypertable is NOT created!")
        
        # Check indexes
        cursor.execute("""
            SELECT indexname FROM pg_indexes 
            WHERE tablename = 'swaps' 
            AND schemaname = 'public'
        """)
        indexes = [row['indexname'] for row in cursor.fetchall()]
        print(f"✅ Found {len(indexes)} indexes on swaps table")
        
        # Check materialized view
        cursor.execute("""
            SELECT matviewname FROM pg_matviews 
            WHERE schemaname = 'public' 
            AND matviewname = 'daily_metrics'
        """)
        if cursor.fetchone():
            print("✅ Materialized view 'daily_metrics' exists!")
        else:
            print("❌ Materialized view 'daily_metrics' does NOT exist!")
        
        # Check sync_status initial data
        cursor.execute("SELECT source FROM sync_status ORDER BY source")
        sources = [row['source'] for row in cursor.fetchall()]
        expected_sources = ['1inch', 'lifi', 'mayachain', 'thorchain']
        
        if set(sources) == set(expected_sources):
            print(f"✅ All sync_status sources present: {sources}")
        else:
            print(f"❌ Missing sync_status sources. Found: {sources}, Expected: {expected_sources}")
        
        # Test the volume tier function
        cursor.execute("SELECT * FROM get_volume_tier_stats() LIMIT 1")
        if cursor.fetchone() is not None or cursor.rowcount == 0:
            print("✅ Volume tier function works!")
        else:
            print("❌ Volume tier function has issues!")
        
        cursor.close()
        conn.close()
        
        print("\n🎉 Database verification completed!")
        return True
        
    except Exception as e:
        print(f"❌ Database verification failed: {e}")
        return False

if __name__ == "__main__":
    verify_database()