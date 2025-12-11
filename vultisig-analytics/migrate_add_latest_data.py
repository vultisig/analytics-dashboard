#!/usr/bin/env python3
"""
Add latest_data_timestamp column to sync_status table
This tracks the timestamp of the most recent transaction from each source
"""

import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')

def main():
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()

    print("Adding latest_data_timestamp column...")

    # Add column
    cursor.execute("""
        ALTER TABLE sync_status
        ADD COLUMN IF NOT EXISTS latest_data_timestamp TIMESTAMP WITH TIME ZONE
    """)

    print("Populating initial values from existing data...")

    # Populate from swaps table
    cursor.execute("""
        UPDATE sync_status s
        SET latest_data_timestamp = (
            SELECT MAX(timestamp)
            FROM swaps
            WHERE source = s.source
        )
        WHERE latest_data_timestamp IS NULL
          AND s.source IN ('thorchain', 'mayachain', 'lifi')
    """)

    # Populate from dex_aggregator_revenue for Arkham
    cursor.execute("""
        UPDATE sync_status s
        SET latest_data_timestamp = (
            SELECT MAX(timestamp)
            FROM dex_aggregator_revenue
            WHERE fee_data_source = 'arkham'
        )
        WHERE s.source = 'arkham' AND latest_data_timestamp IS NULL
    """)

    conn.commit()

    # Verify
    cursor.execute("""
        SELECT source, last_synced_timestamp, latest_data_timestamp
        FROM sync_status
        ORDER BY source
    """)

    print("\nCurrent sync_status:")
    print("Source | Last Synced | Latest Data")
    print("-" * 70)
    for row in cursor.fetchall():
        print(f"{row[0]:12} | {row[1]} | {row[2]}")

    cursor.close()
    conn.close()

    print("\n✅ Migration completed successfully!")

if __name__ == '__main__':
    main()
