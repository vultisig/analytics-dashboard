# database/connection.py
import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
import logging
from config import config

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self):
        self.connection_string = config.DATABASE_URL
    
    @contextmanager
    def get_connection(self):
        conn = None
        try:
            conn = psycopg2.connect(self.connection_string)
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    def execute_query(self, query, params=None, fetch=False):
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params)
                if fetch:
                    return cursor.fetchall()
                conn.commit()
                return cursor.rowcount
    
    def insert_swaps(self, swaps_data):
        """Insert swap data with proper conflict handling - includes ALL Midgard fields"""
        insert_query = """
        INSERT INTO swaps (
            timestamp, tx_hash, source, date_only, block_height, user_address,
            in_asset, in_amount, in_amount_usd, out_asset, out_amount, out_amount_usd,
            total_fee_usd, network_fee_usd, liquidity_fee_usd, affiliate_fee_usd,
            pool_1, pool_2, is_streaming_swap, swap_slip, volume_tier, raw_data, platform,
            in_address, in_tx_id, in_amount_raw, out_addresses, out_tx_ids, out_heights,
            affiliate_addresses, affiliate_fees_bps, metadata_complete,
            in_price_usd, out_price_usd, network_fees_raw, pools_used, swap_status, swap_type, memo
        ) VALUES (
            %(timestamp)s, %(tx_hash)s, %(source)s, %(date_only)s, %(block_height)s, %(user_address)s,
            %(in_asset)s, %(in_amount)s, %(in_amount_usd)s, %(out_asset)s, %(out_amount)s, %(out_amount_usd)s,
            %(total_fee_usd)s, %(network_fee_usd)s, %(liquidity_fee_usd)s, %(affiliate_fee_usd)s,
            %(pool_1)s, %(pool_2)s, %(is_streaming_swap)s, %(swap_slip)s, %(volume_tier)s, %(raw_data)s, %(platform)s,
            %(in_address)s, %(in_tx_id)s, %(in_amount_raw)s, %(out_addresses)s, %(out_tx_ids)s, %(out_heights)s,
            %(affiliate_addresses)s, %(affiliate_fees_bps)s, %(metadata_complete)s,
            %(in_price_usd)s, %(out_price_usd)s, %(network_fees_raw)s, %(pools_used)s, %(swap_status)s, %(swap_type)s, %(memo)s
        ) ON CONFLICT (timestamp, tx_hash, source) DO NOTHING
        """

        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.executemany(insert_query, swaps_data)
                conn.commit()
                return cursor.rowcount
    
    def update_sync_status(self, source, **kwargs):
        set_clauses = []
        params = {'source': source}
        
        for key, value in kwargs.items():
            set_clauses.append(f"{key} = %({key})s")
            params[key] = value
        
        set_clauses.append("updated_at = NOW()")
        
        query = f"""
        UPDATE sync_status 
        SET {', '.join(set_clauses)}
        WHERE source = %(source)s
        """
        
        return self.execute_query(query, params)
    
    def get_sync_status(self, source):
        query = "SELECT * FROM sync_status WHERE source = %s"
        results = self.execute_query(query, (source,), fetch=True)
        return results[0] if results else None
    
    def get_database_stats(self):
        """Get database statistics"""
        query = "SELECT * FROM get_database_stats()"
        return self.execute_query(query, fetch=True)
    
    def test_connection(self):
        """Test database connection"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    return True
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False

db_manager = DatabaseManager()