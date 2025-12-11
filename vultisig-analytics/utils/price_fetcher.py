"""
CoinGecko Price Fetcher Utility
Fetches historical token prices with caching and retry logic for rate limits
"""
import asyncio
import aiohttp
import logging
from datetime import date, datetime
from typing import Optional
import psycopg2
from psycopg2.extras import RealDictCursor
import os

logger = logging.getLogger(__name__)

class RateLimitError(Exception):
    """Raised when CoinGecko rate limit is hit"""
    pass

class PriceFetcher:
    def __init__(self, db_connection_string: str = None):
        self.db_conn_string = db_connection_string or os.getenv('DATABASE_URL')
        self.base_url = 'https://api.coingecko.com/api/v3'
        self.max_retries = 5
        
    def _get_db_connection(self):
        """Get database connection"""
        return psycopg2.connect(self.db_conn_string)
    
    def _check_cache(self, token_id: str, price_date: date) -> Optional[float]:
        """Check if price exists in cache"""
        conn = self._get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        try:
            cursor.execute("""
                SELECT price_usd FROM historical_prices
                WHERE token_id = %s AND date = %s
            """, (token_id, price_date))
            
            result = cursor.fetchone()
            if result:
                logger.info(f"Cache hit for {token_id} on {price_date}")
                return float(result['price_usd'])
            return None
        finally:
            cursor.close()
            conn.close()
    
    def _save_to_cache(self, token_id: str, price_date: date, price_usd: float):
        """Save price to cache"""
        conn = self._get_db_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                INSERT INTO historical_prices (token_id, date, price_usd)
                VALUES (%s, %s, %s)
                ON CONFLICT (token_id, date) DO UPDATE
                SET price_usd = EXCLUDED.price_usd
            """, (token_id, price_date, price_usd))
            conn.commit()
            logger.info(f"Cached price for {token_id} on {price_date}: ${price_usd}")
        finally:
            cursor.close()
            conn.close()
    
    async def _fetch_from_coingecko(self, token_id: str, price_date: date) -> Optional[float]:
        """Fetch historical price from CoinGecko API with retry logic"""
        # Format date as dd-mm-yyyy (CoinGecko requirement)
        date_str = price_date.strftime('%d-%m-%Y')
        url = f"{self.base_url}/coins/{token_id}/history?date={date_str}"
        
        import ssl
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        for attempt in range(self.max_retries):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=10), ssl=ssl_context) as response:
                        if response.status == 200:
                            data = await response.json()
                            # Extract price from response
                            price = data.get('market_data', {}).get('current_price', {}).get('usd')
                            if price:
                                logger.info(f"Fetched {token_id} price for {price_date}: ${price}")
                                return float(price)
                            else:
                                logger.warning(f"No USD price in response for {token_id}")
                                return None
                        
                        elif response.status == 429:
                            # Rate limit hit
                            wait_time = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s, 8s, 16s
                            logger.warning(f"Rate limit hit (attempt {attempt + 1}/{self.max_retries}), waiting {wait_time}s...")
                            await asyncio.sleep(wait_time)
                            continue
                        
                        else:
                            logger.error(f"CoinGecko API error: {response.status} - {await response.text()}")
                            return None
            
            except asyncio.TimeoutError:
                logger.warning(f"Timeout fetching {token_id} (attempt {attempt + 1}/{self.max_retries})")
                await asyncio.sleep(2 ** attempt)
                continue
            
            except Exception as e:
                logger.error(f"Error fetching price from CoinGecko: {e}")
                return None
        
        # All retries exhausted
        logger.error(f"Failed to fetch {token_id} price after {self.max_retries} attempts")
        raise RateLimitError(f"Failed to fetch price for {token_id} after {self.max_retries} retries")
    
    async def get_historical_price_async(self, token_id: str, price_date: date) -> Optional[float]:
        """
        Get historical price for a token on a specific date (async version)
        
        Args:
            token_id: CoinGecko token ID (e.g., 'thorchain', 'cacao')
            price_date: Date object for the price
            
        Returns:
            Price in USD or None if not found
        """
        # Check cache first
        cached_price = self._check_cache(token_id, price_date)
        if cached_price is not None:
            return cached_price
        
        # Fetch from CoinGecko
        try:
            price = await self._fetch_from_coingecko(token_id, price_date)
            if price:
                # Save to cache
                self._save_to_cache(token_id, price_date, price)
                return price
            return None
        
        except RateLimitError:
            # This will be caught by the ingestor and logged to ingestion_errors
            return None
    
    def get_historical_price(self, token_id: str, price_date: date) -> Optional[float]:
        """
        Get historical price for a token on a specific date (synchronous wrapper)
        
        Args:
            token_id: CoinGecko token ID (e.g., 'thorchain', 'cacao')
            price_date: Date object for the price
            
        Returns:
            Price in USD or None if not found
        """
        # Check cache first
        cached_price = self._check_cache(token_id, price_date)
        if cached_price is not None:
            return cached_price
        
        # Use asyncio to run the async fetch
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # If loop is already running, create a new thread
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(asyncio.run, self.get_historical_price_async(token_id, price_date))
                    price = future.result(timeout=30)
            else:
                price = loop.run_until_complete(self.get_historical_price_async(token_id, price_date))
            
            return price
        except Exception as e:
            logger.error(f"Error in synchronous price fetch: {e}")
            return None
    
    def log_ingestion_error(self, tx_hash: str, source: str, error_type: str, 
                           error_message: str, raw_data: dict):
        """Log an ingestion error to the database"""
        conn = self._get_db_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                INSERT INTO ingestion_errors 
                (tx_hash, source, error_type, error_message, raw_data, retry_count, created_at)
                VALUES (%s, %s, %s, %s, %s, 0, NOW())
                ON CONFLICT (tx_hash, source) DO UPDATE
                SET error_type = EXCLUDED.error_type,
                    error_message = EXCLUDED.error_message,
                    raw_data = EXCLUDED.raw_data
            """, (tx_hash, source, error_type, error_message, psycopg2.extras.Json(raw_data)))
            conn.commit()
            logger.info(f"Logged ingestion error for {tx_hash} ({source}): {error_type}")
        finally:
            cursor.close()
            conn.close()
