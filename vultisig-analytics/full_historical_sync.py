#!/usr/bin/env python3
"""
Full Historical Sync - Exact Mirror of vulttcfee.py but saves to database
Uses the same pagination logic that successfully fetched 5,000+ records
"""
import time
import logging
import requests
from datetime import datetime, timezone
from database.connection import db_manager
from ingestors.thorchain import THORChainIngestor

API_URL = "https://vanaheimex.com/actions?type=swap&affiliate=va,vi,v0&limit=50"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def log(msg):
    """Print a log message with current timestamp."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] {msg}")

def countdown(seconds, label="system"):
    """Print a countdown to the console."""
    for i in range(seconds, 0, -1):
        print(f"\r[{label}] Waiting {i}s...", end="", flush=True)
        time.sleep(1)
    print()

def fetch_actions(next_page_token=None, base_delay=2):
    """Fetch a page of actions from the API - exact copy from original script"""
    url = API_URL
    if next_page_token:
        url += f"&nextPageToken={next_page_token}"

    timeout = 60
    retries = 0
    max_retries = 5
    delay = base_delay

    while True:
        try:
            log(f"🌐 Making API request: {url[:100]}{'...' if len(url) > 100 else ''}")
            r = requests.get(url, timeout=timeout)

            if r.status_code == 429:
                cooldown = int(r.headers.get("Retry-After", 60))
                log("⚠️ 429 Too Many Requests. Entering cooldown...")
                countdown(cooldown, label="system")
                continue

            # Handle 504 Gateway Timeout and other 5xx server errors
            if r.status_code in [502, 503, 504]:
                retries += 1
                if retries > max_retries:
                    log(f"❌ Server error {r.status_code} after {max_retries} retries. Giving up.")
                    raise requests.exceptions.HTTPError(f"{r.status_code} Server Error", response=r)

                log(f"⚠️ Server error {r.status_code} (attempt {retries}/{max_retries}). "
                    f"Retrying with timeout={timeout + 30}s and delay={delay * 2}s...")
                countdown(delay, label="server retry")
                timeout += 30
                delay *= 2
                continue

            r.raise_for_status()
            return r.json()

        except requests.exceptions.ReadTimeout:
            retries += 1
            if retries > max_retries:
                log(f"❌ Read timeout after {max_retries} retries. Giving up.")
                raise
            log(f"⚠️ Read timeout (attempt {retries}/{max_retries}). "
                f"Retrying with timeout={timeout + 30}s and delay={delay * 2}s...")
            countdown(delay, label="timeout retry")
            timeout += 30
            delay *= 2
            continue

        except requests.exceptions.ConnectionError as e:
            retries += 1
            if retries > max_retries:
                log(f"❌ Connection error after {max_retries} retries. Giving up.")
                raise
            log(f"⚠️ Connection error (attempt {retries}/{max_retries}): {e}")
            log(f"Retrying with delay={delay * 2}s...")
            countdown(delay, label="connection retry")
            delay *= 2
            continue

        except requests.exceptions.RequestException as e:
            log(f"❌ API request failed: {e}")
            raise

def extract_pagination_tokens(data):
    """Extract pagination tokens from API response - from original script"""
    next_token = data.get("nextPageToken") or data.get("meta", {}).get("nextPageToken")
    prev_token = data.get("prevPageToken") or data.get("meta", {}).get("prevPageToken")
    return next_token, prev_token

def format_date(timestamp_str):
    """Convert THORChain nanosecond timestamp to human-readable UTC string."""
    try:
        ts_sec = int(timestamp_str) // 1_000_000_000
        return datetime.fromtimestamp(ts_sec, timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError, OSError) as e:
        log(f"⚠️ Warning: Could not parse timestamp '{timestamp_str}': {e}")
        return str(timestamp_str)

def get_last_saved_date():
    """Get the most recent date from database"""
    try:
        result = db_manager.execute_query('''
            SELECT MAX(timestamp) as latest_date
            FROM swaps
            WHERE source = 'thorchain'
        ''', fetch=True)

        if result and result[0]['latest_date']:
            # Convert to timestamp format for comparison
            latest_dt = result[0]['latest_date']
            # Convert to nanoseconds timestamp string for comparison with API data
            timestamp_ns = str(int(latest_dt.timestamp()) * 1_000_000_000)
            log(f"📅 Last saved timestamp: {latest_dt} ({timestamp_ns})")
            return timestamp_ns
        return None
    except Exception as e:
        log(f"⚠️ Warning: Could not get last saved date: {e}")
        return None

def main():
    import argparse

    parser = argparse.ArgumentParser(description="Fetch ALL historical Vultisig affiliate data using original pagination logic")
    parser.add_argument("--nextPageToken", "-nextPageToken", type=str, default=None,
                        help="Resume from specific nextPageToken (for fetching historical data).")
    parser.add_argument("--apidelay", "-apidelay", type=int, default=2,
                        help="Delay between API calls in seconds (default=2).")
    parser.add_argument("--max-pages", type=int, default=None,
                        help="Maximum number of pages to fetch (default=unlimited).")
    parser.add_argument("--clear-db", action="store_true",
                        help="Clear existing database before starting.")

    args = parser.parse_args()

    # Determine scraping mode - if nextPageToken provided, we're going backwards for historical data
    append_mode = args.nextPageToken is not None
    next_page_token = args.nextPageToken
    apidelay = args.apidelay
    max_pages = args.max_pages

    log(f"🚀 Starting historical sync (mode: {'append old data' if append_mode else 'fetch new data'})")

    # Clear database if requested
    if args.clear_db:
        log("🧹 Clearing existing database...")
        db_manager.execute_query('DELETE FROM swaps WHERE source = %s', ('thorchain',))

    # Get last saved date for incremental sync
    last_saved_date = None if append_mode else get_last_saved_date()
    log(f"📅 Last saved date: {last_saved_date}")

    # Initialize counters
    page_num = 1
    total_processed = 0
    stop_scraping = False
    ingestor = THORChainIngestor()

    try:
        while True:
            if max_pages and page_num > max_pages:
                log(f"🏁 Reached maximum pages limit ({max_pages})")
                break

            log(f"📄 Fetching page {page_num}...")
            data = fetch_actions(next_page_token, base_delay=apidelay)
            actions = data.get("actions", [])

            if not actions:
                log("📭 No more actions returned.")
                break

            first_date = actions[0].get("date", "")
            last_date = actions[-1].get("date", "")
            log(f"📊 Processing {len(actions)} actions (date range: {format_date(last_date)} to {format_date(first_date)})")

            # Process actions into swap records
            swap_records = []
            for action in actions:
                # Check if we should stop (for incremental sync)
                if not append_mode and last_saved_date and action.get("date", "") <= last_saved_date:
                    log("🔄 Reached last saved date, stopping after this page.")
                    stop_scraping = True
                    break

                # Parse action using the ingestor
                parsed_swap = ingestor.parse_swap(action)
                if parsed_swap:
                    swap_records.append(parsed_swap)
                    total_processed += 1

            # Insert into database
            if swap_records:
                try:
                    inserted_count = db_manager.insert_swaps(swap_records)
                    log(f"💾 Inserted {inserted_count}/{len(swap_records)} swaps from page {page_num}")

                    if inserted_count < len(swap_records):
                        duplicates = len(swap_records) - inserted_count
                        log(f"ℹ️ Skipped {duplicates} duplicate records")

                except Exception as e:
                    log(f"❌ Database insert failed: {e}")
                    break

            # Extract pagination tokens
            next_token, prev_token = extract_pagination_tokens(data)
            log(f"🔗 Pagination: next={next_token[:50] if next_token else None}, prev={prev_token[:50] if prev_token else None}")

            # Check stopping conditions
            if stop_scraping or not next_token:
                log("🏁 Stopping fetch loop.")
                break

            next_page_token = next_token
            page_num += 1

            # Progress report every 10 pages
            if page_num % 10 == 0:
                db_count = db_manager.execute_query('SELECT COUNT(*) as count FROM swaps WHERE source = %s', ('thorchain',), fetch=True)[0]['count']
                log(f"📈 Progress: Page {page_num}, {total_processed} processed, {db_count} in DB")

            # API delay
            if apidelay > 0:
                log(f"⏳ Waiting {apidelay}s before next request...")
                countdown(apidelay, label="rate limit")

    except KeyboardInterrupt:
        log("⚠️ Script interrupted by user. Database already contains processed records.")

    except Exception as e:
        log(f"❌ Unexpected error: {e}")
        raise

    # Final statistics
    try:
        stats = db_manager.execute_query('''
            SELECT
                COUNT(*) as total_swaps,
                COUNT(CASE WHEN affiliate_fee_usd > 0 THEN 1 END) as swaps_with_fees,
                SUM(affiliate_fee_usd) as total_fees,
                MIN(timestamp) as earliest,
                MAX(timestamp) as latest
            FROM swaps WHERE source = 'thorchain'
        ''', fetch=True)[0]

        log("=" * 80)
        log("✅ HISTORICAL SYNC COMPLETED!")
        log(f"📊 Pages processed: {page_num}")
        log(f"📊 Actions processed: {total_processed}")
        log(f"📊 Total swaps in DB: {stats['total_swaps']:,}")
        log(f"💰 Swaps with fees: {stats['swaps_with_fees']:,}")
        log(f"💰 Total affiliate fees: ${stats['total_fees']:,.4f}")
        log(f"📅 Date range: {stats['earliest']} to {stats['latest']}")

        # Refresh materialized views
        log("🔄 Refreshing materialized views...")
        db_manager.execute_query("SELECT refresh_daily_metrics()")
        log("✅ Materialized views refreshed")

        # Compare with original CSV target
        original_count = 5027
        coverage_pct = (stats['total_swaps'] / original_count) * 100 if original_count > 0 else 0
        log(f"📊 Coverage vs original CSV: {coverage_pct:.1f}% ({stats['total_swaps']:,}/{original_count:,})")

    except Exception as e:
        log(f"❌ Error getting final statistics: {e}")

if __name__ == "__main__":
    main()