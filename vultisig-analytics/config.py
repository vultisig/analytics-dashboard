# config.py
import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # Database
    DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:pass@localhost:5432/vultisig_analytics")

    # API Configuration
    THORCHAIN_API_URL = "https://midgard.ninerealms.com/v2/actions"
    MAYACHAIN_API_URL = "https://midgard.mayachain.info/v2/actions"
    LIFI_API_URL = "https://li.quest/v2/analytics/transfers"
    ONEINCH_API_URL = "https://api.1inch.dev/history"
    ONEINCH_RAYNALYTICS_URL = "https://raynalytics.net/api/vultisig-1inch-swap-insights"

    # API Keys
    ONEINCH_API_KEY = os.getenv("ONEINCH_API_KEY", "")
    LIFI_API_KEY = os.getenv("LIFI_API_KEY", "")
    ARKHAM_API_KEY = os.getenv("ARKHAM_API_KEY", "")

    # Rate limiting - Per-source delays (seconds between requests)
    API_DELAY_SECONDS = 2  # Default fallback
    API_DELAYS = {
        'thorchain': 1.5,   # 40 req/min (under 100 limit)
        'mayachain': 1.5,   # 40 req/min (under 100 limit)
        'lifi': 0.8,        # 75 req/min (under 200 limit with API key)
        'arkham': 0.1       # 600 req/min (under 1200 limit)
    }
    MAX_RETRIES = 5
    REQUEST_TIMEOUT = 120  # Increased for slow vanaheimex responses

    # Processing
    BATCH_SIZE = 1000
    SYNC_INTERVAL_MINUTES = 15  # Optimized polling frequency (was 30)

    # Affiliate codes for filtering
    VULTISIG_AFFILIATES = ["va", "vi", "v0"]

config = Config()