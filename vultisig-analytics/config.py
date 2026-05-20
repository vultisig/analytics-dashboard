# config.py
import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # Database
    DATABASE_URL = os.getenv("DATABASE_URL") or (
        "postgresql://{user}:{password}@{host}:{port}/{db}".format(
            user=os.getenv("POSTGRES_USER", "vultisig_user"),
            password=os.getenv("POSTGRES_PASSWORD", ""),
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=os.getenv("POSTGRES_PORT", "5432"),
            db=os.getenv("POSTGRES_DB", "vultisig_analytics"),
        )
    )

    # API Configuration
    THORCHAIN_API_URL = "https://gateway.liquify.com/chain/thorchain_midgard/v2/actions"
    MAYACHAIN_API_URL = "https://midgard.mayachain.info/v2/actions"
    LIFI_API_URL = "https://li.quest/v2/analytics/transfers"
    ONEINCH_API_URL = "https://api.1inch.dev/history"
    ONEINCH_RAYNALYTICS_URL = "https://raynalytics.net/api/vultisig-1inch-swap-insights"

    # API Keys
    ONEINCH_API_KEY = os.getenv("ONEINCH_API_KEY", "")
    LIFI_API_KEY = os.getenv("LIFI_API_KEY", "")
    ARKHAM_API_KEY = os.getenv("ARKHAM_API_KEY", "")

    # Arkham-sourced aggregator fee receivers
    # (source_name, evm_address) — Arkham crawls ERC20 transfers TO these.
    # Single source of truth: ingestor + api_server + enrichers all read here.
    ARKHAM_FEE_RECEIVERS = [
        ("1inch", os.getenv(
            "ONEINCH_FEE_RECEIVER",
            "0xA4a4f610e89488EB4ECc6c63069f241a54485269",
        )),
        ("kyberswap", os.getenv(
            "KYBER_FEE_RECEIVER",
            "0x8E247a480449c84a5fDD25974A8501f3EFa4ABb9",
        )),
    ]
    ARKHAM_PROVIDERS = tuple(name for name, _ in ARKHAM_FEE_RECEIVERS)

    # Rate limiting - Per-source delays (seconds between requests)
    API_DELAY_SECONDS = 2  # Default fallback
    API_DELAYS = {
        'thorchain': 1.5,   # 40 req/min (under 100 limit)
        'mayachain': 1.5,   # 40 req/min (under 100 limit)
        'lifi': 0.8,        # 75 req/min (under 200 limit with API key)
        'arkham': 0.1,      # 600 req/min (under 1200 limit) — kept for parity with retained ArkhamIngestor
        'etherscan': 0.25,  # 4 req/s — under the V2 free-tier 5 req/s shared budget
    }
    MAX_RETRIES = 5
    REQUEST_TIMEOUT = 120  # Increased for slow vanaheimex responses

    # Processing
    BATCH_SIZE = 1000
    SYNC_INTERVAL_MINUTES = 15  # Optimized polling frequency (was 30)

    # Affiliate codes for filtering
    VULTISIG_AFFILIATES = ["va", "vi", "v0"]

config = Config()