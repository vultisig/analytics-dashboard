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

    # Same Kyber fee wallet; classify by sender, never a second integrator.
    SWAPKIT_PROTOCOL = "swapkit"
    SWAPKIT_FEE_SENDERS = frozenset({
        "0x9025b8ff35ca44f7018c3a37fe0f69e63dbb0743",  # SKWrapGeneric_V1
    })
    SWAPKIT_PAYOUT_SENDERS = frozenset({
        "0xf70da97812cb96acdf810712aa562db8dfa3dbef",
        "0x2cff890f0378a11913b6129b2e97417a2c302680",
        "0x8443e89848ef39017184c42171388674c551ff9a",
    })
    DEX_REVENUE_PROVIDERS = ARKHAM_PROVIDERS + (SWAPKIT_PROTOCOL,)
    # 50 bps → volume = fee * 200. Shared so enricher and SKWrap cannot drift.
    AFFILIATE_VOLUME_TO_FEE_MULTIPLIER = 200

    # Volume-only APIs: swaps.source=swapkit, affiliate_fee_usd=0.
    CHAINFLIP_GRAPHQL_URL = "https://explorer-service-processor.chainflip.io/graphql"
    CHAINFLIP_BROKER_SS58 = "cFLTzujZfsG2mdaQ4MJRZ36uD4Y2U5y7sLhEccS7N2gfqQPqj"
    NEAR_INTENTS_API_URL = "https://explorer.near-intents.org/api/v0/transactions"
    NEAR_INTENTS_AFFILIATE = "vultisigswapkit.near"
    NEAR_INTENTS_JWT = os.getenv("NEAR_INTENTS_JWT", "")

    # LI.FI Diamond proxy — tx.to for every LI.FI-routed swap on all EVM chains.
    LIFI_DIAMOND_ADDRESS = "0x1231deb6f5749ef6ce6943a275a1d3e7486f4eae"
    # li.quest `tool` values credited as their own provider instead of 'lifi'.
    ATTRIBUTED_LIFI_TOOLS = ("1inch",)
    # How long a LiFi-Diamond fee row may wait for its li.quest swap to land
    # before the classifier gives up and demotes it to 'other'.
    LIFI_MATCH_GRACE_DAYS = 7

    # Attributed rows live in dex_aggregator_revenue, whose analytics queries
    # filter to ARKHAM_PROVIDERS — a tool outside that set would be credited
    # into rows no query ever shows.
    _unknown_tools = set(ATTRIBUTED_LIFI_TOOLS) - set(ARKHAM_PROVIDERS)
    if _unknown_tools:
        raise ValueError(
            f"ATTRIBUTED_LIFI_TOOLS {_unknown_tools} missing from ARKHAM_PROVIDERS — "
            "attributed swaps would be invisible in analytics"
        )

    # Rate limiting - Per-source delays (seconds between requests)
    API_DELAY_SECONDS = 2  # Default fallback
    API_DELAYS = {
        'thorchain': 1.5,   # 40 req/min (under 100 limit)
        'mayachain': 1.5,   # 40 req/min (under 100 limit)
        'lifi': 0.8,        # 75 req/min (under 200 limit with API key)
        'arkham': 0.1,      # 600 req/min (under 1200 limit) — kept for parity with retained ArkhamIngestor
        'etherscan': 0.25,  # 4 req/s — under the V2 free-tier 5 req/s shared budget
        'chainflip': 1.0,
        'near-intents': 5.0,  # Explorer API: 1 req / 5s per partner JWT
    }
    MAX_RETRIES = 5
    REQUEST_TIMEOUT = 120  # Increased for slow vanaheimex responses

    # Processing
    BATCH_SIZE = 1000
    SYNC_INTERVAL_MINUTES = 15  # Optimized polling frequency (was 30)

    # Affiliate codes for filtering
    VULTISIG_AFFILIATES = ["va", "vi", "v0"]

config = Config()