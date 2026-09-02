"""Fee-wallet inflows no provider claims.

Rows demoted to `other` never reach the dashboard totals, so a sender the
allowlists miss disappears instead of misleading. This makes the gap
visible: transfer count and stable-coin USD by sender over a window.
Only stables are summed — `other` rows are never priced by the enricher.
"""

from typing import Any, Dict, List, Sequence

STABLE_SYMBOLS = ("USDC", "USDT", "DAI")
UNATTRIBUTED_PROTOCOL = "other"
DEFAULT_WINDOW_DAYS = 30
MAX_WINDOW_DAYS = 365
TOP_SENDERS = 5

UNATTRIBUTED_QUERY = """
    SELECT
        LOWER(from_address) AS from_address,
        COUNT(*) AS transfers,
        COALESCE(SUM(
            CASE WHEN UPPER(fee_token_symbol) IN %s
                 THEN CAST(fee_amount_raw AS NUMERIC) ELSE 0 END
        ), 0) AS stable_usd
    FROM dex_aggregator_revenue
    WHERE protocol = %s
      AND fee_data_source = 'etherscan'
      AND timestamp >= NOW() - INTERVAL '1 day' * %s
    GROUP BY 1
    ORDER BY stable_usd DESC, transfers DESC
"""


def clamp_window_days(raw: Any) -> int:
    try:
        days = int(raw)
    except (TypeError, ValueError):
        return DEFAULT_WINDOW_DAYS
    return max(1, min(days, MAX_WINDOW_DAYS))


def query_params(days: int) -> tuple:
    return (STABLE_SYMBOLS, UNATTRIBUTED_PROTOCOL, days)


ADDRESS_PREFIX_CHARS = 10
ADDRESS_SUFFIX_CHARS = 4


def abbreviate_address(address: str) -> str:
    """Public endpoint: enough to recognise a sender, not to enumerate counterparties."""
    if len(address) <= ADDRESS_PREFIX_CHARS + ADDRESS_SUFFIX_CHARS:
        return address
    return f"{address[:ADDRESS_PREFIX_CHARS]}…{address[-ADDRESS_SUFFIX_CHARS:]}"


def summarize_unattributed(rows: Sequence[Dict[str, Any]], days: int) -> Dict[str, Any]:
    senders: List[Dict[str, Any]] = [
        {
            "from_address": abbreviate_address(row["from_address"]),
            "transfers": int(row["transfers"]),
            "stable_usd": round(float(row["stable_usd"]), 2),
        }
        for row in rows
    ]
    return {
        "days": days,
        "transfers": sum(s["transfers"] for s in senders),
        "stable_usd": round(sum(s["stable_usd"] for s in senders), 2),
        "top_senders": senders[:TOP_SENDERS],
    }
