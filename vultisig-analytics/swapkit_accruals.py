"""Latest SwapKit accrual snapshot, shaped for the API.

Accrued means credited at the provider and not yet paid to the fee wallet.
It is the only per-platform view of Near-Intents revenue; the eventual
payout is a lump sum. USD covers stablecoin balances only.
"""

from typing import Any, Dict, List, Sequence

LATEST_ACCRUALS_QUERY = """
    SELECT snapshot_at, platform, token_id, amount_raw, amount_usd
    FROM swapkit_accruals
    WHERE provider = %s
      AND snapshot_at = (SELECT MAX(snapshot_at) FROM swapkit_accruals WHERE provider = %s)
    ORDER BY platform, amount_usd DESC NULLS LAST
"""


def query_params(provider: str) -> tuple:
    return (provider, provider)


def summarize_accruals(provider: str, rows: Sequence[Dict[str, Any]], iso_utc) -> Dict[str, Any]:
    by_platform: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        entry = by_platform.setdefault(row["platform"], {"stable_usd": 0.0, "unpriced_tokens": 0})
        if row["amount_usd"] is None:
            entry["unpriced_tokens"] += 1
        else:
            entry["stable_usd"] += float(row["amount_usd"])
    platforms: List[Dict[str, Any]] = [
        {"platform": name, "stable_usd": round(v["stable_usd"], 2), "unpriced_tokens": v["unpriced_tokens"]}
        for name, v in sorted(by_platform.items())
    ]
    return {
        "provider": provider,
        "snapshot_at": iso_utc(rows[0]["snapshot_at"]) if rows else None,
        "stable_usd": round(sum(p["stable_usd"] for p in platforms), 2),
        "platforms": platforms,
    }
