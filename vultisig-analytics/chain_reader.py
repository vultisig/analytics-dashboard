"""Read-only Ethereum RPC data used by the transparency page."""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from threading import Lock
from time import monotonic
from typing import Callable

import requests

from config import config


BALANCE_OF_SELECTOR = "0x70a08231"
SLOT0_SELECTOR = "0x3850c7bd"
OWNER_OF_SELECTOR = "0x6352211e"
POSITIONS_SELECTOR = "0x99fbab88"
CACHE_TTL_SECONDS = 300
MAX_TICK = 887_272
MAX_UINT256 = (1 << 256) - 1
Q96 = 1 << 96
Q128 = 1 << 128
Q192 = Q96 * Q96
USDC_DECIMALS = 6
VULT_DECIMALS = 18

TICK_MULTIPLIERS = (
    0xfffcb933bd6fad37aa2d162d1a594001,
    0xfff97272373d413259a46990580e213a,
    0xfff2e50f5f656932ef12357cf3c7fdcc,
    0xffe5caca7e10e4e61c3624eaa0941cd0,
    0xffcb9843d60f6159c9db58835c926644,
    0xff973b41fa98c081472e6896dfb254c0,
    0xff2ea16466c96a3843ec78b326b52861,
    0xfe5dee046a99a2a811c461f1969c3053,
    0xfcbe86c7900a88aedcffc83b479aa3a4,
    0xf987a7253ac413176f2b074cf7815e54,
    0xf3392b0822b70005940c7a398e4b70f3,
    0xe7159475a2c29b7443b29c7fa6e889d9,
    0xd097f3bdfd2022b8845ad8f792aa5825,
    0xa9f746462d870fdf8a65dc1f90e061e5,
    0x70d869a156d2a1b890bb3df62baf32f7,
    0x31be135f97d08fd981231505542fcfa6,
    0x9aa508b5b7a84e1c677de54f3e99bc9,
    0x5d6af8dedb81196699c329225ee604,
    0x2216e584f5fa1ea926041bedfe98,
    0x48a170391f7dc42444e8fa2,
)


class ChainReaderError(RuntimeError):
    """An Ethereum RPC response could not be used as a public receipt."""


@dataclass(frozen=True)
class Slot0:
    sqrt_price_x96: int
    tick: int


@dataclass(frozen=True)
class LockedPosition:
    token_id: int
    token0: str
    token1: str
    fee: int
    tick_lower: int
    tick_upper: int
    liquidity: int
    amount0: Decimal
    amount1: Decimal


@dataclass(frozen=True)
class _CacheEntry:
    value: str
    expires_at: float


def _encode_address(address: str) -> str:
    return f"{int(address, 16):064x}"


def _encode_uint256(value: int) -> str:
    if value < 0:
        raise ValueError("uint256 values cannot be negative")
    return f"{value:064x}"


def _decode_words(result: str, expected_count: int) -> list[str]:
    payload = result.removeprefix("0x")
    if not payload or len(payload) % 64 or len(payload) // 64 < expected_count:
        raise ChainReaderError("invalid ABI result length")
    return [payload[index:index + 64] for index in range(0, len(payload), 64)]


def _decode_address(word: str) -> str:
    return f"0x{word[-40:]}"


def _decode_signed_word(word: str) -> int:
    return int.from_bytes(bytes.fromhex(word), "big", signed=True)


def _to_decimal(value: int, decimals: int) -> Decimal:
    return Decimal(value) / (Decimal(10) ** decimals)


def sqrt_ratio_at_tick(tick: int) -> int:
    """Port Uniswap V3 TickMath.getSqrtRatioAtTick without web3.py."""
    if not -MAX_TICK <= tick <= MAX_TICK:
        raise ValueError(f"tick must be between {-MAX_TICK} and {MAX_TICK}")

    ratio = Q128
    for bit, multiplier in enumerate(TICK_MULTIPLIERS):
        if abs(tick) & (1 << bit):
            ratio = (ratio * multiplier) >> 128
    if tick > 0:
        ratio = MAX_UINT256 // ratio
    return (ratio >> 32) + int(bool(ratio & ((1 << 32) - 1)))


def amounts_for_liquidity(
    liquidity: int,
    sqrt_price_x96: int,
    tick_lower: int,
    tick_upper: int,
) -> tuple[int, int]:
    """Return raw token0/token1 balances for a V3 position at current price."""
    sqrt_lower = sqrt_ratio_at_tick(tick_lower)
    sqrt_upper = sqrt_ratio_at_tick(tick_upper)
    if sqrt_price_x96 <= sqrt_lower:
        return liquidity * (sqrt_upper - sqrt_lower) * Q96 // sqrt_upper // sqrt_lower, 0
    if sqrt_price_x96 < sqrt_upper:
        amount0 = liquidity * (sqrt_upper - sqrt_price_x96) * Q96 // sqrt_upper // sqrt_price_x96
        amount1 = liquidity * (sqrt_price_x96 - sqrt_lower) // Q96
        return amount0, amount1
    return 0, liquidity * (sqrt_upper - sqrt_lower) // Q96


class ChainReader:
    """Caches public Ethereum contract reads for one transparency process."""

    def __init__(
        self,
        rpc_url: str | None = None,
        cache_ttl_seconds: int = CACHE_TTL_SECONDS,
    ) -> None:
        self.rpc_url = rpc_url or config.ETH_RPC_URL
        self.cache_ttl_seconds = cache_ttl_seconds
        self._cache: dict[tuple[str, str], _CacheEntry] = {}
        self._cache_lock = Lock()

    def get_erc20_balance(
        self,
        token_address: str,
        wallet_address: str,
        decimals: int,
    ) -> Decimal:
        data = BALANCE_OF_SELECTOR + _encode_address(wallet_address)
        return _to_decimal(int(self._eth_call(token_address, data), 16), decimals)

    def get_native_balance(self, wallet_address: str) -> Decimal:
        result = self._cached(
            ("eth_getBalance", wallet_address.lower()),
            lambda: self._request("eth_getBalance", [wallet_address, "latest"]),
        )
        return _to_decimal(int(result, 16), VULT_DECIMALS)

    def get_fee_treasury_balances(self) -> dict[str, Decimal]:
        return {
            "VULT": self.get_erc20_balance(config.VULT_ADDRESS, config.FEE_TREASURY_ADDRESS, VULT_DECIMALS),
            "USDC": self.get_erc20_balance(config.USDC_ADDRESS, config.FEE_TREASURY_ADDRESS, USDC_DECIMALS),
            "ETH": self.get_native_balance(config.FEE_TREASURY_ADDRESS),
        }

    def get_slot0(self) -> Slot0:
        words = _decode_words(self._eth_call(config.VULT_USDC_POOL_ADDRESS, SLOT0_SELECTOR), 2)
        return Slot0(sqrt_price_x96=int(words[0], 16), tick=_decode_signed_word(words[1]))

    def get_spot_price(self) -> Decimal:
        slot0 = self.get_slot0()
        if slot0.sqrt_price_x96 <= 0:
            raise ChainReaderError("slot0 returned a zero sqrt price")
        numerator = Decimal(Q192) * (Decimal(10) ** (VULT_DECIMALS - USDC_DECIMALS))
        return numerator / Decimal(slot0.sqrt_price_x96 * slot0.sqrt_price_x96)

    def get_locked_positions(self) -> list[LockedPosition]:
        slot0 = self.get_slot0()
        return [
            self.get_position(token_id, slot0)
            for token_id in config.LOCKED_POSITION_IDS
            if self.get_position_owner(token_id).lower() == config.DEAD_ADDRESS.lower()
        ]

    def get_position(self, token_id: int, slot0: Slot0) -> LockedPosition:
        data = POSITIONS_SELECTOR + _encode_uint256(token_id)
        words = _decode_words(self._eth_call(config.NFPM_ADDRESS, data), 12)
        tick_lower = _decode_signed_word(words[5])
        tick_upper = _decode_signed_word(words[6])
        liquidity = int(words[7], 16)
        amount0, amount1 = amounts_for_liquidity(liquidity, slot0.sqrt_price_x96, tick_lower, tick_upper)
        return LockedPosition(
            token_id=token_id,
            token0=_decode_address(words[2]),
            token1=_decode_address(words[3]),
            fee=int(words[4], 16),
            tick_lower=tick_lower,
            tick_upper=tick_upper,
            liquidity=liquidity,
            amount0=_to_decimal(amount0, self._token_decimals(words[2])),
            amount1=_to_decimal(amount1, self._token_decimals(words[3])),
        )

    def get_position_owner(self, token_id: int) -> str:
        result = self._eth_call(config.NFPM_ADDRESS, OWNER_OF_SELECTOR + _encode_uint256(token_id))
        return _decode_address(_decode_words(result, 1)[0])

    def _token_decimals(self, address_word: str) -> int:
        address = _decode_address(address_word).lower()
        if address == config.USDC_ADDRESS.lower():
            return USDC_DECIMALS
        if address == config.VULT_ADDRESS.lower():
            return VULT_DECIMALS
        raise ChainReaderError(f"unexpected position token {address}")

    def _eth_call(self, address: str, data: str) -> str:
        return self._cached(
            (address.lower(), data),
            lambda: self._request("eth_call", [{"to": address, "data": data}, "latest"]),
        )

    def _cached(self, key: tuple[str, str], fetch: Callable[[], str]) -> str:
        now = monotonic()
        with self._cache_lock:
            cached = self._cache.get(key)
            if cached and cached.expires_at > now:
                return cached.value
        value = fetch()
        with self._cache_lock:
            self._cache[key] = _CacheEntry(value=value, expires_at=monotonic() + self.cache_ttl_seconds)
        return value

    def _request(self, method: str, params: list[object]) -> str:
        payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
        try:
            response = requests.post(self.rpc_url, json=payload, timeout=30)
            response.raise_for_status()
            response_data = response.json()
        except (requests.RequestException, ValueError) as error:
            raise ChainReaderError(f"Ethereum RPC request failed: {error}") from error
        if not isinstance(response_data, dict):
            raise ChainReaderError("Ethereum RPC response is not an object")
        if error := response_data.get("error"):
            message = error.get("message") if isinstance(error, dict) else str(error)
            raise ChainReaderError(message or "Ethereum RPC returned an error")
        result = response_data.get("result")
        if not isinstance(result, str):
            raise ChainReaderError("Ethereum RPC response has no hexadecimal result")
        try:
            int(result, 16)
        except ValueError as error:
            raise ChainReaderError("Ethereum RPC response has an invalid hexadecimal result") from error
        return result
