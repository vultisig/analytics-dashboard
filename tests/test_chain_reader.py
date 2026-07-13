"""Unit tests for the transparency page's read-only Ethereum RPC client."""
import os
import sys
import unittest
from decimal import Decimal
from unittest.mock import MagicMock, patch

_VA = os.path.join(os.path.dirname(__file__), "..", "vultisig-analytics")
if os.path.isdir(_VA) and _VA not in sys.path:
    sys.path.insert(0, _VA)

from chain_reader import (  # noqa: E402
    Q96,
    ChainReader,
    ChainReaderError,
    amounts_for_liquidity,
    sqrt_ratio_at_tick,
)
from config import config  # noqa: E402


TOKEN_ID = 1_195_906
LIQUIDITY = 1_000_000_000_000_000_000


def _word(value: int) -> str:
    return value.to_bytes(32, "big", signed=value < 0).hex()


def _address_word(address: str) -> str:
    return f"{int(address, 16):064x}"


def _rpc_response(result: str) -> MagicMock:
    response = MagicMock()
    response.json.return_value = {"jsonrpc": "2.0", "id": 1, "result": result}
    return response


def _slot0_result(sqrt_price_x96: int, tick: int = 0) -> str:
    return "0x" + "".join([
        _word(sqrt_price_x96), _word(tick), _word(0), _word(0),
        _word(0), _word(0), _word(1),
    ])


def _position_result() -> str:
    return "0x" + "".join([
        _word(0), _address_word("0x0000000000000000000000000000000000000000"),
        _address_word(config.USDC_ADDRESS), _address_word(config.VULT_ADDRESS),
        _word(10_000), _word(-60), _word(60), _word(LIQUIDITY),
        _word(0), _word(0), _word(0), _word(0),
    ])


class TestChainReader(unittest.TestCase):
    def test_config_uses_canonical_transparency_addresses(self):
        self.assertEqual(config.VULT_ADDRESS, "0xb788144DF611029C60b859DF47e79B7726C4DEBa")
        self.assertEqual(config.USDC_ADDRESS, "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")
        self.assertEqual(config.FEE_TREASURY_ADDRESS, "0x8E247a480449c84a5fDD25974A8501f3EFa4ABb9")
        self.assertEqual(config.BUYBACK_WALLET_ADDRESS, "0xBee139A9d76840d52F69CADc27FeA936eCBbc285")
        self.assertEqual(config.VULT_USDC_POOL_ADDRESS, "0x6Df52cC6E2E6f6531E4ceB4b083CF49864A89020")
        self.assertEqual(config.NFPM_ADDRESS, "0xC36442b4a4522E871399CD717aBDD847Ab11FE88")
        self.assertEqual(config.DEAD_ADDRESS, "0x000000000000000000000000000000000000dEaD")

    def test_erc20_balance_uses_balance_of_and_caches_result(self):
        reader = ChainReader("https://rpc.example")
        response = _rpc_response(hex(3_004_339 * 10**18))

        with patch("chain_reader.requests.post", return_value=response) as post:
            balance = reader.get_erc20_balance(
                config.VULT_ADDRESS,
                config.FEE_TREASURY_ADDRESS,
                decimals=18,
            )
            cached = reader.get_erc20_balance(
                config.VULT_ADDRESS,
                config.FEE_TREASURY_ADDRESS,
                decimals=18,
            )

        self.assertEqual(balance, Decimal("3004339"))
        self.assertEqual(cached, balance)
        self.assertEqual(post.call_count, 1)
        request_data = post.call_args.kwargs["json"]["params"][0]["data"]
        self.assertEqual(request_data[:10], "0x70a08231")

    def test_spot_price_uses_pool_slot0(self):
        reader = ChainReader("https://rpc.example")
        sqrt_price_x96 = int(Q96 * Decimal("3086066.425"))
        response = _rpc_response(_slot0_result(sqrt_price_x96))

        with patch("chain_reader.requests.post", return_value=response) as post:
            price = reader.get_spot_price()

        self.assertAlmostEqual(float(price), 0.105, places=5)
        request_data = post.call_args.kwargs["json"]["params"][0]["data"]
        self.assertEqual(request_data, "0x3850c7bd")

    def test_position_amounts_cover_each_price_range(self):
        below = amounts_for_liquidity(
            LIQUIDITY,
            sqrt_ratio_at_tick(-120),
            -60,
            60,
        )
        in_range = amounts_for_liquidity(LIQUIDITY, Q96, -60, 60)
        above = amounts_for_liquidity(
            LIQUIDITY,
            sqrt_ratio_at_tick(120),
            -60,
            60,
        )

        self.assertGreater(below[0], 0)
        self.assertEqual(below[1], 0)
        self.assertGreater(in_range[0], 0)
        self.assertGreater(in_range[1], 0)
        self.assertEqual(above[0], 0)
        self.assertGreater(above[1], 0)

    def test_locked_positions_require_dead_address_ownership(self):
        reader = ChainReader("https://rpc.example")

        def post_side_effect(*_args, **kwargs):
            data = kwargs["json"]["params"][0]["data"]
            if data.startswith("0x99fbab88"):
                return _rpc_response(_position_result())
            if data.startswith("0x6352211e"):
                return _rpc_response("0x" + _address_word(config.DEAD_ADDRESS))
            if data == "0x3850c7bd":
                return _rpc_response(_slot0_result(Q96))
            self.fail(f"Unexpected RPC call: {data}")

        with patch.object(config, "LOCKED_POSITION_IDS", [TOKEN_ID]):
            with patch("chain_reader.requests.post", side_effect=post_side_effect):
                positions = reader.get_locked_positions()

        self.assertEqual(len(positions), 1)
        position = positions[0]
        self.assertEqual(position.token_id, TOKEN_ID)
        self.assertEqual(position.tick_lower, -60)
        self.assertEqual(position.tick_upper, 60)
        self.assertGreater(position.amount0, Decimal(0))
        self.assertGreater(position.amount1, Decimal(0))

    def test_rpc_error_raises_instead_of_returning_a_false_balance(self):
        reader = ChainReader("https://rpc.example")
        response = MagicMock()
        response.json.return_value = {"error": {"message": "upstream unavailable"}}

        with patch("chain_reader.requests.post", return_value=response):
            with self.assertRaisesRegex(ChainReaderError, "upstream unavailable"):
                reader.get_spot_price()


if __name__ == "__main__":
    unittest.main()
