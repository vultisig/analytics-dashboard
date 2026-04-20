# tests/test_mayachain.py
"""
Unit tests for ingestors/mayachain.py (MayaChainIngestor).

All fixtures are built on a real MayaChain Midgard /v2/actions response
envelope (4 actions from a single batch, none of which has a Vultisig
affiliate). Variant helpers mutate copies of one canonical action to
exercise each code path.

Run from the repo root with:
    python3 -m unittest discover -s tests -p 'test_*.py' -v
"""
import copy
import json
import os
import sys
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

# Make `ingestors` importable whether the test runs from the repo root
# (CI) or from vultisig-analytics/.
_VA = os.path.join(os.path.dirname(__file__), "..", "vultisig-analytics")
if os.path.isdir(_VA) and _VA not in sys.path:
    sys.path.insert(0, _VA)

from ingestors.mayachain import MayaChainIngestor  # noqa: E402


# ---------------------------------------------------------------------------
# Real-world fixture: full /v2/actions response envelope
# ---------------------------------------------------------------------------

USER_ADDR = "maya176820wmjp536wtsddxn4y52ghp3w23k747hard"

REAL_RESPONSE = {
    "actions": [
        # --- Action 0: DASH~DASH → ZEC~ZEC (133970041 base DASH) --------------
        {
            "date": "1775901483902216760",
            "height": "16067045",
            "in": [
                {
                    "address": USER_ADDR,
                    "coins": [{"amount": "133970041", "asset": "DASH~DASH"}],
                    "txID": "D7CB00D1CA6712E63A808964EB1C1B35BD12C37247A55B9B0A21F8008E9E9FF2",
                }
            ],
            "metadata": {
                "swap": {
                    "affiliateAddress": "",
                    "affiliateFee": "0",
                    "inPriceUSD": "47.44523553679186",
                    "isStreamingSwap": False,
                    "liquidityFee": "4538658096",
                    "memo": f"=:ZEC~ZEC:{USER_ADDR}:16695968",
                    "networkFees": [],
                    "outPriceUSD": "380.32445048991633",
                    "streamingSwapMeta": {
                        "count": "1",
                        "depositedCoin": {"amount": "0", "asset": ""},
                        "inCoin": {"amount": "0", "asset": ""},
                        "interval": "0",
                        "lastHeight": "0",
                        "outCoin": {"amount": "0", "asset": ""},
                        "outEstimation": "16712674",
                        "quantity": "1",
                    },
                    "swapSlip": "7",
                    "swapTarget": "16695968",
                }
            },
            "out": [
                {
                    "address": USER_ADDR,
                    "coins": [{"amount": "16712674", "asset": "ZEC~ZEC"}],
                    "height": "16067045",
                    "txID": "",
                },
                {
                    "address": USER_ADDR,
                    "coins": [{"amount": "16712674", "asset": "ZEC~ZEC"}],
                    "txID": "D7CB00D1CA6712E63A808964EB1C1B35BD12C37247A55B9B0A21F8008E9E9FF2",
                },
            ],
            "pools": ["DASH.DASH", "ZEC.ZEC"],
            "status": "success",
            "type": "swap",
        },
        # --- Action 1: DASH~DASH → ZEC~ZEC (canonical action we mutate) ------
        {
            "date": "1775901478182631963",
            "height": "16067044",
            "in": [
                {
                    "address": USER_ADDR,
                    "coins": [{"amount": "231964753", "asset": "DASH~DASH"}],
                    "txID": "B04FF23501FA01B22019090A979BE6872699A7D09BF81069121327FFCC878E83",
                }
            ],
            "metadata": {
                "swap": {
                    "affiliateAddress": "",
                    "affiliateFee": "0",
                    "inPriceUSD": "47.512560873444905",
                    "isStreamingSwap": False,
                    "liquidityFee": "13643413219",
                    "memo": f"=:ZEC~ZEC:{USER_ADDR}:28909433",
                    "networkFees": [],
                    "outPriceUSD": "380.2924279365528",
                    "streamingSwapMeta": {
                        "count": "1",
                        "depositedCoin": {"amount": "0", "asset": ""},
                        "inCoin": {"amount": "0", "asset": ""},
                        "interval": "0",
                        "lastHeight": "0",
                        "outCoin": {"amount": "0", "asset": ""},
                        "outEstimation": "28980939",
                        "quantity": "1",
                    },
                    "swapSlip": "13",
                    "swapTarget": "28909433",
                }
            },
            "out": [
                {
                    "address": USER_ADDR,
                    "coins": [{"amount": "28980939", "asset": "ZEC~ZEC"}],
                    "height": "16067044",
                    "txID": "",
                },
                {
                    "address": USER_ADDR,
                    "coins": [{"amount": "28980939", "asset": "ZEC~ZEC"}],
                    "txID": "B04FF23501FA01B22019090A979BE6872699A7D09BF81069121327FFCC878E83",
                },
            ],
            "pools": ["DASH.DASH", "ZEC.ZEC"],
            "status": "success",
            "type": "swap",
        },
        # --- Action 2: BTC~BTC → ZEC~ZEC (cross-chain via BTC pool) -----------
        {
            "date": "1775901472397565170",
            "height": "16067043",
            "in": [
                {
                    "address": USER_ADDR,
                    "coins": [{"amount": "204226", "asset": "BTC~BTC"}],
                    "txID": "272984F1A11971634A994C206ED1CE1CF88E2BF55852C9DB089D2ECACD42E0ED",
                }
            ],
            "metadata": {
                "swap": {
                    "affiliateAddress": "",
                    "affiliateFee": "0",
                    "inPriceUSD": "72903.07796151725",
                    "isStreamingSwap": False,
                    "liquidityFee": "2468596169",
                    "memo": f"=:ZEC~ZEC:{USER_ADDR}:39147490",
                    "networkFees": [],
                    "outPriceUSD": "380.23690781080614",
                    "streamingSwapMeta": {
                        "count": "1",
                        "depositedCoin": {"amount": "0", "asset": ""},
                        "inCoin": {"amount": "0", "asset": ""},
                        "interval": "0",
                        "lastHeight": "0",
                        "outCoin": {"amount": "0", "asset": ""},
                        "outEstimation": "39156386",
                        "quantity": "1",
                    },
                    "swapSlip": "2",
                    "swapTarget": "39147490",
                }
            },
            "out": [
                {
                    "address": USER_ADDR,
                    "coins": [{"amount": "39156386", "asset": "ZEC~ZEC"}],
                    "height": "16067043",
                    "txID": "",
                },
                {
                    "address": USER_ADDR,
                    "coins": [{"amount": "39156386", "asset": "ZEC~ZEC"}],
                    "txID": "272984F1A11971634A994C206ED1CE1CF88E2BF55852C9DB089D2ECACD42E0ED",
                },
            ],
            "pools": ["BTC.BTC", "ZEC.ZEC"],
            "status": "success",
            "type": "swap",
        },
        # --- Action 3: DASH~DASH → ZEC~ZEC (smaller, older block) -------------
        {
            "date": "1775901443327006347",
            "height": "16067038",
            "in": [
                {
                    "address": USER_ADDR,
                    "coins": [{"amount": "80198983", "asset": "DASH~DASH"}],
                    "txID": "3C7D727AA063B2F8EACB41FF68AE2474A478F34D7769664D8BD62879EC4296B5",
                }
            ],
            "metadata": {
                "swap": {
                    "affiliateAddress": "",
                    "affiliateFee": "0",
                    "inPriceUSD": "47.62945847162068",
                    "isStreamingSwap": False,
                    "liquidityFee": "1635710534",
                    "memo": f"=:ZEC~ZEC:{USER_ADDR}:10046817",
                    "networkFees": [],
                    "outPriceUSD": "380.16191299656856",
                    "streamingSwapMeta": {
                        "count": "1",
                        "depositedCoin": {"amount": "0", "asset": ""},
                        "inCoin": {"amount": "0", "asset": ""},
                        "interval": "0",
                        "lastHeight": "0",
                        "outCoin": {"amount": "0", "asset": ""},
                        "outEstimation": "10047912",
                        "quantity": "1",
                    },
                    "swapSlip": "4",
                    "swapTarget": "10046817",
                }
            },
            "out": [
                {
                    "address": USER_ADDR,
                    "coins": [{"amount": "10047912", "asset": "ZEC~ZEC"}],
                    "height": "16067038",
                    "txID": "",
                },
                {
                    "address": USER_ADDR,
                    "coins": [{"amount": "10047912", "asset": "ZEC~ZEC"}],
                    "txID": "3C7D727AA063B2F8EACB41FF68AE2474A478F34D7769664D8BD62879EC4296B5",
                },
            ],
            "pools": ["DASH.DASH", "ZEC.ZEC"],
            "status": "success",
            "type": "swap",
        },
    ],
    "count": "-1",
    "meta": {
        "nextPageToken": "160668699000000002",
        "prevPageToken": "160670459000000002",
    },
}

# The canonical action we mutate in most tests: action 1 (DASH→ZEC, ~$110).
REAL_SWAP = REAL_RESPONSE["actions"][1]
REAL_TX_HASH = "B04FF23501FA01B22019090A979BE6872699A7D09BF81069121327FFCC878E83"
REAL_HEIGHT = 16067044

# Pre-computed USD values for the canonical action.
# in_amount_usd  = (231964753 / 1e8) * 47.512560873444905  ≈ 110.21
# out_amount_usd = (28980939  / 1e8) * 380.2924279365528   ≈ 110.21
REAL_IN_USD = (231964753 / 1e8) * 47.512560873444905
REAL_OUT_USD = (28980939 / 1e8) * 380.2924279365528


# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------

def _clone(action_index: int = 1):
    """Return an independent deep-copy of one of the real actions (default: canonical)."""
    return copy.deepcopy(REAL_RESPONSE["actions"][action_index])


def _clone_response():
    """Return an independent deep-copy of the full /v2/actions response envelope."""
    return copy.deepcopy(REAL_RESPONSE)


def _with_vultisig(
    swap=None,
    *,
    code="vi",
    bps=35,
    affiliate_address=None,
    affiliate_fee=None,
    memo=None,
    fee_asset="MAYA.CACAO",
    fee_amount="350000000000",  # 35 CACAO at 1e10
    include_fee_output=True,
):
    """
    Take a base swap and bolt a Vultisig affiliate onto it.

    By default produces a single-affiliate ``vi`` swap at 35 bps with a
    CACAO affiliate fee output. Override any of the kwargs to construct
    dual-affiliate / fee-in-other-asset / missing-fee-output variants.

    Note: MayaChain ALWAYS calculates affiliate_fee_usd from bps, so the
    fee_asset/fee_amount don't directly affect that field — but the test
    helper still wires them through so the affiliate-output detection
    branches can be exercised.
    """
    s = _clone() if swap is None else copy.deepcopy(swap)
    meta = s["metadata"]["swap"]

    meta["affiliateAddress"] = affiliate_address if affiliate_address is not None else code
    meta["affiliateFee"] = affiliate_fee if affiliate_fee is not None else str(bps)
    meta["memo"] = memo if memo is not None else f"=:ZEC~ZEC:dest:0/1/1:{code}:{bps}"

    if include_fee_output:
        s["out"].append(
            {
                "address": "maya1affiliate",
                "coins": [{"asset": fee_asset, "amount": fee_amount}],
                "txID": "AFFTX1",
                # Match the base swap's block height so multi-action tests
                # don't get a mismatched height on the tacked-on output.
                "height": s.get("height", str(REAL_HEIGHT)),
                "affiliate": True,
            }
        )
    return s


# ---------------------------------------------------------------------------
# __init__
# ---------------------------------------------------------------------------

class TestInit(unittest.TestCase):
    def test_source_name_and_endpoint(self):
        ing = MayaChainIngestor()
        self.assertEqual(ing.source_name, "mayachain")
        self.assertIsInstance(ing.api_endpoint, str)
        self.assertTrue(ing.api_endpoint)


# ---------------------------------------------------------------------------
# fetch_data — single endpoint + BackoffRetry
# ---------------------------------------------------------------------------

class TestFetchData(unittest.TestCase):
    def setUp(self):
        self.ing = MayaChainIngestor()

    def test_endpoint_success(self):
        with patch.object(self.ing, "make_request", return_value={"actions": []}) as m:
            result = self.ing.fetch_data(limit=25)
        self.assertEqual(result, {"actions": []})
        m.assert_called_once()
        called_url, called_params = m.call_args[0]
        self.assertEqual(called_url, self.ing.api_endpoint)
        self.assertEqual(called_params["type"], "swap")
        self.assertEqual(called_params["limit"], 25)
        self.assertIn("vi", called_params["affiliate"])
        self.assertNotIn("nextPageToken", called_params)

    def test_full_response_envelope_is_returned_verbatim(self):
        """
        fetch_data is a thin pass-through: whatever Midgard returns must
        be handed back unchanged (including count / meta pagination fields).
        """
        with patch.object(
            self.ing, "make_request", return_value=_clone_response()
        ):
            result = self.ing.fetch_data(limit=4)
        self.assertEqual(len(result["actions"]), 4)
        self.assertEqual(result["count"], "-1")
        self.assertEqual(result["meta"]["nextPageToken"], "160668699000000002")
        self.assertEqual(result["meta"]["prevPageToken"], "160670459000000002")

    def test_next_page_token_is_forwarded(self):
        with patch.object(self.ing, "make_request", return_value={"ok": True}) as m:
            self.ing.fetch_data(next_page_token=REAL_RESPONSE["meta"]["nextPageToken"])
        _, params = m.call_args[0]
        self.assertEqual(params["nextPageToken"], "160668699000000002")

    def test_retry_then_succeed(self):
        side_effects = [Exception("transient"), {"actions": ["x"]}]
        with patch("ingestors.base.time.sleep"), \
             patch.object(self.ing, "make_request", side_effect=side_effects) as m:
            result = self.ing.fetch_data()
        self.assertEqual(result, {"actions": ["x"]})
        self.assertEqual(m.call_count, 2)

    def test_retries_exhausted_raises(self):
        with patch("ingestors.base.time.sleep"), \
             patch.object(self.ing, "make_request", side_effect=Exception("LAST")) as m:
            with self.assertRaises(Exception) as ctx:
                self.ing.fetch_data()
        self.assertIn("Max retries reached", str(ctx.exception))
        self.assertIn("LAST", str(ctx.exception))
        self.assertEqual(m.call_count, self.ing.backoff_retry.max_retries)


# ---------------------------------------------------------------------------
# _extract_vultisig_affiliate
# ---------------------------------------------------------------------------

class TestExtractVultisigAffiliate(unittest.TestCase):
    def setUp(self):
        self.ing = MayaChainIngestor()

    def test_every_real_action_has_no_vultisig_affiliate(self):
        """Every action in the real batch has affiliateAddress="" → None."""
        for i, action in enumerate(REAL_RESPONSE["actions"]):
            meta = action["metadata"]["swap"]
            with self.subTest(action=i, memo=meta["memo"]):
                result = self.ing._extract_vultisig_affiliate(
                    meta["affiliateAddress"], meta["memo"]
                )
                self.assertIsNone(result)

    def test_non_vultisig_affiliate_returns_none(self):
        self.assertIsNone(
            self.ing._extract_vultisig_affiliate("someone_else", "=:A:b:c:d:e:f")
        )

    def test_single_vultisig_affiliate(self):
        info = self.ing._extract_vultisig_affiliate("vi", "=:ZEC~ZEC:dest:0/1/1:vi:35")
        self.assertEqual(info, {"code": "vi", "bps": 35, "address": "vi"})

    def test_dual_affiliate_vultisig_second(self):
        info = self.ing._extract_vultisig_affiliate(
            "VALT/vi", "=:ZEC~ZEC:dest:0/1/1:VALT/vi:10/35"
        )
        self.assertEqual(info["code"], "vi")
        self.assertEqual(info["bps"], 35)
        self.assertEqual(info["address"], "vi")

    def test_dual_affiliate_vultisig_first(self):
        info = self.ing._extract_vultisig_affiliate(
            "va/OTHER", "=:ZEC~ZEC:dest:0/1/1:va/OTHER:42/10"
        )
        self.assertEqual(info["code"], "va")
        self.assertEqual(info["bps"], 42)

    def test_case_insensitive_match(self):
        info = self.ing._extract_vultisig_affiliate("VI", "=:a:b:c:d:VI:50")
        self.assertEqual(info["code"], "vi")
        self.assertEqual(info["bps"], 50)

    def test_memo_too_short_for_bps(self):
        info = self.ing._extract_vultisig_affiliate("vi", "=:ZEC~ZEC:dest")
        self.assertEqual(info["code"], "vi")
        self.assertEqual(info["bps"], 0)

    def test_empty_memo(self):
        info = self.ing._extract_vultisig_affiliate("vi", "")
        self.assertEqual(info["code"], "vi")
        self.assertEqual(info["bps"], 0)

    def test_v0_code_single_bps(self):
        info = self.ing._extract_vultisig_affiliate("v0", "=:a:b:c:d:v0:25")
        self.assertEqual(info["code"], "v0")
        self.assertEqual(info["bps"], 25)

    def test_bps_values_fewer_than_index_falls_back_to_first(self):
        info = self.ing._extract_vultisig_affiliate(
            "A/B/vi", "=:a:b:c:d:A/B/vi:99"
        )
        self.assertEqual(info["code"], "vi")
        self.assertEqual(info["bps"], 99)


# ---------------------------------------------------------------------------
# _find_vultisig_affiliate_output & _find_swap_output
# ---------------------------------------------------------------------------

class TestFindOutputs(unittest.TestCase):
    def setUp(self):
        self.ing = MayaChainIngestor()

    def test_no_real_action_has_affiliate_output(self):
        for i, action in enumerate(REAL_RESPONSE["actions"]):
            with self.subTest(action=i):
                self.assertIsNone(
                    self.ing._find_vultisig_affiliate_output(action["out"], "vi")
                )

    def test_real_swap_swap_output_is_first_zec(self):
        out = self.ing._find_swap_output(REAL_SWAP["out"], "DASH~DASH")
        self.assertEqual(out["coins"][0]["asset"], "ZEC~ZEC")
        # First output has empty txID
        self.assertEqual(out["txID"], "")

    def test_btc_action_swap_output_is_zec(self):
        """Action 2 is a BTC~BTC → ZEC~ZEC cross-chain swap."""
        action = REAL_RESPONSE["actions"][2]
        out = self.ing._find_swap_output(action["out"], "BTC~BTC")
        self.assertEqual(out["coins"][0]["asset"], "ZEC~ZEC")

    def test_find_affiliate_output_returns_first_affiliate(self):
        outputs = [
            {"affiliate": False, "coins": [{"asset": "ZEC~ZEC", "amount": "1"}]},
            {"affiliate": True, "coins": [{"asset": "MAYA.CACAO", "amount": "2"}]},
            {"affiliate": True, "coins": [{"asset": "MAYA.CACAO", "amount": "3"}]},
        ]
        result = self.ing._find_vultisig_affiliate_output(outputs, "vi")
        self.assertEqual(result["coins"][0]["amount"], "2")

    def test_find_affiliate_output_none_when_no_affiliate(self):
        outputs = [{"affiliate": False, "coins": []}]
        self.assertIsNone(self.ing._find_vultisig_affiliate_output(outputs, "vi"))

    def test_find_swap_output_prefers_different_asset_non_affiliate(self):
        outputs = [
            {"affiliate": True, "coins": [{"asset": "MAYA.CACAO", "amount": "1"}]},
            {"affiliate": False, "coins": [{"asset": "DASH~DASH", "amount": "2"}]},
            {"affiliate": False, "coins": [{"asset": "ZEC~ZEC", "amount": "3"}]},
        ]
        result = self.ing._find_swap_output(outputs, "DASH~DASH")
        self.assertEqual(result["coins"][0]["asset"], "ZEC~ZEC")

    def test_find_swap_output_falls_back_to_any_non_affiliate(self):
        outputs = [
            {"affiliate": True, "coins": [{"asset": "MAYA.CACAO", "amount": "1"}]},
            {"affiliate": False, "coins": [{"asset": "DASH~DASH", "amount": "2"}]},
        ]
        result = self.ing._find_swap_output(outputs, "DASH~DASH")
        self.assertEqual(result["coins"][0]["asset"], "DASH~DASH")

    def test_find_swap_output_last_resort_returns_first(self):
        outputs = [
            {"affiliate": True, "coins": [{"asset": "MAYA.CACAO", "amount": "1"}]},
        ]
        result = self.ing._find_swap_output(outputs, "DASH~DASH")
        self.assertEqual(result["coins"][0]["asset"], "MAYA.CACAO")

    def test_find_swap_output_empty_list_returns_none(self):
        self.assertIsNone(self.ing._find_swap_output([], "DASH~DASH"))


# ---------------------------------------------------------------------------
# parse_swap
# ---------------------------------------------------------------------------

class TestParseSwap(unittest.TestCase):
    def setUp(self):
        self.ing = MayaChainIngestor()

    # ---- All four real actions ----------------------------------------------

    def test_every_real_action_returns_none_because_no_affiliate(self):
        """
        Every action in the captured batch has affiliateAddress="" and no
        affiliate:true output — parse_swap must cleanly skip all of them.
        """
        for i, action in enumerate(REAL_RESPONSE["actions"]):
            with self.subTest(action=i):
                self.assertIsNone(self.ing.parse_swap(copy.deepcopy(action)))

    def test_real_swap_with_vultisig_added_parses(self):
        """
        Start from the real payload, add a Vultisig affiliate + fee output,
        and verify every denormalised field matches the real upstream data.
        """
        raw = _with_vultisig(code="vi", bps=35)
        result = self.ing.parse_swap(raw)

        self.assertIsNotNone(result)
        # Pulled straight from the real upstream payload
        self.assertEqual(
            result["tx_hash"],
            "B04FF23501FA01B22019090A979BE6872699A7D09BF81069121327FFCC878E83",
        )
        self.assertEqual(result["block_height"], 16067044)
        self.assertEqual(
            result["user_address"],
            "maya176820wmjp536wtsddxn4y52ghp3w23k747hard",
        )
        self.assertEqual(result["in_address"], result["user_address"])
        self.assertEqual(result["in_asset"], "DASH~DASH")
        self.assertEqual(result["in_amount"], 231964753.0)
        self.assertEqual(result["in_amount_raw"], "231964753")
        self.assertAlmostEqual(result["in_price_usd"], 47.512560873444905)
        self.assertAlmostEqual(result["out_price_usd"], 380.2924279365528)

        # USD amounts match the real upstream values.
        self.assertAlmostEqual(result["in_amount_usd"], REAL_IN_USD, places=6)
        self.assertAlmostEqual(result["out_amount_usd"], REAL_OUT_USD, places=6)

        # The diff between in/out USD is small (≪10%), so no sanity correction.
        diff_pct = abs(REAL_IN_USD - REAL_OUT_USD) / max(REAL_IN_USD, REAL_OUT_USD)
        self.assertLess(diff_pct, 0.01)

        # total_fee = max(0, in - out)
        self.assertAlmostEqual(
            result["total_fee_usd"],
            max(0, REAL_IN_USD - REAL_OUT_USD),
            places=6,
        )
        self.assertEqual(result["liquidity_fee_usd"], 0)
        self.assertEqual(result["network_fee_usd"], 0)

        # MayaChain affiliate fee = (bps/10000) * in_amount_usd, ALWAYS.
        expected_aff = (35 / 10000) * REAL_IN_USD
        self.assertAlmostEqual(result["affiliate_fee_usd"], expected_aff, places=6)

        # The fee output we attached used MAYA.CACAO
        self.assertEqual(result["out_asset"], "MAYA.CACAO")
        self.assertEqual(result["out_amount"], 350000000000.0)

        # Pools, metadata, and misc
        self.assertEqual(result["pool_1"], "DASH.DASH")
        self.assertEqual(result["pool_2"], "ZEC.ZEC")
        self.assertEqual(result["pools_used"], ["DASH.DASH", "ZEC.ZEC"])
        self.assertFalse(result["is_streaming_swap"])
        self.assertEqual(result["swap_slip"], 13.0)
        self.assertEqual(result["platform"], "iOS")
        self.assertEqual(result["swap_status"], "success")
        self.assertEqual(result["swap_type"], "swap")
        self.assertEqual(result["source"], "mayachain")
        self.assertEqual(result["affiliate_addresses"], ["vi"])
        self.assertEqual(result["affiliate_fees_bps"], [35])

        # metadata_complete preserves the streamingSwapMeta blob from upstream.
        meta = json.loads(result["metadata_complete"])
        self.assertIn("streamingSwapMeta", meta)
        self.assertEqual(meta["streamingSwapMeta"]["outEstimation"], "28980939")
        self.assertEqual(meta["liquidityFee"], "13643413219")
        self.assertEqual(meta["swapTarget"], "28909433")

        # out_addresses / tx_ids / heights:
        # Original payload has 2 ZEC outputs (one with empty txID + height, one
        # without height) and we tacked on 1 affiliate output → total 3.
        out_addresses = json.loads(result["out_addresses"])
        self.assertEqual(len(out_addresses), 3)
        self.assertTrue(out_addresses[2]["affiliate"])
        self.assertEqual(
            result["out_tx_ids"],
            [
                "",
                "B04FF23501FA01B22019090A979BE6872699A7D09BF81069121327FFCC878E83",
                "AFFTX1",
            ],
        )
        # Second real output has no `height` key → must round-trip as None.
        self.assertEqual(result["out_heights"], [16067044, None, 16067044])

        # network_fees_raw is an empty array in the real payload
        self.assertEqual(json.loads(result["network_fees_raw"]), [])

        # Volume tier: ~$110 → "100-1000"
        self.assertEqual(result["volume_tier"], "100-1000")

        # Timestamp parsed from the real nanosecond date
        self.assertIsInstance(result["timestamp"], datetime)
        self.assertEqual(result["date_only"], result["timestamp"].date())
        # 1775901478... ns → 2026-04-11 (roughly)
        self.assertEqual(result["timestamp"].year, 2026)

        # raw_data round-trips
        self.assertEqual(
            json.loads(result["raw_data"])["height"], REAL_SWAP["height"]
        )

    # ---- Parsing other real actions with a Vultisig affiliate bolted on -----

    def test_action_0_dash_zec_small_parses_with_vultisig(self):
        """
        Action 0: DASH~DASH → ZEC~ZEC (smaller: 133970041 base DASH ≈ $63.56).
        Volume tier should be '<=$100'.
        """
        raw = _with_vultisig(swap=_clone(0))
        result = self.ing.parse_swap(raw)
        self.assertIsNotNone(result)
        self.assertEqual(result["in_asset"], "DASH~DASH")
        self.assertEqual(result["in_amount"], 133970041.0)
        self.assertEqual(result["block_height"], 16067045)

        expected_in_usd = (133970041 / 1e8) * 47.44523553679186
        expected_out_usd = (16712674 / 1e8) * 380.32445048991633
        self.assertAlmostEqual(result["in_amount_usd"], expected_in_usd, places=6)
        self.assertAlmostEqual(result["out_amount_usd"], expected_out_usd, places=6)
        # ~$63.56 → tier '<=$100'
        self.assertEqual(result["volume_tier"], "<=$100")

        # Streaming meta from upstream round-trips
        meta = json.loads(result["metadata_complete"])
        self.assertEqual(meta["streamingSwapMeta"]["outEstimation"], "16712674")
        self.assertEqual(meta["swapSlip"], "7")

    def test_action_2_btc_zec_parses_with_vultisig(self):
        """
        Action 2: BTC~BTC → ZEC~ZEC cross-chain, small BTC amount (204226 base).
        Volume tier should be '100-1000' (~$148.89).
        """
        raw = _with_vultisig(swap=_clone(2))
        result = self.ing.parse_swap(raw)
        self.assertIsNotNone(result)
        self.assertEqual(result["in_asset"], "BTC~BTC")
        self.assertEqual(result["in_amount"], 204226.0)
        self.assertEqual(result["block_height"], 16067043)
        self.assertEqual(result["pool_1"], "BTC.BTC")
        self.assertEqual(result["pool_2"], "ZEC.ZEC")

        expected_in_usd = (204226 / 1e8) * 72903.07796151725
        expected_out_usd = (39156386 / 1e8) * 380.23690781080614
        self.assertAlmostEqual(result["in_amount_usd"], expected_in_usd, places=6)
        self.assertAlmostEqual(result["out_amount_usd"], expected_out_usd, places=6)
        self.assertEqual(result["volume_tier"], "100-1000")

        # Swap slip from upstream (2 → 0.02%)
        self.assertEqual(result["swap_slip"], 2.0)

        # tx_hash from upstream
        self.assertEqual(
            result["tx_hash"],
            "272984F1A11971634A994C206ED1CE1CF88E2BF55852C9DB089D2ECACD42E0ED",
        )

    def test_action_3_dash_zec_older_block_parses_with_vultisig(self):
        """
        Action 3: DASH~DASH → ZEC~ZEC at the oldest block in the batch.
        Demonstrates the affiliate-output height tracks the base swap height.
        """
        raw = _with_vultisig(swap=_clone(3))
        result = self.ing.parse_swap(raw)
        self.assertIsNotNone(result)
        self.assertEqual(result["block_height"], 16067038)

        # Affiliate output height must follow the base swap (16067038),
        # not the hardcoded canonical one (16067044).
        self.assertEqual(result["out_heights"][2], 16067038)

        expected_in_usd = (80198983 / 1e8) * 47.62945847162068
        self.assertAlmostEqual(result["in_amount_usd"], expected_in_usd, places=6)
        self.assertEqual(result["volume_tier"], "<=$100")

    def test_all_four_real_actions_share_user_address(self):
        """Sanity check: all real actions were made by the same user."""
        for i, action in enumerate(REAL_RESPONSE["actions"]):
            with self.subTest(action=i):
                self.assertEqual(action["in"][0]["address"], USER_ADDR)

    # ---- Affiliate / routing branches --------------------------------------

    def test_dual_affiliate_uses_vultisig_bps(self):
        raw = _with_vultisig(
            affiliate_address="VALT/vi",
            affiliate_fee="10/35",
            memo="=:ZEC~ZEC:dest:0/1/1:VALT/vi:10/35",
        )
        result = self.ing.parse_swap(raw)
        self.assertIsNotNone(result)
        self.assertEqual(result["affiliate_addresses"], ["VALT", "vi"])
        self.assertEqual(result["affiliate_fees_bps"], [10, 35])
        self.assertEqual(result["platform"], "iOS")
        # Vultisig BPS is 35 (second affiliate)
        expected_aff = (35 / 10000) * REAL_IN_USD
        self.assertAlmostEqual(result["affiliate_fee_usd"], expected_aff, places=6)

    def test_skips_when_non_vultisig_affiliate(self):
        raw = _with_vultisig(
            affiliate_address="someone",
            affiliate_fee="10",
            memo="=:ZEC~ZEC:dest:0/1/1:someone:10",
        )
        self.assertIsNone(self.ing.parse_swap(raw))

    def test_skips_when_no_affiliate_fee_output(self):
        raw = _with_vultisig(include_fee_output=False)
        self.assertIsNone(self.ing.parse_swap(raw))

    # ---- MayaChain-specific decimal handling --------------------------------

    def test_cacao_input_uses_1e10_decimals(self):
        """CACAO uses 1e10 base units; everything else uses 1e8."""
        raw = _with_vultisig()
        raw["in"][0]["coins"][0]["asset"] = "MAYA.CACAO"
        raw["in"][0]["coins"][0]["amount"] = "10000000000"  # 1 CACAO
        raw["metadata"]["swap"]["inPriceUSD"] = "0.5"
        result = self.ing.parse_swap(raw)
        # in_amount_usd = (1e10 / 1e10) * 0.5 = 0.5
        self.assertAlmostEqual(result["in_amount_usd"], 0.5, places=8)

    def test_maya_substring_also_uses_1e10(self):
        raw = _with_vultisig()
        raw["in"][0]["coins"][0]["asset"] = "MAYA.MAYA"  # contains 'MAYA'
        raw["in"][0]["coins"][0]["amount"] = "20000000000"  # 2 MAYA
        raw["metadata"]["swap"]["inPriceUSD"] = "1.25"
        result = self.ing.parse_swap(raw)
        self.assertAlmostEqual(result["in_amount_usd"], 2.5, places=8)

    def test_cacao_output_uses_1e10_decimals(self):
        raw = _with_vultisig()
        # Replace the swap output asset/amount with CACAO; keep affiliate output intact.
        raw["out"][0]["coins"][0]["asset"] = "MAYA.CACAO"
        raw["out"][0]["coins"][0]["amount"] = "10000000000"  # 1 CACAO
        raw["out"][1]["coins"][0]["asset"] = "MAYA.CACAO"
        raw["out"][1]["coins"][0]["amount"] = "10000000000"
        raw["metadata"]["swap"]["outPriceUSD"] = "0.5"
        # High slip so the sanity-check correction does NOT collapse out_amount_usd.
        raw["metadata"]["swap"]["swapSlip"] = "9999"
        result = self.ing.parse_swap(raw)
        # out_amount_usd = (1e10 / 1e10) * 0.5 = 0.5
        self.assertAlmostEqual(result["out_amount_usd"], 0.5, places=8)

    # ---- Price sanity check -------------------------------------------------

    def test_price_sanity_check_trusts_lower_value(self):
        """
        out_amount_usd >> in_amount_usd with low slip → both collapse to lower.
        """
        raw = _with_vultisig()
        # Bump output amount 100x → out_amount_usd ≈ $11000 vs in ≈ $110.
        raw["out"][0]["coins"][0]["amount"] = "2898093900"
        result = self.ing.parse_swap(raw)
        self.assertIsNotNone(result)
        self.assertAlmostEqual(result["in_amount_usd"], REAL_IN_USD, places=6)
        self.assertAlmostEqual(result["out_amount_usd"], REAL_IN_USD, places=6)

    def test_price_sanity_check_skipped_for_high_slip(self):
        raw = _with_vultisig()
        raw["out"][0]["coins"][0]["amount"] = "2898093900"
        raw["metadata"]["swap"]["swapSlip"] = "800"  # 8% > 5% threshold
        result = self.ing.parse_swap(raw)
        self.assertIsNotNone(result)
        expected_out = (2898093900 / 1e8) * 380.2924279365528
        self.assertAlmostEqual(result["in_amount_usd"], REAL_IN_USD, places=6)
        self.assertAlmostEqual(result["out_amount_usd"], expected_out, places=4)

    # ---- Flags / platform / pools ------------------------------------------

    def test_streaming_swap_flag(self):
        raw = _with_vultisig()
        raw["metadata"]["swap"]["isStreamingSwap"] = True
        result = self.ing.parse_swap(raw)
        self.assertTrue(result["is_streaming_swap"])

    def test_platform_android(self):
        raw = _with_vultisig(code="va", bps=35, memo="=:ZEC~ZEC:dest:0/1/1:va:35")
        self.assertEqual(self.ing.parse_swap(raw)["platform"], "Android")

    def test_platform_web(self):
        raw = _with_vultisig(code="v0", bps=35, memo="=:ZEC~ZEC:dest:0/1/1:v0:35")
        self.assertEqual(self.ing.parse_swap(raw)["platform"], "Web")

    def test_single_pool(self):
        raw = _with_vultisig()
        raw["pools"] = ["DASH.DASH"]
        result = self.ing.parse_swap(raw)
        self.assertEqual(result["pool_1"], "DASH.DASH")
        self.assertIsNone(result["pool_2"])

    def test_no_pools(self):
        raw = _with_vultisig()
        raw["pools"] = []
        result = self.ing.parse_swap(raw)
        self.assertIsNone(result["pool_1"])
        self.assertIsNone(result["pool_2"])

    def test_empty_out_txid_preserved(self):
        """Real payload has an empty txID on the first output — round-trip it."""
        raw = _with_vultisig()
        result = self.ing.parse_swap(raw)
        self.assertEqual(result["out_tx_ids"][0], "")

    def test_out_height_missing_round_trips_as_none(self):
        """Real payload's second output has no `height` key at all."""
        raw = _with_vultisig()
        result = self.ing.parse_swap(raw)
        # Index 1 in the original payload had no height field
        self.assertIsNone(result["out_heights"][1])

    def test_malformed_swap_returns_none(self):
        # Missing 'in' key — parse_swap must swallow and return None.
        self.assertIsNone(self.ing.parse_swap({"foo": "bar"}))

    def test_block_height_missing_returns_none(self):
        raw = _with_vultisig()
        del raw["height"]
        result = self.ing.parse_swap(raw)
        self.assertIsNone(result["block_height"])

    def test_safe_float_handles_garbage_in_amount(self):
        """
        safe_float() inside parse_swap must coerce non-numeric input gracefully
        instead of bombing out the whole row.
        """
        raw = _with_vultisig()
        raw["in"][0]["coins"][0]["amount"] = "not-a-number"
        result = self.ing.parse_swap(raw)
        self.assertIsNotNone(result)
        self.assertEqual(result["in_amount"], 0)
        self.assertEqual(result["in_amount_usd"], 0)
        # affiliate_fee_usd is 0 too because in_amount_usd is 0
        self.assertEqual(result["affiliate_fee_usd"], 0)

    def test_safe_float_caps_overflow(self):
        """
        Astronomically large amount must be capped to <= 1e30 by safe_float
        rather than raising OverflowError.
        """
        raw = _with_vultisig()
        raw["in"][0]["coins"][0]["amount"] = "1" + "0" * 50  # 1e50
        result = self.ing.parse_swap(raw)
        self.assertIsNotNone(result)
        self.assertLessEqual(result["in_amount"], 1e30)

    def test_affiliate_fallback_when_affiliate_fee_empty(self):
        raw = _with_vultisig(affiliate_address="vi", affiliate_fee="")
        result = self.ing.parse_swap(raw)
        self.assertIsNotNone(result)
        self.assertEqual(result["affiliate_addresses"], ["vi"])
        # affiliate_fee_str == "" → [] → falls back to [vultisig_bps]
        self.assertEqual(result["affiliate_fees_bps"], [35])


if __name__ == "__main__":
    unittest.main()
