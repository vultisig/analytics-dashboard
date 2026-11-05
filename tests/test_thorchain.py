# tests/test_thorchain.py
"""
Unit tests for ingestors/thorchain.py (THORChainIngestor).

All fixtures are built on a real Midgard /v2/actions response envelope
(5 actions from a single batch, none of which has a Vultisig affiliate).
Variant helpers mutate copies of one canonical action to exercise each
code path.

Run from the repo root with:
    python3 -m unittest discover -s tests -p 'test_*.py' -v
"""
import copy
import json
import os
import sys
import unittest
from datetime import datetime
from unittest.mock import patch

# Make `ingestors` importable whether the test runs from the repo root
# (CI) or from vultisig-analytics/.
_VA = os.path.join(os.path.dirname(__file__), "..", "vultisig-analytics")
if os.path.isdir(_VA) and _VA not in sys.path:
    sys.path.insert(0, _VA)

from ingestors.thorchain import THORChainIngestor  # noqa: E402


# ---------------------------------------------------------------------------
# Real-world fixture: full /v2/actions response envelope
# ---------------------------------------------------------------------------

# The user address every action in this batch shares.
USER_ADDR = "thor1n5a08r0zvmqca39ka2tgwlkjy9ugalutk7fjpzptfppqcccnat2ska5t4g"
# Parent tx hash shared across three of the actions (with -1 / -2 suffixes).
PARENT_TX = "B312278E8F2F66CBBEDD8EA76E453D48C2755B102D8B7F24A1CBEFA688102912"

REAL_RESPONSE = {
    "actions": [
        # --- Action 0: ETH-USDC → THOR.TCY (45000 USDC in) --------------------
        {
            "date": "1775898113679688813",
            "height": "25719544",
            "in": [
                {
                    "address": USER_ADDR,
                    "coins": [
                        {
                            "amount": "45000",
                            "asset": "ETH-USDC-0XA0B86991C6218B36C1D19D4A2E9EB0CE3606EB48",
                        }
                    ],
                    "txID": f"{PARENT_TX}-2",
                }
            ],
            "metadata": {
                "swap": {
                    "affiliateAddress": "",
                    "affiliateFee": "0",
                    "inPriceUSD": "0.9994973021724606",
                    "isStreamingSwap": False,
                    "liquidityFee": "224",
                    "memo": f"=:THOR.TCY:{USER_ADDR}/{USER_ADDR}:0/1/1",
                    "networkFees": [],
                    "outPriceUSD": "0.11514031258712394",
                    "swapSlip": "20",
                    "swapTarget": "0",
                    "txType": "swap",
                }
            },
            "out": [
                {
                    "address": USER_ADDR,
                    "coins": [{"amount": "389578", "asset": "THOR.TCY"}],
                    "height": "25719544",
                    "txID": "",
                }
            ],
            "pools": [
                "ETH.USDC-0XA0B86991C6218B36C1D19D4A2E9EB0CE3606EB48",
                "THOR.TCY",
            ],
            "status": "success",
            "type": "swap",
        },
        # --- Action 1: THOR.RUJI → THOR.RUNE (single-pool, short '=:r:' memo) -
        {
            "date": "1775898113679688813",
            "height": "25719544",
            "in": [
                {
                    "address": USER_ADDR,
                    "coins": [{"amount": "20000", "asset": "THOR.RUJI"}],
                    "txID": f"{PARENT_TX}-1",
                }
            ],
            "metadata": {
                "swap": {
                    "affiliateAddress": "",
                    "affiliateFee": "0",
                    "inPriceUSD": "0.17359988248397734",
                    "isStreamingSwap": False,
                    "liquidityFee": "8",
                    "memo": f"=:r:{USER_ADDR}/{USER_ADDR}:0/1/1",
                    "networkFees": [],
                    "outPriceUSD": "0.40054394216376576",
                    "swapSlip": "10",
                    "swapTarget": "0",
                    "txType": "swap",
                }
            },
            "out": [
                {
                    "address": USER_ADDR,
                    "coins": [{"amount": "8660", "asset": "THOR.RUNE"}],
                    "height": "25719544",
                    "txID": "",
                }
            ],
            "pools": ["THOR.RUJI"],
            "status": "success",
            "type": "swap",
        },
        # --- Action 2: THOR.RUJI → ETH-USDC (multi-output duplicates) --------
        {
            "date": "1775898113679688813",
            "height": "25719544",
            "in": [
                {
                    "address": USER_ADDR,
                    "coins": [{"amount": "10000", "asset": "THOR.RUJI"}],
                    "txID": PARENT_TX,
                }
            ],
            "metadata": {
                "swap": {
                    "affiliateAddress": "",
                    "affiliateFee": "0",
                    "inPriceUSD": "0.17359988248397734",
                    "isStreamingSwap": False,
                    "liquidityFee": "6",
                    "memo": f"=:ETH-USDC:{USER_ADDR}/{USER_ADDR}:0/1/1",
                    "networkFees": [],
                    "outPriceUSD": "0.9994973021724606",
                    "swapSlip": "20",
                    "swapTarget": "0",
                    "txType": "swap",
                }
            },
            "out": [
                {
                    "address": USER_ADDR,
                    "coins": [
                        {
                            "amount": "1700",
                            "asset": "ETH-USDC-0XA0B86991C6218B36C1D19D4A2E9EB0CE3606EB48",
                        }
                    ],
                    "height": "25719544",
                    "txID": "",
                },
                {
                    "address": USER_ADDR,
                    "coins": [
                        {
                            "amount": "1700",
                            "asset": "ETH-USDC-0XA0B86991C6218B36C1D19D4A2E9EB0CE3606EB48",
                        }
                    ],
                    "txID": PARENT_TX,
                },
            ],
            "pools": [
                "THOR.RUJI",
                "ETH.USDC-0XA0B86991C6218B36C1D19D4A2E9EB0CE3606EB48",
            ],
            "status": "success",
            "type": "swap",
        },
        # --- Action 3: ETH-USDC → THOR.TCY (the canonical action we mutate) --
        {
            "date": "1775898107708619700",
            "height": "25719543",
            "in": [
                {
                    "address": USER_ADDR,
                    "coins": [
                        {
                            "amount": "1000",
                            "asset": "ETH-USDC-0XA0B86991C6218B36C1D19D4A2E9EB0CE3606EB48",
                        }
                    ],
                    "txID": "8958DACF016493402AA1B15781A41D31285539774FF56D6A354775A01C2CB346",
                }
            ],
            "metadata": {
                "swap": {
                    "affiliateAddress": "",
                    "affiliateFee": "0",
                    "inPriceUSD": "0.9994973024793127",
                    "isStreamingSwap": False,
                    "liquidityFee": "4",
                    "memo": f"=:THOR.TCY:{USER_ADDR}/{USER_ADDR}:0/1/1",
                    "networkFees": [],
                    "outPriceUSD": "0.11514031210088482",
                    "swapSlip": "20",
                    "swapTarget": "0",
                    "txType": "swap",
                }
            },
            "out": [
                {
                    "address": USER_ADDR,
                    "coins": [{"amount": "8340", "asset": "THOR.TCY"}],
                    "height": "25719543",
                    "txID": "",
                }
            ],
            "pools": [
                "ETH.USDC-0XA0B86991C6218B36C1D19D4A2E9EB0CE3606EB48",
                "THOR.TCY",
            ],
            "status": "success",
            "type": "swap",
        },
        # --- Action 4: THOR.RUJI → THOR.RUNE (different parent tx) -----------
        {
            "date": "1775898095386726448",
            "height": "25719541",
            "in": [
                {
                    "address": USER_ADDR,
                    "coins": [{"amount": "20000", "asset": "THOR.RUJI"}],
                    "txID": "E37C569CA62DBF9B28BEC17AA05EC26ACE2BBEA488726B902AD5744BF27C7059",
                }
            ],
            "metadata": {
                "swap": {
                    "affiliateAddress": "",
                    "affiliateFee": "0",
                    "inPriceUSD": "0.17359988255012",
                    "isStreamingSwap": False,
                    "liquidityFee": "8",
                    "memo": f"=:r:{USER_ADDR}/{USER_ADDR}:0/1/1",
                    "networkFees": [],
                    "outPriceUSD": "0.40054394216376576",
                    "swapSlip": "10",
                    "swapTarget": "0",
                    "txType": "swap",
                }
            },
            "out": [
                {
                    "address": USER_ADDR,
                    "coins": [{"amount": "8660", "asset": "THOR.RUNE"}],
                    "height": "25719541",
                    "txID": "",
                }
            ],
            "pools": ["THOR.RUJI"],
            "status": "success",
            "type": "swap",
        },
    ],
    "count": "-1",
    "meta": {
        "nextPageToken": "257195089000001200",
        "prevPageToken": "257195449000001239",
    },
}

# The canonical action we mutate in most tests: ETH-USDC → THOR.TCY.
REAL_SWAP = REAL_RESPONSE["actions"][3]
REAL_TX_HASH = "8958DACF016493402AA1B15781A41D31285539774FF56D6A354775A01C2CB346"
REAL_HEIGHT = "25719543"

# Pre-computed USD values for the canonical action.
# in_amount_usd  = (1000 / 1e8) * 0.9994973024793127  ≈ 9.994973e-6
# out_amount_usd = (8340 / 1e8) * 0.11514031210088482 ≈ 9.604702e-6
REAL_IN_USD = (1000 / 1e8) * 0.9994973024793127
REAL_OUT_USD = (8340 / 1e8) * 0.11514031210088482


# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------

def _clone(action_index: int = 3):
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
    fee_asset="THOR.RUNE",
    fee_amount="10000000",  # 0.1 RUNE
    include_fee_output=True,
):
    """
    Take a base swap and bolt a Vultisig affiliate onto it.

    By default produces a single-affiliate ``vi`` swap at 35 bps with a
    0.1 RUNE affiliate fee output. Override any of the kwargs to construct
    dual-affiliate / missing-fee-output variants.
    """
    s = _clone() if swap is None else copy.deepcopy(swap)
    meta = s["metadata"]["swap"]

    meta["affiliateAddress"] = affiliate_address if affiliate_address is not None else code
    meta["affiliateFee"] = affiliate_fee if affiliate_fee is not None else str(bps)
    meta["memo"] = memo if memo is not None else f"=:THOR.TCY:dest:0/1/1:{code}:{bps}"

    if include_fee_output:
        s["out"].append(
            {
                "address": "thor1affiliate",
                "coins": [{"asset": fee_asset, "amount": fee_amount}],
                "txID": "AFFTX1",
                # Match the base swap's block height so multi-action tests
                # don't get a mismatched height on the tacked-on output.
                "height": s.get("height", REAL_HEIGHT),
                "affiliate": True,
            }
        )
    return s


# ---------------------------------------------------------------------------
# __init__
# ---------------------------------------------------------------------------

class TestInit(unittest.TestCase):
    def test_source_name_and_endpoint(self):
        ing = THORChainIngestor()
        self.assertEqual(ing.source_name, "thorchain")
        self.assertIsInstance(ing.api_endpoint, str)
        self.assertTrue(ing.api_endpoint)


# ---------------------------------------------------------------------------
# fetch_data
# ---------------------------------------------------------------------------

class TestFetchData(unittest.TestCase):
    def setUp(self):
        self.ing = THORChainIngestor()

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
            result = self.ing.fetch_data(limit=5)
        self.assertEqual(len(result["actions"]), 5)
        self.assertEqual(result["count"], "-1")
        self.assertEqual(result["meta"]["nextPageToken"], "257195089000001200")
        self.assertEqual(result["meta"]["prevPageToken"], "257195449000001239")

    def test_next_page_token_is_forwarded(self):
        with patch.object(self.ing, "make_request", return_value={"ok": True}) as m:
            self.ing.fetch_data(next_page_token=REAL_RESPONSE["meta"]["nextPageToken"])
        _, params = m.call_args[0]
        self.assertEqual(params["nextPageToken"], "257195089000001200")

    def test_retry_then_succeed(self):
        side_effects = [Exception("transient"), _clone_response()]
        with patch("ingestors.base.time.sleep"), \
             patch.object(self.ing, "make_request", side_effect=side_effects) as m:
            result = self.ing.fetch_data()
        self.assertEqual(len(result["actions"]), 5)
        self.assertEqual(m.call_count, 2)

    def test_retries_exhausted_raises(self):
        with patch("ingestors.base.time.sleep"), \
             patch.object(self.ing, "make_request", side_effect=Exception("boom")) as m:
            with self.assertRaises(Exception) as ctx:
                self.ing.fetch_data()
        self.assertIn("Max retries reached", str(ctx.exception))
        self.assertIn("boom", str(ctx.exception))
        self.assertEqual(m.call_count, self.ing.backoff_retry.max_retries)


# ---------------------------------------------------------------------------
# _extract_vultisig_affiliate
# ---------------------------------------------------------------------------

class TestExtractVultisigAffiliate(unittest.TestCase):
    def setUp(self):
        self.ing = THORChainIngestor()

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
        info = self.ing._extract_vultisig_affiliate("vi", "=:ETH.ETH:0xabc:0/1/0:vi:35")
        self.assertEqual(info, {"code": "vi", "bps": 35, "address": "vi"})

    def test_dual_affiliate_vultisig_second(self):
        info = self.ing._extract_vultisig_affiliate(
            "VALT/vi", "=:e:0xaddress:0/1/0:VALT/vi:10/35"
        )
        self.assertEqual(info["code"], "vi")
        self.assertEqual(info["bps"], 35)
        self.assertEqual(info["address"], "vi")

    def test_dual_affiliate_vultisig_first(self):
        info = self.ing._extract_vultisig_affiliate(
            "va/OTHER", "=:e:0xaddress:0/1/0:va/OTHER:42/10"
        )
        self.assertEqual(info["code"], "va")
        self.assertEqual(info["bps"], 42)

    def test_case_insensitive_match(self):
        info = self.ing._extract_vultisig_affiliate("VI", "=:a:b:c:d:VI:50")
        self.assertEqual(info["code"], "vi")
        self.assertEqual(info["bps"], 50)

    def test_memo_too_short_for_bps(self):
        info = self.ing._extract_vultisig_affiliate("vi", "=:ETH:0xabc")
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
# find_destination_outputs
# ---------------------------------------------------------------------------

class TestFindDestinationOutputs(unittest.TestCase):
    MEMO = "=:r:thor1dest:0/1/0:vi:50"

    def _out(self, asset, amount="1", address="thor1dest", affiliate=False):
        out = {"address": address, "coins": [{"asset": asset, "amount": amount}]}
        if affiliate:
            out["affiliate"] = True
        return out

    def test_skips_fee_leg_and_refund(self):
        target = self._out("ETH.ETH")
        outputs = [
            self._out("THOR.RUNE", address="thor1collector", affiliate=True),
            self._out("BTC.BTC"),
            target,
        ]
        self.assertEqual(THORChainIngestor.find_destination_outputs(outputs, "BTC.BTC", self.MEMO), [target])

    def test_flagged_output_to_memo_destination_counts(self):
        output = self._out("THOR.RUNE", affiliate=True)
        self.assertEqual(THORChainIngestor.find_destination_outputs([output], "THOR.TCY", self.MEMO), [output])

    def test_returns_every_leg_of_the_target_asset(self):
        legs = [self._out("ETH.USDT", "1"), self._out("ETH.USDT", "2")]
        self.assertEqual(THORChainIngestor.find_destination_outputs(legs, "BTC.BTC", self.MEMO), legs)

    def test_duplicate_listing_of_one_leg_is_returned_once(self):
        first = self._out("ETH.USDT", "5")
        self.assertEqual(
            THORChainIngestor.find_destination_outputs([first, self._out("ETH.USDT", "5")], "BTC.BTC", self.MEMO),
            [first],
        )

    def test_refund_only_returns_empty(self):
        self.assertEqual(THORChainIngestor.find_destination_outputs([self._out("BTC.BTC")], "BTC.BTC", self.MEMO), [])


# ---------------------------------------------------------------------------
# parse_swap
# ---------------------------------------------------------------------------

class TestParseSwap(unittest.TestCase):
    def setUp(self):
        self.ing = THORChainIngestor()

    # ---- All five real actions ---------------------------------------------

    def test_every_real_action_returns_none_because_no_affiliate(self):
        """
        Every action in the captured batch has affiliateAddress="" and no
        affiliate:true output — parse_swap must cleanly skip all of them.
        """
        for i, action in enumerate(REAL_RESPONSE["actions"]):
            with self.subTest(action=i):
                self.assertIsNone(self.ing.parse_swap(copy.deepcopy(action)))

    # ---- Happy path: mutate the canonical action ---------------------------

    def test_real_swap_with_vultisig_added_parses(self):
        """
        Start from the canonical real action, add a Vultisig affiliate +
        fee output, and verify every denormalised field matches the real
        upstream data.
        """
        raw = _with_vultisig(code="vi", bps=35, fee_amount="10000000")  # 0.1 RUNE
        result = self.ing.parse_swap(raw)

        self.assertIsNotNone(result)
        self.assertEqual(result["tx_hash"], REAL_TX_HASH)
        self.assertEqual(result["block_height"], REAL_HEIGHT)
        self.assertEqual(result["user_address"], USER_ADDR)
        self.assertEqual(result["in_address"], USER_ADDR)
        self.assertEqual(
            result["in_asset"],
            "ETH-USDC-0XA0B86991C6218B36C1D19D4A2E9EB0CE3606EB48",
        )
        self.assertEqual(result["in_amount"], 1000.0)
        self.assertEqual(result["in_amount_raw"], "1000")
        self.assertAlmostEqual(result["in_price_usd"], 0.9994973024793127)
        self.assertAlmostEqual(result["out_price_usd"], 0.11514031210088482)

        # USD amounts match the real upstream values.
        self.assertAlmostEqual(result["in_amount_usd"], REAL_IN_USD, places=12)
        self.assertAlmostEqual(result["out_amount_usd"], REAL_OUT_USD, places=12)

        # total_fee = in - out
        self.assertAlmostEqual(
            result["total_fee_usd"], REAL_IN_USD - REAL_OUT_USD, places=12
        )

        self.assertEqual(result["out_asset"], "THOR.TCY")
        self.assertEqual(result["out_amount"], 8340.0)
        self.assertAlmostEqual(result["affiliate_fee_usd"], (35 / 10000) * REAL_IN_USD, places=12)

        # Pools, metadata, and misc
        self.assertEqual(
            result["pool_1"], "ETH.USDC-0XA0B86991C6218B36C1D19D4A2E9EB0CE3606EB48"
        )
        self.assertEqual(result["pool_2"], "THOR.TCY")
        self.assertEqual(result["pools_used"], REAL_SWAP["pools"])
        self.assertFalse(result["is_streaming_swap"])
        self.assertEqual(result["swap_slip"], 20.0)
        self.assertEqual(result["platform"], "iOS")
        self.assertEqual(result["swap_status"], "success")
        self.assertEqual(result["swap_type"], "swap")
        self.assertEqual(result["source"], "thorchain")
        self.assertEqual(result["affiliate_addresses"], ["vi"])
        self.assertEqual(result["affiliate_fees_bps"], [35])

        # metadata_complete round-trips and preserves the extra Midgard fields.
        meta = json.loads(result["metadata_complete"])
        self.assertEqual(meta["liquidityFee"], "4")
        self.assertEqual(meta["swapTarget"], "0")
        self.assertEqual(meta["txType"], "swap")

        # out_addresses / tx_ids / heights: real swap output + bolted-on affiliate output
        out_addresses = json.loads(result["out_addresses"])
        self.assertEqual(len(out_addresses), 2)
        self.assertEqual(out_addresses[0]["address"], USER_ADDR)
        self.assertTrue(out_addresses[1]["affiliate"])
        self.assertEqual(result["out_tx_ids"], ["", "AFFTX1"])
        self.assertEqual(result["out_heights"], [25719543, 25719543])

        # network_fees_raw is an empty array in the real payload
        self.assertEqual(json.loads(result["network_fees_raw"]), [])

        # Volume tier: ~1e-5 USD → "<=$100"
        self.assertEqual(result["volume_tier"], "<=$100")

        # Timestamp comes from the real nanosecond date field
        self.assertIsInstance(result["timestamp"], datetime)
        self.assertEqual(result["date_only"], result["timestamp"].date())

        # raw_data round-trips
        self.assertEqual(json.loads(result["raw_data"])["height"], REAL_HEIGHT)

    # ---- Parsing other real actions with a Vultisig affiliate bolted on -----

    def test_rune_output_swap_parses_with_vultisig(self):
        """
        Action 1: THOR.RUJI → THOR.RUNE (single-pool, '=:r:' short memo).
        Verify the single-pool shape and the RUNE output asset are preserved.
        """
        raw = _with_vultisig(swap=_clone(1))
        result = self.ing.parse_swap(raw)
        self.assertIsNotNone(result)
        self.assertEqual(result["in_asset"], "THOR.RUJI")
        self.assertEqual(result["in_amount"], 20000.0)
        # pools has a single entry
        self.assertEqual(result["pool_1"], "THOR.RUJI")
        self.assertIsNone(result["pool_2"])
        self.assertEqual(result["out_asset"], "THOR.RUNE")

    def test_multi_output_action_parses_with_vultisig(self):
        """
        Action 2 has two duplicate non-affiliate outputs (one with height,
        one without). When we add an affiliate output the total should be 3,
        and out_heights must preserve the None on the height-less entry.
        """
        raw = _with_vultisig(swap=_clone(2))
        result = self.ing.parse_swap(raw)
        self.assertIsNotNone(result)
        self.assertEqual(len(json.loads(result["out_addresses"])), 3)
        # Index 0: real first output (has height 25719544 + empty txID)
        # Index 1: real second output (no height → None)
        # Index 2: bolted-on affiliate output
        self.assertEqual(result["out_heights"], [25719544, None, 25719544])
        self.assertEqual(result["out_tx_ids"][0], "")
        self.assertEqual(result["out_tx_ids"][1], PARENT_TX)
        self.assertEqual(result["out_tx_ids"][2], "AFFTX1")
        # Pools are in expected order
        self.assertEqual(
            result["pools_used"],
            ["THOR.RUJI", "ETH.USDC-0XA0B86991C6218B36C1D19D4A2E9EB0CE3606EB48"],
        )

    def test_larger_volume_action_classifies_correctly(self):
        """
        Action 0 has 45000 base-unit USDC in (still a tiny USD amount but
        different from the canonical). Volume tier must still be '<=$100'.
        """
        raw = _with_vultisig(swap=_clone(0))
        result = self.ing.parse_swap(raw)
        self.assertIsNotNone(result)
        self.assertEqual(result["in_amount"], 45000.0)
        expected_in_usd = (45000 / 1e8) * 0.9994973021724606
        self.assertAlmostEqual(
            result["in_amount_usd"], expected_in_usd, places=10
        )
        self.assertEqual(result["volume_tier"], "<=$100")
        # tx_hash is the action-0 parent tx with -2 suffix
        self.assertEqual(result["tx_hash"], f"{PARENT_TX}-2")

    # ---- Affiliate / routing branches --------------------------------------

    def test_dual_affiliate_uses_vultisig_bps(self):
        raw = _with_vultisig(
            affiliate_address="VALT/vi",
            affiliate_fee="10/35",
            memo="=:THOR.TCY:dest:0/1/1:VALT/vi:10/35",
        )
        result = self.ing.parse_swap(raw)
        self.assertIsNotNone(result)
        self.assertEqual(result["affiliate_addresses"], ["VALT", "vi"])
        self.assertEqual(result["affiliate_fees_bps"], [10, 35])
        self.assertEqual(result["platform"], "iOS")

    def test_skips_when_non_vultisig_affiliate(self):
        raw = _with_vultisig(
            affiliate_address="someone",
            affiliate_fee="10",
            memo="=:THOR.TCY:dest:0/1/1:someone:10",
        )
        self.assertIsNone(self.ing.parse_swap(raw))

    def test_native_rune_input_without_fee_output_uses_bps(self):
        """A THOR.RUNE input pays the affiliate fee inside the protocol, so
        Midgard has no affiliate out-leg. The fee is still earned."""
        raw = _with_vultisig(include_fee_output=False, bps=50)
        raw["in"][0]["coins"][0]["asset"] = "THOR.RUNE"
        raw["in"][0]["coins"][0]["amount"] = "100000000000"  # 1,000 RUNE
        raw["metadata"]["swap"]["inPriceUSD"] = "2.0"
        result = self.ing.parse_swap(raw)
        self.assertIsNotNone(result)
        self.assertAlmostEqual(result["in_amount_usd"], 2000.0, places=8)
        self.assertAlmostEqual(result["affiliate_fee_usd"], 10.0, places=8)  # 50 bps

    def test_referral_swap_uses_vultisig_bps_not_first_affiliate_output(self):
        """With a referral code ahead of ours (ref/vi, 10/35 bps), Midgard has
        two affiliate out-legs and does not say which one is ours. The first
        one is the referrer's 10 bps, so take our 35 bps of the input."""
        raw = _with_vultisig(
            affiliate_address="ref/vi",
            affiliate_fee="0",
            memo="=:THOR.TCY:dest:0/1/1:ref/vi:10/35",
            fee_amount="10000000",  # referrer's leg, listed first
        )
        raw["out"].append(
            {
                "address": "thor1vultisig",
                "coins": [{"asset": "THOR.RUNE", "amount": "35000000"}],
                "txID": "AFFTX2",
                "height": raw.get("height", REAL_HEIGHT),
                "affiliate": True,
            }
        )
        raw["metadata"]["swap"]["swapSlip"] = "9999"
        result = self.ing.parse_swap(raw)
        self.assertIsNotNone(result)
        self.assertGreater(result["affiliate_fee_usd"], 0)
        self.assertAlmostEqual(
            result["affiliate_fee_usd"], (35 / 10000) * result["in_amount_usd"], places=12
        )

    def test_pending_swap_is_skipped(self):
        """A streaming swap in progress has no final outputs. Skip
        it so the rolling Midgard re-check stores it once it completes."""
        raw = _with_vultisig()
        raw["status"] = "pending"
        self.assertIsNone(self.ing.parse_swap(raw))

    # ---- Regressions from real Midgard actions -----------------------------

    def _swap(self, memo, outs, in_asset="THOR.TCY", in_amount="100000000000", in_price="2.0", out_price="1.0"):
        address, fee = memo.split(":")[4:6]
        raw = _clone()
        raw["in"][0]["coins"][0] = {"asset": in_asset, "amount": in_amount}
        raw["out"] = outs
        raw["metadata"]["swap"].update(
            memo=memo, affiliateAddress=address, affiliateFee=fee, inPriceUSD=in_price, outPriceUSD=out_price
        )
        return raw

    def _out(self, asset, amount, address="thor1dest", affiliate=False):
        out = {"address": address, "coins": [{"asset": asset, "amount": amount}], "txID": "", "height": "1"}
        if affiliate:
            out["affiliate"] = True
        return out

    def test_user_output_flagged_as_affiliate_is_not_our_fee(self):
        """Part-refunded streaming swap 5AD6E01D: Midgard flags the user's RUNE
        output, and the memo pays us 0 bps."""
        raw = self._swap(
            "=:r:thor1dest:1754696093927/0/32:vi:0",
            [self._out("THOR.RUNE", "200000000000", affiliate=True), self._out("THOR.TCY", "5000")],
        )
        result = self.ing.parse_swap(raw)
        self.assertEqual(result["affiliate_fee_usd"], 0)
        self.assertEqual(result["in_amount_usd"], 2000.0)
        self.assertEqual(result["out_asset"], "THOR.RUNE")
        self.assertEqual(result["out_amount_usd"], 2000.0)

    def test_flagged_user_output_with_paid_bps_uses_bps(self):
        raw = self._swap(
            "=:r:thor1dest:0/1/0:vi:50",
            [self._out("THOR.RUNE", "200000000000", affiliate=True)],
        )
        self.assertAlmostEqual(self.ing.parse_swap(raw)["affiliate_fee_usd"], 10.0)

    def test_zero_bps_swap_without_fee_output_counts_as_volume(self):
        """Ultimate tier 8B4B77DA: pays no affiliate fee, so no flagged output."""
        raw = self._swap("=:ETH.USDC:0xdest:636/0/27:vi:0", [self._out("ETH.USDC", "200000000000", "0xdest")])
        result = self.ing.parse_swap(raw)
        self.assertEqual(result["in_amount_usd"], 2000.0)
        self.assertEqual(result["affiliate_fee_usd"], 0)
        self.assertEqual(result["affiliate_fees_bps"], [0])

    def test_partner_leg_is_not_our_fee_when_our_bps_is_zero(self):
        raw = self._swap(
            "=:ETH.USDC:0xdest:0/1/0:SIN/va:10/0",
            [self._out("THOR.RUNE", "1000000", "thor1partner", affiliate=True), self._out("ETH.USDC", "1", "0xdest")],
        )
        self.assertEqual(self.ing.parse_swap(raw)["affiliate_fee_usd"], 0)

    def test_outbound_split_across_outputs_keeps_input_value(self):
        """0EAE770D: $3.2M in, paid out in several legs. A single leg must not
        lower the input value."""
        raw = self._swap(
            "=:ETH.USDT:0xdest:0/1/0:vi:50",
            [
                self._out("THOR.RUNE", "100000000", "thor1collector", affiliate=True),
                self._out("ETH.USDT", "120000000000", "0xdest"),
                self._out("ETH.USDT", "80000000000", "0xdest"),
            ],
        )
        result = self.ing.parse_swap(raw)
        self.assertEqual(result["in_amount_usd"], 2000.0)
        self.assertEqual(result["out_amount"], 200000000000.0)
        self.assertEqual(result["out_amount_usd"], 2000.0)
        self.assertAlmostEqual(result["affiliate_fee_usd"], 10.0)

    def test_price_lookup_is_not_needed_for_the_fee(self):
        raw = self._swap("=:ETH.USDT:0xdest:0/1/0:vi:50", [self._out("ETH.USDT", "200000000000", "0xdest")])
        with patch.object(self.ing, "make_request", side_effect=AssertionError("no network")):
            self.assertAlmostEqual(self.ing.parse_swap(raw)["affiliate_fee_usd"], 10.0)

    def test_refund_only_swap_has_no_output_and_no_total_fee(self):
        raw = self._swap("=:ETH.USDT:0xdest:0/1/0:vi:50", [self._out("THOR.TCY", "100000000000")])
        result = self.ing.parse_swap(raw)
        self.assertEqual(result["out_asset"], "")
        self.assertEqual(result["total_fee_usd"], 0)

    # ---- Flags / platform / pools ------------------------------------------

    def test_streaming_swap_flag(self):
        raw = _with_vultisig()
        raw["metadata"]["swap"]["isStreamingSwap"] = True
        result = self.ing.parse_swap(raw)
        self.assertTrue(result["is_streaming_swap"])

    def test_platform_android(self):
        raw = _with_vultisig(code="va", bps=35, memo="=:THOR.TCY:dest:0/1/1:va:35")
        self.assertEqual(self.ing.parse_swap(raw)["platform"], "Android")

    def test_platform_web(self):
        raw = _with_vultisig(code="v0", bps=35, memo="=:THOR.TCY:dest:0/1/1:v0:35")
        self.assertEqual(self.ing.parse_swap(raw)["platform"], "Web")

    def test_single_pool(self):
        raw = _with_vultisig()
        raw["pools"] = ["ETH.USDC-0XA0B86991C6218B36C1D19D4A2E9EB0CE3606EB48"]
        result = self.ing.parse_swap(raw)
        self.assertEqual(
            result["pool_1"], "ETH.USDC-0XA0B86991C6218B36C1D19D4A2E9EB0CE3606EB48"
        )
        self.assertIsNone(result["pool_2"])

    def test_no_pools(self):
        raw = _with_vultisig()
        raw["pools"] = []
        result = self.ing.parse_swap(raw)
        self.assertIsNone(result["pool_1"])
        self.assertIsNone(result["pool_2"])

    def test_out_height_none_preserved(self):
        raw = _with_vultisig()
        raw["out"][0]["height"] = None
        result = self.ing.parse_swap(raw)
        self.assertIsNone(result["out_heights"][0])
        self.assertEqual(result["out_heights"][1], 25719543)

    def test_empty_out_txid_preserved(self):
        """Real payload has an empty txID on the main output — round-trip it."""
        raw = _with_vultisig()
        result = self.ing.parse_swap(raw)
        self.assertEqual(result["out_tx_ids"][0], "")

    def test_malformed_swap_returns_none(self):
        self.assertIsNone(self.ing.parse_swap({"foo": "bar"}))

    def test_affiliate_fallback_when_affiliate_fee_empty(self):
        raw = _with_vultisig(affiliate_address="vi", affiliate_fee="")
        result = self.ing.parse_swap(raw)
        self.assertIsNotNone(result)
        self.assertEqual(result["affiliate_addresses"], ["vi"])
        # affiliate_fee_str == "" → [] → falls back to [vultisig_bps]
        self.assertEqual(result["affiliate_fees_bps"], [35])

if __name__ == "__main__":
    unittest.main()
