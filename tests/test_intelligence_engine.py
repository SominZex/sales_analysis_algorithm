"""
tests/test_intelligence_engine.py
─────────────────────────────────────────────────────────────────────────────
Unit tests for all pure-function logic in llm_recommender.py.
No DB, no LLM, no API calls — fully isolated.

Run:  pytest tests/ -v
─────────────────────────────────────────────────────────────────────────────
"""

import math
import pytest
import numpy as np
import pandas as pd
import sys
import os

# Allow import from project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from automation.llm_recommender import (
    _clean_numeric,
    _compute_trend,
    _enrich_with_trends,
    _compute_predictions,
    _compute_intelligence,
    _compute_stock_intelligence,
)


# ══════════════════════════════════════════════════════════════════════════════
# Fixtures
# ══════════════════════════════════════════════════════════════════════════════

def make_brand_df(rows: list[dict]) -> pd.DataFrame:
    """Build a minimal brand DataFrame matching what weekly_llm.py produces."""
    defaults = {
        "brandName":      "TestBrand",
        "total_sales":    1000.0,
        "quantity_sold":  10,
        "contrib_percent": 10.0,
        "profit_margin":  20.0,
    }
    data = [{**defaults, **r} for r in rows]
    return pd.DataFrame(data)


def make_stock_df(rows: list[dict]) -> pd.DataFrame:
    """Build a minimal stock CSV DataFrame."""
    defaults = {
        "productName":  "Test Product",
        "brand":        "TestBrand",
        "categoryName": "TestCategory",
        "quantity":     10.0,
        "sellingPrice": 100.0,
        "costPrice":    60.0,
        "vendorName":   "Test Vendor",
    }
    data = [{**defaults, **r} for r in rows]
    return pd.DataFrame(data)


# ══════════════════════════════════════════════════════════════════════════════
# _clean_numeric
# ══════════════════════════════════════════════════════════════════════════════

class TestCleanNumeric:

    def test_plain_float_string(self):
        s = pd.Series(["100.5", "200.0", "50.25"])
        result = _clean_numeric(s)
        assert list(result) == [100.5, 200.0, 50.25]

    def test_strips_percent_sign(self):
        s = pd.Series(["23.5%", "10%", "0%"])
        result = _clean_numeric(s)
        assert list(result) == [23.5, 10.0, 0.0]

    def test_negative_values(self):
        s = pd.Series(["-5.5", "-10%"])
        result = _clean_numeric(s)
        assert result.iloc[0] == -5.5
        assert result.iloc[1] == -10.0

    def test_non_numeric_becomes_nan(self):
        s = pd.Series(["N/A", "unknown", ""])
        result = _clean_numeric(s)
        assert result.isna().all()

    def test_mixed_series(self):
        s = pd.Series(["100", "N/A", "50%"])
        result = _clean_numeric(s)
        assert result.iloc[0] == 100.0
        assert math.isnan(result.iloc[1])
        assert result.iloc[2] == 50.0

    def test_integer_string(self):
        s = pd.Series(["42"])
        result = _clean_numeric(s)
        assert result.iloc[0] == 42.0

    def test_zero(self):
        s = pd.Series(["0", "0.0", "0%"])
        result = _clean_numeric(s)
        assert list(result) == [0.0, 0.0, 0.0]


# ══════════════════════════════════════════════════════════════════════════════
# _compute_trend
# ══════════════════════════════════════════════════════════════════════════════

class TestComputeTrend:

    def test_positive_growth(self):
        result = _compute_trend(120.0, 100.0)
        assert result == "+20.0%"

    def test_negative_decline(self):
        result = _compute_trend(80.0, 100.0)
        assert result == "-20.0%"

    def test_no_change(self):
        result = _compute_trend(100.0, 100.0)
        assert result == "+0.0%"

    def test_zero_prev_returns_none(self):
        result = _compute_trend(100.0, 0.0)
        assert result is None

    def test_large_growth(self):
        result = _compute_trend(300.0, 100.0)
        assert result == "+200.0%"

    def test_total_decline(self):
        result = _compute_trend(0.0, 100.0)
        assert result == "-100.0%"

    def test_fractional_change(self):
        result = _compute_trend(101.5, 100.0)
        assert result == "+1.5%"

    def test_both_zero(self):
        result = _compute_trend(0.0, 0.0)
        assert result is None


# ══════════════════════════════════════════════════════════════════════════════
# _enrich_with_trends
# ══════════════════════════════════════════════════════════════════════════════

class TestEnrichWithTrends:

    def test_no_trend_data_returns_unchanged(self):
        records = [{"brandName": "A", "_sales": 100}]
        result = _enrich_with_trends(records, {}, "brandName")
        assert result == records

    def test_attaches_sales_change(self):
        records = [{"brandName": "A", "_sales": 120.0, "_qty": 10, "_margin": 20.0}]
        trend = {"A": {"prev_sales": 100.0, "prev_qty": 8, "prev_margin": 20.0}}
        result = _enrich_with_trends(records, trend, "brandName")
        assert result[0]["sales_change"] == "+20.0%"

    def test_attaches_qty_change(self):
        records = [{"brandName": "A", "_sales": 100.0, "_qty": 15.0, "_margin": 20.0}]
        trend = {"A": {"prev_sales": 100.0, "prev_qty": 10.0, "prev_margin": 20.0}}
        result = _enrich_with_trends(records, trend, "brandName")
        assert result[0]["qty_change"] == "+50.0%"

    def test_attaches_margin_shift(self):
        records = [{"brandName": "A", "_sales": 100.0, "_qty": 10, "_margin": 25.0}]
        trend = {"A": {"prev_sales": 100.0, "prev_qty": 10, "prev_margin": 20.0}}
        result = _enrich_with_trends(records, trend, "brandName")
        assert result[0]["margin_shift"] == "+5.0pp"

    def test_negative_margin_shift(self):
        records = [{"brandName": "A", "_sales": 100.0, "_qty": 10, "_margin": 15.0}]
        trend = {"A": {"prev_sales": 100.0, "prev_qty": 10, "prev_margin": 20.0}}
        result = _enrich_with_trends(records, trend, "brandName")
        assert result[0]["margin_shift"] == "-5.0pp"

    def test_missing_brand_not_enriched(self):
        records = [{"brandName": "B", "_sales": 100.0, "_qty": 10, "_margin": 20.0}]
        trend = {"A": {"prev_sales": 100.0, "prev_qty": 10, "prev_margin": 20.0}}
        result = _enrich_with_trends(records, trend, "brandName")
        assert "sales_change" not in result[0]

    def test_multiple_records_partial_match(self):
        records = [
            {"brandName": "A", "_sales": 120.0, "_qty": 10, "_margin": 20.0},
            {"brandName": "B", "_sales": 80.0,  "_qty": 5,  "_margin": 15.0},
        ]
        trend = {"A": {"prev_sales": 100.0, "prev_qty": 10, "prev_margin": 20.0}}
        result = _enrich_with_trends(records, trend, "brandName")
        assert "sales_change" in result[0]
        assert "sales_change" not in result[1]


# ══════════════════════════════════════════════════════════════════════════════
# _compute_predictions
# ══════════════════════════════════════════════════════════════════════════════

class TestComputePredictions:

    def _make_rows(self, items):
        return [
            {"brandName": name, "_sales": sales, "_qty": qty, "_margin": margin}
            for name, sales, qty, margin in items
        ]

    def test_empty_trend_data_returns_empty(self):
        rows = self._make_rows([("A", 1000, 50, 25)])
        result = _compute_predictions(rows, {}, "brandName")
        assert result == {"stockout_risk": [], "margin_erosion": [], "rising_stars": []}

    def test_stockout_risk_detected(self):
        # Top-revenue brand with qty growing >20%
        rows = self._make_rows([
            ("A", 5000, 120, 25),
            ("B", 100,  10,  20),
        ])
        trend = {
            "A": {"prev_sales": 4000, "prev_qty": 90, "prev_margin": 25},
        }
        result = _compute_predictions(rows, trend, "brandName")
        names = [r["name"] for r in result["stockout_risk"]]
        assert "A" in names

    def test_stockout_risk_not_triggered_below_threshold(self):
        # Qty growth only 10% — below the 20% threshold
        rows = self._make_rows([("A", 5000, 110, 25)])
        trend = {"A": {"prev_sales": 4000, "prev_qty": 100, "prev_margin": 25}}
        result = _compute_predictions(rows, trend, "brandName")
        assert result["stockout_risk"] == []

    def test_margin_erosion_detected(self):
        rows = self._make_rows([("A", 1000, 50, 10.0)])
        trend = {"A": {"prev_sales": 1000, "prev_qty": 50, "prev_margin": 15.0}}
        result = _compute_predictions(rows, trend, "brandName")
        names = [r["name"] for r in result["margin_erosion"]]
        assert "A" in names

    def test_margin_erosion_not_triggered_small_shift(self):
        # Margin drop only 2pp — below the 3pp threshold
        rows = self._make_rows([("A", 1000, 50, 18.0)])
        trend = {"A": {"prev_sales": 1000, "prev_qty": 50, "prev_margin": 20.0}}
        result = _compute_predictions(rows, trend, "brandName")
        assert result["margin_erosion"] == []

    def test_rising_star_detected(self):
        # Non-top-5 brand with sales growth >25%
        rows = self._make_rows([
            ("TopBrand1", 9000, 100, 20),
            ("TopBrand2", 8000, 90,  20),
            ("TopBrand3", 7000, 80,  20),
            ("TopBrand4", 6000, 70,  20),
            ("TopBrand5", 5000, 60,  20),
            ("RisingStar", 500, 30,  30),
        ])
        trend = {
            "RisingStar": {"prev_sales": 300, "prev_qty": 20, "prev_margin": 28},
        }
        result = _compute_predictions(rows, trend, "brandName")
        names = [r["name"] for r in result["rising_stars"]]
        assert "RisingStar" in names

    def test_rising_star_not_triggered_for_top5(self):
        # Top brand should not appear as rising star even with high growth
        rows = self._make_rows([("A", 9000, 100, 20)])
        trend = {"A": {"prev_sales": 100, "prev_qty": 10, "prev_margin": 20}}
        result = _compute_predictions(rows, trend, "brandName")
        assert result["rising_stars"] == []

    def test_results_capped_at_3(self):
        rows = self._make_rows([
            (f"Brand{i}", 1000 - i * 10, 50, 5.0) for i in range(10)
        ])
        trend = {
            f"Brand{i}": {"prev_sales": 1000, "prev_qty": 50, "prev_margin": 15.0}
            for i in range(10)
        }
        result = _compute_predictions(rows, trend, "brandName")
        assert len(result["margin_erosion"]) <= 3


# ══════════════════════════════════════════════════════════════════════════════
# _compute_intelligence
# ══════════════════════════════════════════════════════════════════════════════

class TestComputeIntelligence:

    def test_empty_df_returns_empty_dict(self):
        result = _compute_intelligence(pd.DataFrame(), "brandName", {})
        assert result == {}

    def test_returns_all_expected_keys(self):
        df = make_brand_df([
            {"brandName": "A", "total_sales": 1000, "quantity_sold": 10,
             "contrib_percent": 50, "profit_margin": 20},
            {"brandName": "B", "total_sales": 500,  "quantity_sold": 5,
             "contrib_percent": 25, "profit_margin": 15},
        ])
        result = _compute_intelligence(df, "brandName", {})
        expected_keys = [
            "top_10_by_revenue", "bottom_5_by_quantity", "avg_qty_sold",
            "low_margin_items", "hidden_margin_gems", "high_risk_items",
            "mix_risk_items", "concentration_flag", "top1_revenue_share_pct",
            "top3_revenue_share_pct", "avg_margin_pct", "anomalies",
            "trend_declining", "trend_margin_shifts", "has_trend_data",
            "predictions",
        ]
        for key in expected_keys:
            assert key in result, f"Missing key: {key}"

    def test_top_10_sorted_by_revenue_descending(self):
        df = make_brand_df([
            {"brandName": "A", "total_sales": 500},
            {"brandName": "B", "total_sales": 2000},
            {"brandName": "C", "total_sales": 1000},
        ])
        result = _compute_intelligence(df, "brandName", {})
        top = result["top_10_by_revenue"]
        assert top[0]["brandName"] == "B"
        assert top[1]["brandName"] == "C"
        assert top[2]["brandName"] == "A"

    def test_concentration_flag_above_50_percent(self):
        df = make_brand_df([
            {"brandName": "Dominant", "total_sales": 8000},
            {"brandName": "B",        "total_sales": 1000},
            {"brandName": "C",        "total_sales": 1000},
        ])
        result = _compute_intelligence(df, "brandName", {})
        assert result["concentration_flag"] == True

    def test_concentration_flag_not_set_when_spread(self):
        df = make_brand_df([
            {"brandName": f"Brand{i}", "total_sales": 1000} for i in range(10)
        ])
        result = _compute_intelligence(df, "brandName", {})
        assert result["concentration_flag"] == False

    def test_negative_margin_appears_in_anomalies(self):
        df = make_brand_df([
            {"brandName": "A", "total_sales": 1000, "profit_margin": -5.0},
            {"brandName": "B", "total_sales": 500,  "profit_margin": 20.0},
        ])
        result = _compute_intelligence(df, "brandName", {})
        anomaly_text = " ".join(result["anomalies"])
        assert "A" in anomaly_text
        assert "NEGATIVE" in anomaly_text

    def test_single_unit_sold_appears_in_anomalies(self):
        df = make_brand_df([
            {"brandName": "A", "total_sales": 1000, "quantity_sold": 1},
            {"brandName": "B", "total_sales": 500,  "quantity_sold": 50},
        ])
        result = _compute_intelligence(df, "brandName", {})
        anomaly_text = " ".join(result["anomalies"])
        assert "A" in anomaly_text
        assert "dead stock" in anomaly_text

    def test_no_trend_data_has_trend_data_is_false(self):
        df = make_brand_df([{"brandName": "A", "total_sales": 1000}])
        result = _compute_intelligence(df, "brandName", {})
        assert result["has_trend_data"] is False

    def test_with_trend_data_has_trend_data_is_true(self):
        df = make_brand_df([{"brandName": "A", "total_sales": 1000}])
        trend = {"A": {"prev_sales": 900, "prev_qty": 10, "prev_margin": 20}}
        result = _compute_intelligence(df, "brandName", trend)
        assert result["has_trend_data"] is True

    def test_avg_margin_computed_correctly(self):
        df = make_brand_df([
            {"brandName": "A", "profit_margin": 20.0},
            {"brandName": "B", "profit_margin": 40.0},
        ])
        result = _compute_intelligence(df, "brandName", {})
        assert result["avg_margin_pct"] == 30.0

    def test_bottom_5_excludes_zero_qty(self):
        df = make_brand_df([
            {"brandName": "A", "quantity_sold": 0},
            {"brandName": "B", "quantity_sold": 2},
            {"brandName": "C", "quantity_sold": 5},
        ])
        result = _compute_intelligence(df, "brandName", {})
        bottom_names = [r["brandName"] for r in result["bottom_5_by_quantity"]]
        assert "A" not in bottom_names

    def test_percent_string_margins_parsed_correctly(self):
        df = make_brand_df([
            {"brandName": "A", "profit_margin": "25.5%"},
            {"brandName": "B", "profit_margin": "10.0%"},
        ])
        result = _compute_intelligence(df, "brandName", {})
        assert result["avg_margin_pct"] == 17.75

    def test_declining_trend_detected(self):
        df = make_brand_df([
            {"brandName": "A", "total_sales": 700, "quantity_sold": 10, "profit_margin": 20},
        ])
        trend = {"A": {"prev_sales": 1000, "prev_qty": 12, "prev_margin": 20}}
        result = _compute_intelligence(df, "brandName", trend)
        declining_names = [r["name"] for r in result["trend_declining"]]
        assert "A" in declining_names

    def test_stable_trend_not_flagged_as_declining(self):
        df = make_brand_df([
            {"brandName": "A", "total_sales": 950, "quantity_sold": 10, "profit_margin": 20},
        ])
        trend = {"A": {"prev_sales": 1000, "prev_qty": 10, "prev_margin": 20}}
        result = _compute_intelligence(df, "brandName", trend)
        # Only >10% decline is flagged
        assert result["trend_declining"] == []

    def test_margin_shift_detected(self):
        df = make_brand_df([
            {"brandName": "A", "total_sales": 1000, "profit_margin": 10.0},
        ])
        trend = {"A": {"prev_sales": 1000, "prev_qty": 10, "prev_margin": 15.0}}
        result = _compute_intelligence(df, "brandName", trend)
        shifts = result["trend_margin_shifts"]
        assert len(shifts) == 1
        assert shifts[0]["name"] == "A"
        assert "deteriorated" in shifts[0]["margin_shift"]


# ══════════════════════════════════════════════════════════════════════════════
# _compute_stock_intelligence
# ══════════════════════════════════════════════════════════════════════════════

class TestComputeStockIntelligence:

    def test_empty_df_returns_empty(self):
        result = _compute_stock_intelligence(pd.DataFrame(), "brand", 5)
        assert result == {}

    def test_missing_group_col_returns_empty(self):
        df = make_stock_df([{"quantity": 0}])
        result = _compute_stock_intelligence(df, "nonexistent_col", 5)
        assert result == {}

    def test_oos_count_correct(self):
        df = make_stock_df([
            {"quantity": 0},
            {"quantity": 0},
            {"quantity": 10},
        ])
        result = _compute_stock_intelligence(df, "brand", 5)
        assert result["oos_count"] == 2

    def test_low_stock_count_correct(self):
        df = make_stock_df([
            {"quantity": 3},
            {"quantity": 5},
            {"quantity": 6},   # above threshold
            {"quantity": 0},   # OOS, not low
        ])
        result = _compute_stock_intelligence(df, "brand", 5)
        assert result["low_count"] == 2

    def test_negative_stock_count_correct(self):
        df = make_stock_df([
            {"quantity": -5},
            {"quantity": -1},
            {"quantity": 10},
        ])
        result = _compute_stock_intelligence(df, "brand", 5)
        assert result["neg_count"] == 2

    def test_neg_items_most_negative_first(self):
        df = make_stock_df([
            {"brand": "A", "quantity": -1,  "productName": "P1"},
            {"brand": "A", "quantity": -10, "productName": "P2"},
            {"brand": "B", "quantity": -2,  "productName": "P3"},
        ])
        result = _compute_stock_intelligence(df, "brand", 5)
        brand_a = next(r for r in result["neg_items"] if r["brand"] == "A")
        assert brand_a["min_qty"] == -10

    def test_oos_items_sorted_by_sku_count_descending(self):
        df = make_stock_df([
            {"brand": "A", "quantity": 0, "productName": "P1"},
            {"brand": "A", "quantity": 0, "productName": "P2"},
            {"brand": "B", "quantity": 0, "productName": "P3"},
        ])
        result = _compute_stock_intelligence(df, "brand", 5)
        assert result["oos_items"][0]["brand"] == "A"
        assert result["oos_items"][0]["sku_count"] == 2

    def test_high_value_oos_sorted_by_selling_price(self):
        df = make_stock_df([
            {"quantity": 0, "productName": "Cheap",     "sellingPrice": 50},
            {"quantity": 0, "productName": "Expensive", "sellingPrice": 500},
        ])
        result = _compute_stock_intelligence(df, "brand", 5)
        assert result["high_value_oos"][0]["productName"] == "Expensive"

    def test_all_in_stock_returns_zero_counts(self):
        df = make_stock_df([
            {"quantity": 20},
            {"quantity": 50},
        ])
        result = _compute_stock_intelligence(df, "brand", 5)
        assert result["oos_count"] == 0
        assert result["low_count"] == 0
        assert result["neg_count"] == 0

    def test_total_skus_count(self):
        df = make_stock_df([{} for _ in range(7)])
        result = _compute_stock_intelligence(df, "brand", 5)
        assert result["total_skus"] == 7

    def test_threshold_boundary_low_stock_inclusive(self):
        # quantity == threshold should be counted as low stock
        df = make_stock_df([{"quantity": 5}])
        result = _compute_stock_intelligence(df, "brand", 5)
        assert result["low_count"] == 1

    def test_threshold_boundary_above_threshold_not_low(self):
        # quantity == threshold + 1 should NOT be low stock
        df = make_stock_df([{"quantity": 6}])
        result = _compute_stock_intelligence(df, "brand", 5)
        assert result["low_count"] == 0

    def test_neg_products_sorted_most_negative_first(self):
        df = make_stock_df([
            {"productName": "P1", "quantity": -2},
            {"productName": "P2", "quantity": -8},
            {"productName": "P3", "quantity": -1},
        ])
        result = _compute_stock_intelligence(df, "brand", 5)
        assert result["neg_products"][0]["productName"] == "P2"

    def test_category_dimension(self):
        df = make_stock_df([
            {"categoryName": "Beverages", "quantity": 0, "productName": "P1"},
            {"categoryName": "Beverages", "quantity": 0, "productName": "P2"},
            {"categoryName": "Snacks",    "quantity": 3, "productName": "P3"},
        ])
        result = _compute_stock_intelligence(df, "categoryName", 5)
        assert result["oos_count"] == 2
        assert result["low_count"] == 1
        cats = [r["categoryName"] for r in result["oos_items"]]
        assert "Beverages" in cats