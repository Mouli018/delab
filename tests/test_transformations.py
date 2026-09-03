"""
Unit Tests for Data Transformation Logic
=========================================
Week 6 – CI/CD and Version Control Exercise

These are PURE unit tests:
  - No file I/O (no CSV, no SQLite)
  - No network calls (no HTTP / API)
  - No subprocess spawning
  - All data is built in-memory using small synthetic DataFrames

Each test targets a specific transformation function imported directly
from the pipeline modules, covering:
  1. Week 4 transform()  — column derivation, null handling, whitespace cleaning
  2. Week 2 run_cdc()    — change detection logic (insert / update / delete)
  3. Week 2 incremental watermark — load_incremental() watermark filter

Run with:
    pytest tests/test_transformations.py -v
    pytest tests/test_transformations.py -v -m unit

All tests complete in < 5 seconds with no external dependencies.
"""

import json
import sys
from pathlib import Path

import pandas as pd
import pytest

BASE = Path(__file__).resolve().parent.parent
if str(BASE) not in sys.path:
    sys.path.insert(0, str(BASE))

# flake8: noqa: E402
from week4_pipeline.pipeline_week4 import transform as week4_transform
from week2_etl.pipeline_week2 import _row_hash, run_cdc, load_incremental


# ===========================================================================
# FIXTURES — shared synthetic DataFrames
# ===========================================================================

@pytest.fixture
def sample_raw_df():
    """Small clean DataFrame mimicking the raw extraction output from the API."""
    return pd.DataFrame({
        "id":                 [101, 102, 103, 104, 105],
        "productDisplayName": ["  Blue Shirt ", "Red Jeans", " Green Dress", "White Shoes  ", "Black Cap"],
        "year":               [2010, 2016, 2015, 2013, 2020],
        "masterCategory":     ["Apparel", "Apparel", "Apparel", "Footwear", "Accessories"],
        "articleType":        ["Shirts", "Jeans", "Dresses", "Shoes", "Caps"],
        "baseColour":         ["Blue", "Red", "Green", "White", "Black"],
        "usage":              ["Casual", "Casual", "Formal", "Sports", "Casual"],
    })


@pytest.fixture
def sample_raw_df_with_nulls(sample_raw_df):
    """DataFrame containing rows with null `id` values — should be dropped."""
    extra = pd.DataFrame({
        "id":                 [None, float("nan")],
        "productDisplayName": ["Ghost Row", "Another Ghost"],
        "year":               [2019, 2018],
        "masterCategory":     ["Apparel", "Apparel"],
        "articleType":        ["Unknown", "Unknown"],
        "baseColour":         ["N/A", "N/A"],
        "usage":              ["N/A", "N/A"],
    })
    return pd.concat([sample_raw_df, extra], ignore_index=True)


@pytest.fixture
def inventory_df():
    """Inventory DataFrame matching the Week-2 RDBMS source structure."""
    return pd.DataFrame({
        "id":         [101, 102, 103, 104, 105],
        "articleType": ["Shirts", "Jeans", "Dresses", "Shoes", "Caps"],
        "baseColour": ["Blue", "Red", "Green", "White", "Black"],
        "usage":      ["Casual", "Casual", "Formal", "Sports", "Casual"],
        "stock_qty":  [10, 25, 5, 3, 40],
        "updated_at": ["2024-01-01"] * 5,
    })


# ===========================================================================
# SECTION 1: Week 4 — transform() unit tests
# ===========================================================================

@pytest.mark.unit
class TestWeek4Transform:
    """Unit tests for week4_pipeline.pipeline_week4.transform()"""

    def test_drops_null_ids(self, sample_raw_df_with_nulls):
        """transform() must drop rows where `id` is NaN/None."""
        result = week4_transform(sample_raw_df_with_nulls)
        assert result["id"].isna().sum() == 0, "Null id rows must be removed"
        assert len(result) == 5, "Only the 5 clean rows should survive"

    def test_strips_display_name_whitespace(self, sample_raw_df):
        """productDisplayName must have leading/trailing whitespace removed."""
        result = week4_transform(sample_raw_df)
        for name in result["productDisplayName"]:
            assert name == name.strip(), f"Display name has whitespace: '{name}'"

    def test_price_estimate_formula(self, sample_raw_df):
        """price_estimate must equal (id % 4000 + 299) rounded to 2 decimal places."""
        result = week4_transform(sample_raw_df)
        for _, row in result.iterrows():
            expected = round((row["id"] % 4000 + 299), 2)
            assert row["price_estimate"] == pytest.approx(expected, abs=0.001), (
                f"id={row['id']}: expected price_estimate={expected}, got {row['price_estimate']}"
            )

    def test_is_recent_flag_true_for_2015_and_later(self, sample_raw_df):
        """Rows with year >= 2015 must have is_recent == True."""
        result = week4_transform(sample_raw_df)
        recent_rows = result[result["year"] >= 2015]
        assert recent_rows["is_recent"].all(), "All rows with year >= 2015 must be marked is_recent=True"

    def test_is_recent_flag_false_for_before_2015(self, sample_raw_df):
        """Rows with year < 2015 must have is_recent == False."""
        result = week4_transform(sample_raw_df)
        old_rows = result[result["year"] < 2015]
        assert not old_rows["is_recent"].any(), "Rows with year < 2015 must be marked is_recent=False"

    def test_derived_columns_present(self, sample_raw_df):
        """Both derived columns (price_estimate, is_recent) must exist in output."""
        result = week4_transform(sample_raw_df)
        assert "price_estimate" in result.columns, "price_estimate column must be added"
        assert "is_recent" in result.columns, "is_recent column must be added"

    def test_clean_input_preserves_row_count(self, sample_raw_df):
        """No rows should be dropped when all ids are valid."""
        result = week4_transform(sample_raw_df)
        assert len(result) == len(sample_raw_df), (
            "Row count must not change when there are no null ids"
        )

    def test_does_not_mutate_original_dataframe(self, sample_raw_df):
        """transform() must not modify the input DataFrame (uses df.copy())."""
        original_cols = list(sample_raw_df.columns)
        original_len = len(sample_raw_df)
        _ = week4_transform(sample_raw_df)
        assert list(sample_raw_df.columns) == original_cols, "Original DataFrame columns must not change"
        assert len(sample_raw_df) == original_len, "Original DataFrame row count must not change"

    def test_price_estimate_dtype_is_numeric(self, sample_raw_df):
        """price_estimate must be a numeric (float) column."""
        result = week4_transform(sample_raw_df)
        assert pd.api.types.is_numeric_dtype(result["price_estimate"]), (
            "price_estimate must be a numeric dtype"
        )

    def test_is_recent_dtype_is_bool(self, sample_raw_df):
        """is_recent must be a boolean column."""
        result = week4_transform(sample_raw_df)
        assert pd.api.types.is_bool_dtype(result["is_recent"]), (
            "is_recent must be a boolean dtype"
        )


# ===========================================================================
# SECTION 2: Week 2 — CDC (Change Data Capture) unit tests
# ===========================================================================

@pytest.mark.unit
class TestRowHash:
    """Unit tests for week2_etl.pipeline_week2._row_hash()"""

    def test_same_data_produces_same_hash(self, inventory_df):
        """Identical rows must produce the same hash (deterministic)."""
        row = inventory_df.rename(columns={"id": "product_id"}).iloc[0]
        hash1 = _row_hash(row)
        hash2 = _row_hash(row)
        assert hash1 == hash2, "Hash must be deterministic for identical row data"

    def test_different_data_produces_different_hash(self, inventory_df):
        """Rows with different content must produce different hashes."""
        inv = inventory_df.rename(columns={"id": "product_id"})
        hash_a = _row_hash(inv.iloc[0])
        hash_b = _row_hash(inv.iloc[1])
        assert hash_a != hash_b, "Different rows must produce different hashes"

    def test_hash_is_md5_hex_string(self, inventory_df):
        """Hash output must be a 32-character hex string (MD5)."""
        row = inventory_df.rename(columns={"id": "product_id"}).iloc[0]
        h = _row_hash(row)
        assert isinstance(h, str), "Hash must be a string"
        assert len(h) == 32, "MD5 hash must be 32 characters"
        assert all(c in "0123456789abcdef" for c in h), "Hash must be hex characters"


@pytest.mark.unit
class TestCDC:
    """Unit tests for week2_etl.pipeline_week2.run_cdc()"""

    def _make_df(self, records):
        """Helper: build a minimal DataFrame compatible with run_cdc()."""
        return pd.DataFrame({
            "product_id":   [r["id"] for r in records],
            "articleType":  [r.get("articleType", "Shirt") for r in records],
            "baseColour":   [r.get("baseColour", "Blue") for r in records],
            "usage":        [r.get("usage", "Casual") for r in records],
            "stock_qty":    [r.get("stock_qty", 10) for r in records],
        })

    def test_detects_inserts_on_first_run(self, tmp_path, monkeypatch):
        """All rows are inserts when there is no previous snapshot."""
        # Point cdc snapshot to a temp directory so no pre-existing snapshot
        monkeypatch.setattr(
            "week2_etl.pipeline_week2.OUT", tmp_path
        )
        df = self._make_df([
            {"id": 1}, {"id": 2}, {"id": 3}
        ])
        result = run_cdc(df)
        assert result["inserted_count"] == 3, "All 3 rows should be inserts on first run"
        assert result["updated_count"] == 0
        assert result["deleted_count"] == 0

    def test_detects_no_changes_on_identical_run(self, tmp_path, monkeypatch):
        """Running CDC twice with identical data should show 0 inserts/updates/deletes."""
        monkeypatch.setattr("week2_etl.pipeline_week2.OUT", tmp_path)
        df = self._make_df([{"id": 1}, {"id": 2}])
        run_cdc(df)         # first run — creates snapshot
        result = run_cdc(df)  # second run — same data
        assert result["inserted_count"] == 0
        assert result["updated_count"] == 0
        assert result["deleted_count"] == 0

    def test_detects_updates(self, tmp_path, monkeypatch):
        """Rows with changed field values must be counted as updates."""
        monkeypatch.setattr("week2_etl.pipeline_week2.OUT", tmp_path)
        df_before = self._make_df([{"id": 1, "stock_qty": 10}])
        run_cdc(df_before)  # snapshot with stock_qty=10

        df_after = self._make_df([{"id": 1, "stock_qty": 99}])  # stock_qty changed
        result = run_cdc(df_after)
        assert result["updated_count"] == 1, "Changed row should be detected as update"
        assert result["inserted_count"] == 0
        assert result["deleted_count"] == 0

    def test_detects_deletes(self, tmp_path, monkeypatch):
        """Rows present in the previous snapshot but missing now must be deletes."""
        monkeypatch.setattr("week2_etl.pipeline_week2.OUT", tmp_path)
        df_before = self._make_df([{"id": 1}, {"id": 2}, {"id": 3}])
        run_cdc(df_before)  # snapshot with 3 rows

        df_after = self._make_df([{"id": 1}])  # rows 2 and 3 removed
        result = run_cdc(df_after)
        assert result["deleted_count"] == 2, "2 missing rows should be detected as deletes"

    def test_detects_mixed_changes(self, tmp_path, monkeypatch):
        """CDC should simultaneously detect inserts, updates, and deletes."""
        monkeypatch.setattr("week2_etl.pipeline_week2.OUT", tmp_path)
        df_before = self._make_df([
            {"id": 1, "stock_qty": 10},
            {"id": 2, "stock_qty": 5},
        ])
        run_cdc(df_before)

        df_after = self._make_df([
            {"id": 1, "stock_qty": 99},  # id=1 updated
            # id=2 deleted
            {"id": 3, "stock_qty": 20},  # id=3 inserted
        ])
        result = run_cdc(df_after)
        assert result["inserted_count"] == 1, "id=3 is a new insert"
        assert result["updated_count"] == 1, "id=1 changed — update"
        assert result["deleted_count"] == 1, "id=2 is gone — delete"

    def test_cdc_writes_snapshot_file(self, tmp_path, monkeypatch):
        """run_cdc() must persist a cdc_snapshot.json to OUT directory."""
        monkeypatch.setattr("week2_etl.pipeline_week2.OUT", tmp_path)
        df = self._make_df([{"id": 1}, {"id": 2}])
        run_cdc(df)
        snapshot_file = tmp_path / "cdc_snapshot.json"
        assert snapshot_file.exists(), "cdc_snapshot.json must be created after CDC run"
        state = json.loads(snapshot_file.read_text())
        assert len(state) == 2, "Snapshot should contain exactly 2 product_id entries"

    def test_total_tracked_matches_current_rows(self, tmp_path, monkeypatch):
        """total_tracked must equal the number of rows in the current DataFrame."""
        monkeypatch.setattr("week2_etl.pipeline_week2.OUT", tmp_path)
        df = self._make_df([{"id": 10}, {"id": 20}, {"id": 30}])
        result = run_cdc(df)
        assert result["total_tracked"] == 3


# ===========================================================================
# SECTION 3: Week 2 — Incremental Load watermark logic (mock DB)
# ===========================================================================

@pytest.mark.unit
class TestIncrementalLoadWatermark:
    """Unit tests for the watermark filtering logic in load_incremental()."""

    def _temp_warehouse(self, tmp_path):
        """Create a temp SQLite warehouse and return its path."""
        db_path = tmp_path / "test_warehouse.db"
        return str(db_path)

    def test_first_load_writes_all_rows(self, tmp_path, monkeypatch):
        """With no previous watermark (watermark=0), all rows should be written."""
        db_path = self._temp_warehouse(tmp_path)
        monkeypatch.setattr("week2_etl.pipeline_week2.WAREHOUSE_DB", db_path)

        df = pd.DataFrame({
            "product_id": [1, 2, 3],
            "articleType": ["Shirt", "Jeans", "Dress"],
            "baseColour": ["Blue", "Red", "Green"],
            "usage": ["Casual", "Casual", "Formal"],
            "stock_qty": [10, 20, 5],
            "updated_at": ["2024-01-01"] * 3,
        })
        result = load_incremental(df)
        assert result["rows_written"] == 3, "All 3 rows should be written when watermark=0"
        assert result["prev_watermark"] == 0

    def test_incremental_load_skips_seen_rows(self, tmp_path, monkeypatch):
        """Rows with product_id <= current watermark must NOT be re-written."""
        db_path = self._temp_warehouse(tmp_path)
        monkeypatch.setattr("week2_etl.pipeline_week2.WAREHOUSE_DB", db_path)

        base_df = pd.DataFrame({
            "product_id": [1, 2, 3],
            "articleType": ["Shirt", "Jeans", "Dress"],
            "baseColour": ["Blue", "Red", "Green"],
            "usage": ["Casual", "Casual", "Formal"],
            "stock_qty": [10, 20, 5],
            "updated_at": ["2024-01-01"] * 3,
        })
        load_incremental(base_df)  # watermark → 3

        # Now add rows 4 and 5 (rows 1–3 are old)
        new_df = pd.DataFrame({
            "product_id": [1, 2, 3, 4, 5],
            "articleType": ["Shirt", "Jeans", "Dress", "Cap", "Shoes"],
            "baseColour": ["Blue", "Red", "Green", "Black", "White"],
            "usage": ["Casual", "Casual", "Formal", "Casual", "Sports"],
            "stock_qty": [10, 20, 5, 15, 8],
            "updated_at": ["2024-01-01"] * 5,
        })
        result = load_incremental(new_df)
        assert result["rows_written"] == 2, "Only rows 4 and 5 should be written"
        assert result["prev_watermark"] == 3

    def test_no_new_rows_writes_zero(self, tmp_path, monkeypatch):
        """If no rows exceed the watermark, zero rows should be written."""
        db_path = self._temp_warehouse(tmp_path)
        monkeypatch.setattr("week2_etl.pipeline_week2.WAREHOUSE_DB", db_path)

        df = pd.DataFrame({
            "product_id": [1, 2, 3],
            "articleType": ["Shirt", "Jeans", "Dress"],
            "baseColour": ["Blue", "Red", "Green"],
            "usage": ["Casual", "Casual", "Formal"],
            "stock_qty": [10, 20, 5],
            "updated_at": ["2024-01-01"] * 3,
        })
        load_incremental(df)  # watermark → 3
        result = load_incremental(df)  # same data, nothing new
        assert result["rows_written"] == 0, "No rows should be written when all are below watermark"


# ===========================================================================
# SECTION 4: Edge cases and data quality guards
# ===========================================================================

@pytest.mark.unit
class TestEdgeCases:
    """Edge case tests for robustness of transformation logic."""

    def test_transform_empty_dataframe(self):
        """transform() must handle an empty DataFrame gracefully."""
        empty_df = pd.DataFrame({
            "id": pd.Series([], dtype=float),
            "productDisplayName": pd.Series([], dtype=str),
            "year": pd.Series([], dtype=int),
        })
        result = week4_transform(empty_df)
        assert len(result) == 0, "Empty input should produce empty output"
        assert "price_estimate" in result.columns, "price_estimate column should still be added"
        assert "is_recent" in result.columns, "is_recent column should still be added"

    def test_transform_single_row(self):
        """transform() must work correctly for a single-row DataFrame."""
        single = pd.DataFrame({
            "id": [500],
            "productDisplayName": ["  Test Product  "],
            "year": [2022],
        })
        result = week4_transform(single)
        assert len(result) == 1
        assert result.iloc[0]["productDisplayName"] == "Test Product"
        assert result.iloc[0]["price_estimate"] == pytest.approx(500 % 4000 + 299, abs=0.001)
        assert bool(result.iloc[0]["is_recent"]) is True

    def test_transform_boundary_year_2015(self):
        """Year exactly 2015 must be flagged as is_recent=True (boundary condition)."""
        df = pd.DataFrame({
            "id": [1],
            "productDisplayName": ["Boundary Year Item"],
            "year": [2015],
        })
        result = week4_transform(df)
        assert bool(result.iloc[0]["is_recent"]) is True, "year=2015 is the boundary; must be True"

    def test_transform_boundary_year_2014(self):
        """Year 2014 (one year before threshold) must be flagged as is_recent=False."""
        df = pd.DataFrame({
            "id": [1],
            "productDisplayName": ["Just Before Threshold"],
            "year": [2014],
        })
        result = week4_transform(df)
        assert bool(result.iloc[0]["is_recent"]) is False, "year=2014 must be False (< 2015)"

    def test_price_estimate_large_id(self):
        """price_estimate formula must correctly apply modulo for large IDs."""
        df = pd.DataFrame({
            "id": [8001],
            "productDisplayName": ["Large ID Item"],
            "year": [2020],
        })
        result = week4_transform(df)
        expected = round((8001 % 4000) + 299, 2)  # 8001 % 4000 = 1; 1 + 299 = 300.0
        assert result.iloc[0]["price_estimate"] == pytest.approx(expected, abs=0.001)
