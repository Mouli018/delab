"""
Unit and Integration Tests for Week 3 — Data Architecture & Schema Design (OLTP/OLAP/Cube)
"""
import json
import sqlite3
from pathlib import Path
import pandas as pd
import pytest

try:
    import duckdb
    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False

BASE = Path(__file__).resolve().parent.parent
OUT  = BASE / "week3_schema" / "outputs"
OLTP_DB = BASE / "data" / "warehouse" / "oltp_relational.db"
OLAP_DB = BASE / "data" / "warehouse" / "olap_star_schema.duckdb"

def test_week3_summary_exists():
    """Verify week3_summary.json exists and reports valid row counts."""
    summary_file = OUT / "week3_summary.json"
    assert summary_file.exists(), "week3_summary.json is missing"
    summary = json.loads(summary_file.read_text())
    assert "oltp_row_counts" in summary
    assert "star_schema" in summary

def test_week3_oltp_relational_schema():
    """Verify SQLite 3NF OLTP database structure."""
    if not OLTP_DB.exists():
        pytest.skip("oltp_relational.db missing")
    conn = sqlite3.connect(OLTP_DB)
    tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
    conn.close()
    expected = {"products", "master_categories", "sub_categories", "article_types", "customers", "stores", "orders", "order_items"}
    assert expected.issubset(set(tables)), f"Missing expected 3NF tables in OLTP DB. Found: {tables}"

def test_week3_olap_star_schema_duckdb():
    """Verify DuckDB Star Schema tables and row integrity."""
    if not DUCKDB_AVAILABLE or not OLAP_DB.exists():
        pytest.skip("DuckDB not installed or olap_star_schema.duckdb missing")
    con = duckdb.connect(str(OLAP_DB), read_only=True)
    tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
    con.close()
    expected = {"fact_sales", "dim_product", "dim_customer", "dim_time", "dim_store"}
    assert expected.issubset(set(tables)), f"Missing expected Star Schema tables in DuckDB. Found: {tables}"

def test_week3_data_cube_outputs():
    """Verify Data Cube analytical query outputs exist."""
    cube_csv = OUT / "cube_full_grouping_sets.csv"
    assert cube_csv.exists(), "cube_full_grouping_sets.csv is missing"
    df = pd.read_csv(cube_csv)
    assert len(df) > 0, "Data Cube output CSV should contain aggregated rows"
