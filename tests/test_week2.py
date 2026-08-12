"""
Unit and Integration Tests for Week 2 — Building Core Data Pipeline (ETL & CDC)
"""
import json
import sqlite3
from pathlib import Path
import pandas as pd
import pytest

BASE = Path(__file__).resolve().parent.parent
OUT  = BASE / "week2_etl" / "outputs"
WH_DB = BASE / "data" / "warehouse" / "warehouse.db"

def test_week2_summary_report():
    """Verify week2_summary.json exists and reports valid ETL steps."""
    summary_file = OUT / "week2_summary.json"
    assert summary_file.exists(), "week2_summary.json is missing"
    summary = json.loads(summary_file.read_text())
    assert "extraction" in summary
    assert "transformation" in summary
    assert "loading" in summary

def test_week2_rdbms_source_seeded():
    """Verify SQLite RDBMS source DB has inventory table."""
    rdbms_db = BASE / "data" / "warehouse" / "rdbms_source.db"
    if not rdbms_db.exists():
        rdbms_db = OUT / "rdbms_source.db"
    assert rdbms_db.exists(), "rdbms_source.db is missing"
    conn = sqlite3.connect(rdbms_db)
    count = conn.execute("SELECT COUNT(*) FROM inventory").fetchone()[0]
    conn.close()
    assert count > 0, "rdbms_source.db inventory table should contain rows"

def test_week2_full_load_warehouse_table():
    """Verify full load strategy wrote to products_full table."""
    if not WH_DB.exists():
        pytest.skip("warehouse.db not found")
    conn = sqlite3.connect(WH_DB)
    count = conn.execute("SELECT COUNT(*) FROM products_full").fetchone()[0]
    conn.close()
    assert count > 0, "products_full warehouse table is empty"

def test_week2_incremental_load_watermark():
    """Verify watermark tracking table exists in warehouse."""
    if not WH_DB.exists():
        pytest.skip("warehouse.db not found")
    conn = sqlite3.connect(WH_DB)
    tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
    conn.close()
    assert "load_watermark" in tables, "load_watermark tracking table missing in warehouse.db"
