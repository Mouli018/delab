"""
Unit and Integration Tests for Week 4 — Batch API Pipeline & Processing Engine
"""
import json
import sqlite3
from pathlib import Path
import pandas as pd
import requests
import pytest

BASE = Path(__file__).resolve().parent.parent
OUT  = BASE / "week4_pipeline" / "outputs"
TARGET_DB = BASE / "data" / "warehouse" / "warehouse.db"

def test_week4_summary_report():
    """Verify week4_summary.json exists and reports batch results."""
    summary_file = OUT / "week4_summary.json"
    assert summary_file.exists(), "week4_summary.json is missing"
    summary = json.loads(summary_file.read_text())
    assert "load_verification" in summary
    assert summary["load_verification"]["match"] is True

def test_week4_transformed_batch_csv():
    """Verify transformed batch CSV output."""
    csv_file = OUT / "api_batch_transformed.csv"
    assert csv_file.exists(), "api_batch_transformed.csv is missing"
    df = pd.read_csv(csv_file)
    assert "price_estimate" in df.columns, "Derived column 'price_estimate' missing"
    assert "is_recent" in df.columns, "Derived column 'is_recent' missing"

def test_week4_warehouse_table_verification():
    """Verify target table api_batch_load in SQLite warehouse.db."""
    if not TARGET_DB.exists():
        pytest.skip("warehouse.db missing")
    conn = sqlite3.connect(TARGET_DB)
    count = conn.execute("SELECT COUNT(*) FROM api_batch_load").fetchone()[0]
    conn.close()
    assert count > 0, "Warehouse table api_batch_load is empty"
