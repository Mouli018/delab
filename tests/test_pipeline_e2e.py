"""
End-to-End Pipeline & Power BI Data Export Integration Tests
"""
import subprocess
import sys
from pathlib import Path
import pandas as pd
import pytest

BASE = Path(__file__).resolve().parent.parent

def test_export_for_powerbi_data_files():
    """Verify Power BI export script generates valid CSV files."""
    powerbi_dir = BASE / "powerbi" / "data"
    assert powerbi_dir.exists(), "powerbi/data directory missing"
    
    expected_files = [
        "fact_sales.csv", "dim_product.csv", "dim_customer.csv",
        "dim_store.csv", "dim_time.csv", "snowflake_dim_category.csv"
    ]
    for filename in expected_files:
        filepath = powerbi_dir / filename
        assert filepath.exists(), f"Power BI export file {filename} is missing"
        df = pd.read_csv(filepath)
        assert len(df) > 0, f"Export file {filename} is empty"

def test_full_pipeline_compilation():
    """Verify all Python scripts in the project compile cleanly without syntax errors."""
    import glob, py_compile
    py_files = [f for f in glob.glob(str(BASE / "**/*.py"), recursive=True) if "venv" not in f]
    for py_file in py_files:
        py_compile.compile(py_file, doraise=True)
