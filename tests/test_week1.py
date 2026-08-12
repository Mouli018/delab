"""
Unit and Integration Tests for Week 1 — Data Collection, Cleaning, Feature Engineering & EDA
"""
import json
from pathlib import Path
import pandas as pd
import pytest

BASE = Path(__file__).resolve().parent.parent
OUT  = BASE / "week1_eda" / "outputs"
CLEAN_CSV = OUT / "cleaned_data.csv"

def test_week1_cleaned_data_exists():
    """Verify that cleaned_data.csv exists and is non-empty."""
    assert CLEAN_CSV.exists(), "cleaned_data.csv output file is missing"
    df = pd.read_csv(CLEAN_CSV)
    assert len(df) > 0, "cleaned_data.csv should contain rows"
    expected_cols = {"id", "gender", "masterCategory", "subCategory", "articleType", "baseColour", "season", "year", "usage", "productDisplayName"}
    assert expected_cols.issubset(set(df.columns)), f"Missing expected columns in cleaned_data.csv. Found: {df.columns.tolist()}"

def test_week1_no_nulls_in_cleaned_data():
    """Verify that mandatory fields in cleaned_data.csv have no null values."""
    df = pd.read_csv(CLEAN_CSV)
    mandatory_fields = ["id", "masterCategory", "productDisplayName"]
    for col in mandatory_fields:
        null_count = df[col].isnull().sum()
        assert null_count == 0, f"Field '{col}' contains {null_count} null values after cleaning"

def test_week1_data_collection_report():
    """Verify that data_collection_report.json is generated correctly."""
    report_file = OUT / "data_collection_report.json"
    assert report_file.exists(), "data_collection_report.json is missing"
    report = json.loads(report_file.read_text())
    assert "sources" in report
    assert len(report["sources"]) > 0

def test_week1_engineered_features():
    """Verify feature engineering produces derived columns."""
    feat_file = OUT / "engineered_features.csv"
    assert feat_file.exists(), "engineered_features.csv is missing"
    df = pd.read_csv(feat_file)
    assert "name_word_count" in df.columns, "Feature 'name_word_count' missing"
    assert "product_age_years" in df.columns, "Feature 'product_age_years' missing"
    assert "name_length" in df.columns, "Feature 'name_length' missing"

def test_week1_eda_insights():
    """Verify eda_insights.json contains summary statistics."""
    insights_file = OUT / "eda_insights.json"
    assert insights_file.exists(), "eda_insights.json is missing"
    insights = json.loads(insights_file.read_text())
    assert "summary_stats" in insights
    assert insights["summary_stats"]["total_products"] > 0
