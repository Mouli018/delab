"""
Unit and Integration Tests for Week 5 — Resilient Production-Ready Pipelines
"""
import json
import sqlite3
from pathlib import Path
import pandas as pd
import pytest

BASE = Path(__file__).resolve().parent.parent
OUT  = BASE / "week5_resilience" / "outputs"
RESILIENT_DB = BASE / "data" / "warehouse" / "resilient_warehouse.db"

def test_week5_summary_report():
    """Verify week5_summary.json exists and reports resilience patterns."""
    summary_file = OUT / "week5_summary.json"
    assert summary_file.exists(), "week5_summary.json is missing"
    summary = json.loads(summary_file.read_text())
    assert "staging_validation" in summary
    assert "idempotency" in summary
    assert "atomicity" in summary
    assert "error_handling_backfill" in summary

def test_week5_staging_validation_passed():
    """Verify staging validation checks succeeded."""
    summary_file = OUT / "week5_summary.json"
    summary = json.loads(summary_file.read_text())
    checks = summary["staging_validation"]
    assert checks["all_passed"] is True, "Staging validation checks failed"
    assert checks["schema_valid"] is True
    assert checks["null_check_passed"] is True
    assert checks["duplicate_check_passed"] is True

def test_week5_idempotency_confirmation():
    """Verify idempotency logic: rerunning identical data produces 0 net new rows."""
    summary_file = OUT / "week5_summary.json"
    summary = json.loads(summary_file.read_text())
    idem = summary["idempotency"]
    assert idem["idempotent_confirmed"] is True, "Idempotency confirmation failed"
    assert idem["second_run_same_data"]["net_new_rows"] == 0

def test_week5_atomicity_transaction_rollback():
    """Verify atomicity logic: bad batch causes full transaction rollback."""
    summary_file = OUT / "week5_summary.json"
    summary = json.loads(summary_file.read_text())
    atom = summary["atomicity"]
    assert atom["failure_case"]["outcome"] == "ROLLED_BACK"
    assert atom["failure_case"]["final_row_count"] == 0

def test_week5_error_handling_backfill():
    """Verify error handling backfill resolved corrupted rows."""
    summary_file = OUT / "week5_summary.json"
    summary = json.loads(summary_file.read_text())
    bf = summary["error_handling_backfill"]
    assert bf["backfill_successful"] is True
    assert bf["remaining_bad_rows"] == 0
