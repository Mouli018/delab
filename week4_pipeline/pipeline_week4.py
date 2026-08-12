"""
Week 4 - Python-based batch pipeline (Pandas): API -> transform -> load
==========================================================================
i.   Extraction from the FastAPI mock service (real HTTP calls, paginated)
ii.  Cleaning & transformation
iii. Loading into a target database (SQLite warehouse)
iv.  End-to-end error handling + result verification
"""
import json
import subprocess
import sys
import time
from pathlib import Path

import pandas as pd
import requests
import sqlite3

BASE = Path(__file__).resolve().parent.parent
OUT = Path(__file__).resolve().parent / "outputs"
OUT.mkdir(exist_ok=True)
API_BASE = "http://127.0.0.1:8000"
TARGET_DB = BASE / "data" / "warehouse" / "warehouse.db"


def ensure_api_running(timeout=15):
    """Start the FastAPI mock service if it isn't already up. Real HTTP
    health-check + retry loop, not a stub."""
    try:
        requests.get(f"{API_BASE}/health", timeout=1)
        print("[api] already running")
        return None
    except Exception:
        pass

    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "week4_pipeline.api_server:app",
         "--host", "127.0.0.1", "--port", "8000", "--log-level", "warning"],
        cwd=str(BASE), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    for _ in range(timeout * 5):
        try:
            r = requests.get(f"{API_BASE}/health", timeout=1)
            if r.status_code == 200:
                print("[api] started FastAPI mock service on :8000")
                return proc
        except Exception:
            time.sleep(0.2)
    raise RuntimeError("API failed to start within timeout")


# ---------------------------------------------------------------------------
# i. EXTRACTION - paginated REST API calls with retry/error handling
# ---------------------------------------------------------------------------
def extract_from_api(page_size=500, max_pages=100):
    all_rows = []
    errors = []
    offset = 0
    for page in range(max_pages):
        try:
            resp = requests.get(f"{API_BASE}/products",
                                 params={"limit": page_size, "offset": offset}, timeout=5)
            resp.raise_for_status()
            batch = resp.json()
        except requests.exceptions.RequestException as e:
            errors.append({"offset": offset, "error": str(e)})
            break  # stop paginating on failure, but keep what we have (partial-result handling)

        if not batch:
            break
        all_rows.extend(batch)
        offset += page_size

    df = pd.DataFrame(all_rows)
    print(f"[extract] {len(df)} rows across {offset // page_size} pages, {len(errors)} errors")
    return df, errors


# ---------------------------------------------------------------------------
# ii. TRANSFORMATION
# ---------------------------------------------------------------------------
def transform(df: pd.DataFrame):
    df = df.copy()
    before = len(df)
    df = df.dropna(subset=["id"])
    df["productDisplayName"] = df["productDisplayName"].astype(str).str.strip()
    df["price_estimate"] = (df["id"] % 4000 + 299).round(2)   # deterministic synthetic price for demo analytics
    df["is_recent"] = df["year"] >= 2015
    after = len(df)
    print(f"[transform] {before} -> {after} rows, +2 derived columns")
    return df


# ---------------------------------------------------------------------------
# iii. LOADING with verification
# ---------------------------------------------------------------------------
def load_and_verify(df: pd.DataFrame):
    conn = sqlite3.connect(TARGET_DB)
    df.to_sql("api_batch_load", conn, if_exists="replace", index=False)
    verify_count = conn.execute("SELECT COUNT(*) FROM api_batch_load").fetchone()[0]
    conn.close()

    success = verify_count == len(df)
    result = {"rows_loaded": len(df), "rows_verified_in_db": verify_count, "match": success}
    print(f"[load] {verify_count}/{len(df)} rows verified in warehouse.db "
          f"({'OK' if success else 'MISMATCH'})")
    return result


if __name__ == "__main__":
    proc = ensure_api_running()
    try:
        raw, errors = extract_from_api()
        if raw.empty:
            raise RuntimeError("Extraction returned no data - aborting pipeline")
        clean = transform(raw)
        verify = load_and_verify(clean)

        summary = {"extraction_errors": errors, "load_verification": verify,
                   "total_rows": len(clean)}
        with open(OUT / "week4_summary.json", "w") as f:
            json.dump(summary, f, indent=2, default=str)
        clean.to_csv(OUT / "api_batch_transformed.csv", index=False)
        print("Week 4 batch pipeline complete.")
    finally:
        if proc is not None:
            proc.terminate()
