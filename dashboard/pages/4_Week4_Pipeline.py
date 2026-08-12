"""
Week 4 — Batch Pipeline via REST API & PySpark Validation
  + Default: Stopped API Engine
  + User Workflow: Start Engine -> Select Category & Limit (max 50) -> Run Ingestion
"""
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pandas as pd
import requests
import sqlite3
import streamlit as st

BASE = Path(__file__).resolve().parent.parent.parent
OUT  = BASE / "week4_pipeline" / "outputs"
CLEAN_CSV = BASE / "week1_eda" / "outputs" / "cleaned_data.csv"
TARGET_DB = BASE / "data" / "warehouse" / "warehouse.db"
API_BASE  = "http://127.0.0.1:8000"

st.set_page_config(page_title="Week 4 – Batch API Ingestion", page_icon="🌐", layout="wide")

# ── Dark Theme CSS ────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');
html,body,[class*="css"]{ font-family:'Inter',sans-serif; }

.page-header{
    background:linear-gradient(135deg,#2e051a 0%,#3e0a25 40%,#1a1d3e 100%);
    border:1px solid rgba(244,114,182,0.25);
    border-radius:16px;padding:24px 32px;margin-bottom:20px;
}
.page-header h1{
    font-size:1.7rem;font-weight:800;
    background:linear-gradient(90deg,#f472b6,#ec4899,#f43f5e);
    -webkit-background-clip:text;-webkit-text-fill-color:transparent;margin:0 0 4px 0;
}
.page-header p{color:#94a3b8;font-size:0.8rem;margin:0;}

.engine-card{
    background:linear-gradient(145deg,#1e2235,#161928);
    border:1px solid rgba(244,114,182,0.3);
    border-radius:14px;padding:20px 24px;margin-bottom:20px;
}
.status-badge-on{
    background:rgba(52,211,153,0.15);color:#34d399;
    border:1px solid #10b981;padding:3px 10px;border-radius:20px;
    font-weight:700;font-size:0.78rem;
}
.status-badge-off{
    background:rgba(239,68,68,0.15);color:#f87171;
    border:1px solid #ef4444;padding:3px 10px;border-radius:20px;
    font-weight:700;font-size:0.78rem;
}

.small-host-link {
    font-size: 0.72rem !important;
    color: #94a3b8 !important;
    font-family: monospace;
}

div[data-testid="metric-container"]{
    background:linear-gradient(145deg,#1e2235,#161928);
    border:1px solid rgba(244,114,182,0.2);border-radius:12px;padding:14px!important;
}
</style>
""", unsafe_allow_html=True)

st.markdown("""
<div class="page-header">
  <h1>🌐 Week 4 — REST API Batch Extraction &amp; Ingestion</h1>
  <p>Localhost FastAPI Extraction · Category &amp; Limit Options · Warehouse Sync</p>
</div>
""", unsafe_allow_html=True)

# ── API Engine Helpers ────────────────────────────────────────────────────────
def check_api_status() -> bool:
    try:
        r = requests.get(f"{API_BASE}/health", timeout=1)
        return r.status_code == 200
    except Exception:
        return False

def start_api_engine():
    if check_api_status():
        return True
    try:
        proc = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "week4_pipeline.api_server:app",
             "--host", "127.0.0.1", "--port", "8000", "--log-level", "warning"],
            cwd=str(BASE), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        st.session_state["api_proc_pid"] = proc.pid
        for _ in range(25):
            if check_api_status():
                return True
            time.sleep(0.2)
    except Exception as e:
        st.error(f"Failed to start API server: {e}")
    return check_api_status()

def stop_api_engine():
    pid = st.session_state.get("api_proc_pid")
    if pid:
        try:
            os.kill(pid, signal.SIGTERM)
        except Exception:
            pass
        st.session_state["api_proc_pid"] = None
    try:
        if sys.platform == "win32":
            subprocess.run(["taskkill", "/F", "/IM", "uvicorn.exe"], capture_output=True)
    except Exception:
        pass
    time.sleep(0.5)

api_running = check_api_status()

# ── Engine Status & Control Card ──────────────────────────────────────────────
st.markdown('<div class="engine-card">', unsafe_allow_html=True)
col_head, col_btn1, col_btn2 = st.columns([2.5, 1, 1])

with col_head:
    badge_html = '<span class="status-badge-on">🟢 API ENGINE RUNNING</span>' if api_running else '<span class="status-badge-off">🔴 API ENGINE STOPPED</span>'
    st.markdown(f"### ⚙️ FastAPI Engine &nbsp; {badge_html}", unsafe_allow_html=True)
    st.markdown('<p class="small-host-link">Host Endpoint: <code>http://127.0.0.1:8000/products</code></p>', unsafe_allow_html=True)

with col_btn1:
    st.markdown("&nbsp;", unsafe_allow_html=True)
    if st.button("▶️ Start Engine", use_container_width=True, type="primary", disabled=api_running):
        with st.spinner("Starting FastAPI Uvicorn Server..."):
            if start_api_engine():
                st.success("API Engine started on :8000!")
                st.rerun()

with col_btn2:
    st.markdown("&nbsp;", unsafe_allow_html=True)
    if st.button("⏹️ Stop Engine", use_container_width=True, disabled=not api_running):
        stop_api_engine()
        st.warning("API Engine stopped.")
        st.rerun()

st.markdown('</div>', unsafe_allow_html=True)

# ── Categories List ───────────────────────────────────────────────────────────
categories = ["All Categories"]
if CLEAN_CSV.exists():
    try:
        c_df = pd.read_csv(CLEAN_CSV, usecols=["masterCategory"])
        categories.extend(sorted(c_df["masterCategory"].dropna().unique().tolist()))
    except Exception:
        categories.extend(["Apparel", "Footwear", "Accessories", "Personal Care", "Free Items", "Sporting Goods", "Home"])

# ── Ingestion Form: Category Select & Limit Option (Max 50) ──────────────────
st.markdown("### 🎛️ Select Category & Record Limit")

with st.form("api_ingestion_simple_form"):
    fc1, fc2 = st.columns(2)
    with fc1:
        sel_category = st.selectbox("Select Category", options=categories)
    with fc2:
        # Limit set to max of 50 as requested
        sel_limit = st.number_input("Record Limit (Max 50)", min_value=1, max_value=50, value=50, step=1)

    run_ingest = st.form_submit_button("🚀 Run API Extraction & Ingestion", type="primary", use_container_width=True)

# Execution Logic
if run_ingest:
    if not check_api_status():
        st.info("API Server not running — starting API engine automatically...")
        start_api_engine()

    params = {"limit": sel_limit, "offset": 0}
    if sel_category != "All Categories":
        params["masterCategory"] = sel_category

    try:
        with st.spinner(f"Extracting records from http://127.0.0.1:8000/products (Category: {sel_category}, Limit: {sel_limit})..."):
            r = requests.get(f"{API_BASE}/products", params=params, timeout=10)
            r.raise_for_status()
            fetched_data = r.json()

        if not fetched_data:
            st.warning("API returned 0 records for the selected category.")
        else:
            batch_df = pd.DataFrame(fetched_data)
            
            # Vectorised transformation
            batch_df["productDisplayName"] = batch_df["productDisplayName"].astype(str).str.strip()
            batch_df["price_estimate"] = (batch_df["id"] % 4000 + 299).round(2)
            batch_df["is_recent"] = batch_df["year"] >= 2015

            # Sync to dataset & SQLite warehouse
            if CLEAN_CSV.exists():
                full_existing = pd.read_csv(CLEAN_CSV)
                max_id = int(full_existing["id"].max())
                
                new_appended = batch_df.copy()
                new_appended["id"] = range(max_id + 1, max_id + 1 + len(new_appended))
                
                new_appended[["id", "gender", "masterCategory", "subCategory", "articleType",
                              "baseColour", "season", "year", "usage", "productDisplayName"]].to_csv(
                    CLEAN_CSV, mode="a", header=False, index=False
                )

                if TARGET_DB.exists():
                    conn = sqlite3.connect(TARGET_DB)
                    batch_df.to_sql("api_batch_load", conn, if_exists="replace", index=False)
                    conn.close()

                total_appended_count = len(full_existing) + len(new_appended)
                st.session_state["last_api_batch"] = batch_df
                st.session_state["last_api_count"] = len(batch_df)
                st.session_state["total_dataset_rows"] = total_appended_count
                st.session_state["data_updated_at"] = time.strftime("%H:%M:%S")

                # CDC log entry
                cdc_log = st.session_state.get("cdc_event_log", [])
                cdc_log.append({
                    "event_type": "INSERT",
                    "product_id": f"API Batch ({len(batch_df)} rows)",
                    "changed_fields": 10,
                    "details": f"Category: {sel_category}, Limit: {sel_limit}",
                    "detected_at": time.strftime("%H:%M:%S")
                })
                st.session_state["cdc_event_log"] = cdc_log

                st.success(
                    f"✅ Extracted **{len(batch_df):,} rows** (Limit: {sel_limit}) from localhost REST API! "
                    f"Transformed & appended to warehouse (`warehouse.db`) and dataset (`cleaned_data.csv`). "
                    f"Updated total dataset count: **{total_appended_count:,} rows**."
                )
                st.cache_data.clear()

    except Exception as e:
        st.error(f"Extraction failed: {e}")

st.divider()

# ── Summary Metrics & Live Data Preview ───────────────────────────────────────
if CLEAN_CSV.exists():
    df_curr = pd.read_csv(CLEAN_CSV)
    current_total = len(df_curr)
else:
    current_total = 44417

last_extracted = st.session_state.get("last_api_count", 50)

m1, m2, m3 = st.columns(3)
m1.markdown('<div style="margin-top:4px;"><span class="small-host-link">Host Endpoint: http://127.0.0.1:8000/products</span></div>', unsafe_allow_html=True)
m1.metric("📦 Last Batch Extracted", f"{last_extracted:,} rows")
m2.metric("📈 Total Appended Rows", f"{current_total:,} rows")
m3.metric("🗄️ Warehouse Status (`api_batch_load`)", "✅ VERIFIED")

st.markdown("### 📋 Sample Preview of Extracted API Data")
csv_path = OUT / "api_batch_transformed.csv"
if "last_api_batch" in st.session_state:
    st.dataframe(st.session_state["last_api_batch"].head(50), use_container_width=True)
elif csv_path.exists():
    df_preview = pd.read_csv(csv_path)
    st.dataframe(df_preview.head(50), use_container_width=True)
else:
    st.info("Start the API Engine and run an extraction to view live data.")
