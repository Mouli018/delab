"""
Week 5 — Designing Resilient, Production-Ready Pipelines
"""
import json
import random
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import streamlit as st

BASE = Path(__file__).resolve().parent.parent.parent
OUT  = BASE / "week5_resilience" / "outputs"
CLEAN_CSV = BASE / "week1_eda" / "outputs" / "cleaned_data.csv"
RESILIENT_DB = BASE / "data" / "warehouse" / "resilient_warehouse.db"

st.set_page_config(page_title="Week 5 – Resilience", page_icon="🛡️", layout="wide")

# ── Dark Theme CSS ────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');
html,body,[class*="css"]{ font-family:'Inter',sans-serif; }
.page-header{
    background:linear-gradient(135deg,#12052e 0%,#1a0a40 40%,#1a1d3e 100%);
    border:1px solid rgba(167,139,250,0.25);border-radius:16px;
    padding:28px 36px;margin-bottom:24px;
}
.page-header h1{
    font-size:1.8rem;font-weight:800;
    background:linear-gradient(90deg,#a78bfa,#7c3aed,#c4b5fd);
    -webkit-background-clip:text;-webkit-text-fill-color:transparent;margin:0 0 4px 0;
}
.page-header p{color:#94a3b8;font-size:0.85rem;margin:0;}
div[data-testid="metric-container"]{
    background:linear-gradient(145deg,#1e2235,#161928);
    border:1px solid rgba(167,139,250,0.2);border-radius:12px;padding:14px!important;
}
.staging-practice{
    background:rgba(167,139,250,0.06);border:1px solid rgba(167,139,250,0.15);
    border-radius:10px;padding:14px 18px;margin:8px 0;
}
</style>
""", unsafe_allow_html=True)

st.markdown("""
<div class="page-header">
  <h1>🛡️ Week 5 — Resilient &amp; Production-Ready Pipelines</h1>
  <p>Staging · Validation · Idempotency · Atomicity · Error Handling · Apache Airflow · Apache Kafka</p>
</div>
""", unsafe_allow_html=True)

if not (OUT / "week5_summary.json").exists():
    st.error("No pipeline outputs found. Run `python run_pipeline.py` first.")
    st.stop()

summary = json.loads((OUT / "week5_summary.json").read_text())

tab1, tab2, tab3 = st.tabs([
    "🧪 Staging & Validation",
    "♻️ Idempotency & Atomicity",
    "🩹 Error Handling & Orchestration",
])

with tab1:
    checks = summary["staging_validation"]
    status = "✅ PASSED" if checks["all_passed"] else "❌ FAILED"

    st.markdown(f"### Batch `{checks['batch_id'][:8]}…` — {status}")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Schema validation", "✅ Pass" if checks["schema_valid"] else "❌ Fail")
    c2.metric("Null checks",       "✅ Pass" if checks["null_check_passed"] else "❌ Fail")
    c3.metric("Outlier detection", "✅ Pass" if checks["outlier_check_passed"] else "❌ Fail")
    c4.metric("Duplicate check",   "✅ Pass" if checks["duplicate_check_passed"] else "❌ Fail")

    st.markdown(f"**Staging File:** `{checks['staging_path']}` — warehouse tables are untouched until **all** validation checks pass.")

    st.divider()
    st.markdown("### Staging & Quality Control Practices")

    practices = [
        ("🗂️ Parquet Isolated Staging",
         "Each batch is written to an isolated Parquet file (`data/staging/batch_<uuid>.parquet`) before warehouse writes."),
        ("🔍 Schema Validation Gate",
         "Required columns are verified. Any missing column raises a `ValidationError` and halts the pipeline."),
        ("🚫 Null Check on Mandatory Fields",
         "Primary fields `id`, `productDisplayName`, `masterCategory` are scanned for nulls."),
        ("📏 Outlier Range Validation",
         "Numeric fields are checked against valid boundaries (e.g., `year` between 1990 and 2026)."),
        ("🔑 Duplicate Primary-Key Check",
         "The batch `id` column is checked for duplicates before loading."),
        ("📡 Kafka Streaming Staging Buffer",
         "Kafka topic `product-cdc-events` acts as a durable staging queue between producer and consumer."),
        ("🌬️ Airflow Staging Gate Task",
         "The `stage_and_validate` task in the Airflow DAG acts as an automated quality gate."),
    ]

    for i, (title, desc) in enumerate(practices, 1):
        st.markdown(f"""
        <div class="staging-practice">
          <strong>{i}. {title}</strong><br/>
          <span style="color:#94a3b8;font-size:0.85rem;">{desc}</span>
        </div>
        """, unsafe_allow_html=True)

    st.divider()
    st.markdown("### 🔴 Live Validation — Try It Yourself")
    st.markdown("Upload a CSV file and the staging validation pipeline will run dynamically right here.")

    val_upload = st.file_uploader("Upload CSV to validate", type=["csv"], key="w5_validate")
    if val_upload:
        try:
            vdf = pd.read_csv(val_upload, on_bad_lines="skip")
            required_cols = {"id", "gender", "masterCategory", "subCategory",
                             "articleType", "baseColour", "season", "year", "usage",
                             "productDisplayName"}
            missing_cols = required_cols - set(vdf.columns)
            null_counts  = {}
            for col in required_cols & set(vdf.columns):
                n = int(vdf[col].isnull().sum())
                if n > 0:
                    null_counts[col] = n
            has_year = "year" in vdf.columns and pd.api.types.is_numeric_dtype(vdf["year"])
            outliers = int((~vdf["year"].between(1990, 2026)).sum()) if has_year else 0
            dup_ids  = int(vdf["id"].duplicated().sum()) if "id" in vdf.columns else 0

            schema_ok  = len(missing_cols) == 0
            null_ok    = len(null_counts) == 0
            outlier_ok = outliers == 0
            dup_ok     = dup_ids == 0
            all_ok     = schema_ok and null_ok and outlier_ok and dup_ok

            st.markdown(f"#### Validation Result: {'✅ PASSED — safe to promote to warehouse' if all_ok else '❌ FAILED — batch quarantined in staging'}")
            r1, r2, r3, r4 = st.columns(4)
            r1.metric("Schema", "✅ OK" if schema_ok else f"❌ Missing {len(missing_cols)} cols")
            r2.metric("Nulls", "✅ OK" if null_ok else f"❌ {sum(null_counts.values())} nulls")
            r3.metric("Outliers", "✅ OK" if outlier_ok else f"❌ {outliers} out-of-range years")
            r4.metric("Duplicates", "✅ OK" if dup_ok else f"❌ {dup_ids} duplicate IDs")
            if missing_cols:
                st.error(f"Missing required columns: `{', '.join(sorted(missing_cols))}`")
            if null_counts:
                st.warning(f"Null violations: {null_counts}")
        except Exception as e:
            st.error(f"Could not validate file: {e}")

with tab2:
    st.markdown("## ♻️ Idempotency")
    st.markdown("Running the pipeline multiple times with identical input produces the exact same result — no duplicate rows.")

    idem = summary["idempotency"]
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("#### Run 1 — First Load")
        st.json(idem["first_run"])
    with c2:
        st.markdown("#### Run 2 — Identical Rerun")
        st.json(idem["second_run_same_data"])

    if idem["idempotent_confirmed"]:
        st.success("✅ **Confirmed Idempotent:** Rerunning the batch added **0 net new rows**. Mechanism: `INSERT ... ON CONFLICT(id) DO UPDATE`.")
    else:
        st.error("❌ Idempotency check failed.")

    st.divider()
    st.markdown("## ⚛️ Atomicity")
    st.markdown("A batch either fully commits or fully rolls back — no partial loads that corrupt warehouse state.")

    atom = summary["atomicity"]
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("#### ✅ Success Case — Valid Batch")
        st.json(atom["success_case"])
        st.success("200 rows attempted → 200 committed cleanly.")
    with c2:
        st.markdown("#### ❌ Failure Case — Bad Row Injected")
        st.json(atom["failure_case"])
        st.error(f"Row 150 failed constraint → Entire transaction rolled back → **{atom['failure_case']['final_row_count']} rows** in table.")

with tab3:
    bf = summary["error_handling_backfill"]
    st.markdown("## 🩹 Dynamic Backfill & Historical Data Fix")
    c1, c2, c3 = st.columns(3)
    c1.metric("Corrupted rows detected", bf["corrupted_rows_detected"])
    c2.metric("Rows backfilled",         bf["rows_backfilled"])
    c3.metric("Remaining bad rows",      bf["remaining_bad_rows"])

    if bf["backfill_successful"]:
        st.success("✅ Backfill fully resolved all corrupted rows without reprocessing the entire dataset.")

    st.divider()
    st.markdown("## 🔴 Dynamic Error Injection — Try It Live")
    st.markdown("Inject a bad row into a test batch to see the pipeline detect and reject it in real-time.")

    if CLEAN_CSV.exists():
        col_a, col_b = st.columns(2)
        with col_a:
            inject_null = st.button("💉 Inject NULL in mandatory field", use_container_width=True)
        with col_b:
            inject_dup  = st.button("💉 Inject Duplicate primary key", use_container_width=True)

        if inject_null or inject_dup:
            df_base = pd.read_csv(CLEAN_CSV, nrows=50)
            df_test = df_base.copy()

            errors = []
            if inject_null:
                df_test.loc[df_test.index[10], "productDisplayName"] = None
                errors.append(("NULL Injection", "Row 10: productDisplayName = NULL"))
            if inject_dup:
                df_test = pd.concat([df_test, df_test.iloc[[0]]], ignore_index=True)
                errors.append(("Duplicate PK", f"Row {len(df_test)-1}: duplicate id={df_test.iloc[0]['id']}"))

            null_violations = int(df_test[["id", "productDisplayName", "masterCategory"]].isnull().sum().sum())
            dup_count       = int(df_test["id"].duplicated().sum())
            all_ok = null_violations == 0 and dup_count == 0

            st.markdown(f"### Pipeline Response: {'✅ PASSED' if all_ok else '🚨 REJECTED — Batch quarantined in staging'}")
            if not all_ok:
                for err_type, err_detail in errors:
                    st.error(f"**{err_type}:** {err_detail}")

    st.divider()
    st.markdown("## 🌬️ Orchestration Architecture (Apache Airflow & Kafka)")

    st.markdown("""
    ```mermaid
    flowchart LR
        W1["📥 Week 1: Collect & Clean"]
        W2["🔄 Week 2: Core ETL"]
        W3["🏗️ Week 3: Schema Design"]
        W4["🌐 Week 4: API Pipeline"]
        W5["🛡️ Week 5: Staging & Validation"]
        AG["⚛️ Atomicity Gate"]
        PC["✅ Pipeline Complete"]

        W1 --> W2 --> W3 --> W4 --> W5 --> AG --> PC
    ```
    """)

    st.markdown("""
    ```mermaid
    flowchart LR
        SRC["📦 Product Catalog Source"]
        PROD["🟢 Kafka Producer (CDC)"]
        TOPIC["📨 Topic: product-cdc-events"]
        CONS["🔵 Kafka Consumer"]
        DB["🗄️ SQLite Warehouse"]

        SRC --> PROD --> TOPIC --> CONS --> DB
    ```
    """)
