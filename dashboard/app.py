"""
Data Engineering Laboratory (22MDCEL10) - Interactive Showcase Dashboard
==========================================================================
Run with:  streamlit run dashboard/app.py
"""
import json
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

BASE = Path(__file__).resolve().parent.parent
CLEAN_CSV = BASE / "week1_eda" / "outputs" / "cleaned_data.csv"

st.set_page_config(
    page_title="Data Engineering Lab — 22MDCEL10",
    page_icon="🧬",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Dark Theme CSS ────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');

html, body, [class*="css"] {
    font-family: 'Inter', sans-serif;
}

.hero-banner {
    background: linear-gradient(135deg, #1a1d3e 0%, #0f2027 40%, #1a1d3e 100%);
    border: 1px solid rgba(108,99,255,0.3);
    border-radius: 20px;
    padding: 36px 44px;
    margin-bottom: 28px;
    position: relative;
}
.hero-title {
    font-size: 2.2rem;
    font-weight: 800;
    background: linear-gradient(90deg, #a78bfa, #6C63FF, #38bdf8);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    margin: 0 0 8px 0;
    line-height: 1.2;
}
.hero-sub {
    color: #94a3b8;
    font-size: 0.92rem;
    font-weight: 400;
    margin: 0;
}

.week-grid {
    display: grid;
    grid-template-columns: repeat(5, 1fr);
    gap: 16px;
    margin: 24px 0;
}
.week-card {
    background: linear-gradient(145deg, #1e2235, #161928);
    border: 1px solid rgba(108,99,255,0.2);
    border-radius: 16px;
    padding: 20px 16px;
    text-align: center;
    position: relative;
}
.week-icon { font-size: 2rem; margin-bottom: 8px; display: block; }
.week-label {
    font-size: 0.75rem;
    font-weight: 700;
    color: #6C63FF;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    margin-bottom: 4px;
}
.week-title { font-size: 0.85rem; font-weight: 600; color: #e2e8f0; line-height: 1.3; }
.week-desc { font-size: 0.72rem; color: #64748b; margin-top: 4px; }

div[data-testid="metric-container"] {
    background: linear-gradient(145deg, #1e2235, #161928);
    border: 1px solid rgba(108,99,255,0.2);
    border-radius: 12px;
    padding: 16px !important;
}

.form-card {
    background: linear-gradient(145deg, #1e2235, #161928);
    border: 1px solid rgba(108,99,255,0.25);
    border-radius: 16px;
    padding: 24px 28px;
    margin-top: 20px;
}
</style>
""", unsafe_allow_html=True)

# ── Hero Banner ───────────────────────────────────────────────────────────────
st.markdown("""
<div class="hero-banner">
  <p class="hero-title">🧬 Data Engineering Laboratory</p>
  <p class="hero-sub">22MDCEL10 &nbsp;·&nbsp; M.Sc Decision &amp; Computing Sciences &nbsp;·&nbsp;
     CIT Coimbatore &nbsp;·&nbsp; AY 2026-27 ODD &nbsp;·&nbsp;
     Dataset: Fashion Product Images (Small) — Kaggle</p>
</div>
""", unsafe_allow_html=True)

st.markdown("""
This dashboard is the **central management portal & live showcase** of a continuous data-engineering pipeline.
You can **add, update, or delete product records** right here on the main page, and the changes propagate
live across all weekly module pipelines.
""")

# ── Week cards ────────────────────────────────────────────────────────────────
st.markdown("""
<div class="week-grid">
  <div class="week-card">
    <span class="week-icon">📥</span>
    <div class="week-label">Week 1</div>
    <div class="week-title">Data Collection &amp; EDA</div>
    <div class="week-desc">CSV Validation · Feature Eng · EDA</div>
  </div>
  <div class="week-card">
    <span class="week-icon">🔄</span>
    <div class="week-label">Week 2</div>
    <div class="week-title">Core ETL Pipeline</div>
    <div class="week-desc">Extract · Transform · Load · CDC</div>
  </div>
  <div class="week-card">
    <span class="week-icon">🏗️</span>
    <div class="week-label">Week 3</div>
    <div class="week-title">Data Architecture</div>
    <div class="week-desc">OLTP · Star Schema · Data Cube</div>
  </div>
  <div class="week-card">
    <span class="week-icon">🌐</span>
    <div class="week-label">Week 4</div>
    <div class="week-title">Batch API Pipeline</div>
    <div class="week-desc">REST API · PySpark · Warehouse</div>
  </div>
  <div class="week-card">
    <span class="week-icon">🛡️</span>
    <div class="week-label">Week 5</div>
    <div class="week-title">Resilient Pipelines</div>
    <div class="week-desc">Staging · Idempotency · Airflow · Kafka</div>
  </div>
</div>
""", unsafe_allow_html=True)

# ── Live headline metrics ─────────────────────────────────────────────────────
def load_json(path):
    p = BASE / path
    return json.loads(p.read_text()) if p.exists() else None

w1 = load_json("week1_eda/outputs/eda_insights.json")
w2 = load_json("week2_etl/outputs/week2_summary.json")
w3 = load_json("week3_schema/outputs/week3_summary.json")
w4 = load_json("week4_pipeline/outputs/week4_summary.json")
w5 = load_json("week5_resilience/outputs/week5_summary.json")

live_product_count = None
if CLEAN_CSV.exists():
    try:
        df_clean = pd.read_csv(CLEAN_CSV)
        live_product_count = len(df_clean)
    except Exception:
        pass

st.subheader("📊 Live Pipeline Headline Metrics")
m1, m2, m3, m4, m5 = st.columns(5)

p_count = f"{live_product_count:,}" if live_product_count else (f"{w1['summary_stats']['total_products']:,}" if w1 else "—")
m1.metric("🛍️ Products (raw catalog)", p_count, "Week 1")
m2.metric("🔗 ETL merged rows", p_count, "Week 2")
m3.metric("⭐ Fact table rows", f"{w3['star_schema']['fact_sales_rows']:,}" if w3 else "—", "Week 3")
m4.metric("🌐 API rows loaded", f"{w4['load_verification']['rows_loaded']:,}" if (w4 and 'load_verification' in w4) else "—", "Week 4")
m5.metric("♻️ Idempotency", "✅ Verified" if w5 and w5['idempotency']['idempotent_confirmed'] else "—", "Week 5")

st.divider()

# ─────────────────────────────────────────────────────────────────────────────
# CENTRAL DATASET MANAGEMENT: ADD, UPDATE, DELETE RECORDS
# ─────────────────────────────────────────────────────────────────────────────
st.subheader("🛠️ Live Dataset Record Management")
st.markdown("Add new product entries, update existing records, or delete products. Changes update `cleaned_data.csv` and sync live to EDA & ETL pipelines.")

@st.cache_data(show_spinner=False)
def get_unique_options(mtime: float) -> dict:
    df = pd.read_csv(CLEAN_CSV)
    return {
        "gender":         sorted(df["gender"].dropna().unique().tolist()),
        "masterCategory": sorted(df["masterCategory"].dropna().unique().tolist()),
        "subCategory":    sorted(df["subCategory"].dropna().unique().tolist()),
        "articleType":    sorted(df["articleType"].dropna().unique().tolist()),
        "baseColour":     sorted(df["baseColour"].dropna().unique().tolist()),
        "season":         sorted(df["season"].dropna().unique().tolist()),
        "usage":          sorted(df["usage"].dropna().unique().tolist()),
        "year_min":       int(df["year"].min()),
        "year_max":       int(df["year"].max()),
    }

mtime = CLEAN_CSV.stat().st_mtime if CLEAN_CSV.exists() else 0
uniq = get_unique_options(mtime) if CLEAN_CSV.exists() else {}

tab_add, tab_upd, tab_del = st.tabs(["➕ Add New Record", "✏️ Update Record", "🗑️ Delete Record"])

# ── Add Record Tab ────────────────────────────────────────────────────────────
with tab_add:
    with st.form("main_add_product_form", clear_on_submit=True):
        st.markdown("#### 🛍️ Add a New Product to Catalog")
        col1, col2, col3 = st.columns(3)
        with col1:
            f_gender = st.selectbox("Gender *", options=uniq.get("gender", ["Men", "Women", "Unisex"]))
            f_master = st.selectbox("Master Category *", options=uniq.get("masterCategory", ["Apparel", "Footwear", "Accessories"]))
            f_sub    = st.selectbox("Sub Category *", options=uniq.get("subCategory", ["Topwear", "Shoes", "Bags"]))

        with col2:
            f_article = st.selectbox("Article Type *", options=uniq.get("articleType", ["Shirts", "Casual Shoes", "Backpacks"]))
            f_colour  = st.selectbox("Base Colour *", options=uniq.get("baseColour", ["Blue", "Black", "Red", "White"]))
            f_season  = st.selectbox("Season *", options=uniq.get("season", ["Summer", "Fall", "Winter", "Spring"]))

        with col3:
            f_year  = st.number_input("Catalog Year *", min_value=1990, max_value=2026, value=2024, step=1)
            f_usage = st.selectbox("Usage *", options=uniq.get("usage", ["Casual", "Formal", "Sports"]))
            f_name  = st.text_input("Product Display Name *", placeholder="e.g. Peter England Men Blue Formal Shirt")

        submitted_add = st.form_submit_button("✅ Add Product Record", type="primary", use_container_width=True)

    if submitted_add:
        if not f_name.strip():
            st.error("❌ Product Display Name is required.")
        else:
            current_df = pd.read_csv(CLEAN_CSV)
            new_id = int(current_df["id"].max()) + 1
            new_row = {
                "id": new_id, "gender": f_gender, "masterCategory": f_master,
                "subCategory": f_sub, "articleType": f_article, "baseColour": f_colour,
                "season": f_season, "year": int(f_year), "usage": f_usage,
                "productDisplayName": f_name.strip(),
            }
            pd.DataFrame([new_row]).to_csv(CLEAN_CSV, mode="a", header=False, index=False)
            st.session_state["last_added_row"] = new_row
            _ts = datetime.now().strftime("%H:%M:%S")
            st.session_state["data_updated_at"] = _ts

            cdc_log = st.session_state.get("cdc_event_log", [])
            cdc_log.append({
                "event_type": "INSERT", "product_id": new_id,
                "changed_fields": 9, "details": f_name.strip()[:60], "detected_at": _ts
            })
            st.session_state["cdc_event_log"] = cdc_log

            st.success(f"✅ Product ID **{new_id}** (*{f_name.strip()}*) added cleanly!")
            st.cache_data.clear()
            st.rerun()

# ── Update Record Tab ─────────────────────────────────────────────────────────
with tab_upd:
    upd_id = st.number_input("Enter Product ID to Update", min_value=1, max_value=999999, value=15970, step=1, key="main_upd_id")
    _upd_df = pd.read_csv(CLEAN_CSV) if CLEAN_CSV.exists() else pd.DataFrame()
    _match  = _upd_df[_upd_df["id"] == int(upd_id)] if not _upd_df.empty else pd.DataFrame()

    if _match.empty:
        st.warning(f"No product found with ID {upd_id}.")
    else:
        _cur = _match.iloc[0].to_dict()
        st.info(f"Editing Product ID `{upd_id}`: **{_cur['productDisplayName']}**")
        with st.form("main_update_form"):
            uc1, uc2, uc3 = st.columns(3)
            with uc1:
                uf_gender = st.selectbox("Gender", uniq.get("gender", []), index=uniq.get("gender", []).index(_cur["gender"]) if _cur["gender"] in uniq.get("gender", []) else 0)
                uf_master = st.selectbox("Master Category", uniq.get("masterCategory", []), index=uniq.get("masterCategory", []).index(_cur["masterCategory"]) if _cur["masterCategory"] in uniq.get("masterCategory", []) else 0)
                uf_sub    = st.selectbox("Sub Category", uniq.get("subCategory", []), index=uniq.get("subCategory", []).index(_cur["subCategory"]) if _cur["subCategory"] in uniq.get("subCategory", []) else 0)
            with uc2:
                uf_article = st.selectbox("Article Type", uniq.get("articleType", []), index=uniq.get("articleType", []).index(_cur["articleType"]) if _cur["articleType"] in uniq.get("articleType", []) else 0)
                uf_colour  = st.selectbox("Base Colour", uniq.get("baseColour", []), index=uniq.get("baseColour", []).index(_cur["baseColour"]) if _cur["baseColour"] in uniq.get("baseColour", []) else 0)
                uf_season  = st.selectbox("Season", uniq.get("season", []), index=uniq.get("season", []).index(_cur["season"]) if _cur["season"] in uniq.get("season", []) else 0)
            with uc3:
                uf_year  = st.number_input("Catalog Year", min_value=1990, max_value=2026, value=int(_cur["year"]), step=1)
                uf_usage = st.selectbox("Usage", uniq.get("usage", []), index=uniq.get("usage", []).index(_cur["usage"]) if _cur["usage"] in uniq.get("usage", []) else 0)
                uf_name  = st.text_input("Product Display Name", value=str(_cur["productDisplayName"]))

            submitted_upd = st.form_submit_button("✏️ Save Updated Product", type="primary", use_container_width=True)

        if submitted_upd:
            if not uf_name.strip():
                st.error("Product Display Name is required.")
            else:
                updated_vals = {
                    "id": int(upd_id), "gender": uf_gender, "masterCategory": uf_master,
                    "subCategory": uf_sub, "articleType": uf_article, "baseColour": uf_colour,
                    "season": uf_season, "year": int(uf_year), "usage": uf_usage,
                    "productDisplayName": uf_name.strip(),
                }
                for col, val in updated_vals.items():
                    _upd_df.loc[_upd_df["id"] == int(upd_id), col] = val
                _upd_df.to_csv(CLEAN_CSV, index=False)

                _ts = datetime.now().strftime("%H:%M:%S")
                cdc_log = st.session_state.get("cdc_event_log", [])
                cdc_log.append({
                    "event_type": "UPDATE", "product_id": int(upd_id),
                    "changed_fields": 1, "details": f"Updated {uf_name.strip()[:50]}", "detected_at": _ts
                })
                st.session_state["cdc_event_log"] = cdc_log
                st.success(f"✅ Product ID {upd_id} updated successfully!")
                st.cache_data.clear()
                st.rerun()

# ── Delete Record Tab ─────────────────────────────────────────────────────────
with tab_del:
    del_id = st.number_input("Enter Product ID to Delete", min_value=1, max_value=999999, value=15970, step=1, key="main_del_id")
    _del_df = pd.read_csv(CLEAN_CSV) if CLEAN_CSV.exists() else pd.DataFrame()
    _d_match = _del_df[_del_df["id"] == int(del_id)] if not _del_df.empty else pd.DataFrame()

    if _d_match.empty:
        st.warning(f"No product found with ID {del_id}.")
    else:
        _d_row = _d_match.iloc[0].to_dict()
        st.error(f"Selected for deletion: **ID {del_id}** — *{_d_row['productDisplayName']}*")
        st.dataframe(_d_match, use_container_width=True)

        with st.form("main_delete_form"):
            confirm_del = st.checkbox(f"Confirm permanent deletion of product ID {del_id}")
            submitted_del = st.form_submit_button("🗑️ Delete Product Record", type="primary", use_container_width=True)

        if submitted_del:
            if not confirm_del:
                st.error("Please check the confirmation box first.")
            else:
                _del_df = _del_df[_del_df["id"] != int(del_id)]
                _del_df.to_csv(CLEAN_CSV, index=False)
                _ts = datetime.now().strftime("%H:%M:%S")

                cdc_log = st.session_state.get("cdc_event_log", [])
                cdc_log.append({
                    "event_type": "DELETE", "product_id": int(del_id),
                    "changed_fields": "-", "details": f"Deleted {_d_row['productDisplayName']}", "detected_at": _ts
                })
                st.session_state["cdc_event_log"] = cdc_log
                st.success(f"✅ Product ID {del_id} permanently deleted.")
                st.cache_data.clear()
                st.rerun()
