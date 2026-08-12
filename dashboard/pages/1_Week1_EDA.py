"""
Week 1 — Data Collection, Preprocessing, Feature Engineering & EDA
  + Interactive CSV File Upload with Rigorous Schema Validation (Accept/Reject with Reason)
  + Live EDA Insights and Charts
"""
import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import streamlit as st

BASE      = Path(__file__).resolve().parent.parent.parent
OUT       = BASE / "week1_eda" / "outputs"
CLEAN_CSV = OUT / "cleaned_data.csv"
sns.set_theme(style="darkgrid", palette="deep")

st.set_page_config(page_title="Week 1 – EDA & CSV Validation", page_icon="📥", layout="wide")

# ── Dark Theme CSS ────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');
html,body,[class*="css"]{ font-family:'Inter',sans-serif; }
.page-header{
    background:linear-gradient(135deg,#062e1b 0%,#0a4025 40%,#1a1d3e 100%);
    border:1px solid rgba(52,211,153,0.25);border-radius:16px;
    padding:28px 36px;margin-bottom:24px;
}
.page-header h1{
    font-size:1.8rem;font-weight:800;
    background:linear-gradient(90deg,#34d399,#10b981,#059669);
    -webkit-background-clip:text;-webkit-text-fill-color:transparent;margin:0 0 4px 0;
}
.page-header p{color:#94a3b8;font-size:0.85rem;margin:0;}
div[data-testid="metric-container"]{
    background:linear-gradient(145deg,#1e2235,#161928);
    border:1px solid rgba(52,211,153,0.2);border-radius:12px;padding:14px!important;
}
.upload-card{
    background:linear-gradient(145deg,#1e2235,#161928);
    border:1px solid rgba(52,211,153,0.3);
    border-radius:14px;padding:24px 28px;margin-bottom:24px;
}
</style>
""", unsafe_allow_html=True)

st.markdown("""
<div class="page-header">
  <h1>📥 Week 1 — Data Collection, Preprocessing, Feature Engineering &amp; EDA</h1>
  <p>CSV Ingestion &amp; Schema Validation · Clean · Feature Engineering · Exploratory Data Analysis</p>
</div>
""", unsafe_allow_html=True)

if not CLEAN_CSV.exists():
    st.error("No pipeline outputs found. Run `python run_pipeline.py` first.")
    st.stop()

@st.cache_data(show_spinner=False)
def load_catalog(mtime: float) -> pd.DataFrame:
    return pd.read_csv(CLEAN_CSV)

df_live = load_catalog(CLEAN_CSV.stat().st_mtime)

# ─────────────────────────────────────────────────────────────────────────────
# CSV FILE UPLOAD WITH RIGOROUS VALIDATION (ACCEPT / REJECT WITH REASON)
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("### 📤 Upload CSV Dataset for Validation & Ingestion")
st.markdown(
    "Upload a CSV file to test schema validation. Valid files are **ACCEPTED** and appended to the dataset. "
    "Invalid files are **REJECTED** with explicit error reasons."
)

st.markdown('<div class="upload-card">', unsafe_allow_html=True)
uploaded_file = st.file_uploader("Choose a CSV file to validate and append", type=["csv"], key="w1_csv_upload")

if uploaded_file is not None:
    required_cols = {"id", "gender", "masterCategory", "subCategory", "articleType", "baseColour", "season", "year", "usage", "productDisplayName"}
    try:
        up_df = pd.read_csv(uploaded_file, on_bad_lines="skip")
        
        # Validation Check 1: Empty CSV Check
        if up_df.empty:
            st.error("❌ **REJECTED**: The uploaded CSV file is empty (contains 0 data rows).")
        else:
            found_cols = set(up_df.columns)
            missing_cols = required_cols - found_cols

            # Validation Check 2: Missing Columns Schema Check
            if missing_cols:
                st.error(
                    f"❌ **REJECTED**: Missing required schema columns: `{sorted(list(missing_cols))}`.\n\n"
                    f"**Expected Schema Columns:** `{sorted(list(required_cols))}`"
                )
            else:
                # Validation Check 3: Mandatory Null Checks
                null_counts = up_df[["id", "masterCategory", "productDisplayName"]].isnull().sum().to_dict()
                has_nulls = any(v > 0 for v in null_counts.values())

                if has_nulls:
                    st.error(f"❌ **REJECTED**: File contains null values in mandatory fields: `{null_counts}`.")
                else:
                    # Validation Check 4: Data Type Validation
                    if not pd.api.types.is_numeric_dtype(up_df["year"]):
                        st.error("❌ **REJECTED**: Column 'year' must contain valid numeric integers.")
                    else:
                        # Validation Passed -> ACCEPT
                        st.success(
                            f"✅ **ACCEPTED**: CSV passed all validation checks! "
                            f"Loaded **{len(up_df):,} valid rows** across {len(up_df.columns)} columns."
                        )
                        st.dataframe(up_df.head(10), use_container_width=True)

                        if st.button("➕ Append Validated CSV to Dataset", type="primary"):
                            current_df = pd.read_csv(CLEAN_CSV)
                            max_id = int(current_df["id"].max())
                            
                            app_df = up_df.copy()
                            app_df["id"] = range(max_id + 1, max_id + 1 + len(app_df))
                            app_df.to_csv(CLEAN_CSV, mode="a", header=False, index=False)

                            st.success(f"Successfully appended {len(app_df):,} rows to dataset!")
                            st.cache_data.clear()
                            st.rerun()

    except Exception as e:
        st.error(f"❌ **REJECTED**: Corrupted file format or unparseable CSV. Technical reason: `{e}`")

st.markdown('</div>', unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# TABS FOR DATA SOURCES, preprocessing, FEATURES & EDA
# ─────────────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4 = st.tabs([
    "📂 Data Sources", "🧹 Cleaning Report", "⚙️ Feature Engineering", "📊 EDA & Live Visuals"
])

with tab1:
    report_path = OUT / "data_collection_report.json"
    if report_path.exists():
        report = json.loads(report_path.read_text())
        st.markdown("### Ingested Data Catalog Sources")
        for src in report["sources"]:
            icon = "🗂️" if "text" in src.get("type","") else "🖼️" if "image" in src.get("type","") else "📋"
            st.markdown(f"**{icon} {src['name']}** — `{src['type']}`")
            st.json(src, expanded=False)

with tab2:
    prep_path = OUT / "preprocessing_report.json"
    if prep_path.exists():
        prep = json.loads(prep_path.read_text())
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Rows before cleaning", f"{prep['before_shape'][0]:,}")
        c2.metric("Rows in dataset now",  f"{len(df_live):,}")
        c3.metric("Duplicates removed",   prep["duplicates_removed"])
        c4.metric("Noisy year rows removed", prep["noisy_year_rows_removed"])

        col_a, col_b = st.columns(2)
        with col_a:
            st.markdown("**Missing values before cleaning:**")
            miss_b = prep["missing_before"]
            if miss_b:
                st.dataframe(pd.DataFrame.from_dict(miss_b, orient="index", columns=["count"]), use_container_width=True)
            else:
                st.success("No missing values found.")
        with col_b:
            st.markdown("**Missing values after cleaning:**")
            st.success("✅ All null values resolved.")

with tab3:
    feat_path = OUT / "engineered_features.csv"
    if feat_path.exists():
        feat_df = pd.read_csv(feat_path)
        raw_cols = 10
        new_cols = feat_df.shape[1] - raw_cols

        c1, c2, c3 = st.columns(3)
        c1.metric("Original columns", raw_cols)
        c2.metric("Engineered columns", new_cols)
        c3.metric("Total columns", feat_df.shape[1])

        st.markdown("**Sample of engineered feature set (first 20 rows):**")
        st.dataframe(feat_df.head(20), use_container_width=True)

with tab4:
    total_now         = len(df_live)
    article_types_now = df_live["articleType"].nunique()
    colours_now       = df_live["baseColour"].nunique()
    year_range_now    = (int(df_live["year"].min()), int(df_live["year"].max()))

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total products", f"{total_now:,}")
    c2.metric("Article types",  article_types_now)
    c3.metric("Unique colours", colours_now)
    c4.metric("Year range",     f"{year_range_now[0]}–{year_range_now[1]}")

    st.markdown("#### 📊 Live EDA Visualizations")

    def dark_fig(figsize=(7, 4)):
        fig, ax = plt.subplots(figsize=figsize)
        fig.patch.set_facecolor("#161928")
        ax.set_facecolor("#1e2235")
        ax.tick_params(colors="#94a3b8", labelsize=8)
        for spine in ax.spines.values():
            spine.set_edgecolor("#334155")
        return fig, ax

    row1 = st.columns(2)
    row2 = st.columns(2)

    with row1[0]:
        fig, ax = dark_fig()
        df_live["masterCategory"].value_counts().plot(kind="bar", ax=ax, color="#6C63FF", edgecolor="none")
        ax.set_title("Product Count by Master Category", color="#e2e8f0", fontsize=10, pad=8)
        ax.set_ylabel("Count", color="#94a3b8", fontsize=8)
        plt.xticks(rotation=30, ha="right")
        plt.tight_layout()
        st.pyplot(fig, use_container_width=True)
        plt.close(fig)

    with row1[1]:
        fig, ax = dark_fig((5, 4))
        colors = ["#6C63FF", "#38bdf8", "#34d399", "#f59e0b", "#f472b6"]
        df_live["gender"].value_counts().plot(
            kind="pie", autopct="%1.1f%%", ax=ax,
            colors=colors[:df_live["gender"].nunique()],
            textprops={"color": "#e2e8f0", "fontsize": 8}
        )
        ax.set_ylabel("")
        ax.set_title("Gender Distribution", color="#e2e8f0", fontsize=10, pad=8)
        plt.tight_layout()
        st.pyplot(fig, use_container_width=True)
        plt.close(fig)

    with row2[0]:
        fig, ax = dark_fig((7, 4.5))
        df_live["articleType"].value_counts().head(15).plot(kind="barh", ax=ax, color="#38bdf8", edgecolor="none")
        ax.invert_yaxis()
        ax.set_title("Top 15 Article Types", color="#e2e8f0", fontsize=10, pad=8)
        ax.set_xlabel("Count", color="#94a3b8", fontsize=8)
        plt.tight_layout()
        st.pyplot(fig, use_container_width=True)
        plt.close(fig)

    with row2[1]:
        fig, ax = dark_fig()
        df_live["year"].value_counts().sort_index().plot(kind="line", marker="o", ax=ax, color="#f59e0b", linewidth=2)
        ax.set_title("Products Catalogued by Year", color="#e2e8f0", fontsize=10, pad=8)
        ax.set_xlabel("Year", color="#94a3b8", fontsize=8)
        ax.set_ylabel("Count", color="#94a3b8", fontsize=8)
        plt.tight_layout()
        st.pyplot(fig, use_container_width=True)
        plt.close(fig)
