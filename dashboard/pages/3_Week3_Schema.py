"""
Week 3 — Data Architecture & Schema Design
  - OLTP vs OLAP
  - Relational (3NF) schema
  - Star Schema
  - Snowflake variant
  - Data Cube (interactive OLAP queries)
"""
import json
from pathlib import Path

import pandas as pd
import streamlit as st

try:
    import duckdb
    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False

BASE    = Path(__file__).resolve().parent.parent.parent
OUT     = BASE / "week3_schema" / "outputs"
OLAP_DB = BASE / "data" / "warehouse" / "olap_star_schema.duckdb"

st.set_page_config(page_title="Week 3 – Schema Design", page_icon="🏗️", layout="wide")

# ── Dark Theme CSS ────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');
html,body,[class*="css"]{ font-family:'Inter',sans-serif; }
.page-header{
    background:linear-gradient(135deg,#301c03 0%,#422405 40%,#1a1d3e 100%);
    border:1px solid rgba(245,158,11,0.25);border-radius:16px;
    padding:28px 36px;margin-bottom:24px;
}
.page-header h1{
    font-size:1.8rem;font-weight:800;
    background:linear-gradient(90deg,#fbbf24,#f59e0b,#d97706);
    -webkit-background-clip:text;-webkit-text-fill-color:transparent;margin:0 0 4px 0;
}
.page-header p{color:#94a3b8;font-size:0.85rem;margin:0;}
div[data-testid="metric-container"]{
    background:linear-gradient(145deg,#1e2235,#161928);
    border:1px solid rgba(245,158,11,0.2);border-radius:12px;padding:14px!important;
}
</style>
""", unsafe_allow_html=True)

st.markdown("""
<div class="page-header">
  <h1>🏗️ Week 3 — Data Architecture &amp; Schema Design</h1>
  <p>OLTP vs OLAP · Relational 3NF · Star Schema · Snowflake · Data Cube</p>
</div>
""", unsafe_allow_html=True)

if not (OUT / "week3_summary.json").exists():
    st.error("No pipeline outputs found. Run `python run_pipeline.py` first.")
    st.stop()

summary   = json.loads((OUT / "week3_summary.json").read_text())
oltp_olap = json.loads((OUT / "oltp_vs_olap.json").read_text())

tab1, tab2, tab3, tab4 = st.tabs([
    "⚖️ OLTP vs OLAP", "🧩 Relational (3NF)", "⭐ Star & Snowflake Schema", "🧊 Data Cube"
])

with tab1:
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("### 🔵 OLTP — Online Transaction Processing")
        st.markdown(f"*{oltp_olap['OLTP']['purpose']}*")
        st.markdown("**Characteristics:**")
        for x in oltp_olap["OLTP"]["characteristics"]:
            st.markdown(f"- {x}")
        st.markdown("**Example tables:** " + ", ".join(f"`{t}`" for t in oltp_olap["OLTP"]["example_tables"]))

    with c2:
        st.markdown("### 🟠 OLAP — Online Analytical Processing")
        st.markdown(f"*{oltp_olap['OLAP']['purpose']}*")
        st.markdown("**Characteristics:**")
        for x in oltp_olap["OLAP"]["characteristics"]:
            st.markdown(f"- {x}")
        st.markdown("**Example tables:** " + ", ".join(f"`{t}`" for t in oltp_olap["OLAP"]["example_tables"]))

    st.divider()
    st.markdown("### Comparison Table")
    st.markdown("""
    | Property | OLTP | OLAP |
    |---|---|---|
    | Primary goal | Record transactions | Analyse patterns |
    | Query type | Short R/W (INSERT, UPDATE) | Long-running aggregations |
    | Schema | Normalised (3NF) | Denormalised (star/snowflake) |
    | Data volume | Current / operational | Historical (years) |
    | Users | Application servers, clerks | Analysts, BI tools |
    | Update frequency | Real-time, continuous | Batch (daily/weekly ETL) |
    | Example DB | SQLite OLTP (`oltp_relational.db`) | DuckDB OLAP (`olap_star_schema.duckdb`) |
    """)

with tab2:
    counts = summary["oltp_row_counts"]
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Products",    f"{counts['products']:,}")
    c2.metric("Customers",   f"{counts['customers']:,}")
    c3.metric("Orders",      f"{counts['orders']:,}")
    c4.metric("Order Items", f"{counts['order_items']:,}")

    st.markdown("### 3NF Relational Schema — Fashion E-Commerce")
    st.markdown("""
    ```
    master_categories(category_id PK, category_name)
          │
    sub_categories(sub_category_id PK, sub_category_name, category_id FK)
          │
    article_types(article_type_id PK, article_type_name, sub_category_id FK)
          │
    products(product_id PK, product_name, gender, master_category_id FK,
             sub_category_id FK, article_type_id FK, base_colour, season,
             usage, catalog_year)
          │
    order_items(item_id PK, order_id FK, product_id FK, quantity, unit_price)
          │
    orders(order_id PK, customer_id FK, store_id FK, order_date, total_amount)
          │                              │
    customers(customer_id PK,...)  stores(store_id PK, store_name, region, city)
    ```
    """)

with tab3:
    star = summary["star_schema"]
    c1, c2 = st.columns(2)
    c1.metric("fact_sales rows",    f"{star['fact_sales_rows']:,}")
    c2.metric("dim_product rows",   f"{star['dim_product_rows']:,}")

    left, right = st.columns(2)

    with left:
        st.markdown("### ⭐ Star Schema")
        st.markdown("A **fact table** at the center surrounded directly by flat **dimension tables** — fastest for BI queries.")
        st.markdown("""
```mermaid
graph LR
    FS["🟦 fact_sales\n──────────────\nsale_id PK\nproduct_sk FK\ncustomer_sk FK\ntime_sk FK\nstore_sk FK\nquantity · unit_price\nrevenue"]

    DP["🟩 dim_product\n──────────────\nproduct_sk PK\nproduct_name · gender\nmaster_category\nsub_category\narticle_type · season"]

    DC["🟩 dim_customer\n──────────────\ncustomer_sk PK\ncustomer_name\ncity · region\ngender · age_group"]

    DT["🟩 dim_time\n──────────────\ntime_sk PK\nsale_date\nday · month\nquarter · year"]

    DST["🟩 dim_store\n──────────────\nstore_sk PK\nstore_name\nregion · city"]

    FS -->|product_sk| DP
    FS -->|customer_sk| DC
    FS -->|time_sk| DT
    FS -->|store_sk| DST
```
        """)

    with right:
        st.markdown("### ❄️ Snowflake Schema Variant")
        st.markdown("Dimension tables are **normalised further** — sub-dimensions split out repeating data into child tables.")
        st.markdown("""
```mermaid
graph TD
    FS["🟦 fact_sales\n──────────────\nsale_id PK\nproduct_sk FK\nquantity · unit_price\nrevenue"]

    SDP["🟧 snowflake_dim_product\n──────────────────────\nproduct_sk PK\nproduct_id · product_name\ngender · base_colour · season\narticle_type_id FK\ncategory_id FK"]

    SDAT["🟣 snowflake_dim_article_type\n─────────────────────────────\narticle_type_id PK\narticle_type_name"]

    SDC["🟣 snowflake_dim_category\n──────────────────────────\ncategory_id PK\nmaster_category\nsub_category"]

    FS -->|"hop 1: product_sk"| SDP
    SDP -->|"hop 2: article_type_id"| SDAT
    SDP -->|"hop 2: category_id"| SDC
```
        """)

        st.markdown("""
---
**Star vs Snowflake — Key Difference:**

| Feature | ⭐ Star | ❄️ Snowflake |
|---|---|---|
| Dimension joins | 1 hop (flat) | 2+ hops (normalised) |
| Query speed | ⚡ Faster | 🐢 Slightly slower |
| Storage | More redundancy | Less redundancy |
| BI tool fit | Excellent | Good |
        """)

    st.divider()
    st.markdown("### 📋 Real Sample Rows from Each Dimension (DuckDB OLAP)")

    if DUCKDB_AVAILABLE and OLAP_DB.exists():
        try:
            con = duckdb.connect(str(OLAP_DB), read_only=True)
            tables = {
                "fact_sales":    ("fact_sales",    10),
                "dim_product":   ("dim_product",    5),
                "dim_customer":  ("dim_customer",   5),
                "dim_time":      ("dim_time",       8),
                "dim_store":     ("dim_store",      5),
            }
            dim_choice = st.selectbox("Choose a table to preview", list(tables.keys()))
            tbl_name, n = tables[dim_choice]
            df_sample = con.execute(f"SELECT * FROM {tbl_name} LIMIT {n}").df()
            st.dataframe(df_sample, use_container_width=True)
            con.close()
        except Exception as e:
            st.warning(f"Could not query DuckDB: {e}")

with tab4:
    st.markdown("### 🧊 Analytical (OLAP) Queries — Data Cube")

    query_files = {
        "Revenue by category & month":  "cube_revenue_by_category_season.csv",
        "Top-selling article types":    "cube_top_selling_article_types.csv",
        "Revenue by store region":      "cube_revenue_by_store_region.csv",
        "Quarterly revenue trend":      "cube_quarterly_trend.csv",
        "Gender × category cube":       "cube_gender_category_cube.csv",
    }
    choice = st.selectbox("Choose an analytical query", list(query_files.keys()))
    df = pd.read_csv(OUT / query_files[choice])

    col_l, col_r = st.columns([1.5, 1])
    with col_l:
        st.dataframe(df, use_container_width=True)
    with col_r:
        if choice == "Top-selling article types":
            st.bar_chart(df.set_index("article_type_name")["revenue"])
        elif choice == "Revenue by store region":
            st.bar_chart(df.set_index("region")["revenue"])

    st.divider()
    st.markdown("### Full Multidimensional Cube (`GROUP BY CUBE`)")
    cube_df = pd.read_csv(OUT / "cube_full_grouping_sets.csv")
    st.dataframe(cube_df.head(30), use_container_width=True)

st.divider()
st.markdown("### ⚡ Live SQL Query Console (OLAP DuckDB & OLTP SQLite)")
st.markdown("Run custom SQL queries live against the Week 3 databases:")

db_choice = st.radio(
    "Select Target Database",
    ["DuckDB OLAP Star Schema (olap_star_schema.duckdb)", "SQLite OLTP 3NF Database (oltp_relational.db)"],
    horizontal=True
)

if "DuckDB" in db_choice:
    if DUCKDB_AVAILABLE and OLAP_DB.exists():
        sql_input = st.text_area(
            "DuckDB SQL Query (Tables: fact_sales, dim_product, dim_customer, dim_time, dim_store)",
            value="""SELECT p.category_name, s.region, SUM(f.revenue) as total_revenue
FROM fact_sales f
JOIN dim_product p ON f.product_id = p.product_id
JOIN dim_store s ON f.store_id = s.store_id
GROUP BY p.category_name, s.region
ORDER BY total_revenue DESC
LIMIT 10;""",
            height=130
        )
        if st.button("▶️ Run DuckDB Query", type="primary"):
            try:
                con = duckdb.connect(str(OLAP_DB), read_only=True)
                res_df = con.execute(sql_input).df()
                con.close()
                st.success(f"Returned {len(res_df):,} rows")
                st.dataframe(res_df, use_container_width=True)
            except Exception as e:
                st.error(f"DuckDB Error: {e}")
    else:
        st.info("DuckDB database file not found.")
else:
    oltp_db = BASE / "data" / "warehouse" / "oltp_relational.db"
    if not oltp_db.exists():
        oltp_db = OUT / "oltp_relational.db"
    if oltp_db.exists():
        import sqlite3
        sql_input = st.text_area(
            "SQLite 3NF SQL Query (Tables: products, orders, order_items, customers, stores, master_categories)",
            value="""SELECT c.category_name, COUNT(p.product_id) as product_count
FROM products p
JOIN master_categories c ON p.master_category_id = c.category_id
GROUP BY c.category_name;""",
            height=120
        )
        if st.button("▶️ Run SQLite Query", type="primary"):
            try:
                conn = sqlite3.connect(oltp_db)
                res_df = pd.read_sql_query(sql_input, conn)
                conn.close()
                st.success(f"Returned {len(res_df):,} rows")
                st.dataframe(res_df, use_container_width=True)
            except Exception as e:
                st.error(f"SQLite Error: {e}")
    else:
        st.info("OLTP SQLite database file not found.")
