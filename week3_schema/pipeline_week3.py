"""
Week 3 - Data Architecture & Schema Design
=============================================
Business use case: Fashion e-commerce sales & inventory analytics
(built on the same catalog dataset, so Weeks 1-5 form one continuous pipeline)

i.   OLTP vs OLAP components identified for this domain
ii.  Relational (3NF, OLTP) schema
iii. Star schema (OLAP, dimensional model) + Snowflake variant
iv.  Data cube (DuckDB) for multidimensional analysis + analytical queries
"""
import json
import sqlite3
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

BASE = Path(__file__).resolve().parent.parent
MERGED_CSV = BASE / "week2_etl" / "outputs" / "transformed_merged.csv"
OUT = Path(__file__).resolve().parent / "outputs"
OUT.mkdir(exist_ok=True)
OLTP_DB = BASE / "data" / "warehouse" / "oltp_relational.db"
OLAP_DB = BASE / "data" / "warehouse" / "olap_star_schema.duckdb"


# ---------------------------------------------------------------------------
# i. OLTP vs OLAP COMPONENT IDENTIFICATION (documented as structured JSON,
#    used directly by the dashboard)
# ---------------------------------------------------------------------------
OLTP_VS_OLAP = {
    "OLTP": {
        "purpose": "Day-to-day transactional operations (order placement, "
                   "inventory updates, catalog edits)",
        "characteristics": ["High write volume", "Row-level operations",
                             "Normalized (3NF) to avoid update anomalies",
                             "Small, fast, indexed queries"],
        "example_tables": ["products", "customers", "orders", "order_items", "inventory"],
    },
    "OLAP": {
        "purpose": "Analytical reporting (sales trend by category/season, "
                   "stock health, rating analysis)",
        "characteristics": ["High read volume, batch-loaded", "Denormalized "
                             "(star/snowflake) for query speed",
                             "Aggregations over large historical windows",
                             "Optimized for GROUP BY / rollups, not single-row lookups"],
        "example_tables": ["fact_sales", "dim_product", "dim_customer",
                            "dim_time", "dim_store"],
    },
}


def document_oltp_olap():
    with open(OUT / "oltp_vs_olap.json", "w") as f:
        json.dump(OLTP_VS_OLAP, f, indent=2)
    print("[schema] OLTP vs OLAP components documented -> oltp_vs_olap.json")


# ---------------------------------------------------------------------------
# ii. RELATIONAL SCHEMA (3NF, OLTP)
# ---------------------------------------------------------------------------
RELATIONAL_DDL = """
CREATE TABLE IF NOT EXISTS customers (
    customer_id     INTEGER PRIMARY KEY,
    customer_name   TEXT NOT NULL,
    gender          TEXT,
    signup_date     TEXT
);

CREATE TABLE IF NOT EXISTS products (
    product_id          INTEGER PRIMARY KEY,
    product_name         TEXT NOT NULL,
    gender               TEXT,
    master_category_id   INTEGER,
    sub_category_id      INTEGER,
    article_type_id      INTEGER,
    base_colour          TEXT,
    season                TEXT,
    usage                 TEXT,
    catalog_year          INTEGER,
    FOREIGN KEY (master_category_id) REFERENCES master_categories(category_id),
    FOREIGN KEY (sub_category_id) REFERENCES sub_categories(sub_category_id),
    FOREIGN KEY (article_type_id) REFERENCES article_types(article_type_id)
);

CREATE TABLE IF NOT EXISTS master_categories (
    category_id     INTEGER PRIMARY KEY,
    category_name   TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS sub_categories (
    sub_category_id     INTEGER PRIMARY KEY,
    sub_category_name    TEXT,
    category_id           INTEGER,
    FOREIGN KEY (category_id) REFERENCES master_categories(category_id)
);

CREATE TABLE IF NOT EXISTS article_types (
    article_type_id     INTEGER PRIMARY KEY,
    article_type_name    TEXT
);

CREATE TABLE IF NOT EXISTS stores (
    store_id     INTEGER PRIMARY KEY,
    store_name   TEXT,
    region       TEXT
);

CREATE TABLE IF NOT EXISTS orders (
    order_id      INTEGER PRIMARY KEY,
    customer_id   INTEGER,
    store_id      INTEGER,
    order_date    TEXT,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY (store_id) REFERENCES stores(store_id)
);

CREATE TABLE IF NOT EXISTS order_items (
    order_item_id   INTEGER PRIMARY KEY,
    order_id        INTEGER,
    product_id      INTEGER,
    quantity        INTEGER,
    unit_price      REAL,
    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);
"""


def build_relational_schema(df: pd.DataFrame):
    conn = sqlite3.connect(OLTP_DB)
    conn.executescript(RELATIONAL_DDL)

    master_cats = pd.DataFrame({"category_name": df["masterCategory"].unique()})
    master_cats.insert(0, "category_id", range(1, len(master_cats) + 1))
    master_cats.to_sql("master_categories", conn, if_exists="replace", index=False)
    cat_map = dict(zip(master_cats["category_name"], master_cats["category_id"]))

    sub_cats = df[["subCategory", "masterCategory"]].drop_duplicates().reset_index(drop=True)
    sub_cats.insert(0, "sub_category_id", range(1, len(sub_cats) + 1))
    sub_cats["category_id"] = sub_cats["masterCategory"].map(cat_map)
    sub_cats = sub_cats.rename(columns={"subCategory": "sub_category_name"})[
        ["sub_category_id", "sub_category_name", "category_id"]]
    sub_cats.to_sql("sub_categories", conn, if_exists="replace", index=False)

    article_types = pd.DataFrame({"article_type_name": df["articleType"].unique()})
    article_types.insert(0, "article_type_id", range(1, len(article_types) + 1))
    article_types.to_sql("article_types", conn, if_exists="replace", index=False)
    art_map = dict(zip(article_types["article_type_name"], article_types["article_type_id"]))

    sub_map = dict(zip(sub_cats["sub_category_name"], sub_cats["sub_category_id"]))
    products = df[["product_id", "productDisplayName", "gender", "masterCategory",
                   "subCategory", "articleType", "baseColour", "season", "usage"]].copy()
    products["master_category_id"] = products["masterCategory"].map(cat_map)
    products["sub_category_id"] = products["subCategory"].map(sub_map)
    products["article_type_id"] = products["articleType"].map(art_map)
    products["catalog_year"] = 2020
    products = products.rename(columns={"productDisplayName": "product_name",
                                         "baseColour": "base_colour"})[
        ["product_id", "product_name", "gender", "master_category_id",
         "sub_category_id", "article_type_id", "base_colour", "season", "usage", "catalog_year"]
    ]
    products.to_sql("products", conn, if_exists="replace", index=False)

    stores = pd.DataFrame({
        "store_id": range(1, 6),
        "store_name": [f"Store {i}" for i in range(1, 6)],
        "region": ["North", "South", "East", "West", "Central"],
    })
    stores.to_sql("stores", conn, if_exists="replace", index=False)

    rng = np.random.default_rng(42)
    n_customers = 300
    customers = pd.DataFrame({
        "customer_id": range(1, n_customers + 1),
        "customer_name": [f"Customer_{i}" for i in range(1, n_customers + 1)],
        "gender": rng.choice(["Men", "Women", "Unisex"], size=n_customers),
        "signup_date": pd.date_range("2023-01-01", periods=n_customers, freq="D").strftime("%Y-%m-%d"),
    })
    customers.to_sql("customers", conn, if_exists="replace", index=False)

    n_orders = 2000
    orders = pd.DataFrame({
        "order_id": range(1, n_orders + 1),
        "customer_id": rng.integers(1, n_customers + 1, n_orders),
        "store_id": rng.integers(1, 6, n_orders),
        "order_date": pd.to_datetime(
            rng.integers(pd.Timestamp("2024-01-01").value // 10**9,
                         pd.Timestamp("2025-12-31").value // 10**9, n_orders), unit="s"
        ).strftime("%Y-%m-%d"),
    })
    orders.to_sql("orders", conn, if_exists="replace", index=False)

    sampled_products = products["product_id"].sample(n=min(20000, len(products)),
                                                       random_state=42, replace=True).values
    order_items = pd.DataFrame({
        "order_item_id": range(1, len(sampled_products) + 1),
        "order_id": rng.integers(1, n_orders + 1, len(sampled_products)),
        "product_id": sampled_products,
        "quantity": rng.integers(1, 4, len(sampled_products)),
        "unit_price": rng.uniform(299, 4999, len(sampled_products)).round(2),
    })
    order_items.to_sql("order_items", conn, if_exists="replace", index=False)

    conn.commit()
    conn.close()
    print(f"[schema] Relational (3NF) OLTP schema built -> {OLTP_DB.name} "
          f"({len(products)} products, {n_orders} orders, {len(order_items)} order_items)")
    return {"products": len(products), "customers": n_customers,
            "orders": n_orders, "order_items": len(order_items)}


# ---------------------------------------------------------------------------
# iii. STAR SCHEMA (+ SNOWFLAKE VARIANT) - dimensional model
# ---------------------------------------------------------------------------
def build_star_schema():
    oltp = sqlite3.connect(OLTP_DB)
    products = pd.read_sql("SELECT * FROM products", oltp)
    categories = pd.read_sql("SELECT * FROM master_categories", oltp)
    subcats = pd.read_sql("SELECT * FROM sub_categories", oltp)
    article_types = pd.read_sql("SELECT * FROM article_types", oltp)
    customers = pd.read_sql("SELECT * FROM customers", oltp)
    orders = pd.read_sql("SELECT * FROM orders", oltp)
    order_items = pd.read_sql("SELECT * FROM order_items", oltp)
    stores = pd.read_sql("SELECT * FROM stores", oltp)
    oltp.close()

    # dim_product : STAR variant - fully denormalized (category/subcategory/
    # articleType flattened into one dimension row per product)
    dim_product = products.merge(categories, left_on="master_category_id", right_on="category_id") \
                            .merge(subcats, on="sub_category_id", suffixes=("", "_sc")) \
                            .merge(article_types, on="article_type_id")
    dim_product = dim_product[["product_id", "product_name", "gender",
                                "category_name", "sub_category_name",
                                "article_type_name", "base_colour", "season", "usage"]]

    dim_customer = customers.copy()
    dim_store = stores.copy()

    dim_time = orders[["order_date"]].drop_duplicates().reset_index(drop=True)
    dim_time["date"] = pd.to_datetime(dim_time["order_date"])
    dim_time["time_id"] = range(1, len(dim_time) + 1)
    dim_time["year"] = dim_time["date"].dt.year
    dim_time["quarter"] = dim_time["date"].dt.quarter
    dim_time["month"] = dim_time["date"].dt.month
    dim_time["day_of_week"] = dim_time["date"].dt.day_name()
    dim_time = dim_time[["time_id", "order_date", "year", "quarter", "month", "day_of_week"]]

    fact_sales = order_items.merge(orders, on="order_id")
    fact_sales = fact_sales.merge(dim_time[["time_id", "order_date"]], on="order_date")
    fact_sales["revenue"] = fact_sales["quantity"] * fact_sales["unit_price"]
    fact_sales = fact_sales[["order_item_id", "order_id", "product_id", "customer_id",
                              "store_id", "time_id", "quantity", "unit_price", "revenue"]]

    global_duckdb_conn = None
    try:
        if OLAP_DB.exists():
            try:
                OLAP_DB.unlink()
            except Exception:
                pass
        olap = duckdb.connect(str(OLAP_DB))
        active_db_path = OLAP_DB
    except Exception:
        fallback_db = OLAP_DB.parent / "olap_star_schema_active.duckdb"
        if fallback_db.exists():
            try:
                fallback_db.unlink()
            except Exception:
                pass
        olap = duckdb.connect(str(fallback_db))
        active_db_path = fallback_db

    olap.execute("CREATE OR REPLACE TABLE dim_product AS SELECT * FROM dim_product")
    olap.execute("CREATE OR REPLACE TABLE dim_customer AS SELECT * FROM dim_customer")
    olap.execute("CREATE OR REPLACE TABLE dim_store AS SELECT * FROM dim_store")
    olap.execute("CREATE OR REPLACE TABLE dim_time AS SELECT * FROM dim_time")
    olap.execute("CREATE OR REPLACE TABLE fact_sales AS SELECT * FROM fact_sales")

    # SNOWFLAKE variant : normalize dim_product further into dim_category/
    # dim_subcategory/dim_article_type (documented + created for comparison)
    olap.execute("CREATE OR REPLACE TABLE snowflake_dim_category AS SELECT * FROM categories")
    olap.execute("CREATE OR REPLACE TABLE snowflake_dim_subcategory AS SELECT * FROM subcats")
    olap.execute("CREATE OR REPLACE TABLE snowflake_dim_article_type AS SELECT * FROM article_types")
    olap.execute("""CREATE OR REPLACE TABLE snowflake_dim_product AS
                     SELECT product_id, product_name, gender, base_colour, season, usage,
                            master_category_id, sub_category_id, article_type_id
                     FROM products""")
    olap.close()

    print(f"[schema] Star schema built -> {active_db_path.name} "
          f"(fact_sales={len(fact_sales)}, dim_product={len(dim_product)}, "
          f"dim_time={len(dim_time)}, dim_customer={len(dim_customer)}, dim_store={len(dim_store)})")
    return {"fact_sales_rows": len(fact_sales), "dim_product_rows": len(dim_product)}


# ---------------------------------------------------------------------------
# iv. DATA CUBE + ANALYTICAL (OLAP) QUERIES
# ---------------------------------------------------------------------------
ANALYTICAL_QUERIES = {
    "revenue_by_category_season": """
        SELECT dp.category_name, dt.month, ROUND(SUM(fs.revenue),2) AS revenue
        FROM fact_sales fs
        JOIN dim_product dp ON fs.product_id = dp.product_id
        JOIN dim_time dt ON fs.time_id = dt.time_id
        GROUP BY dp.category_name, dt.month
        ORDER BY revenue DESC
    """,
    "top_selling_article_types": """
        SELECT dp.article_type_name, SUM(fs.quantity) AS units_sold,
               ROUND(SUM(fs.revenue),2) AS revenue
        FROM fact_sales fs JOIN dim_product dp ON fs.product_id = dp.product_id
        GROUP BY dp.article_type_name
        ORDER BY revenue DESC LIMIT 10
    """,
    "revenue_by_store_region": """
        SELECT ds.region, ROUND(SUM(fs.revenue),2) AS revenue, COUNT(*) AS transactions
        FROM fact_sales fs JOIN dim_store ds ON fs.store_id = ds.store_id
        GROUP BY ds.region ORDER BY revenue DESC
    """,
    "quarterly_trend": """
        SELECT dt.year, dt.quarter, ROUND(SUM(fs.revenue),2) AS revenue
        FROM fact_sales fs JOIN dim_time dt ON fs.time_id = dt.time_id
        GROUP BY dt.year, dt.quarter ORDER BY dt.year, dt.quarter
    """,
    "gender_category_cube": """
        SELECT dp.gender, dp.category_name, COUNT(*) AS txns, ROUND(SUM(fs.revenue),2) AS revenue
        FROM fact_sales fs JOIN dim_product dp ON fs.product_id = dp.product_id
        GROUP BY dp.gender, dp.category_name
        ORDER BY dp.gender, revenue DESC
    """,
}


def run_data_cube_queries():
    target_db = OLAP_DB if OLAP_DB.exists() else OLAP_DB.parent / "olap_star_schema_active.duckdb"
    try:
        olap = duckdb.connect(str(target_db), read_only=True)
    except Exception:
        olap = duckdb.connect(str(target_db))
    results = {}
    for name, q in ANALYTICAL_QUERIES.items():
        res_df = olap.execute(q).fetchdf()
        res_df.to_csv(OUT / f"cube_{name}.csv", index=False)
        results[name] = {"rows": len(res_df), "preview": res_df.head(5).to_dict(orient="records")}

    # A true multidimensional CUBE using DuckDB's GROUPING SETS
    cube_query = """
        SELECT dp.category_name, ds.region, dt.quarter,
               ROUND(SUM(fs.revenue),2) AS revenue, COUNT(*) AS txns
        FROM fact_sales fs
        JOIN dim_product dp ON fs.product_id = dp.product_id
        JOIN dim_store ds ON fs.store_id = ds.store_id
        JOIN dim_time dt ON fs.time_id = dt.time_id
        GROUP BY CUBE (dp.category_name, ds.region, dt.quarter)
        ORDER BY revenue DESC NULLS LAST
    """
    cube_df = olap.execute(cube_query).fetchdf()
    cube_df.to_csv(OUT / "cube_full_grouping_sets.csv", index=False)
    olap.close()

    with open(OUT / "analytical_query_results.json", "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"[cube] {len(ANALYTICAL_QUERIES)} analytical queries + full CUBE "
          f"({len(cube_df)} rows) -> outputs/")
    return results


if __name__ == "__main__":
    document_oltp_olap()
    merged = pd.read_csv(MERGED_CSV)
    counts = build_relational_schema(merged)
    star_counts = build_star_schema()
    cube_results = run_data_cube_queries()

    summary = {"oltp_row_counts": counts, "star_schema": star_counts,
               "analytical_queries_run": list(ANALYTICAL_QUERIES.keys())}
    with open(OUT / "week3_summary.json", "w") as f:
        json.dump(summary, f, indent=2)
    print("Week 3 schema design pipeline complete.")
