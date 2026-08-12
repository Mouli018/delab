"""
Exports the Week 3 star-schema tables (with full real data) as CSVs that
Power BI's "Get Data > Folder/Text-CSV" connector can import directly.
Also drops a copy of the OLTP relational tables for reference.

Run after week3_schema/pipeline_week3.py has produced data/warehouse/olap_star_schema.duckdb
"""
import sqlite3
from pathlib import Path

import duckdb
import pandas as pd

BASE = Path(__file__).resolve().parent.parent
OLAP_DB = BASE / "data" / "warehouse" / "olap_star_schema.duckdb"
OLTP_DB = BASE / "data" / "warehouse" / "oltp_relational.db"
OUT = Path(__file__).resolve().parent / "data"
OUT.mkdir(exist_ok=True)

STAR_TABLES = ["fact_sales", "dim_product", "dim_customer", "dim_store", "dim_time"]
SNOWFLAKE_TABLES = ["snowflake_dim_category", "snowflake_dim_subcategory",
                     "snowflake_dim_article_type", "snowflake_dim_product"]

if __name__ == "__main__":
    target_olap = OLAP_DB if OLAP_DB.exists() else OLAP_DB.parent / "olap_star_schema_active.duckdb"
    olap = duckdb.connect(str(target_olap), read_only=True)
    for t in STAR_TABLES + SNOWFLAKE_TABLES:
        df = olap.execute(f"SELECT * FROM {t}").fetchdf()
        df.to_csv(OUT / f"{t}.csv", index=False)
        print(f"[export] {t}: {len(df):,} rows -> powerbi/data/{t}.csv")
    olap.close()

    oltp = sqlite3.connect(OLTP_DB)
    for t in ["products", "master_categories", "sub_categories", "article_types",
              "customers", "stores", "orders", "order_items"]:
        df = pd.read_sql(f"SELECT * FROM {t}", oltp)
        df.to_csv(OUT / f"oltp_{t}.csv", index=False)
        print(f"[export] oltp_{t}: {len(df):,} rows -> powerbi/data/oltp_{t}.csv")
    oltp.close()
    print("Power BI data export complete.")
