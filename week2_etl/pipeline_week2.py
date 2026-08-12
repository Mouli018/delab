"""
Week 2 - Building Core Data Pipeline (ETL)
============================================
i.   Extraction: REST API, RDBMS (SQLite), NoSQL (TinyDB), flat file (CSV)
ii.  Transformation: cleansing, standardizing, joining, aggregating
iii. Loading: full load vs incremental load
iv.  Advanced Extraction: Change Data Capture (CDC) via row hashing
"""
import hashlib
import json
import sqlite3
import time
from pathlib import Path

import pandas as pd
import requests
from tinydb import TinyDB, Query

BASE = Path(__file__).resolve().parent.parent
CLEAN_CSV = BASE / "week1_eda" / "outputs" / "cleaned_data.csv"
OUT = Path(__file__).resolve().parent / "outputs"
OUT.mkdir(exist_ok=True)
SQLITE_PATH = BASE / "data" / "warehouse" / "rdbms_source.db"
NOSQL_PATH = BASE / "data" / "staging" / "nosql_store.json"
WAREHOUSE_DB = BASE / "data" / "warehouse" / "warehouse.db"
API_BASE = "http://127.0.0.1:8000"   # Week 4's FastAPI mock service


# ---------------------------------------------------------------------------
# SETUP: seed an RDBMS source + a NoSQL source from the same catalog so we
# have 3 genuinely different extraction surfaces to demonstrate against.
# ---------------------------------------------------------------------------
def seed_sources(df: pd.DataFrame):
    # RDBMS source (SQLite) - holds "inventory" style records
    conn = sqlite3.connect(SQLITE_PATH)
    inventory = df[["id", "articleType", "baseColour", "usage"]].copy()
    inventory["stock_qty"] = (inventory["id"] % 50) + 1          # synthetic op. field, deterministic from real id
    inventory["updated_at"] = pd.Timestamp.now("UTC").isoformat()
    inventory.to_sql("inventory", conn, if_exists="replace", index=False)
    conn.close()

    # NoSQL source (TinyDB / document store) - holds "customer reviews" style docs
    if NOSQL_PATH.exists():
        NOSQL_PATH.unlink()
    db = TinyDB(NOSQL_PATH)
    sample = df.head(500)  # keep the doc store lightweight
    for _, row in sample.iterrows():
        db.insert({
            "product_id": int(row["id"]),
            "product_name": row["productDisplayName"],
            "rating": float(((row["id"] * 7) % 50) / 10),        # deterministic synthetic 0-5 rating
            "review_count": int(row["id"] % 200),
        })
    db.close()
    print(f"[seed] RDBMS -> {SQLITE_PATH.name} ({len(inventory)} rows), "
          f"NoSQL -> {NOSQL_PATH.name} ({len(sample)} docs)")


# ---------------------------------------------------------------------------
# i. EXTRACTION STRATEGIES
# ---------------------------------------------------------------------------
def extract_flat_file():
    df = pd.read_csv(CLEAN_CSV)
    print(f"[extract:flat_file] {len(df)} rows from cleaned_data.csv")
    return df


def extract_rdbms():
    conn = sqlite3.connect(SQLITE_PATH)
    df = pd.read_sql("SELECT * FROM inventory", conn)
    conn.close()
    print(f"[extract:rdbms] {len(df)} rows from SQLite `inventory` table")
    return df


def extract_nosql():
    db = TinyDB(NOSQL_PATH)
    docs = db.all()
    db.close()
    df = pd.DataFrame(docs)
    print(f"[extract:nosql] {len(df)} documents from TinyDB")
    return df


def extract_rest_api(limit=200):
    """Extract from the Week-4 FastAPI mock service. Falls back gracefully
    (documented, not a silent failure) if the service isn't running, so this
    script remains runnable standalone."""
    try:
        resp = requests.get(f"{API_BASE}/products", params={"limit": limit}, timeout=3)
        resp.raise_for_status()
        df = pd.DataFrame(resp.json())
        print(f"[extract:rest_api] {len(df)} rows from live API {API_BASE}/products")
        return df, "live"
    except Exception as e:
        # graceful degradation, logged not hidden
        df = extract_flat_file().head(limit)
        print(f"[extract:rest_api] API unreachable ({e}); "
              f"used flat-file fallback for demo continuity ({len(df)} rows)")
        return df, "fallback"


# ---------------------------------------------------------------------------
# ii. TRANSFORMATION: cleansing, standardizing, joining, aggregating
# ---------------------------------------------------------------------------
def transform(catalog_df, inventory_df, reviews_df):
    # Standardize column casing/keys before join
    catalog_df = catalog_df.rename(columns={"id": "product_id"})
    inventory_df = inventory_df.rename(columns={"id": "product_id"})

    # Cleansing: strip text columns again post-extraction (defensive)
    for col in ["articleType", "baseColour", "usage"]:
        if col in catalog_df.columns:
            catalog_df[col] = catalog_df[col].astype(str).str.strip().str.title()

    # JOIN: catalog (flat file) + inventory (RDBMS) on product_id
    merged = catalog_df.merge(
        inventory_df[["product_id", "stock_qty", "updated_at"]],
        on="product_id", how="left"
    )

    # JOIN: + reviews (NoSQL) on product_id
    if not reviews_df.empty:
        merged = merged.merge(
            reviews_df[["product_id", "rating", "review_count"]],
            on="product_id", how="left"
        )

    # Standardize missing post-join (products with no inventory/review record)
    merged["stock_qty"] = merged["stock_qty"].fillna(0).astype(int)
    merged["rating"] = merged.get("rating", pd.Series(dtype=float)).fillna(0.0)
    merged["review_count"] = merged.get("review_count", pd.Series(dtype=float)).fillna(0).astype(int)

    # AGGREGATION: category-level rollup (used by dashboard + star schema later)
    agg = merged.groupby("masterCategory").agg(
        product_count=("product_id", "count"),
        avg_stock=("stock_qty", "mean"),
        avg_rating=("rating", "mean"),
        total_reviews=("review_count", "sum"),
    ).reset_index()

    merged.to_csv(OUT / "transformed_merged.csv", index=False)
    agg.to_csv(OUT / "aggregated_by_category.csv", index=False)
    print(f"[transform] merged {merged.shape}, aggregated {agg.shape}")
    return merged, agg


# ---------------------------------------------------------------------------
# iii. LOADING STRATEGIES: full load vs incremental load
# ---------------------------------------------------------------------------
def load_full(df: pd.DataFrame):
    conn = sqlite3.connect(WAREHOUSE_DB)
    t0 = time.time()
    df.to_sql("products_full", conn, if_exists="replace", index=False)
    elapsed = time.time() - t0
    conn.close()
    print(f"[load:full] {len(df)} rows -> products_full ({elapsed:.3f}s)")
    return {"strategy": "full_load", "rows_written": len(df), "seconds": round(elapsed, 4)}


def load_incremental(df: pd.DataFrame, watermark_col="product_id"):
    """Incremental load using a high-watermark: only rows with id greater
    than the last-loaded max id are appended."""
    conn = sqlite3.connect(WAREHOUSE_DB)
    cur = conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS load_watermark
                   (table_name TEXT PRIMARY KEY, last_value INTEGER)""")
    cur.execute("SELECT last_value FROM load_watermark WHERE table_name='products_incr'")
    row = cur.fetchone()
    watermark = row[0] if row else 0

    new_rows = df[df[watermark_col] > watermark]
    t0 = time.time()
    if not new_rows.empty:
        new_rows.to_sql("products_incr", conn, if_exists="append", index=False)
        new_watermark = int(new_rows[watermark_col].max())
        cur.execute("""INSERT INTO load_watermark (table_name, last_value)
                       VALUES ('products_incr', ?)
                       ON CONFLICT(table_name) DO UPDATE SET last_value=excluded.last_value""",
                    (new_watermark,))
        conn.commit()
    elapsed = time.time() - t0
    conn.close()
    print(f"[load:incremental] watermark {watermark} -> {len(new_rows)} new rows appended ({elapsed:.3f}s)")
    return {"strategy": "incremental_load", "prev_watermark": watermark,
            "rows_written": len(new_rows), "seconds": round(elapsed, 4)}


# ---------------------------------------------------------------------------
# iv. CHANGE DATA CAPTURE (CDC) - detect inserts/updates/deletes via row hash
# ---------------------------------------------------------------------------
def _row_hash(row):
    payload = "|".join(str(row[c]) for c in ["articleType", "baseColour", "usage", "stock_qty"])
    return hashlib.md5(payload.encode()).hexdigest()


def run_cdc(current_df: pd.DataFrame):
    snapshot_path = OUT / "cdc_snapshot.json"
    current_df = current_df.copy()
    current_df["row_hash"] = current_df.apply(_row_hash, axis=1)
    current_state = dict(zip(current_df["product_id"], current_df["row_hash"]))

    if snapshot_path.exists():
        prev_state = json.loads(snapshot_path.read_text())
        prev_state = {int(k): v for k, v in prev_state.items()}
    else:
        prev_state = {}

    inserted = [pid for pid in current_state if pid not in prev_state]
    deleted = [pid for pid in prev_state if pid not in current_state]
    updated = [pid for pid in current_state
               if pid in prev_state and prev_state[pid] != current_state[pid]]

    cdc_result = {
        "inserted_count": len(inserted), "inserted_sample": inserted[:5],
        "updated_count": len(updated), "updated_sample": updated[:5],
        "deleted_count": len(deleted), "deleted_sample": deleted[:5],
        "total_tracked": len(current_state),
    }
    with open(OUT / "cdc_result.json", "w") as f:
        json.dump(cdc_result, f, indent=2)
    snapshot_path.write_text(json.dumps(current_state))
    print(f"[cdc] +{len(inserted)} inserts / ~{len(updated)} updates / -{len(deleted)} deletes")
    return cdc_result


if __name__ == "__main__":
    catalog = extract_flat_file()
    seed_sources(catalog)

    inv_df = extract_rdbms()
    rev_df = extract_nosql()
    api_df, api_mode = extract_rest_api()

    merged, agg = transform(catalog, inv_df, rev_df)

    full_stats = load_full(merged)
    incr_stats = load_incremental(merged)
    cdc_stats = run_cdc(merged)

    summary = {
        "extraction": {
            "flat_file_rows": len(catalog),
            "rdbms_rows": len(inv_df),
            "nosql_docs": len(rev_df),
            "rest_api_rows": len(api_df),
            "rest_api_mode": api_mode,
        },
        "transformation": {"merged_shape": list(merged.shape), "agg_categories": len(agg)},
        "loading": {"full_load": full_stats, "incremental_load": incr_stats},
        "cdc": cdc_stats,
    }
    with open(OUT / "week2_summary.json", "w") as f:
        json.dump(summary, f, indent=2, default=str)
    print("Week 2 ETL pipeline complete.")
