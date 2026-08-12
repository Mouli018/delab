"""
Week 5 - Designing Resilient, Production-Ready Pipelines
=============================================================
i.   Staging & validation: secure staging area + data quality checks
ii.  Idempotency: safe re-runs without duplication/side effects
iii. Atomicity: all-or-nothing transactional loads
iv.  Error handling: backfill + replay strategies for historical fixes
"""
import json
import sqlite3
import uuid
from pathlib import Path

import pandas as pd

BASE = Path(__file__).resolve().parent.parent
CLEAN_CSV = BASE / "week1_eda" / "outputs" / "cleaned_data.csv"
OUT = Path(__file__).resolve().parent / "outputs"
OUT.mkdir(exist_ok=True)
STAGING_DIR = BASE / "data" / "staging"
STAGING_DIR.mkdir(exist_ok=True)
RESILIENT_DB = BASE / "data" / "warehouse" / "resilient_warehouse.db"


# ---------------------------------------------------------------------------
# i. STAGING AREA + DATA QUALITY VALIDATION
# ---------------------------------------------------------------------------
class ValidationError(Exception):
    pass


def stage_and_validate(df: pd.DataFrame, batch_id: str):
    """Writes to an isolated staging file first (never touches the warehouse
    directly), then runs schema/null/outlier checks. Only a batch that
    passes ALL checks is promoted."""
    staging_path = STAGING_DIR / f"batch_{batch_id}.parquet"
    df.to_parquet(staging_path, index=False)

    checks = {}

    # Schema validation
    required_cols = {"id", "gender", "masterCategory", "subCategory",
                      "articleType", "baseColour", "season", "year", "usage",
                      "productDisplayName"}
    missing_cols = required_cols - set(df.columns)
    checks["schema_valid"] = len(missing_cols) == 0
    checks["missing_columns"] = list(missing_cols)

    # Null checks on mandatory fields
    null_counts = df[["id", "productDisplayName", "masterCategory"]].isnull().sum()
    checks["null_check_passed"] = bool((null_counts == 0).all())
    checks["null_counts"] = null_counts.to_dict()

    # Outlier / range checks
    year_outliers = int((~df["year"].between(1990, 2026)).sum())
    checks["outlier_year_count"] = year_outliers
    checks["outlier_check_passed"] = year_outliers == 0

    # Duplicate primary key check
    dup_count = int(df["id"].duplicated().sum())
    checks["duplicate_check_passed"] = dup_count == 0
    checks["duplicate_count"] = dup_count

    checks["all_passed"] = all([
        checks["schema_valid"], checks["null_check_passed"],
        checks["outlier_check_passed"], checks["duplicate_check_passed"],
    ])
    checks["batch_id"] = batch_id
    checks["staging_path"] = str(staging_path)

    with open(OUT / f"validation_report_{batch_id}.json", "w") as f:
        json.dump(checks, f, indent=2, default=str)

    status = "PASSED" if checks["all_passed"] else "FAILED"
    print(f"[staging] batch {batch_id[:8]} validation: {status}")
    if not checks["all_passed"]:
        raise ValidationError(f"Batch {batch_id} failed validation: {checks}")
    return checks


# ---------------------------------------------------------------------------
# ii. IDEMPOTENCY - safe reruns via upsert on primary key, not blind append
# ---------------------------------------------------------------------------
def idempotent_load(df: pd.DataFrame, run_id: str):
    conn = sqlite3.connect(RESILIENT_DB)
    cur = conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS products_resilient (
        id INTEGER PRIMARY KEY, gender TEXT, masterCategory TEXT,
        subCategory TEXT, articleType TEXT, baseColour TEXT, season TEXT,
        year INTEGER, usage TEXT, productDisplayName TEXT, loaded_by_run TEXT
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS run_log (
        run_id TEXT PRIMARY KEY, rows_upserted INTEGER, ran_at TEXT
    )""")

    before_count = cur.execute("SELECT COUNT(*) FROM products_resilient").fetchone()[0]

    records = df.to_dict(orient="records")
    for r in records:
        cur.execute("""
            INSERT INTO products_resilient
                (id, gender, masterCategory, subCategory, articleType,
                 baseColour, season, year, usage, productDisplayName, loaded_by_run)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(id) DO UPDATE SET
                gender=excluded.gender, masterCategory=excluded.masterCategory,
                subCategory=excluded.subCategory, articleType=excluded.articleType,
                baseColour=excluded.baseColour, season=excluded.season,
                year=excluded.year, usage=excluded.usage,
                productDisplayName=excluded.productDisplayName,
                loaded_by_run=excluded.loaded_by_run
        """, (r["id"], r["gender"], r["masterCategory"], r["subCategory"],
              r["articleType"], r["baseColour"], r["season"], int(r["year"]),
              r["usage"], r["productDisplayName"], run_id))

    after_count = cur.execute("SELECT COUNT(*) FROM products_resilient").fetchone()[0]
    cur.execute("INSERT OR REPLACE INTO run_log VALUES (?,?,datetime('now'))",
                (run_id, len(records)))
    conn.commit()
    conn.close()

    result = {"run_id": run_id, "rows_in_batch": len(records),
              "row_count_before": before_count, "row_count_after": after_count,
              "net_new_rows": after_count - before_count}
    print(f"[idempotency] run {run_id[:8]}: {before_count} -> {after_count} rows "
          f"(net new: {result['net_new_rows']})")
    return result


def demonstrate_idempotency(df: pd.DataFrame):
    """Runs the SAME load twice to prove reruns don't duplicate data."""
    r1 = idempotent_load(df, run_id=str(uuid.uuid4()))
    r2 = idempotent_load(df, run_id=str(uuid.uuid4()))  # identical data, rerun
    proof = {
        "first_run": r1, "second_run_same_data": r2,
        "idempotent_confirmed": r2["net_new_rows"] == 0,
    }
    with open(OUT / "idempotency_proof.json", "w") as f:
        json.dump(proof, f, indent=2)
    print(f"[idempotency] confirmed={proof['idempotent_confirmed']} "
          f"(rerun added {r2['net_new_rows']} new rows, expected 0)")
    return proof


# ---------------------------------------------------------------------------
# iii. ATOMICITY - all-or-nothing transaction
# ---------------------------------------------------------------------------
def atomic_load(df: pd.DataFrame, inject_failure=False):
    """Loads a batch inside a single explicit transaction. If any row fails,
    the ENTIRE transaction is rolled back - proven here by deliberately
    injecting a bad row partway through and confirming zero rows persist."""
    conn = sqlite3.connect(RESILIENT_DB)
    cur = conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS atomic_test (
        id INTEGER PRIMARY KEY, productDisplayName TEXT NOT NULL
    )""")
    conn.commit()

    table = "atomic_test"
    cur.execute(f"DELETE FROM {table}")  # reset for a clean demo run
    conn.commit()

    rows = df[["id", "productDisplayName"]].head(200).to_dict(orient="records")
    if inject_failure:
        rows.insert(150, {"id": rows[10]["id"], "productDisplayName": None})  # violates NOT NULL + dup PK

    result = {"attempted_rows": len(rows), "inject_failure": inject_failure}
    try:
        cur.execute("BEGIN")
        for r in rows:
            cur.execute(f"INSERT INTO {table} (id, productDisplayName) VALUES (?,?)",
                        (r["id"], r["productDisplayName"]))
        conn.commit()
        result["outcome"] = "COMMITTED"
    except sqlite3.Error as e:
        conn.rollback()
        result["outcome"] = "ROLLED_BACK"
        result["error"] = str(e)

    final_count = cur.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    result["final_row_count"] = final_count
    conn.close()
    print(f"[atomicity] inject_failure={inject_failure} -> {result['outcome']}, "
          f"table now has {final_count} rows")
    return result


def demonstrate_atomicity(df: pd.DataFrame):
    success_case = atomic_load(df, inject_failure=False)
    failure_case = atomic_load(df, inject_failure=True)
    with open(OUT / "atomicity_proof.json", "w") as f:
        json.dump({"success_case": success_case, "failure_case": failure_case}, f, indent=2)
    return success_case, failure_case


# ---------------------------------------------------------------------------
# iv. ERROR HANDLING - backfill / replay for historical fixes
# ---------------------------------------------------------------------------
def simulate_error_and_backfill(df: pd.DataFrame):
    """Simulates a historical batch that was loaded with a known bad value
    (e.g. an upstream bug wrote 'UNKNOWN_COLOUR' for a date range), then
    replays/backfills just the affected rows - not a full reload."""
    conn = sqlite3.connect(RESILIENT_DB)
    cur = conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS backfill_target (
        id INTEGER PRIMARY KEY, baseColour TEXT, backfilled INTEGER DEFAULT 0
    )""")

    # 1. Simulate the original (buggy) load: corrupt baseColour for a slice
    corrupted = df.head(1000).copy()
    corrupted.loc[corrupted.index[200:400], "baseColour"] = "UNKNOWN_COLOUR"
    for _, r in corrupted[["id", "baseColour"]].iterrows():
        cur.execute("""INSERT INTO backfill_target (id, baseColour, backfilled)
                       VALUES (?,?,0) ON CONFLICT(id) DO UPDATE SET baseColour=excluded.baseColour""",
                    (int(r["id"]), r["baseColour"]))
    conn.commit()

    affected_before = cur.execute(
        "SELECT COUNT(*) FROM backfill_target WHERE baseColour='UNKNOWN_COLOUR'").fetchone()[0]

    # 2. Detect affected rows (error handling: identify blast radius)
    affected_ids = pd.read_sql(
        "SELECT id FROM backfill_target WHERE baseColour='UNKNOWN_COLOUR'", conn)["id"].tolist()

    # 3. Replay/backfill: re-extract correct values from source-of-truth
    #    (the cleaned catalog) for ONLY the affected ids
    correct = df[df["id"].isin(affected_ids)][["id", "baseColour"]]
    for _, r in correct.iterrows():
        cur.execute("""UPDATE backfill_target SET baseColour=?, backfilled=1
                       WHERE id=?""", (r["baseColour"], int(r["id"])))
    conn.commit()

    affected_after = cur.execute(
        "SELECT COUNT(*) FROM backfill_target WHERE baseColour='UNKNOWN_COLOUR'").fetchone()[0]
    backfilled_count = cur.execute(
        "SELECT COUNT(*) FROM backfill_target WHERE backfilled=1").fetchone()[0]
    conn.close()

    result = {
        "corrupted_rows_detected": affected_before,
        "rows_backfilled": backfilled_count,
        "remaining_bad_rows": affected_after,
        "backfill_successful": affected_after == 0,
    }
    with open(OUT / "backfill_report.json", "w") as f:
        json.dump(result, f, indent=2)
    print(f"[error_handling] detected {affected_before} corrupted rows, "
          f"backfilled {backfilled_count}, remaining bad: {affected_after}")
    return result


if __name__ == "__main__":
    df = pd.read_csv(CLEAN_CSV)
    batch_id = str(uuid.uuid4())

    try:
        checks = stage_and_validate(df, batch_id)
    except ValidationError as e:
        print(f"Pipeline halted: {e}")
        raise SystemExit(1)

    idem_proof = demonstrate_idempotency(df.head(2000))
    success_case, failure_case = demonstrate_atomicity(df)
    backfill_result = simulate_error_and_backfill(df)

    summary = {
        "staging_validation": checks,
        "idempotency": idem_proof,
        "atomicity": {"success_case": success_case, "failure_case": failure_case},
        "error_handling_backfill": backfill_result,
    }
    with open(OUT / "week5_summary.json", "w") as f:
        json.dump(summary, f, indent=2, default=str)
    print("Week 5 resilience pipeline complete.")
