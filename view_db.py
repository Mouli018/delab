"""
Database Viewer Utility Script for Data Engineering Lab (22MDCEL10)
=====================================================================
Usage:
  python view_db.py                  # Lists all .db files and their tables
  python view_db.py <path_or_keyword> # Shows schema & sample rows for matching DB
  python view_db.py week3 oltp_products # Shows schema & sample rows for specific table

Examples:
  python view_db.py week2
  python view_db.py week3
  python view_db.py week3 oltp_products
"""
import sys
import sqlite3
from pathlib import Path

BASE = Path(__file__).resolve().parent

def get_all_dbs():
    return sorted(list(BASE.glob("**/*.db")))

def inspect_db(db_path: Path, table_filter: str = None):
    rel_path = db_path.relative_to(BASE)
    print(f"\n{'='*75}")
    print(f"Database File: {rel_path}")
    print(f"{'='*75}")
    
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
        tables = [r[0] for r in cur.fetchall()]
        
        if not tables:
            print("  (No tables found)")
            conn.close()
            return
            
        for t in tables:
            if table_filter and table_filter.lower() not in t.lower():
                continue
                
            cur.execute(f'SELECT COUNT(*) FROM "{t}"')
            count = cur.fetchone()[0]
            print(f"\n[Table] {t} ({count:,} rows)")
            
            # Show columns
            cur.execute(f'PRAGMA table_info("{t}")')
            cols = cur.fetchall()
            col_names = [c[1] for c in cols]
            col_types = [c[2] for c in cols]
            schema_str = ", ".join([f"{name} ({tp})" for name, tp in zip(col_names, col_types)])
            print(f"   Schema: {schema_str}")
            
            # Show sample rows (first 3 rows)
            cur.execute(f'SELECT * FROM "{t}" LIMIT 3')
            rows = cur.fetchall()
            if rows:
                print("   Sample Rows (first 3):")
                for idx, r in enumerate(rows, 1):
                    r_str = ", ".join([str(val)[:30] + ("..." if len(str(val)) > 30 else "") for val in r])
                    print(f"     Row {idx}: [{r_str}]")
        conn.close()
    except Exception as e:
        print(f"  [ERROR] Failed reading database: {e}")

def main():
    dbs = get_all_dbs()
    if not dbs:
        print("No .db files found in project.")
        return

    if len(sys.argv) == 1:
        print("\n--- Found SQLite Databases in project ---\n")
        for i, db in enumerate(dbs, 1):
            rel = db.relative_to(BASE)
            try:
                conn = sqlite3.connect(db)
                cur = conn.cursor()
                cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
                tbls = [r[0] for r in cur.fetchall()]
                tbl_info = []
                for t in tbls:
                    cur.execute(f'SELECT COUNT(*) FROM "{t}"')
                    cnt = cur.fetchone()[0]
                    tbl_info.append(f"{t} ({cnt:,} rows)")
                conn.close()
                info_str = ", ".join(tbl_info) if tbl_info else "Empty"
            except Exception as e:
                info_str = f"Error: {e}"
            print(f"  [{i}] {rel}\n      Tables: {info_str}\n")
        print("Tip: Run `python view_db.py <keyword>` (e.g. `python view_db.py week3`) for detailed table view!")
    else:
        keyword = sys.argv[1].lower()
        table_filter = sys.argv[2] if len(sys.argv) > 2 else None
        
        matched = [db for db in dbs if keyword in str(db.relative_to(BASE)).lower()]
        if not matched:
            print(f"No database path matching '{keyword}'. Available options:")
            for db in dbs:
                print(f"  - {db.relative_to(BASE)}")
            return
            
        for db in matched:
            inspect_db(db, table_filter)

if __name__ == "__main__":
    main()
