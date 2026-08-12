"""
Master orchestrator - runs the full continuous pipeline, Week 1 -> Week 5.
Run this once before launching the dashboard so every week's `outputs/`
folder is freshly populated from the real dataset.

Usage:  python3 run_pipeline.py
"""
import subprocess
import sys
import time
from pathlib import Path

BASE = Path(__file__).resolve().parent

STEPS = [
    ("Week 1 - Data Collection, Cleaning, Feature Engineering, EDA",
     [sys.executable, "week1_eda/pipeline_week1.py"]),
    ("Week 2 - Core ETL Pipeline (Extract/Transform/Load/CDC)",
     [sys.executable, "week2_etl/pipeline_week2.py"]),
    ("Week 3 - Data Architecture & Schema Design (OLTP/OLAP/Star/Cube)",
     [sys.executable, "week3_schema/pipeline_week3.py"]),
    ("Power BI data export (star schema tables)",
     [sys.executable, "powerbi/export_for_powerbi.py"]),
    ("Week 4 - Batch Pipeline via REST API",
     [sys.executable, "week4_pipeline/pipeline_week4.py"]),
    ("Week 5 - Resilient Production Pipeline Patterns",
     [sys.executable, "week5_resilience/pipeline_week5.py"]),
]

if __name__ == "__main__":
    t_start = time.time()
    for title, cmd in STEPS:
        print(f"\n{'='*70}\n{title}\n{'='*70}")
        t0 = time.time()
        result = subprocess.run(cmd, cwd=str(BASE))
        elapsed = time.time() - t0
        if result.returncode != 0:
            print(f"\n[FAILED] {title} exited with code {result.returncode}")
            sys.exit(result.returncode)
        print(f"[OK] {title} ({elapsed:.1f}s)")
    print(f"\nFull pipeline complete in {time.time() - t_start:.1f}s. "
          f"Run: streamlit run dashboard/app.py")
