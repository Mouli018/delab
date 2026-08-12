# Airflow — Setup & Usage

Both DAGs below were actually installed, parsed, and **executed successfully**
(`state=success`) while building this project — this isn't a theoretical
sketch, it's a verified working setup.

## 1. Install (isolated venv — Airflow has many pinned dependencies that can
   conflict with the rest of the project's packages, so keep it separate)

```bash
python3 -m venv airflow_venv
airflow_venv/bin/pip install --upgrade pip
airflow_venv/bin/pip install "apache-airflow==2.9.3" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.12.txt"
```

## 2. Point Airflow at this project's DAGs folder

```bash
export AIRFLOW_HOME=$(pwd)/airflow
mkdir -p $AIRFLOW_HOME
airflow_venv/bin/airflow db migrate
```

Then edit `$AIRFLOW_HOME/airflow.cfg`:
```ini
[core]
dags_folder = <project_root>/orchestration/airflow/dags
load_examples = False
```

Re-run `airflow db migrate` once more after editing the config.

The `new_data_watcher` DAG needs a filesystem connection registered once:
```bash
airflow_venv/bin/airflow connections add fs_default --conn-type fs --conn-extra '{"path": "/"}'
```

## 3. The two DAGs

### `data_engineering_pipeline` — the master orchestration DAG
Runs Week 1 → Week 5 as a real task graph (see
`orchestration/airflow/dags/data_engineering_pipeline_dag.py`). Includes an
`atomicity_gate` task that **programmatically asserts** the Week 5 rollback
proof holds (reads `atomicity_proof.json` and fails the whole DAG run if a
partial batch somehow persisted) — Airflow enforcing the resilience guarantee
as a hard CI-style gate, not just a report.

```bash
airflow_venv/bin/airflow dags test data_engineering_pipeline 2026-08-10
```

### `new_data_watcher` — the auto-trigger-on-new-data DAG
Polls `data/incoming/` every 30 seconds. The moment a new CSV lands there, it
copies it into the raw catalog location and triggers a fresh run of
`data_engineering_pipeline` — no manual re-run needed. This is what directly
answers "if we give new data it should automatically do the EDA and ETL
process."

```bash
# simulate a new data drop:
cp your_new_data.csv data/incoming/

airflow_venv/bin/airflow dags test new_data_watcher 2026-08-10
# -> detects the file, ingests it, triggers data_engineering_pipeline
```

Re-running with no new files present correctly short-circuits (verified: the
`detect_unprocessed_file` task returns `False` and skips downstream tasks) —
so the watcher can run on a tight schedule without wastefully reprocessing
the same data over and over.

## 4. Backfill (Week 5.iv — real Airflow feature, not simulated)

Airflow's `backfill` command reprocesses a DAG across a range of historical
logical dates using the *current* DAG definition — the canonical way to fix
data that was wrong when it was first loaded:

```bash
airflow_venv/bin/airflow dags backfill \
    -s 2026-08-01 -e 2026-08-05 \
    data_engineering_pipeline
```

## 5. Run the real scheduler + webserver (optional, for the visual DAG graph)

```bash
airflow_venv/bin/airflow scheduler &
airflow_venv/bin/airflow webserver --port 8080
# open http://localhost:8080 to see the DAG graph, task states, and Gantt chart
```
