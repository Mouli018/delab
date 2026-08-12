"""
New Data Watcher DAG
======================
Directly answers "if we give new data it should automatically do the EDA/ETL
process": this DAG polls `data/incoming/` every 30 seconds. The moment a new
file appears there, it (a) moves it into the raw catalog location and (b)
triggers the full `data_engineering_pipeline` DAG — no manual re-run needed.

In production this sensor would instead watch S3/a landing DB/a Kafka topic;
a filesystem sensor is used here so it's runnable with zero external services.

Run manually:
    export AIRFLOW_HOME=<project>/airflow
    airflow dags test new_data_watcher 2026-08-10
"""
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.filesystem import FileSensor

PROJECT_ROOT = Path(__file__).resolve().parents[3]
INCOMING_DIR = PROJECT_ROOT / "data" / "incoming"
INCOMING_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_MARKER = PROJECT_ROOT / "data" / "incoming" / ".last_processed"

default_args = {"owner": "22MDCEL10", "retries": 1, "retry_delay": timedelta(seconds=15)}

with DAG(
    dag_id="new_data_watcher",
    description="Polls data/incoming/ for new files and auto-triggers the "
                 "full EDA/ETL pipeline when one lands",
    default_args=default_args,
    schedule=timedelta(seconds=30),
    start_date=datetime(2026, 8, 1),
    catchup=False,
    max_active_runs=1,
    tags=["22MDCEL10", "auto-trigger"],
) as dag:

    wait_for_new_file = FileSensor(
        task_id="wait_for_new_file",
        filepath=str(INCOMING_DIR),
        fs_conn_id="fs_default",
        poke_interval=10,
        timeout=25,
        soft_fail=True,   # don't fail the DAG run just because nothing arrived yet
    )

    def _detect_unprocessed_file(**context):
        """Returns True (continue) only if there's a genuinely new file that
        hasn't been processed in a prior run - this is what makes reruns of
        this sensor DAG idempotent rather than reprocessing the same drop
        forever."""
        candidates = sorted(INCOMING_DIR.glob("*.csv"))
        if not candidates:
            print("[watcher] no CSV files in data/incoming/")
            return False

        processed = set()
        if PROCESSED_MARKER.exists():
            processed = set(PROCESSED_MARKER.read_text().splitlines())

        new_files = [f for f in candidates if f.name not in processed]
        if not new_files:
            print("[watcher] all files already processed, nothing new")
            return False

        print(f"[watcher] {len(new_files)} new file(s) detected: "
              f"{[f.name for f in new_files]}")
        context["ti"].xcom_push(key="new_files", value=[f.name for f in new_files])
        return True

    detect_new_file = ShortCircuitOperator(
        task_id="detect_unprocessed_file",
        python_callable=_detect_unprocessed_file,
    )

    def _ingest_new_file(**context):
        """Copies the new file into the raw catalog location the Week 1
        pipeline reads from, then records it as processed."""
        import shutil
        new_files = context["ti"].xcom_pull(key="new_files", task_ids="detect_unprocessed_file")
        raw_target = PROJECT_ROOT / "data" / "raw" / "styles.csv"
        for fname in new_files:
            src = INCOMING_DIR / fname
            shutil.copy(src, raw_target)
            print(f"[watcher] ingested {fname} -> {raw_target}")

        processed = set()
        if PROCESSED_MARKER.exists():
            processed = set(PROCESSED_MARKER.read_text().splitlines())
        processed.update(new_files)
        PROCESSED_MARKER.write_text("\n".join(sorted(processed)))

    ingest_new_file = PythonOperator(
        task_id="ingest_new_file",
        python_callable=_ingest_new_file,
    )

    trigger_full_pipeline = TriggerDagRunOperator(
        task_id="trigger_full_pipeline",
        trigger_dag_id="data_engineering_pipeline",
        wait_for_completion=False,
    )

    wait_for_new_file >> detect_new_file >> ingest_new_file >> trigger_full_pipeline
