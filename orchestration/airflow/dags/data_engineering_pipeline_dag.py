"""
Data Engineering Lab — Master Orchestration DAG
==================================================
Runs the full Week 1 -> Week 5 pipeline as a real Airflow task graph:
    collect_and_clean (W1) -> etl_extract_transform_load (W2)
        -> schema_and_star_cube (W3) -> api_batch_pipeline (W4)
        -> stage_and_validate -> idempotent_load -> atomic_load_check
        -> backfill_check (W5)

Demonstrates, using Airflow's own primitives (not just the Python scripts):
  - Task dependencies / DAG structure          -> the graph itself
  - Idempotent operations                       -> retries are safe by design
                                                    (each task re-runs the same
                                                    upsert/full-load logic)
  - Atomicity                                    -> `atomic_load_check` task
                                                    fails the whole DAG run if
                                                    the transaction rolled back
  - Error handling / retries                     -> retries=2, exponential
                                                    backoff, on_failure_callback
  - Backfill / replay for historical data fixes  -> genuine Airflow feature:
        airflow dags backfill -s 2026-08-01 -e 2026-08-05 data_engineering_pipeline
    reprocesses each logical date's run using this exact DAG definition.

Run manually:
    export AIRFLOW_HOME=<project>/airflow
    airflow dags test data_engineering_pipeline 2026-08-10
"""
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

PROJECT_ROOT = Path(__file__).resolve().parents[3]  # .../delab
PYTHON_BIN = "python3"


def log_failure(context):
    ti = context["task_instance"]
    print(f"[ALERT] Task {ti.task_id} failed on run {context['run_id']}. "
          f"In production this would page on-call / post to Slack.")


default_args = {
    "owner": "22MDCEL10",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
    "on_failure_callback": log_failure,
    "depends_on_past": False,
}

with DAG(
    dag_id="data_engineering_pipeline",
    description="Week 1-5 fashion catalog pipeline: collect -> ETL -> schema -> "
                 "API batch -> resilience checks",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 8, 1),
    catchup=False,
    tags=["22MDCEL10", "data-engineering-lab"],
) as dag:

    week1_eda = BashOperator(
        task_id="week1_collect_clean_eda",
        bash_command=f"cd {PROJECT_ROOT} && {PYTHON_BIN} week1_eda/pipeline_week1.py",
    )

    week2_etl = BashOperator(
        task_id="week2_etl_extract_transform_load",
        bash_command=f"cd {PROJECT_ROOT} && {PYTHON_BIN} week2_etl/pipeline_week2.py",
    )

    week3_schema = BashOperator(
        task_id="week3_schema_star_cube",
        bash_command=f"cd {PROJECT_ROOT} && {PYTHON_BIN} week3_schema/pipeline_week3.py",
    )

    week3_powerbi_export = BashOperator(
        task_id="week3_export_for_powerbi",
        bash_command=f"cd {PROJECT_ROOT} && {PYTHON_BIN} powerbi/export_for_powerbi.py",
    )

    week4_batch = BashOperator(
        task_id="week4_api_batch_pipeline",
        bash_command=f"cd {PROJECT_ROOT} && {PYTHON_BIN} week4_pipeline/pipeline_week4.py",
    )

    week5_resilience = BashOperator(
        task_id="week5_stage_validate_idempotent_atomic_backfill",
        bash_command=f"cd {PROJECT_ROOT} && {PYTHON_BIN} week5_resilience/pipeline_week5.py",
    )

    def _check_atomicity_proof():
        """Reads Week 5's atomicity proof and FAILS this task (and therefore
        the DAG run) if the rollback guarantee wasn't actually upheld —
        Airflow enforcing atomicity as a hard gate, not just a log line."""
        import json
        report_path = PROJECT_ROOT / "week5_resilience" / "outputs" / "atomicity_proof.json"
        report = json.loads(report_path.read_text())
        failure_case = report["failure_case"]
        assert failure_case["outcome"] == "ROLLED_BACK", "Expected rollback on bad batch"
        assert failure_case["final_row_count"] == 0, (
            f"Atomicity violated: {failure_case['final_row_count']} rows persisted "
            f"from a batch that should have fully rolled back"
        )
        print("[atomicity_gate] PASSED - partial batch correctly rolled back to 0 rows")

    atomicity_gate = PythonOperator(
        task_id="atomicity_gate",
        python_callable=_check_atomicity_proof,
    )

    def _pipeline_complete():
        print("Full Week 1-5 pipeline run complete. Outputs refreshed for "
              "the dashboard and Power BI folder-query.")

    pipeline_complete = PythonOperator(
        task_id="pipeline_complete",
        python_callable=_pipeline_complete,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    (week1_eda
     >> week2_etl
     >> week3_schema
     >> week3_powerbi_export
     >> week4_batch
     >> week5_resilience
     >> atomicity_gate
     >> pipeline_complete)
