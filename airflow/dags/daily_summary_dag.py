from datetime import datetime, timedelta
import os

# pyrefly: ignore [missing-import]
from airflow import DAG
# pyrefly: ignore [missing-import]
from airflow.operators.bash import BashOperator


default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="daily_summary_dbt",
    default_args=default_args,
    schedule="5 0 * * *",
    start_date=datetime(2026, 7, 1),
    catchup=False,
    tags=["dbt", "clickhouse", "summary"],
) as dag:

    run_dbt_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command="dbt run --project-dir /opt/dbt/project --profiles-dir /opt/dbt/project",
        env={
            **os.environ,
            "CLICKHOUSE_PASSWORD": os.environ.get("CLICKHOUSE_PASSWORD", ""),
        }
    )

    run_dbt_marts
