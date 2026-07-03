"""Daily data quality checks across bronze/silver/gold tables (null rates,
schema drift, row-count sanity). Skeleton only.
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="data_quality_checks",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["data-quality"],
) as dag:
    checks = EmptyOperator(task_id="run_data_quality_checks")
