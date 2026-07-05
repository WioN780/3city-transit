"""Daily data quality checks across bronze/silver/gold tables (null rates,
schema drift, row-count sanity).
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from alerts import on_failure_alert

DELTA_PACKAGE = "io.delta:delta-spark_2.13:4.0.0"

with DAG(
    dag_id="data_quality_checks",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["data-quality"],
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": on_failure_alert,
    },
) as dag:
    run_checks = SparkSubmitOperator(
        task_id="run_data_quality_checks",
        conn_id="spark_default",
        application="/opt/airflow/spark_jobs/data_quality/check_trip_delays.py",
        packages=DELTA_PACKAGE,
        name="data-quality-checks-{{ ts_nodash }}",
        env_vars={"SERVICE_DATE": "{{ ds }}"},
    )

