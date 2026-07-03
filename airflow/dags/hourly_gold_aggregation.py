"""Hourly rollup of silver.trip_delays into gold.route_performance and
gold.delay_hotspots (spark_jobs/gold/build_route_performance.py and
spark_jobs/gold/build_delay_hotspots.py -- see those modules for the
aggregation logic itself; this DAG just re-runs them for today's service
date so newly arrived silver rows get picked up each hour).

Both jobs are idempotent (overwrite-by-partition on SERVICE_DATE), so
re-running hourly for the same day is safe -- it just refreshes the numbers
with whatever silver data has landed so far.

Same unverified runtime prereqs as gtfs_static_refresh.py: a JRE and a
hadoop-aws jar on the Airflow container's local spark-submit driver
classpath (client mode runs the driver here, not on spark-master, which
already has both baked into its image).
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from alerts import on_failure_alert

DELTA_PACKAGE = "io.delta:delta-spark_2.13:4.0.0"

with DAG(
    dag_id="hourly_gold_aggregation",
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["gold", "aggregation"],
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=3),
        "on_failure_callback": on_failure_alert,
    },
) as dag:
    route_performance = SparkSubmitOperator(
        task_id="build_route_performance",
        conn_id="spark_default",
        application="/opt/airflow/spark_jobs/gold/build_route_performance.py",
        packages=DELTA_PACKAGE,
        name="gold-route-performance-{{ ts_nodash }}",
        env_vars={"SERVICE_DATE": "{{ ds }}"},
    )
    delay_hotspots = SparkSubmitOperator(
        task_id="build_delay_hotspots",
        conn_id="spark_default",
        application="/opt/airflow/spark_jobs/gold/build_delay_hotspots.py",
        packages=DELTA_PACKAGE,
        name="gold-delay-hotspots-{{ ts_nodash }}",
        env_vars={"SERVICE_DATE": "{{ ds }}"},
    )
