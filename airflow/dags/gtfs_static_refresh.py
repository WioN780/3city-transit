"""Daily refresh of the GTFS static feed (routes, stops, schedule) used to
compute scheduled arrival times in the silver layer.

Runs spark_jobs/silver/download_gtfs_static.py, which already fails loudly
on its own -- it raises ValueError on a malformed/incomplete zip (missing
files or required columns) and propagates urllib errors on an unreachable
URL. This DAG's job is just to run it on a schedule and make that failure
visible via on_failure_alert instead of it disappearing into a cron log.

Requires (see docker-compose.yml / airflow/requirements.txt):
  - spark_jobs/ mounted into the Airflow containers at /opt/airflow/spark_jobs
  - an Airflow connection `spark_default` (spark://spark-master:7077)
  - a JRE in the Airflow image, since SparkSubmitOperator drives spark-submit
    locally in client mode -- not currently provided.
  - hadoop-aws + a matching aws-java-sdk-bundle on that same local driver's
    classpath: pip-installed pyspark ships Hadoop's client jars but not
    hadoop-aws, so an s3a:// read/write from a client-mode driver raises
    ClassNotFoundException: S3AFileSystem otherwise. spark-master/worker
    don't need this (bitnamilegacy/spark:4.0.0 already bundles it -- see
    the version-skew comment in spark_jobs/bronze/ingest_bronze.py), but
    the driver here runs in the Airflow container, not on that image.
  None of this is wired up or verified yet -- see PR notes.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from alerts import on_failure_alert

with DAG(
    dag_id="gtfs_static_refresh",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["gtfs", "static"],
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": on_failure_alert,
    },
) as dag:
    refresh = SparkSubmitOperator(
        task_id="refresh_gtfs_static",
        conn_id="spark_default",
        application="/opt/airflow/spark_jobs/silver/download_gtfs_static.py",
        packages="io.delta:delta-spark_2.13:4.0.0",
        name="gtfs-static-refresh-{{ ds }}",
        env_vars={"FEED_DATE": "{{ ds }}"},
    )
