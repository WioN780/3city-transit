"""Batch job (Airflow-triggered): silver.trip_delays -> gold.route_performance.

Hourly on-time / delay aggregates per route (docs/architecture.md
"Gold: gold.route_performance").

hour_of_day is derived from scheduled_arrival_utc converted to
AGENCY_TIMEZONE, not the raw UTC timestamp -- an after-midnight trip
scheduled for 00:20 local should bucket into hour 0, matching how riders
experience "rush hour", not hour 22/23 of the UTC day. day_of_week comes
from service_date itself (the GTFS service day), not that same local
timestamp -- otherwise an after-midnight trip would flip to the next
calendar day's weekday while still belonging to the prior day's service.

"On time" = delay_seconds in [ON_TIME_MIN_DELAY_SECONDS, ON_TIME_MAX_DELAY_SECONDS].
gold.delay_hotspots uses the same window to decide what counts as an
incident -- keep the two in sync if you change either.

Idempotent: overwrites only the target SERVICE_DATE partition, safe to re-run.

Run:
    docker exec spark-master spark-submit \\
        --packages io.delta:delta-spark_2.13:4.0.0 \\
        /opt/spark_jobs/gold/build_route_performance.py
"""
import json
import logging
import os
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("gold.route_performance")

SERVICE_DATE = os.environ.get("SERVICE_DATE") or datetime.now(timezone.utc).date().isoformat()
AGENCY_TIMEZONE = os.environ.get("AGENCY_TIMEZONE", "Europe/Warsaw")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
SILVER_TABLE_PATH = os.environ.get("SILVER_TABLE_PATH", "s3a://lakehouse/silver/trip_delays")
GOLD_TABLE_PATH = os.environ.get("GOLD_ROUTE_PERFORMANCE_PATH", "s3a://lakehouse/gold/route_performance")
ON_TIME_MIN_DELAY_SECONDS = int(os.environ.get("ON_TIME_MIN_DELAY_SECONDS", "-60"))
ON_TIME_MAX_DELAY_SECONDS = int(os.environ.get("ON_TIME_MAX_DELAY_SECONDS", "300"))

GOLD_COLUMNS = [
    "route_id",
    "service_date",
    "hour_of_day",
    "day_of_week",
    "on_time_pct",
    "avg_delay_seconds",
    "p90_delay_seconds",
    "avg_speed_mps",
    "sample_count",
]


def build_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000")
        .config("spark.hadoop.fs.s3a.connection.timeout", "200000")
        .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400")
        .getOrCreate()
    )


def main() -> None:
    spark = build_spark("gold-route-performance")
    spark.sparkContext.setLogLevel("WARN")

    silver = (
        spark.read.format("delta").load(SILVER_TABLE_PATH)
        .filter(F.col("service_date") == F.lit(SERVICE_DATE))
        .withColumn("local_scheduled_arrival", F.from_utc_timestamp("scheduled_arrival_utc", AGENCY_TIMEZONE))
        .withColumn("hour_of_day", F.hour("local_scheduled_arrival"))
        .withColumn("day_of_week", F.date_format("service_date", "EEEE"))
        .withColumn("is_on_time", F.col("delay_seconds").between(ON_TIME_MIN_DELAY_SECONDS, ON_TIME_MAX_DELAY_SECONDS))
    )

    result = (
        silver.groupBy("route_id", "service_date", "hour_of_day", "day_of_week")
        .agg(
            (F.avg(F.col("is_on_time").cast("int")) * 100).alias("on_time_pct"),
            F.avg("delay_seconds").alias("avg_delay_seconds"),
            F.percentile_approx("delay_seconds", 0.9).alias("p90_delay_seconds"),
            F.avg("speed_mps").alias("avg_speed_mps"),
            F.count(F.lit(1)).alias("sample_count"),
        )
        .select(*GOLD_COLUMNS)
    )

    written = result.count()
    if written:
        (
            result.write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", f"service_date = '{SERVICE_DATE}'")
            .partitionBy("service_date")
            .save(GOLD_TABLE_PATH)
        )

    logger.info(json.dumps({
        "event": "route_performance_written",
        "service_date": SERVICE_DATE,
        "rows_written": written,
    }))


if __name__ == "__main__":
    main()
