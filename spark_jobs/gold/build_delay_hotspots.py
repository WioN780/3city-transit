"""Batch job (Airflow-triggered): silver.trip_delays -> gold.delay_hotspots.

Buckets every stop observation (docs/architecture.md
"Gold: gold.delay_hotspots") into a ~100m grid by rounding lat/lon to 3
decimal places (~111m at the equator -- fine for "which corner of town is
bad", not lane-level precision), and reports avg delay + observation count
per bucket.

Idempotent: overwrites only the target SERVICE_DATE partition, safe to re-run.

Run:
    docker exec spark-master spark-submit \\
        --packages io.delta:delta-spark_2.13:4.0.0 \\
        /opt/spark_jobs/gold/build_delay_hotspots.py
"""
import json
import logging
import os
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("gold.delay_hotspots")

SERVICE_DATE = os.environ.get("SERVICE_DATE") or datetime.now(timezone.utc).date().isoformat()
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
SILVER_TABLE_PATH = os.environ.get("SILVER_TABLE_PATH", "s3a://lakehouse/silver/trip_delays")
GOLD_TABLE_PATH = os.environ.get("GOLD_DELAY_HOTSPOTS_PATH", "s3a://lakehouse/gold/delay_hotspots")
GRID_DECIMAL_PLACES = int(os.environ.get("HOTSPOT_GRID_DECIMAL_PLACES", "3"))

GOLD_COLUMNS = [
    "route_id",
    "latitude_bucket",
    "longitude_bucket",
    "avg_delay_seconds",
    "incident_count",
    "service_date",
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
    spark = build_spark("gold-delay-hotspots")
    spark.sparkContext.setLogLevel("WARN")

    observations = (
        spark.read.format("delta").load(SILVER_TABLE_PATH)
        .filter(F.col("service_date") == F.lit(SERVICE_DATE))
        .withColumn("latitude_bucket", F.round("latitude", GRID_DECIMAL_PLACES))
        .withColumn("longitude_bucket", F.round("longitude", GRID_DECIMAL_PLACES))
    )

    result = (
        observations.groupBy("route_id", "latitude_bucket", "longitude_bucket")
        .agg(
            F.avg("delay_seconds").alias("avg_delay_seconds"),
            F.count(F.lit(1)).alias("incident_count"),
        )
        .withColumn("service_date", F.lit(SERVICE_DATE).cast("date"))
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
        "event": "delay_hotspots_written",
        "service_date": SERVICE_DATE,
        "rows_written": written,
    }))


if __name__ == "__main__":
    main()
