"""GTFS static downloader/validator: ZTM's daily GTFS zip -> versioned Delta
tables, partitioned by feed_date.

Stores gtfs_static.routes / trips / stop_times / stops / calendar_dates.
calendar_dates is stored alongside the four requested tables because ZTM's
feed ships no calendar.txt -- service_id activity is calendar_dates
exceptions-only, so it's required to know which trips actually run on a
given service_date (build_trip_delays.py needs exactly this).

Run:
    docker exec spark-master spark-submit \\
        --packages io.delta:delta-spark_2.13:4.0.0 \\
        /opt/spark_jobs/silver/download_gtfs_static.py
"""
import csv
import io
import json
import logging
import os
import tempfile
import urllib.request
import zipfile
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("silver.gtfs_static")

GTFS_STATIC_URL = os.environ.get(
    "GTFS_STATIC_URL",
    "https://ckan.multimediagdansk.pl/dataset/c24aa637-3619-4dc2-a171-a23eec8f2172/"
    "resource/30e783e4-2bec-4a7d-bb22-ee3e3b26ca96/download/gtfsgoogle.zip",
)
FEED_DATE = os.environ.get("FEED_DATE") or datetime.now(timezone.utc).date().isoformat()
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
GTFS_STATIC_BASE_PATH = os.environ.get("GTFS_STATIC_BASE_PATH", "s3a://lakehouse/gtfs_static")
DOWNLOAD_TIMEOUT_SECONDS = int(os.environ.get("DOWNLOAD_TIMEOUT_SECONDS", "60"))

# filename -> columns a valid GTFS feed must have in that file
REQUIRED_FILES = {
    "routes.txt": {"route_id", "route_short_name"},
    "trips.txt": {"route_id", "service_id", "trip_id"},
    "stop_times.txt": {"trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence"},
    "stops.txt": {"stop_id", "stop_lat", "stop_lon"},
    "calendar_dates.txt": {"service_id", "date", "exception_type"},
}

TABLES = {
    "routes": (
        "routes.txt",
        StructType([
            StructField("route_id", StringType()),
            StructField("agency_id", StringType()),
            StructField("route_short_name", StringType()),
            StructField("route_long_name", StringType()),
            StructField("route_desc", StringType()),
            StructField("route_type", IntegerType()),
            StructField("route_color", StringType()),
            StructField("route_text_color", StringType()),
        ]),
    ),
    "trips": (
        "trips.txt",
        StructType([
            StructField("route_id", StringType()),
            StructField("service_id", StringType()),
            StructField("trip_id", StringType()),
            StructField("trip_headsign", StringType()),
            StructField("trip_short_name", StringType()),
            StructField("direction_id", IntegerType()),
            StructField("shape_id", StringType()),
            StructField("wheelchair_accessible", IntegerType()),
        ]),
    ),
    "stop_times": (
        "stop_times.txt",
        StructType([
            StructField("trip_id", StringType()),
            StructField("arrival_time", StringType()),
            StructField("departure_time", StringType()),
            StructField("stop_id", StringType()),
            StructField("stop_sequence", IntegerType()),
        ]),
    ),
    "stops": (
        "stops.txt",
        StructType([
            StructField("stop_id", StringType()),
            StructField("stop_name", StringType()),
            StructField("stop_lat", DoubleType()),
            StructField("stop_lon", DoubleType()),
            StructField("stop_code", StringType()),
        ]),
    ),
    "calendar_dates": (
        "calendar_dates.txt",
        StructType([
            StructField("service_id", StringType()),
            StructField("date", StringType()),
            StructField("exception_type", IntegerType()),
        ]),
    ),
}


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
        # see spark_jobs/bronze/ingest_bronze.py for why: hadoop-aws 3.3.4 vs
        # hadoop-client 3.4.1 version skew in bitnamilegacy/spark:4.0.0.
        .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000")
        .config("spark.hadoop.fs.s3a.connection.timeout", "200000")
        .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400")
        .getOrCreate()
    )


def download_and_extract(url: str, dest_dir: str) -> None:
    logger.info(json.dumps({"event": "download_start", "url": url}))
    with urllib.request.urlopen(url, timeout=DOWNLOAD_TIMEOUT_SECONDS) as resp:
        data = resp.read()
    logger.info(json.dumps({"event": "download_complete", "bytes": len(data)}))

    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        names = set(zf.namelist())
        missing_files = set(REQUIRED_FILES) - names
        if missing_files:
            raise ValueError(f"GTFS zip missing required files: {sorted(missing_files)}")

        for filename, required_columns in REQUIRED_FILES.items():
            with zf.open(filename) as f:
                header = next(csv.reader(io.TextIOWrapper(f, encoding="utf-8-sig")))
            missing_columns = required_columns - set(header)
            if missing_columns:
                raise ValueError(f"{filename} missing required columns: {sorted(missing_columns)}")
            zf.extract(filename, dest_dir)

    logger.info(json.dumps({"event": "validated", "files": sorted(REQUIRED_FILES)}))


def write_table(spark: SparkSession, name: str, csv_path: str, schema: StructType) -> None:
    df = (
        spark.read.option("header", "true").schema(schema).csv(csv_path)
        .withColumn("feed_date", F.lit(FEED_DATE).cast("date"))
    )
    table_path = f"{GTFS_STATIC_BASE_PATH}/{name}"
    row_count = df.count()
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"feed_date = '{FEED_DATE}'")
        .partitionBy("feed_date")
        .save(table_path)
    )
    spark.sql("CREATE DATABASE IF NOT EXISTS gtfs_static")
    spark.sql(f"CREATE TABLE IF NOT EXISTS gtfs_static.{name} USING DELTA LOCATION '{table_path}'")
    logger.info(json.dumps({"event": "table_written", "table": f"gtfs_static.{name}", "rows": row_count}))


def main() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        download_and_extract(GTFS_STATIC_URL, tmpdir)

        spark = build_spark("silver-gtfs-static-download")
        spark.sparkContext.setLogLevel("WARN")

        for name, (filename, schema) in TABLES.items():
            write_table(spark, name, os.path.join(tmpdir, filename), schema)

    logger.info(json.dumps({"event": "gtfs_static_refresh_complete", "feed_date": FEED_DATE}))


if __name__ == "__main__":
    main()
