"""PySpark Data Quality Checks on silver.trip_delays.

Validates:
1. Schema drift (checks columns and types).
2. Null rates (ensures critical columns have 0% nulls).
3. Sanity bounds (enforces realistic ranges for delay_seconds and speed_mps).

Runnable locally, in Docker, or via Airflow.
"""
import json
import logging
import os
import sys
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("data_quality.check_trip_delays")

SERVICE_DATE = os.environ.get("SERVICE_DATE") or datetime.now(timezone.utc).date().isoformat()
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
SILVER_TABLE_PATH = os.environ.get("SILVER_TABLE_PATH", "s3a://lakehouse/silver/trip_delays")

# Expected schema columns and their PySpark simple types
EXPECTED_SCHEMA = {
    "vehicle_id": "string",
    "route_id": "string",
    "trip_id": "string",
    "stop_id": "string",
    "scheduled_arrival_utc": "timestamp",
    "estimated_arrival_utc": "timestamp",
    "delay_seconds": "int",
    "speed_mps": "double",
    "latitude": "double",
    "longitude": "double",
    "service_date": "date"
}

CRITICAL_COLUMNS = [
    "vehicle_id",
    "route_id",
    "trip_id",
    "stop_id",
    "scheduled_arrival_utc",
    "estimated_arrival_utc",
    "delay_seconds",
    "service_date"
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
    logger.info("Initializing Spark session for data quality checks...")
    spark = build_spark("silver-data-quality-checks")
    spark.sparkContext.setLogLevel("WARN")

    logger.info("Loading silver.trip_delays for SERVICE_DATE = %s", SERVICE_DATE)
    try:
        df = (
            spark.read.format("delta")
            .load(SILVER_TABLE_PATH)
            .filter(F.col("service_date") == F.lit(SERVICE_DATE))
        )
    except Exception as e:
        logger.error("Failed to load silver.trip_delays table at %s: %s", SILVER_TABLE_PATH, e)
        sys.exit(1)

    total_rows = df.count()
    logger.info("Total rows in partition: %d", total_rows)

    if total_rows == 0:
        logger.warning("No rows found for service date %s. Skipping detailed quality checks.", SERVICE_DATE)
        # We don't fail the job here as there might genuinely be no data for a given date (e.g. off hours),
        # but we log a warning.
        sys.exit(0)

    # 1. Schema Drift Check
    logger.info("Running schema drift checks...")
    actual_schema = {field.name: field.dataType.simpleString() for field in df.schema}
    schema_errors = []
    
    for col_name, expected_type in EXPECTED_SCHEMA.items():
        if col_name not in actual_schema:
            schema_errors.append(f"Missing column '{col_name}'")
            continue
        
        actual_type = actual_schema[col_name]
        # Allow float/double skew if precision varies between different environments
        if expected_type == "double" and actual_type == "float":
            continue
        if actual_type != expected_type:
            schema_errors.append(
                f"Column '{col_name}' has type '{actual_type}', expected '{expected_type}'"
            )

    if schema_errors:
        logger.error("Schema drift detected:\n%s", "\n".join(schema_errors))
        sys.exit(2)
    logger.info("Schema drift check passed.")

    # 2. Null Rates Check
    logger.info("Running null rate checks...")
    null_exprs = [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in CRITICAL_COLUMNS]
    null_counts_row = df.select(*null_exprs).first()
    
    null_rate_errors = []
    for c in CRITICAL_COLUMNS:
        count = null_counts_row[c] or 0
        rate = count / total_rows
        logger.info("Column '%s' null rate: %.4f (%d/%d)", c, rate, count, total_rows)
        # Critical columns should have 0% null rate
        if rate > 0.0:
            null_rate_errors.append(f"Column '{c}' has null rate {rate:.4f} (> 0.0)")

    if null_rate_errors:
        logger.error("Null rate validations failed:\n%s", "\n".join(null_rate_errors))
        sys.exit(3)
    logger.info("Null rate checks passed.")

    # 3. Delay-value and speed sanity bounds
    logger.info("Running delay-value and speed sanity bounds checks...")
    stats = df.select(
        F.min("delay_seconds").alias("min_delay"),
        F.max("delay_seconds").alias("max_delay"),
        F.max("speed_mps").alias("max_speed")
    ).first()

    min_delay = stats["min_delay"]
    max_delay = stats["max_delay"]
    max_speed = stats["max_speed"]

    logger.info("Partition metrics: min_delay=%s, max_delay=%s, max_speed=%s", min_delay, max_delay, max_speed)

    bounds_errors = []
    
    # Assertions:
    # Lower bound: early arrival (negative delay) of more than 1 hour is considered insane.
    if min_delay is not None and min_delay < -3600:
        bounds_errors.append(f"min_delay ({min_delay}s) is below sanity limit of -3600s (1h early)")
        
    # Upper bound: delayed departure/arrival of more than 5 hours is considered insane.
    if max_delay is not None and max_delay > 18000:
        bounds_errors.append(f"max_delay ({max_delay}s) is above sanity limit of 18000s (5h delayed)")

    if bounds_errors:
        logger.error("Sanity bounds checks failed:\n%s", "\n".join(bounds_errors))
        sys.exit(4)
        
    # Speed check: Warn if a public transport vehicle speed is > 180 km/h (50 m/s)
    if max_speed is not None and max_speed > 50.0:
        logger.warning(
            "Unusually high speed detected in partition: max_speed = %.2f m/s (%.2f km/h)",
            max_speed, max_speed * 3.6
        )

    logger.info("All data quality checks passed successfully!")
    sys.exit(0)


if __name__ == "__main__":
    main()
