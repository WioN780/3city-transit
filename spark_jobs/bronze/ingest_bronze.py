"""Structured Streaming job: gps_raw (Kafka) -> bronze.gps_positions (Delta/MinIO).

gps_raw record shape (docs/architecture.md §3.1):
    vehicle_id, route_id, trip_id: string
    latitude, longitude: float (required)
    bearing, speed_mps: float | null
    timestamp_utc: ISO-8601 string
    feed_sequence: int

Malformed records (unparseable JSON, missing required fields, or
out-of-range coordinates) are dropped and logged -- they never fail the
stream. Delta write + Kafka offset commit happen together per micro-batch
via the streaming checkpoint, giving at-least-once delivery: a clean
restart resumes from the last committed offset with no gap or replay; only
a crash between the Delta commit and the checkpoint commit can produce a
duplicate micro-batch.

Run against the local cluster, e.g.:
    docker exec spark-master spark-submit \\
        --master spark://spark-master:7077 \\
        --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0,io.delta:delta-spark_2.13:4.0.0 \\
        /opt/spark_jobs/bronze/ingest_bronze.py

Requires the target MinIO bucket to exist first, e.g.:
    mc mb local/lakehouse
"""
import json
import logging
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("bronze.ingest")

GPS_RAW_SCHEMA = StructType(
    [
        StructField("vehicle_id", StringType()),
        StructField("route_id", StringType()),
        StructField("trip_id", StringType()),
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
        StructField("bearing", DoubleType()),
        StructField("speed_mps", DoubleType()),
        StructField("timestamp_utc", StringType()),
        StructField("feed_sequence", IntegerType()),
    ]
)

BRONZE_COLUMNS = [
    "vehicle_id",
    "route_id",
    "trip_id",
    "latitude",
    "longitude",
    "bearing",
    "speed_mps",
    "timestamp_utc",
    "feed_sequence",
    "ingest_timestamp_utc",
    "ingest_date",
    "ingest_hour",
]

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "redpanda:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "gps_raw")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
BRONZE_TABLE_PATH = os.environ.get("BRONZE_TABLE_PATH", "s3a://lakehouse/bronze/gps_positions")
CHECKPOINT_PATH = os.environ.get("CHECKPOINT_PATH", "s3a://lakehouse/checkpoints/bronze_gps_positions")
STARTING_OFFSETS = os.environ.get("KAFKA_STARTING_OFFSETS", "earliest")


def build_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        # ingest_date/ingest_hour must be true UTC regardless of the host's
        # default JVM timezone -- current_timestamp() follows session tz.
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        # bitnamilegacy/spark:4.0.0 bundles hadoop-aws 3.3.4 against hadoop-client
        # 3.4.1. Several fs.s3a.* defaults in 3.4.1's core-default.xml switched
        # to duration-string format (e.g. "60s") that 3.3.4's own parsing code
        # reads as a plain int/long -- NumberFormatException at S3A/checkpoint
        # init. Override with the plain-number values 3.3.4 itself used to ship.
        .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000")
        .config("spark.hadoop.fs.s3a.connection.timeout", "200000")
        .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400")
        .getOrCreate()
    )


def parse_and_validate(batch_df: DataFrame) -> tuple[DataFrame, int, int]:
    """Returns (valid_rows, malformed_json_count, invalid_field_count)."""
    parsed = batch_df.select(
        F.col("value").cast("string").alias("json_value")
    ).withColumn("data", F.from_json(F.col("json_value"), GPS_RAW_SCHEMA))

    malformed = parsed.filter(F.col("data").isNull())
    malformed_count = malformed.count()
    if malformed_count:
        samples = [r["json_value"] for r in malformed.select("json_value").limit(5).collect()]
        logger.warning(json.dumps({
            "event": "malformed_json",
            "count": malformed_count,
            "samples": samples,
        }))

    candidates = parsed.filter(F.col("data").isNotNull()).select("data.*")

    is_valid = (
        F.col("vehicle_id").isNotNull()
        & F.col("latitude").isNotNull() & F.col("latitude").between(-90, 90)
        & F.col("longitude").isNotNull() & F.col("longitude").between(-180, 180)
        & F.col("timestamp_utc").isNotNull()
        & F.to_timestamp(F.col("timestamp_utc")).isNotNull()
        & F.col("feed_sequence").isNotNull()
    )

    invalid = candidates.filter(~is_valid)
    invalid_count = invalid.count()
    if invalid_count:
        invalid_samples = [r.asDict() for r in invalid.limit(5).collect()]
        logger.warning(json.dumps({
            "event": "invalid_fields",
            "count": invalid_count,
            "samples": invalid_samples,
        }, default=str))

    return candidates.filter(is_valid), malformed_count, invalid_count


def process_batch(batch_df: DataFrame, batch_id: int) -> None:
    valid, malformed_count, invalid_count = parse_and_validate(batch_df)

    enriched = (
        valid.withColumn("ingest_timestamp_utc", F.current_timestamp())
        .withColumn("ingest_date", F.to_date("ingest_timestamp_utc"))
        .withColumn("ingest_hour", F.hour("ingest_timestamp_utc"))
        .select(*BRONZE_COLUMNS)
    )

    written = enriched.count()
    if written:
        enriched.write.format("delta").mode("append").partitionBy(
            "ingest_date", "ingest_hour"
        ).save(BRONZE_TABLE_PATH)

    logger.info(json.dumps({
        "event": "batch_processed",
        "batch_id": batch_id,
        "records_written": written,
        "malformed_json": malformed_count,
        "invalid_fields": invalid_count,
    }))


def main() -> None:
    spark = build_spark("bronze-gps-ingest")
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
        .load()
    )

    query = (
        raw.writeStream.foreachBatch(process_batch)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="10 seconds")
        .start()
    )

    logger.info(json.dumps({
        "event": "stream_started",
        "kafka_bootstrap": KAFKA_BOOTSTRAP,
        "topic": KAFKA_TOPIC,
        "table_path": BRONZE_TABLE_PATH,
        "checkpoint_path": CHECKPOINT_PATH,
    }))

    query.awaitTermination()


if __name__ == "__main__":
    main()
