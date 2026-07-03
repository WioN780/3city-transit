"""Smoke-test script: prints row counts for bronze.gps_positions.

    docker exec spark-master spark-submit \\
        --master spark://spark-master:7077 \\
        --packages io.delta:delta-spark_2.13:4.0.0 \\
        /opt/spark_jobs/bronze/query_bronze.py
"""
import os

from pyspark.sql import SparkSession

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
BRONZE_TABLE_PATH = os.environ.get("BRONZE_TABLE_PATH", "s3a://lakehouse/bronze/gps_positions")


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
        # see ingest_bronze.py: hadoop-aws/hadoop-client version skew in the
        # bitnami image breaks several duration-string S3A defaults.
        .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000")
        .config("spark.hadoop.fs.s3a.connection.timeout", "200000")
        .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400")
        .getOrCreate()
    )


def main() -> None:
    spark = build_spark("bronze-gps-query")
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.format("delta").load(BRONZE_TABLE_PATH)

    print(f"total rows: {df.count()}")
    print("rows by ingest_date / ingest_hour:")
    df.groupBy("ingest_date", "ingest_hour").count().orderBy("ingest_date", "ingest_hour").show(50, truncate=False)


if __name__ == "__main__":
    main()
