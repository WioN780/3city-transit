"""Batch job (Airflow-triggered): bronze.gps_positions -> silver.trip_delays.

Joins GPS pings against the current GTFS static schedule to compute
per-stop delay and per-vehicle speed.

Key data quirk: ZTM's realtime trip_id always carries a trailing "_gps"
that the static feed's trip_id doesn't have (e.g. realtime
"409202607022354_12_409-19_gps" vs static "409202607022354_12_409-19").
That suffix is stripped before joining -- confirmed against a real
same-day download, not assumed.

"Current" GTFS static schedule = the latest gtfs_static feed_date at or
before service_date (ZTM's daily snapshot already covers several days via
calendar_dates, so a fresh snapshot isn't required for every service_date).

A ping's trip_id that doesn't match any trip active for service_date
(missing from trips.txt, or present but not active per calendar_dates) is
logged and excluded -- it never fails the job.

Run:
    docker exec spark-master spark-submit \\
        --packages io.delta:delta-spark_2.13:4.0.0 \\
        /opt/spark_jobs/silver/build_trip_delays.py
"""
import json
import logging
import os
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("silver.trip_delays")

SERVICE_DATE = os.environ.get("SERVICE_DATE") or datetime.now(timezone.utc).date().isoformat()
AGENCY_TIMEZONE = os.environ.get("AGENCY_TIMEZONE", "Europe/Warsaw")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
BRONZE_TABLE_PATH = os.environ.get("BRONZE_TABLE_PATH", "s3a://lakehouse/bronze/gps_positions")
GTFS_STATIC_BASE_PATH = os.environ.get("GTFS_STATIC_BASE_PATH", "s3a://lakehouse/gtfs_static")
SILVER_TABLE_PATH = os.environ.get("SILVER_TABLE_PATH", "s3a://lakehouse/silver/trip_delays")
# Beyond this, a "nearest scheduled stop" match is more likely a passed
# stop / wrong branch of a loop route than the vehicle's next stop. This is
# a plain distance cutoff, not map-matching -- it just keeps obviously
# bogus matches out rather than trying to resolve them correctly.
MAX_STOP_DISTANCE_METERS = float(os.environ.get("MAX_STOP_DISTANCE_METERS", "500"))

SILVER_COLUMNS = [
    "vehicle_id",
    "route_id",
    "trip_id",
    "stop_id",
    "scheduled_arrival_utc",
    "estimated_arrival_utc",
    "delay_seconds",
    "speed_mps",
    "latitude",
    "longitude",
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


def haversine_meters(lat1, lon1, lat2, lon2):
    radius = 6371000.0
    lat1r, lon1r, lat2r, lon2r = (F.radians(c) for c in (lat1, lon1, lat2, lon2))
    dlat = lat2r - lat1r
    dlon = lon2r - lon1r
    a = F.sin(dlat / 2) ** 2 + F.cos(lat1r) * F.cos(lat2r) * F.sin(dlon / 2) ** 2
    return 2 * radius * F.asin(F.sqrt(a))


def latest_feed_date(spark: SparkSession):
    row = (
        spark.read.format("delta").load(f"{GTFS_STATIC_BASE_PATH}/trips")
        .filter(F.col("feed_date") <= F.lit(SERVICE_DATE))
        .agg(F.max("feed_date").alias("feed_date"))
        .first()
    )
    return row["feed_date"] if row else None


def load_active_trips(spark: SparkSession, feed_date):
    trips = (
        spark.read.format("delta").load(f"{GTFS_STATIC_BASE_PATH}/trips")
        .filter(F.col("feed_date") == feed_date)
        .select("route_id", "service_id", "trip_id")
    )
    active_service_ids = (
        spark.read.format("delta").load(f"{GTFS_STATIC_BASE_PATH}/calendar_dates")
        .filter(F.col("feed_date") == feed_date)
        .filter(F.col("date") == F.date_format(F.lit(SERVICE_DATE), "yyyyMMdd"))
        .filter(F.col("exception_type") == 1)
        .select("service_id")
        .distinct()
    )
    return trips.join(active_service_ids, on="service_id", how="inner").select("route_id", "trip_id")


def load_stop_times_with_stops(spark: SparkSession, feed_date):
    stop_times = (
        spark.read.format("delta").load(f"{GTFS_STATIC_BASE_PATH}/stop_times")
        .filter(F.col("feed_date") == feed_date)
        .select("trip_id", "arrival_time", "stop_id")
    )
    stops = (
        spark.read.format("delta").load(f"{GTFS_STATIC_BASE_PATH}/stops")
        .filter(F.col("feed_date") == feed_date)
        .select("stop_id", "stop_lat", "stop_lon")
    )
    return stop_times.join(stops, on="stop_id", how="inner")


def add_speed(bronze: DataFrame) -> DataFrame:
    w = Window.partitionBy("vehicle_id").orderBy("timestamp_utc")
    prev_lat = F.lag("latitude").over(w)
    prev_lon = F.lag("longitude").over(w)
    prev_ts = F.lag("timestamp_utc").over(w)

    dist_m = haversine_meters(prev_lat, prev_lon, F.col("latitude"), F.col("longitude"))
    dt_s = F.col("timestamp_utc").cast("long") - prev_ts.cast("long")

    # dt<=0 covers exact-duplicate pings (at-least-once redelivery); tiny
    # nonzero dt with GPS jitter can still spike speed_mps -- not filtered
    # here, that's a data-quality pass, not this job's job.
    return bronze.withColumn("speed_mps", F.when(dt_s > 0, dist_m / dt_s).otherwise(F.lit(None)))


def scheduled_arrival_utc_col(arrival_time_col, service_date: str):
    # arrival_time is "HH:MM:SS" and routinely >= 24:00:00 for after-midnight
    # trips (still "yesterday's" service_date in GTFS). Don't parse it as a
    # clock string -- decompose to seconds and add to local midnight so the
    # >24h case and the DST-at-service-date-anchor case both fall out of
    # plain epoch-second arithmetic instead of string parsing.
    parts = F.split(arrival_time_col, ":")
    total_seconds = (
        parts.getItem(0).cast("long") * 3600
        + parts.getItem(1).cast("long") * 60
        + parts.getItem(2).cast("long")
    )
    local_midnight_utc = F.to_utc_timestamp(
        F.to_timestamp(F.lit(service_date)), AGENCY_TIMEZONE
    )
    return F.timestamp_seconds(F.unix_timestamp(local_midnight_utc) + total_seconds)


def main() -> None:
    spark = build_spark("silver-trip-delays")
    spark.sparkContext.setLogLevel("WARN")

    feed_date = latest_feed_date(spark)
    if feed_date is None:
        logger.error(json.dumps({
            "event": "no_gtfs_static_available",
            "service_date": SERVICE_DATE,
        }))
        return

    active_trips = load_active_trips(spark, feed_date)
    stop_times_with_stops = load_stop_times_with_stops(spark, feed_date)

    bronze = (
        spark.read.format("delta").load(BRONZE_TABLE_PATH)
        # ingest_date is when bronze ingested the ping (UTC calendar day),
        # not the GTFS service day. An after-midnight trip's pings land
        # under tomorrow's ingest_date, so on a run for today they're
        # missed here, and on a run for tomorrow their service_id is only
        # active for today (see load_active_trips) -- dropped as unmatched
        # either way. Known gap, not handled: see README.
        .filter(F.col("ingest_date") == F.lit(SERVICE_DATE))
        .withColumn("ping_id", F.monotonically_increasing_id())
        # bronze.timestamp_utc is stored as the original ISO-8601 string
        # (see spark_jobs/bronze/ingest_bronze.py); parse it once, up front,
        # so every downstream arithmetic op sees a real TimestampType instead
        # of silently null-ing out on a string->long cast.
        .withColumn("timestamp_utc", F.to_timestamp("timestamp_utc"))
    )
    bronze = add_speed(bronze)
    bronze = bronze.withColumn("static_trip_id", F.regexp_replace("trip_id", "_gps$", ""))

    matched = bronze.join(
        active_trips, bronze["static_trip_id"] == active_trips["trip_id"], "inner"
    ).select(
        bronze["ping_id"], bronze["vehicle_id"], active_trips["route_id"],
        active_trips["trip_id"], bronze["latitude"], bronze["longitude"],
        bronze["timestamp_utc"], bronze["speed_mps"],
    )

    unmatched_trip_ids = (
        bronze.select("static_trip_id").distinct()
        .join(active_trips.select("trip_id"), bronze["static_trip_id"] == active_trips["trip_id"], "left_anti")
    )
    unmatched_count = unmatched_trip_ids.count()
    if unmatched_count:
        samples = [r["static_trip_id"] for r in unmatched_trip_ids.limit(10).collect()]
        logger.warning(json.dumps({
            "event": "unmatched_trip_ids",
            "service_date": SERVICE_DATE,
            "distinct_count": unmatched_count,
            "samples": samples,
        }))

    candidates = matched.join(stop_times_with_stops, on="trip_id", how="inner")
    candidates = candidates.withColumn(
        "stop_distance_m",
        haversine_meters(candidates["latitude"], candidates["longitude"], candidates["stop_lat"], candidates["stop_lon"]),
    )

    nearest_window = Window.partitionBy("ping_id").orderBy("stop_distance_m")
    nearest = (
        candidates.withColumn("rn", F.row_number().over(nearest_window))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    low_confidence = nearest.filter(F.col("stop_distance_m") > MAX_STOP_DISTANCE_METERS).count()
    if low_confidence:
        logger.warning(json.dumps({
            "event": "low_confidence_stop_matches_dropped",
            "service_date": SERVICE_DATE,
            "count": low_confidence,
            "max_stop_distance_meters": MAX_STOP_DISTANCE_METERS,
        }))
    nearest = nearest.filter(F.col("stop_distance_m") <= MAX_STOP_DISTANCE_METERS)

    result = (
        nearest
        .withColumn("scheduled_arrival_utc", scheduled_arrival_utc_col(F.col("arrival_time"), SERVICE_DATE))
        .withColumn("estimated_arrival_utc", F.col("timestamp_utc"))
        .withColumn(
            "delay_seconds",
            (F.col("estimated_arrival_utc").cast("long") - F.col("scheduled_arrival_utc").cast("long")).cast("int"),
        )
        .withColumn("service_date", F.lit(SERVICE_DATE).cast("date"))
        .select(*SILVER_COLUMNS)
    )

    written = result.count()
    if written:
        (
            result.write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", f"service_date = '{SERVICE_DATE}'")
            .partitionBy("service_date")
            .save(SILVER_TABLE_PATH)
        )

    logger.info(json.dumps({
        "event": "trip_delays_written",
        "service_date": SERVICE_DATE,
        "feed_date_used": str(feed_date),
        "rows_written": written,
        "unmatched_trip_ids": unmatched_count,
        "low_confidence_dropped": low_confidence,
    }))


if __name__ == "__main__":
    main()
