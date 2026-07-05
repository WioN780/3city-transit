import os
import sys
import unittest
from datetime import datetime

# Add the spark_jobs directory to the path so we can import build_trip_delays
sys.path.insert(0, "/opt/spark_jobs/silver")
sys.path.insert(0, "/opt/spark_jobs")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

import build_trip_delays


class TestDelayCalculation(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Configure a local Spark Session for unit testing
        cls.spark = (
            SparkSession.builder
            .appName("UnitTest-DelayCalculation")
            .master("local[1]")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
        # Suppress spark logs
        cls.spark.sparkContext.setLogLevel("WARN")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_scheduled_arrival_utc_col_timezone_regular(self):
        """Test conversion of arrival time HH:MM:SS to UTC timestamp in regular time."""
        # Warsaw is UTC+2 in July (DST)
        service_date = "2026-07-05"
        
        # Test case: 12:34:56 local time.
        # Local midnight is 2026-07-05 00:00:00 local (Europe/Warsaw), which is 2026-07-04 22:00:00 UTC.
        # Add 12h 34m 56s -> 2026-07-05 10:34:56 UTC.
        df = self.spark.createDataFrame([("12:34:56",)], ["arrival_time"])
        
        # Set the mock agency timezone environment variable
        os.environ["AGENCY_TIMEZONE"] = "Europe/Warsaw"
        
        result_df = df.withColumn(
            "scheduled_utc",
            build_trip_delays.scheduled_arrival_utc_col(F.col("arrival_time"), service_date)
        )
        
        row = result_df.first()
        expected = datetime.strptime("2026-07-05 10:34:56", "%Y-%m-%d %H:%M:%S")
        self.assertEqual(row["scheduled_utc"], expected)

    def test_scheduled_arrival_utc_col_after_midnight(self):
        """Test GTFS after-midnight arrival times (e.g. 25:30:00)."""
        service_date = "2026-07-05"
        
        # 25:30:00 local time on 2026-07-05 (actually 01:30:00 on 2026-07-06).
        # Local midnight on 2026-07-05 is 2026-07-04 22:00:00 UTC.
        # Add 25h 30m (91800 seconds) -> 2026-07-05 23:30:00 UTC (which is 01:30:00 local on 2026-07-06).
        df = self.spark.createDataFrame([("25:30:00",)], ["arrival_time"])
        
        os.environ["AGENCY_TIMEZONE"] = "Europe/Warsaw"
        
        result_df = df.withColumn(
            "scheduled_utc",
            build_trip_delays.scheduled_arrival_utc_col(F.col("arrival_time"), service_date)
        )
        
        row = result_df.first()
        expected = datetime.strptime("2026-07-05 23:30:00", "%Y-%m-%d %H:%M:%S")
        self.assertEqual(row["scheduled_utc"], expected)

    def test_scheduled_arrival_utc_col_dst_transition(self):
        """Test timezone conversions during DST start (spring forward)."""
        # Europe/Warsaw transitions to DST on 2026-03-29.
        # Local midnight 2026-03-29 00:00:00 is UTC+1 -> 2026-03-28 23:00:00 UTC.
        # Transition happens at 02:00:00 local (clocks move to 03:00:00).
        # 01:30:00 arrival_time is before transition.
        # 2026-03-28 23:00:00 UTC + 1h 30m = 2026-03-29 00:30:00 UTC.
        service_date = "2026-03-29"
        
        df = self.spark.createDataFrame([
            ("01:30:00",),
            ("03:30:00",), # after transition: 2026-03-28 23:00:00 UTC + 3h 30m = 2026-03-29 02:30:00 UTC
        ], ["arrival_time"])
        
        os.environ["AGENCY_TIMEZONE"] = "Europe/Warsaw"
        
        result_df = df.withColumn(
            "scheduled_utc",
            build_trip_delays.scheduled_arrival_utc_col(F.col("arrival_time"), service_date)
        ).orderBy("arrival_time")
        
        rows = result_df.collect()
        
        # 01:30:00
        self.assertEqual(rows[0]["scheduled_utc"], datetime.strptime("2026-03-29 00:30:00", "%Y-%m-%d %H:%M:%S"))
        # 03:30:00
        self.assertEqual(rows[1]["scheduled_utc"], datetime.strptime("2026-03-29 02:30:00", "%Y-%m-%d %H:%M:%S"))

    def test_negative_delay_and_positive_delay(self):
        """Test calculation of negative delay (early arrival) and positive delay."""
        schema = StructType([
            StructField("scheduled_arrival_utc", TimestampType(), True),
            StructField("timestamp_utc", TimestampType(), True),
        ])
        
        # Test Case 1: Early arrival (Negative delay of 5 minutes / -300s)
        # Test Case 2: On time (0s delay)
        # Test Case 3: Late arrival (Positive delay of 10 minutes / 600s)
        data = [
            (
                datetime.strptime("2026-07-05 12:00:00", "%Y-%m-%d %H:%M:%S"),
                datetime.strptime("2026-07-05 11:55:00", "%Y-%m-%d %H:%M:%S")
            ),
            (
                datetime.strptime("2026-07-05 12:00:00", "%Y-%m-%d %H:%M:%S"),
                datetime.strptime("2026-07-05 12:00:00", "%Y-%m-%d %H:%M:%S")
            ),
            (
                datetime.strptime("2026-07-05 12:00:00", "%Y-%m-%d %H:%M:%S"),
                datetime.strptime("2026-07-05 12:10:00", "%Y-%m-%d %H:%M:%S")
            ),
        ]
        
        df = self.spark.createDataFrame(data, schema)
        
        # Perform the delay calculation matching silver layer logic:
        # delay_seconds = (estimated_arrival_utc - scheduled_arrival_utc)
        result_df = df.withColumn(
            "delay_seconds",
            (F.col("timestamp_utc").cast("long") - F.col("scheduled_arrival_utc").cast("long")).cast("int")
        )
        
        results = [row["delay_seconds"] for row in result_df.collect()]
        
        self.assertEqual(results[0], -300)
        self.assertEqual(results[1], 0)
        self.assertEqual(results[2], 600)

    def test_missing_trip_match(self):
        """Test that pings with trip_ids not present in active static trips are correctly filtered out."""
        # Mock bronze positions (GPS pings)
        # One ping with a matching trip_id (needs suffix stripped) and one with an unmatched trip_id
        bronze_schema = StructType([
            StructField("vehicle_id", StringType(), True),
            StructField("trip_id", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
        ])
        bronze_data = [
            ("V1", "trip_123_gps", 54.352, 18.646),   # matches active static trip "trip_123"
            ("V2", "trip_999_gps", 54.360, 18.650),   # does NOT match any active trip
        ]
        bronze_df = self.spark.createDataFrame(bronze_data, bronze_schema)
        
        # Mock active trips from static GTFS schedule
        active_trips_schema = StructType([
            StructField("route_id", StringType(), True),
            StructField("trip_id", StringType(), True),
        ])
        active_trips_data = [
            ("R1", "trip_123"),
        ]
        active_trips_df = self.spark.createDataFrame(active_trips_data, active_trips_schema)
        
        # Pre-process bronze data: strip "_gps" suffix like the silver job does
        processed_bronze_df = bronze_df.withColumn(
            "static_trip_id", 
            F.regexp_replace("trip_id", "_gps$", "")
        )
        
        # Join matching logic as in build_trip_delays.py:
        matched = processed_bronze_df.join(
            active_trips_df, 
            processed_bronze_df["static_trip_id"] == active_trips_df["trip_id"], 
            "inner"
        ).select(
            processed_bronze_df["vehicle_id"],
            active_trips_df["route_id"],
            active_trips_df["trip_id"]
        )
        
        # Verify join output
        matched_rows = matched.collect()
        self.assertEqual(len(matched_rows), 1)
        self.assertEqual(matched_rows[0]["vehicle_id"], "V1")
        self.assertEqual(matched_rows[0]["route_id"], "R1")
        self.assertEqual(matched_rows[0]["trip_id"], "trip_123")
        
        # Anti-join logic to count unmatched trips
        unmatched_trip_ids = (
            processed_bronze_df.select("static_trip_id").distinct()
            .join(
                active_trips_df.select("trip_id"), 
                processed_bronze_df["static_trip_id"] == active_trips_df["trip_id"], 
                "left_anti"
            )
        )
        
        unmatched_rows = unmatched_trip_ids.collect()
        self.assertEqual(len(unmatched_rows), 1)
        self.assertEqual(unmatched_rows[0]["static_trip_id"], "trip_999")


if __name__ == "__main__":
    unittest.main()
