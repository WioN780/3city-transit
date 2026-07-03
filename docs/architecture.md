# Architecture

## Topology

```
Go poller (30s cron) -> Kafka topic gps_raw
                              |
                    PySpark Structured Streaming
                              |
                     bronze.gps_positions (Delta/MinIO)
                              |
                  PySpark batch (Airflow-triggered)
                              |
                      silver.trip_delays
                              |
                  PySpark batch (Airflow-triggered)
                              |
              gold.route_performance / gold.delay_hotspots
                              |
                    Go API (DuckDB reads from MinIO)
                              |
                       React dashboard
```

Airflow orchestrates: daily GTFS static refresh, streaming health checks,
hourly gold aggregation, data quality checks.

## Schemas

### Kafka topic: `gps_raw`

```json
{
  "vehicle_id": "string",
  "route_id": "string",
  "trip_id": "string",
  "latitude": "float",
  "longitude": "float",
  "bearing": "float | null",
  "speed_mps": "float | null",
  "timestamp_utc": "ISO-8601 string",
  "feed_sequence": "int"
}
```

### Bronze: `bronze.gps_positions`

Same fields as `gps_raw`, plus:

```json
{
  "ingest_timestamp_utc": "ISO-8601 string",
  "ingest_date": "date (partition key)",
  "ingest_hour": "int (partition key)"
}
```

### Silver: `silver.trip_delays`

```json
{
  "vehicle_id": "string",
  "route_id": "string",
  "trip_id": "string",
  "stop_id": "string",
  "scheduled_arrival_utc": "ISO-8601 string",
  "estimated_arrival_utc": "ISO-8601 string",
  "delay_seconds": "int",
  "speed_mps": "float",
  "latitude": "float",
  "longitude": "float",
  "service_date": "date (partition key)"
}
```

### Gold: `gold.route_performance` (hourly grain)

```json
{
  "route_id": "string",
  "service_date": "date",
  "hour_of_day": "int",
  "day_of_week": "string",
  "on_time_pct": "float",
  "avg_delay_seconds": "float",
  "p90_delay_seconds": "float",
  "avg_speed_mps": "float",
  "sample_count": "int"
}
```

### Gold: `gold.delay_hotspots`

```json
{
  "route_id": "string",
  "latitude_bucket": "float",
  "longitude_bucket": "float",
  "avg_delay_seconds": "float",
  "incident_count": "int",
  "service_date": "date"
}
```

### Go API responses

```
GET /api/v1/routes/worst-offenders?window=7d
{
  "window": "7d",
  "routes": [
    { "route_id": "string", "route_name": "string", "on_time_pct": "float", "avg_delay_seconds": "float", "rank": "int" }
  ]
}

GET /api/v1/hotspots?route_id=&window=
{
  "hotspots": [
    { "latitude": "float", "longitude": "float", "avg_delay_seconds": "float", "incident_count": "int" }
  ]
}

GET /api/v1/healthz
{ "status": "ok|degraded|down", "kafka_lag": "int", "last_gps_ingest_utc": "ISO-8601 string" }
```
