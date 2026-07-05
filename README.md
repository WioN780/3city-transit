# 3city-transit

A real-time transit analytics pipeline for Gdańsk's public transport network:
GPS pings are ingested from the live GTFS-RT feed, streamed through Kafka into
a Spark/Delta lakehouse on MinIO, aggregated into hourly on-time performance
metrics, and surfaced on a React dashboard through a Go API.

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

See [docs/architecture.md](docs/architecture.md) for the full schema
reference (Kafka payload, bronze/silver/gold table layouts, API contract).

## System Architecture & Data Flow

```
+------------------+     +-------------------+
|  GTFS-RT Feed    |     |   GTFS Static     |
| (ZTM Gdańsk API) |     | (ZTM Gdańsk Zip)  |
+--------+---------+     +---------+---------+
         | (30s HTTP poll)         | (Daily HTTP download)
         v                         v
+--------+---------+     +---------+---------+
|    Go Poller     |     |   Airflow DAG     |
| (ingestion serv) |     |  gtfs_static_ref  |
+--------+---------+     +---------+---------+
         | (Produce)               | (Submit Spark Job)
         v                         v
+--------+---------+     +---------+---------+
|     Redpanda     |     |  Spark Master/Wrk |
|  (Kafka: raw)    |     | (gtfs_static.*)   |
+--------+---------+     +---------+---------+
         |                         |
         | (Spark Streaming)       | (Overwrite Delta)
         +----------+     +--------+
                    |     |
                    v     v
+-------------------+-----+------------------+
|                  MinIO Bucket              |
|                   (Lakehouse)              |
|                                            |
|   +------------------------------------+   |
|   | bronze.gps_positions (Delta)       |   v
|   +-----------------+------------------+   |  +--------------------+
|                     |                      |  |     Go Watchdog    |
|                     | (Join & calculate)   |  | (Metadata Polling) |
|                     v                      |  +--------------------+
|   +------------------------------------+   |
|   | silver.trip_delays (Delta)         |   v
|   +-----------------+------------------+   |  +--------------------+
|                     |                      |  | Data Quality checks|
|                     | (Rollup aggregations)|  | (Airflow Spark job)|
|                     v                      |  +--------------------+
|   +------------------------------------+   |
|   | gold.route_performance             |   |
|   | gold.delay_hotspots (Delta)        |   |
|   +-----------------+------------------+   |
+---------------------+----------------------+
                      |
                      | (delta_scan / DuckDB httpfs)
                      v
             +--------+---------+
             |      Go API      |
             |  (api service)   |
             +--------+---------+
                      |
                      | (JSON HTTP REST)
                      v
             +--------+---------+
             | React Dashboard  |
             | (Vite/Nginx serv)|
             +------------------+
```

The system implements a real-time Medallion Lakehouse architecture orchestrated by Airflow and validated by data quality sidecars:

1. **Ingestion Layer**:
   - **Go Ingestion Poller** (`ingestion/`): Polls Gdańsk's GTFS-RT endpoint every 30 seconds, maps protobuf payloads to JSON, and publishes them to the Kafka topic `gps_raw` hosted on **Redpanda**.
2. **Lakehouse Storage Layer (MinIO)**:
   - S3-compatible object store hosting all Delta tables under the `lakehouse` bucket.
   - **Bronze**: Raw, streaming appends in `bronze.gps_positions`.
   - **Silver**: Stop-level delay table `silver.trip_delays`, enriched by joining GPS coordinates with the schedule.
   - **Gold**: High-performance hourly aggregations `gold.route_performance` (leaderboards) and `gold.delay_hotspots` (geographical map grids).
3. **Processing Layer (Spark)**:
   - **Bronze Streaming** (`ingest_bronze.py`): A Structured Streaming job that reads from Kafka, filters malformed JSON payloads, and streams raw pings into the bronze table.
   - **GTFS Static Refresh** (`download_gtfs_static.py`): Spark batch job triggered daily to download, parse, and write GTFS static files (`routes`, `stops`, `stop_times`, `trips`, `calendar_dates`) to MinIO.
   - **Trip Delays** (`build_trip_delays.py`): Joins bronze GPS pings against scheduled stop times using the Haversine formula for spatial alignment and timezone-aware calculations for Warsaw local time, outputting stop-by-stop delay metrics.
   - **Gold rollups** (`build_route_performance.py` / `build_delay_hotspots.py`): Aggregates delay statistics hourly.
4. **Data Quality & Observability (New)**:
   - **Go Watchdog Sidecar** (`data_quality/`): Runs continuously as a lightweight sidecar. It queries MinIO metadata (Delta transaction logs) directly for `bronze.gps_positions` and issues warnings or webhook alerts if the table has not received updates for over 5 minutes.
   - **Data Quality Checks DAG** (`spark_jobs/data_quality/check_trip_delays.py`): Runs as an Airflow task checking `silver.trip_delays` for schema drift, null rates on critical columns, and delay-value sanity bounds (e.g. flagging impossible negative or extreme delays).
5. **Serving & Presentation Layer**:
   - **Go API** (`api/`): Serving Go REST service using embedded **DuckDB**'s `delta_scan` over S3/MinIO to query gold tables at high speed with low overhead, fronted by an in-memory TTL cache.
   - **React Dashboard** (`dashboard/`): Nginx-served frontend rendering live leaderboards, interactive maps of delay hotspots, and route on-time performance charts.

## Features

- **Live Ingestion** — a Go poller pulls ZTM Gdańsk's real-time vehicle positions and publishes them to Kafka.
- **Medallion Lakehouse** — Spark Structured Streaming lands raw GPS into Delta; scheduled batch jobs compute delay metrics against the static schedule.
- **Go API with DuckDB** — Serves aggregated results directly out of MinIO using DuckDB's in-memory engine, bypassing the need for a persistent Spark cluster for reads.
- **React Dashboard** — Responsive worst-performing-routes leaderboard, Leaflet delay-hotspot map, and route timeseries.
- **Automatic Data Quality Watchdog** — Independent Go sidecar alerting if raw data ingestion stops.
- **Airflow Quality Gates** — Validates schema, null rates, and metrics on silver data before gold ingestion.

## Tech stack

| Layer          | Technology                                          |
|----------------|------------------------------------------------------|
| Ingestion      | Go, GTFS-RT (protobuf)                                |
| Streaming      | Kafka (Redpanda)                                      |
| Processing     | PySpark Structured Streaming + batch, Delta Lake      |
| Storage        | MinIO (S3-compatible object storage)                  |
| Orchestration  | Apache Airflow (LocalExecutor + Postgres)             |
| API            | Go, DuckDB (`delta_scan`)                             |
| Dashboard      | React, Vite, react-leaflet, recharts                  |
| Quality/Obs.   | Go Watchdog Sidecar, PySpark Quality Asserts         |
| Infra          | Docker Compose                                        |

## Repository layout

```
ingestion/      Go GPS poller -- polls live GTFS-RT feed and publishes to Kafka
api/            Go API served to the dashboard -- reads gold tables via DuckDB delta_scan
spark_jobs/     PySpark jobs (bronze streaming, silver delays, gold rollup, data quality)
  bronze/       gps_raw (Kafka) -> bronze.gps_positions (Delta/MinIO)
  silver/       GTFS downloader and bronze.gps_positions -> silver.trip_delays
  gold/         silver.trip_delays -> gold tables
  data_quality/ check_trip_delays.py assertions (schema, nulls, sanity bounds)
airflow/dags/   GTFS refresh, hourly gold aggregation, streaming health check, and data-quality checks
dashboard/      React (Vite) dashboard served via Nginx in Docker Compose
data_quality/   Go watchdog sidecar daemon -- monitors bronze MinIO table metadata freshness
docs/           Architecture and schema reference
tests/          PySpark delay-calculation unit test suite (timezone DST, early arrivals, trip filtering)
```

## Getting started & 5-minute demo

Prerequisites: Docker + Docker Desktop, Go 1.23+ and Node 20+ (only if running locally outside Docker).

### 1. Bring up the Stack
Build and launch all services from a clean state (Redpanda, MinIO, Postgres, Airflow, Spark master/worker, Go API, React dashboard, Go Watchdog):

```bash
docker compose up -d --build
```

### 2. Run One-Time Configuration
Redo these setup commands whenever the Compose containers are completely recreated (they do not persist across `docker compose down -v`):

```bash
# Create the raw Kafka topic
docker exec redpanda rpk topic create gps_raw

# Setup the MinIO CLI alias and create the lakehouse bucket
docker exec minio mc alias set local http://localhost:9000 minioadmin minioadmin
docker exec minio mc mb local/lakehouse

# Patch passwd files to resolve Spark's local username lookups in container
docker exec --user root spark-master sh -c "echo 'spark:x:1001:0:spark:/tmp:/bin/sh' >> /etc/passwd"
docker exec --user root spark-worker sh -c "echo 'spark:x:1001:0:spark:/tmp:/bin/sh' >> /etc/passwd"
```

### 3. Run the E2E Pipeline (5-Minute Demo)

Once the stack is configured, execute these steps in order to stream, clean, check, and serve transit data:

*Note: ZTM's public feed only reports vehicles during actual service hours (approx. 5:00 AM - 11:59 PM Warsaw time).*

**Step A: Start the Bronze streaming job** (runs indefinitely in the background to capture Kafka stream into Delta):
```bash
docker exec -d spark-master spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0,io.delta:delta-spark_2.13:4.0.0 /opt/spark_jobs/bronze/ingest_bronze.py
```

**Step B: Trigger the GTFS Static Download** (downloads the transit routes/schedule that silver joins against):
```bash
docker exec airflow-webserver airflow dags unpause gtfs_static_refresh
docker exec airflow-webserver airflow dags trigger gtfs_static_refresh
```
*Wait a few seconds for this to complete. You can monitor it in the Airflow UI at http://localhost:8085 (credentials: `admin` / `admin`).*

**Step C: Build the Silver Trip Delays** (once bronze has ingested some GPS pings, run the clean and match job):
```bash
docker exec spark-master spark-submit --packages io.delta:delta-spark_2.13:4.0.0 /opt/spark_jobs/silver/build_trip_delays.py
```

**Step D: Run the Data Quality Checks** (runs the PySpark quality gates to validate silver schema and bounds):
```bash
docker exec airflow-webserver airflow dags unpause data_quality_checks
docker exec airflow-webserver airflow dags trigger data_quality_checks
```

**Step E: Run the Gold Rollups** (aggregates silver delays into performance stats and hot spots):
```bash
docker exec airflow-webserver airflow dags unpause hourly_gold_aggregation
docker exec airflow-webserver airflow dags trigger hourly_gold_aggregation
```

**Step F: Verify the pipeline outputs**
Verify the Go API serves the gold route statistics:
```bash
curl http://localhost:8090/api/v1/routes/worst-offenders
```
Then open the frontend React dashboard at http://localhost:5173 to interact with the performance charts, worst-performing leaderboards, and Leaflet delay maps.

### 4. Running the PySpark Unit Tests
To run the PySpark delay calculation unit tests (testing Warsaw DST transitions, early-arrival delays, and missing trip filters) inside the Spark master environment:

```bash
docker exec --user root spark-master python /opt/tests/test_delay_calculation.py
```

## Component reference

### Ingestion (`ingestion/`)

```
cd ingestion
go run .
```

Env vars (all optional): `FEED_URL` (defaults to ZTM Gdańsk's live
vehicle-positions feed), `KAFKA_BROKERS`, `KAFKA_TOPIC` (default `gps_raw`),
`POLL_INTERVAL_SECONDS` (default `30`), `HEALTH_PORT` (default `8091`),
`HTTP_TIMEOUT_SECONDS` (default `10`). Health check: `GET /healthz` on
`HEALTH_PORT`, returning `{status, last_successful_poll_utc,
consecutive_failures}`.

Tests: `go test ./...` (run from `ingestion/`).

The upstream feed doesn't populate `route_id` on every vehicle (only
`trip_id`) and rarely reports `bearing` — `route_id` is backfilled downstream
by the silver-layer join against the GTFS static schedule.

### Bronze streaming job (`spark_jobs/bronze/`)

See step 2 above for the run command. Smoke-test row counts once it's been
running for a bit:

```
docker exec spark-master spark-submit --master spark://spark-master:7077 --packages io.delta:delta-spark_2.13:4.0.0 /opt/spark_jobs/bronze/query_bronze.py
```

Env vars (all optional): `KAFKA_BOOTSTRAP` (default `redpanda:9092`),
`KAFKA_TOPIC` (default `gps_raw`), `MINIO_ENDPOINT` / `MINIO_ACCESS_KEY` /
`MINIO_SECRET_KEY`, `BRONZE_TABLE_PATH` (default
`s3a://lakehouse/bronze/gps_positions`), `CHECKPOINT_PATH`.

### GTFS static downloader (`spark_jobs/silver/download_gtfs_static.py`)

Run via the `gtfs_static_refresh` Airflow DAG (step 3), or manually:

```
docker exec spark-master spark-submit --packages io.delta:delta-spark_2.13:4.0.0 /opt/spark_jobs/silver/download_gtfs_static.py
```

Downloads ZTM's daily GTFS zip, validates required files/columns, and writes
`gtfs_static.routes` / `.trips` / `.stop_times` / `.stops` / `.calendar_dates`
as Delta tables partitioned by `feed_date`. `calendar_dates` is stored even
though ZTM ships no `calendar.txt` — it's the only source of which
`service_id`s run on a given date, which the trip-delays job needs to know
what's scheduled "today." Env vars: `GTFS_STATIC_URL`, `FEED_DATE` (default:
today, UTC), `GTFS_STATIC_BASE_PATH`.

### Trip-delays job (`spark_jobs/silver/build_trip_delays.py`)

Run via step 4, or manually with the same command. Joins
`bronze.gps_positions` (filtered to `ingest_date == SERVICE_DATE`) against the
latest `gtfs_static` snapshot at or before `SERVICE_DATE` to produce
`silver.trip_delays`. Env vars: `SERVICE_DATE` (default: today, UTC),
`AGENCY_TIMEZONE` (default `Europe/Warsaw`), `MAX_STOP_DISTANCE_METERS`
(default `500`), `SILVER_TABLE_PATH`.

Worth knowing before reading the code:

- The realtime feed's `trip_id` always carries a trailing `_gps` that the
  static feed's `trip_id` doesn't — stripped before joining. A `trip_id` that
  still doesn't match anything scheduled for `SERVICE_DATE` is logged and
  excluded rather than failing the job.
- `scheduled_arrival_utc` is computed from local midnight + raw seconds, not
  by parsing `arrival_time` as a clock string — GTFS arrival times routinely
  exceed `24:00:00` for after-midnight trips, which a naive
  `to_timestamp(date || arrival_time)` would silently turn into `NULL`.
- Nearest-stop matching is plain Haversine distance, not sequence-aware
  map-matching, bounded by `MAX_STOP_DISTANCE_METERS` — a ping nearest a stop
  the vehicle already passed (e.g. on a loop route) can be misattributed.
- Gold and silver both overwrite by partition (`replaceWhere`), so re-running
  either for the same date is safe and idempotent.

### Gold aggregation (`spark_jobs/gold/`)

Run via the `hourly_gold_aggregation` Airflow DAG (step 5), which re-runs
`build_route_performance.py` and `build_delay_hotspots.py` for today's service
date every hour so newly arrived silver rows get picked up.

### Go API (`api/`)

```
cd api
go run .
```

This package uses cgo (`go-duckdb`), so it needs a C compiler on `PATH`. On
Linux/macOS that's usually already present (`gcc`/`clang`); on Windows,
install a MinGW-w64 toolchain (e.g. via `winget install -e --id
MSYS2.MSYS2`, then add its `mingw64/bin` to `PATH`) or run it inside Docker
instead, where the image already has one:

```
docker compose up -d --build api
```

Serves:

- `GET /api/v1/routes/worst-offenders?window=7d`
- `GET /api/v1/routes/{route_id}/timeseries?window=7d`
- `GET /api/v1/hotspots?route_id=&window=`
- `GET /api/v1/healthz`

Reads `gold.route_performance` / `gold.delay_hotspots` straight off MinIO via
DuckDB's `delta_scan()` — not a raw Parquet glob, since the gold jobs
overwrite a partition on every hourly rerun without a `VACUUM`, so stale
physical Parquet files linger on disk; only the Delta transaction log
reflects the current row set. Query results are cached in-memory for
`CACHE_TTL_SECONDS` (default 60s) since the gold tables only update hourly.

Env vars (all optional, defaults shown):

| Var                             | Default                                                  |
|----------------------------------|------------------------------------------------------------|
| `PORT`                           | `8090`                                                      |
| `MINIO_ENDPOINT`                 | `minio:9000`                                                |
| `MINIO_ACCESS_KEY`               | `minioadmin`                                                |
| `MINIO_SECRET_KEY`               | `minioadmin`                                                |
| `MINIO_USE_SSL`                  | `false`                                                     |
| `GOLD_ROUTE_PERFORMANCE_SOURCE`  | `delta_scan('s3://lakehouse/gold/route_performance')`       |
| `GOLD_DELAY_HOTSPOTS_SOURCE`     | `delta_scan('s3://lakehouse/gold/delay_hotspots')`          |
| `GTFS_ROUTES_SOURCE`             | `delta_scan('s3://lakehouse/gtfs_static/routes')`           |
| `BRONZE_GPS_SOURCE`              | `delta_scan('s3://lakehouse/bronze/gps_positions')`         |
| `DASHBOARD_ORIGIN`               | `http://localhost:5173` (sent as the CORS allowed origin)   |
| `CACHE_TTL_SECONDS`              | `60`                                                         |

Tests (parse/cache logic plus all four endpoints against fixture Parquet
files in `api/testdata/`, no MinIO required):

```
cd api
go test ./...
```

Same cgo/compiler requirement as above applies to running the tests.

### Dashboard (`dashboard/`)

```
cd dashboard
npm install
npm run dev                # http://localhost:5173
```

Talks to the API at `VITE_API_BASE_URL` (see `dashboard/.env.example`,
defaults to `http://localhost:8090`). Also built and served via
`dashboard/Dockerfile` (Vite build → nginx) as the `dashboard` service in
`docker-compose.yml`; since the browser calls the API directly rather than
through the dashboard container, `VITE_API_BASE_URL` needs to stay a
host-reachable URL rather than the in-network `api` service name.

## Tearing down

```
docker compose down        # add -v to also drop the minio/postgres volumes
```

## Ports

| Service             | URL                                    | Notes                                        |
|---------------------|------------------------------------------|-----------------------------------------------|
| Redpanda (Kafka)    | `localhost:19092`                        | External Kafka API listener                    |
| Redpanda Pandaproxy | http://localhost:18082                   | REST proxy                                     |
| Redpanda Admin      | http://localhost:19644                   | Health/admin API                               |
| MinIO API           | http://localhost:9000                    | S3-compatible endpoint                         |
| MinIO Console       | http://localhost:9001                    | user: `minioadmin` / `minioadmin`              |
| Spark Master UI     | http://localhost:8080                    |                                                 |
| Spark Worker UI     | http://localhost:8081                    |                                                 |
| Spark Master RPC    | `localhost:7077`                         | `spark://spark-master:7077`                    |
| Airflow Webserver   | http://localhost:8085                    | user: `admin` / `admin`                        |
| Go API              | http://localhost:8090/api/v1/healthz     | Reads gold tables via DuckDB `delta_scan()`    |
| Dashboard           | http://localhost:5173                    | React (Vite) dashboard, served via nginx        |

Inside the Compose network, services address each other by service name
(e.g. `redpanda:9092`, `postgres:5432`, `spark-master:7077`, `minio:9000`).

## Known limitations

- The bronze streaming job has been verified in `local[2]` mode; cluster mode
  (a separate `spark-worker` executor) is wired up the same way but not yet
  independently exercised.
- After-midnight trip attribution has a known edge case: bronze partitions by
  UTC ingest time, not service date, so a post-midnight ping's `SERVICE_DATE`
  filter can miss it in the trip-delays join. The delay/speed arithmetic
  itself is correct; the cross-partition attribution isn't yet.
- `data_quality/` and the `data_quality_checks` DAG are skeletons — no checks
  are implemented yet.
- No `AIRFLOW__WEBSERVER__SECRET_KEY` is set; fine for a single-node setup,
  but set one before running multiple webserver replicas.
- Airflow runs on 2.11.x (LocalExecutor + Postgres) rather than 3.x, which
  would require re-architecting the executor/API-server wiring.
- Spark images come from `bitnamilegacy/spark` (`4.0.0`), since Bitnami moved
  current `bitnami/spark` tags behind a paid catalog in 2025.
