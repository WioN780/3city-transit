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

## Features

- **Live ingestion** — a Go poller pulls ZTM Gdańsk's real-time vehicle
  positions every 30 seconds and publishes them to Kafka.
- **Streaming + batch lakehouse** — Spark Structured Streaming lands raw GPS
  into a partitioned Delta table; scheduled batch jobs join it against the
  GTFS static schedule to compute per-stop delay and per-vehicle speed, then
  roll that up into hourly route-performance and delay-hotspot aggregates.
- **Go API** — reads the gold tables directly out of MinIO with DuckDB's
  `delta_scan()` (no Spark cluster needed to serve reads), with an in-memory
  TTL cache in front of it.
- **React dashboard** — a worst-performing-routes leaderboard, a delay-hotspot
  map, and an on-time % time series for a selected route, all backed by live
  API data.
- **Orchestration** — Airflow schedules the GTFS refresh, hourly gold
  aggregation, a streaming-health monitor, and a data-quality check skeleton.

## Tech stack

| Layer          | Technology                                          |
|----------------|------------------------------------------------------|
| Ingestion      | Go, GTFS-RT (protobuf)                                |
| Streaming      | Kafka (Redpanda)                                      |
| Processing     | PySpark Structured Streaming + batch, Delta Lake      |
| Storage        | MinIO (S3-compatible object storage)                  |
| Orchestration  | Apache Airflow (LocalExecutor)                        |
| API            | Go, DuckDB (`delta_scan`)                             |
| Dashboard      | React, Vite, react-leaflet, recharts                  |
| Infra          | Docker Compose                                        |

## Repository layout

```
ingestion/      Go GPS poller -- polls ZTM Gdańsk's GTFS-RT feed and
                publishes to Kafka topic gps_raw
api/            Go API served to the dashboard -- reads gold.route_performance
                / gold.delay_hotspots (and bronze, for /healthz) straight out
                of MinIO via DuckDB's delta_scan(), no Spark involved
spark_jobs/     PySpark bronze/silver/gold jobs
  bronze/       gps_raw (Kafka) -> bronze.gps_positions (Delta/MinIO),
                partitioned by ingest_date/ingest_hour
  silver/       GTFS static downloader (gtfs_static.*, partitioned by
                feed_date) and bronze.gps_positions -> silver.trip_delays
  gold/         silver.trip_delays -> gold.route_performance /
                gold.delay_hotspots
airflow/dags/   GTFS refresh, hourly gold aggregation, streaming health
                check, and a data-quality check skeleton
dashboard/      React (Vite) dashboard -- leaderboard, hotspot map, and
                per-route time series against the api/ contract
data_quality/   Reserved for standalone data-quality check scripts
docs/           Architecture and schema reference
tests/          Reserved for test suites
```

## Getting started

Prerequisites: Docker + Docker Compose, Go 1.24+, Node 20+ (only needed for
local dev outside Docker).

```
docker compose up -d --build
```

This builds and starts Redpanda, MinIO, Postgres (Airflow's metadata DB),
Airflow (webserver + scheduler), a Spark master + worker, the Go API, and the
dashboard.

One-time setup after the first `up` (these don't persist across
`docker compose down`, so redo them whenever containers are recreated):

```
docker exec redpanda rpk topic create gps_raw
docker exec minio mc alias set local http://localhost:9000 minioadmin minioadmin
docker exec minio mc mb local/lakehouse
docker exec --user root spark-master sh -c "echo 'spark:x:1001:0:spark:/tmp:/bin/sh' >> /etc/passwd"
docker exec --user root spark-worker sh -c "echo 'spark:x:1001:0:spark:/tmp:/bin/sh' >> /etc/passwd"
```

(The `/etc/passwd` entry works around `bitnamilegacy/spark:4.0.0` running as a
UID with no matching passwd entry, which otherwise crashes `spark-submit
--packages` with `LoginException: invalid null input: name` the moment Hadoop
needs to resolve a username. `1001` is that image's fixed UID — confirm with
`docker exec spark-master id -u` if it ever changes. The command is written
without `$(id -u)` so it runs unmodified in both bash and PowerShell, since
PowerShell's native-argument parsing splits on double quotes rather than
single quotes and mangles the substitution form.)

## Running the full pipeline (5-minute demo)

The dashboard reads from the gold tables, which only get populated once data
has flowed all the way through bronze and silver. Bring up the stack and the
one-time setup above, then, in order:

**1. Start the ingestion poller** (not wired into Compose — it polls a public
external feed, so it's kept standalone):

Linux/macOS (bash):

```
cd ingestion
KAFKA_BROKERS=localhost:19092 go run .
```

Windows (PowerShell):

```powershell
cd ingestion
$env:KAFKA_BROKERS = "localhost:19092"
go run .
```

**2. Start the bronze streaming job** (long-running — leave it in its own
terminal):

```
docker exec spark-master spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0,io.delta:delta-spark_2.13:4.0.0 /opt/spark_jobs/bronze/ingest_bronze.py
```

**3. Trigger the GTFS static refresh DAG** (downloads the schedule that
silver joins against):

```
docker exec airflow-webserver airflow dags unpause gtfs_static_refresh
docker exec airflow-webserver airflow dags trigger gtfs_static_refresh
```

Or from the Airflow UI at http://localhost:8085 (`admin` / `admin`): find
`gtfs_static_refresh`, toggle it on, click the ▶ trigger button.

**4. Run the silver trip-delays job**, once bronze has accumulated some data:

```
docker exec spark-master spark-submit --packages io.delta:delta-spark_2.13:4.0.0 /opt/spark_jobs/silver/build_trip_delays.py
```

**5. Trigger the hourly gold aggregation DAG**:

```
docker exec airflow-webserver airflow dags unpause hourly_gold_aggregation
docker exec airflow-webserver airflow dags trigger hourly_gold_aggregation
```

**6. Check the result:**

```
curl http://localhost:8090/api/v1/routes/worst-offenders
```

then open the dashboard at http://localhost:5173 — the leaderboard, hotspot
map, and time series should now show live data.

Notes on timing: ZTM's feed only reports vehicles during real service hours,
so step 1 produces nothing overnight. Steps 3–5 are idempotent (gold and
silver both overwrite by partition), so re-running them after more data has
landed is always safe and is exactly what `hourly_gold_aggregation`'s
`@hourly` schedule does automatically once unpaused.

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
