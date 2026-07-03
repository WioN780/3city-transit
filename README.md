# 3city-transit

Real-time transit analytics pipeline: GPS pings flow from a Go poller through
Kafka (Redpanda) into a Spark/Delta lakehouse on MinIO, get aggregated into
hourly performance metrics, and are served to a React dashboard via a Go API.

See [docs/architecture.md](docs/architecture.md) for the full topology
diagram and every schema (Kafka payload, bronze/silver/gold tables, API
responses).

Most of this repo is still a **skeleton** (folder structure, docker-compose
topology, Dockerfiles). Real pieces so far: `ingestion/` polls ZTM
Gdańsk's live GTFS-RT feed and publishes to Kafka; `spark_jobs/bronze/`
consumes that topic into a partitioned Delta table; `spark_jobs/silver/`
downloads/validates the GTFS static schedule and joins it against bronze
to compute per-stop delay and per-vehicle speed; `api/` serves the gold
tables to the dashboard over the `/api/v1/*` contract in
[docs/architecture.md](docs/architecture.md).

## Layout

```
ingestion/      Go GPS poller -- polls ZTM Gdańsk's GTFS-RT feed and
                publishes to Kafka topic gps_raw (implemented)
api/            Go API served to the dashboard -- reads gold.route_performance
                / gold.delay_hotspots (and bronze, for /healthz) straight out
                of MinIO via DuckDB's delta_scan(), no Spark involved (implemented)
spark_jobs/     PySpark bronze/silver/gold jobs
  bronze/       gps_raw (Kafka) -> bronze.gps_positions (Delta/MinIO),
                partitioned by ingest_date/ingest_hour (implemented)
  silver/       GTFS static downloader (gtfs_static.*, partitioned by
                feed_date) and bronze.gps_positions -> silver.trip_delays
                (implemented)
  gold/         stub
airflow/dags/   DAG skeletons for GTFS refresh, streaming health, gold
                aggregation, and data quality checks
dashboard/      React dashboard -- not scaffolded yet
data_quality/   Data quality check scripts -- not implemented yet
docs/           Architecture and schema reference
tests/          Reserved for test suites
```

## Running

```
docker compose up -d --build
```

This starts Redpanda, MinIO, Postgres (Airflow's metadata DB), Airflow
(webserver + scheduler, LocalExecutor), a Spark master + worker, and the
Go API.

Create the Kafka topic manually:

```
docker exec redpanda rpk topic create gps_raw
docker exec redpanda rpk topic list
```

### Running the ingestion service

`ingestion/` is not wired into `docker-compose.yml` yet -- run it standalone
against Redpanda's published port:

```
cd ingestion
KAFKA_BROKERS=localhost:19092 go run .
```

Env vars (all optional): `FEED_URL` (defaults to ZTM Gdańsk's live
vehicle-positions feed), `KAFKA_BROKERS`, `KAFKA_TOPIC` (default `gps_raw`),
`POLL_INTERVAL_SECONDS` (default `30`), `HEALTH_PORT` (default `8091`),
`HTTP_TIMEOUT_SECONDS` (default `10`). Health check: `GET /healthz` on
`HEALTH_PORT`, returning `{status, last_successful_poll_utc,
consecutive_failures}` (`status` is `ok` / `degraded` / `down` depending on
consecutive poll failures).

Run tests: `cd ingestion && go test ./...`.

Note: ZTM's feed doesn't populate `route_id` on every vehicle (only
`trip_id`) and rarely reports `bearing` -- that's an upstream feed trait, not
a mapping bug; `route_id` needs a join against the GTFS static schedule to
backfill reliably, which is what the silver-layer job does downstream.

### Running the bronze streaming job

`spark_jobs/` is mounted into `spark-master`/`spark-worker` at
`/opt/spark_jobs`. Two one-time steps are needed after `docker compose up`
before `spark-submit` will work in this image -- neither persists across
`docker compose down`/`up`, so redo them each time the containers are
recreated:

1. **Create the target MinIO bucket:**
   ```
   docker exec minio mc alias set local http://localhost:9000 minioadmin minioadmin
   docker exec minio mc mb local/lakehouse
   ```

2. **Add a `/etc/passwd` entry for the container's UID**, on both
   `spark-master` and `spark-worker`. `bitnamilegacy/spark:4.0.0` runs as a
   non-root UID with no matching passwd entry (`whoami` fails, `HOME=/`),
   and unlike some other Bitnami images this one does *not* patch that up
   in its entrypoint -- `spark-submit --packages` crashes with
   `LoginException: invalid null input: name` the moment Hadoop's
   `UserGroupInformation` needs a username (e.g. resolving the checkpoint
   filesystem). `/etc/passwd` is root-owned `644`, so this needs `--user
   root`:
   ```
   docker exec --user root spark-master sh -c 'echo "spark:x:$(id -u):0:spark:/tmp:/bin/sh" >> /etc/passwd'
   docker exec --user root spark-worker sh -c 'echo "spark:x:$(id -u):0:spark:/tmp:/bin/sh" >> /etc/passwd'
   ```

Then submit the job:

```
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0,io.delta:delta-spark_2.13:4.0.0 \
  /opt/spark_jobs/bronze/ingest_bronze.py
```

`ingest_bronze.py`'s own `build_spark()` already sets
`spark.hadoop.fs.s3a.threads.keepalivetime` and a few other S3A timeout
configs as plain numbers -- a second, independent version-skew bug where
`hadoop-aws` 3.3.4 (bundled) can't parse the duration-string defaults
(`"60s"`) that ship in this image's newer `hadoop-client` 3.4.1. See the
comment in `ingest_bronze.py` if you hit `NumberFormatException: "60s"`
elsewhere (e.g. in `query_bronze.py`, which sets the same overrides).

Verified against a live run in `local[2]` mode (single JVM, no separate
executor container) with all of the above applied: stream starts, rows
land with `ingest_date=.../ingest_hour=.../` partitioning, and a kill +
restart resumes from the checkpoint (batch counter continues, row count
grows incrementally, no reprocessing from offset 0). Cluster mode
(`spark://spark-master:7077` with a separate `spark-worker` executor) is
wired up the same way but wasn't itself exercised -- executors doing the
S3A write is a different code path from the driver doing it in local mode,
so the same `/etc/passwd` fix likely needs to hold on the worker too, per
above.

Smoke-test row counts:

```
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages io.delta:delta-spark_2.13:4.0.0 \
  /opt/spark_jobs/bronze/query_bronze.py
```

Env vars (all optional): `KAFKA_BOOTSTRAP` (default `redpanda:9092`),
`KAFKA_TOPIC` (default `gps_raw`), `MINIO_ENDPOINT`/`MINIO_ACCESS_KEY`/
`MINIO_SECRET_KEY`, `BRONZE_TABLE_PATH` (default
`s3a://lakehouse/bronze/gps_positions`), `CHECKPOINT_PATH`.

### Running the GTFS static downloader

Same one-time setup as above (bucket + `/etc/passwd` fix), then:

```
docker exec spark-master spark-submit \
  --packages io.delta:delta-spark_2.13:4.0.0 \
  /opt/spark_jobs/silver/download_gtfs_static.py
```

Downloads ZTM's daily GTFS zip, validates required files/columns are
present, and writes `gtfs_static.routes` / `.trips` / `.stop_times` /
`.stops` / `.calendar_dates` as Delta tables partitioned by `feed_date`
(the day the snapshot was downloaded). `calendar_dates` is stored even
though the prompt only named the other four -- ZTM ships no `calendar.txt`,
so it's the only source of which `service_id`s actually run on a given
date, and `build_trip_delays.py` needs that to know which trips are
scheduled "today." Env vars: `GTFS_STATIC_URL`, `FEED_DATE` (default:
today, UTC), `GTFS_STATIC_BASE_PATH`.

Verified against a real download (21MB zip, ~2.2M `stop_times` rows): all
five tables land with correct row counts, and a spot-checked trip's
`stop_times`/`stops`/`trips` rows matched the raw CSV exactly (the CSV
header-count-mismatch warning from `stop_times.txt` carrying 3 more
trailing columns than our schema declares is harmless -- GTFS always
orders `trip_id,arrival_time,departure_time,stop_id,stop_sequence` first,
and positional matching confirmed correct).

### Running the trip-delays job

```
docker exec spark-master spark-submit \
  --packages io.delta:delta-spark_2.13:4.0.0 \
  /opt/spark_jobs/silver/build_trip_delays.py
```

Joins `bronze.gps_positions` (filtered to `ingest_date == SERVICE_DATE`)
against the latest `gtfs_static` snapshot at or before `SERVICE_DATE`, to
produce `silver.trip_delays`. Env vars: `SERVICE_DATE` (default: today,
UTC), `AGENCY_TIMEZONE` (default `Europe/Warsaw`), `MAX_STOP_DISTANCE_METERS`
(default `500`), `SILVER_TABLE_PATH`.

Two non-obvious things worth knowing before reading the code:

- **Realtime `trip_id` always has a trailing `_gps` that the static
  feed's `trip_id` doesn't** (confirmed against a real same-day download,
  not assumed) -- stripped before joining. A `trip_id` that still doesn't
  match any trip scheduled for `SERVICE_DATE` is logged (with samples) and
  excluded; it never fails the job.
- **`scheduled_arrival_utc` is computed from local midnight + raw seconds,
  not by parsing `arrival_time` as a clock string** -- GTFS arrival times
  routinely exceed `24:00:00` for after-midnight trips, which would
  silently parse to `NULL` (and vanish) under a naive
  `to_timestamp(date || arrival_time)` approach.

Verified by construction, not against live accumulated data -- the live
feed returns zero vehicles overnight, so this ran against a synthetic
dataset built from a real downloaded GTFS snapshot (real trip_id, real
stop lat/lon, hand-computed expected delay/speed): an on-time-ish ping
(+300s), the same vehicle's prior ping 60s and ~100m earlier (speed_mps
came out matching the hand-computed haversine value to 10 significant
figures), an unmatched fake `trip_id` (correctly logged and dropped), and
an after-midnight trip's stop (arrival_time `28:16:00`, expected to land
as `02:16:00` the *next* UTC day -- it did, and a 120s-early ping
correctly produced `delay_seconds=-120`, not a sign flip or a dropped
row). Reran for the same `SERVICE_DATE` and confirmed the `replaceWhere`
overwrite left exactly 3 rows, not 6 -- idempotent re-runs, no
duplication.

Every synthetic ping sat exactly on its stop (distance ~= 0) by
construction, to isolate the delay/speed arithmetic. That arithmetic is
rigorously ground-truthed; the nearest-stop *ranking* -- the stop_times
join producing multiple candidate distances per ping, `row_number()`
picking the closest, `MAX_STOP_DISTANCE_METERS` filtering the rest -- was
never exercised against a real spread of candidate distances. Nearest-stop
matching is plain Haversine distance (as asked), not sequence-aware
map-matching -- see the comment on `MAX_STOP_DISTANCE_METERS` in
`build_trip_delays.py` for the known failure mode (a ping nearest a stop
the vehicle already passed, e.g. on a loop route) and why it's a distance
cutoff rather than a fix.

The after-midnight case is verified for the arithmetic only, not the
attribution path end-to-end. The test forced `ingest_date=2026-07-02` on
the 02:14Z ping specifically to isolate the >=24:00:00 scheduled-time
math from `bronze`'s real ingest-date assignment. In an actual streaming
run that ping lands under `ingest_date=2026-07-03` (bronze partitions by
when it's ingested, in UTC, not by service day) -- so on `SERVICE_DATE`
2026-07-02 the `ingest_date == SERVICE_DATE` filter misses it, and on
2026-07-03 its trip's `service_id` is only active for 2026-07-02, so it's
dropped as unmatched either way. The scheduled-time math is proven
correct; the end-to-end attribution of after-midnight trips is proven
broken and undocumented-until-now, not working.

### Running the Go API

```
cd api && go run .
```

Serves `GET /api/v1/routes/worst-offenders?window=7d`,
`GET /api/v1/hotspots?route_id=&window=`, and `GET /api/v1/healthz`,
reading `gold.route_performance` / `gold.delay_hotspots` straight off MinIO
via DuckDB's `delta_scan()` (not a raw Parquet glob -- the gold jobs
`replaceWhere`-overwrite a partition on every hourly rerun without a
`VACUUM`, so old physical Parquet files linger on disk; only the Delta
transaction log that `delta_scan()` reads reflects the current, non-doubled
row set). Query results are cached in-memory for `CACHE_TTL_SECONDS`
(default 60s) since the gold tables only update hourly.

Env vars (all optional, defaults shown):

| Var                              | Default                                                    |
|-----------------------------------|------------------------------------------------------------|
| `PORT`                            | `8090`                                                      |
| `MINIO_ENDPOINT`                  | `minio:9000`                                                |
| `MINIO_ACCESS_KEY`                | `minioadmin`                                                |
| `MINIO_SECRET_KEY`                | `minioadmin`                                                |
| `MINIO_USE_SSL`                   | `false`                                                     |
| `GOLD_ROUTE_PERFORMANCE_SOURCE`   | `delta_scan('s3://lakehouse/gold/route_performance')`       |
| `GOLD_DELAY_HOTSPOTS_SOURCE`      | `delta_scan('s3://lakehouse/gold/delay_hotspots')`          |
| `GTFS_ROUTES_SOURCE`              | `delta_scan('s3://lakehouse/gtfs_static/routes')`           |
| `BRONZE_GPS_SOURCE`               | `delta_scan('s3://lakehouse/bronze/gps_positions')`         |
| `DASHBOARD_ORIGIN`                | `http://localhost:5173` (sent as the CORS allowed origin)   |
| `CACHE_TTL_SECONDS`               | `60`                                                         |

Tests (parse/cache logic plus all three endpoints against fixture Parquet
files in `api/testdata/`, no MinIO required):

```
cd api && go test ./...
```

Verified: `go build`/`go vet`/`go test` all pass (7/7 tests green) against
local Parquet fixtures; `docker build --network=host` (BuildKit disabled)
succeeds and the resulting container starts and stays up without a
missing-library crash (go-duckdb's CGO build links glibc + libstdc++, so
the image is Debian-based rather than this repo's usual Alpine); the
`CREATE SECRET`/`delta_scan` SQL was validated against a real MinIO
instance from a native-host DuckDB session, including reproducing and then
fixing the AWS-metadata-endpoint lookup that the older `SET
s3_endpoint=...` config style triggers.

Not verified in this sandbox, and confirmed to be an environment limitation
rather than a code issue: running this against real gold-layer output from
a live Phase-4 run, and the `hey`/`wrk` load test. Diagnosed directly this
session -- brought up `minio` + `redpanda` via `docker compose`, both
report `healthy`, but no TCP payload crosses the container network
boundary in either direction: the host can't `curl`, `ping`, or complete a
Kafka handshake against a container's published port *or* its bridge IP
directly (connection accepted, zero bytes ever returned, confirmed with
`docker-proxy` correctly listening on the host side), and one compose
container can't reach another by service name either (`redpanda` ->
`minio:9000` inside the compose network also returns nothing). This is a
sandbox-wide restriction on data-plane traffic to/from container IPs, not
specific to this API or its Dockerfile -- retry in a normal Docker
environment (a real machine, CI, etc.), where the SQL/extension/secret
approach validated above should carry straight through to a live MinIO +
Spark-produced Delta table.

Tear down:

```
docker compose down        # add -v to also drop the minio/postgres volumes
```

## Ports

| Service            | URL                          | Notes                          |
|--------------------|-------------------------------|--------------------------------|
| Redpanda (Kafka)   | localhost:19092                | External Kafka API listener    |
| Redpanda Pandaproxy| http://localhost:18082         | REST proxy                     |
| Redpanda Admin     | http://localhost:19644         | Health/admin API               |
| MinIO API          | http://localhost:9000          | S3-compatible endpoint         |
| MinIO Console      | http://localhost:9001          | user: `minioadmin` / `minioadmin` |
| Spark Master UI    | http://localhost:8080          |                                 |
| Spark Worker UI    | http://localhost:8081          |                                 |
| Spark Master RPC   | localhost:7077                 | `spark://spark-master:7077`    |
| Airflow Webserver  | http://localhost:8085          | user: `admin` / `admin`        |
| Go API             | http://localhost:8090/api/v1/healthz | Reads gold tables via DuckDB `delta_scan()` |

Inside the compose network, services address each other by service name
(e.g. `redpanda:9092`, `postgres:5432`, `spark-master:7077`, `minio:9000`).

## Notes

- Image versions are pinned to the latest available at time of writing:
  `redpanda` v26.1.12, `minio` RELEASE.2025-09-07, `postgres` 16.14-alpine,
  `airflow` 2.11.2 (latest 2.x -- deliberately **not** 3.x, which splits
  out a separate API server and changes executor/config wiring; that's a
  re-architecture of this compose stack, not an image bump), `golang`
  1.26-alpine, `alpine` 3.22. Re-verified the full `docker compose up`
  against this repo's known-good baseline after bumping: same services
  healthy, `rpk topic create` still works on v26.1.12 with the existing
  command-line flags. All four DAGs were also parsed directly against the
  2.11.2 image (`python /opt/airflow/dags/<dag>.py` in a throwaway
  container) to confirm the 2.10->2.11 bump doesn't break DAG imports --
  they're trivial DAGs, so this mostly rules out an import-path break, not
  a deep behavioral one. `airflow db migrate`'s Postgres 16->16.14 schema
  compatibility was *not* independently re-verified here -- `airflow-init`
  still fails at the same pre-migration Postgres-connectivity point this
  sandbox's broken bridge network has hit since ticket 1, so this relies on
  Airflow's own supported upgrade path rather than a check run here.
- Spark images come from `bitnamilegacy/spark` -- Bitnami moved current
  `bitnami/spark` tags behind a paid catalog in 2025, so the skeleton pins
  to the last free legacy tag (`4.0.0`, itself already at its latest
  available build as of writing). Revisit if that repo goes away too.
- Airflow uses `LocalExecutor`, which requires Postgres (not SQLite) --
  `airflow-init` runs `airflow db migrate` and creates the `admin` user
  before the webserver/scheduler start.
- No `AIRFLOW__WEBSERVER__SECRET_KEY` is set; fine for a single-node
  skeleton, but set one before running multiple webserver replicas.
- No Redpanda Console / Kafka UI service -- not requested, and the
  acceptance check only needs `rpk topic create`, which works via
  `docker exec`.
