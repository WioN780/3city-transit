package main

import (
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2"
)

// openDuckDB opens an in-process DuckDB handle and wires it up to read
// Delta/Parquet straight out of MinIO: httpfs gives DuckDB an S3
// filesystem, delta lets delta_scan() resolve the transaction log instead
// of globbing raw Parquet files (the gold jobs overwrite a partition with
// replaceWhere on every hourly rerun -- the old files stay on disk until a
// VACUUM that never runs, so a raw-Parquet glob double-counts every
// reprocessed partition; only log-aware delta_scan reads the current rows).
func openDuckDB(cfg config) (*sql.DB, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}

	stmts := []string{
		"INSTALL httpfs",
		"LOAD httpfs",
		"INSTALL delta",
		"LOAD delta",
		fmt.Sprintf(`CREATE SECRET minio (
			TYPE S3,
			KEY_ID %s,
			SECRET %s,
			ENDPOINT %s,
			URL_STYLE 'path',
			USE_SSL %t
		)`, quoteLiteral(cfg.minioAccessKey), quoteLiteral(cfg.minioSecretKey), quoteLiteral(cfg.minioEndpoint), cfg.minioUseSSL),
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			db.Close()
			return nil, fmt.Errorf("duckdb setup %q: %w", s, err)
		}
	}
	return db, nil
}

// quoteLiteral escapes a SQL string literal for CREATE SECRET, whose
// grammar doesn't accept bound (?) parameters like ordinary DML does.
func quoteLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

type routePerformanceRow struct {
	RouteID     string  `json:"route_id"`
	RouteName   string  `json:"route_name"`
	OnTimePct   float64 `json:"on_time_pct"`
	AvgDelaySec float64 `json:"avg_delay_seconds"`
	Rank        int     `json:"rank"`
}

func fetchWorstOffenders(db *sql.DB, cfg config, sinceDate time.Time) ([]routePerformanceRow, error) {
	rows, err := db.Query(fmt.Sprintf(`
		SELECT route_id,
		       SUM(on_time_pct * sample_count) AS on_time_weighted_sum,
		       SUM(avg_delay_seconds * sample_count) AS delay_weighted_sum,
		       SUM(sample_count) AS total_samples
		FROM %s
		WHERE service_date >= ?
		GROUP BY route_id
	`, cfg.routePerformanceSource), sinceDate.Format("2006-01-02"))
	if err != nil {
		return nil, fmt.Errorf("query route performance: %w", err)
	}
	defer rows.Close()

	type agg struct {
		onTimeSum, delaySum, samples float64
	}
	byRoute := map[string]agg{}
	for rows.Next() {
		var routeID string
		var onTimeSum, delaySum, samples float64
		if err := rows.Scan(&routeID, &onTimeSum, &delaySum, &samples); err != nil {
			return nil, fmt.Errorf("scan route performance: %w", err)
		}
		byRoute[routeID] = agg{onTimeSum, delaySum, samples}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	names, err := fetchRouteNames(db, cfg)
	if err != nil {
		return nil, err
	}

	result := make([]routePerformanceRow, 0, len(byRoute))
	for routeID, a := range byRoute {
		if a.samples == 0 {
			continue
		}
		result = append(result, routePerformanceRow{
			RouteID:     routeID,
			RouteName:   names[routeID],
			OnTimePct:   a.onTimeSum / a.samples,
			AvgDelaySec: a.delaySum / a.samples,
		})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].OnTimePct < result[j].OnTimePct })
	for i := range result {
		result[i].Rank = i + 1
	}
	return result, nil
}

// fetchRouteNames returns route_id -> route_short_name using the latest
// gtfs_static feed_date snapshot per route. A route missing from the
// static schedule (or the table itself being unreachable/empty) yields an
// empty name rather than dropping the route from the response.
func fetchRouteNames(db *sql.DB, cfg config) (map[string]string, error) {
	rows, err := db.Query(fmt.Sprintf(`
		SELECT route_id, route_short_name
		FROM (
			SELECT route_id, route_short_name,
			       ROW_NUMBER() OVER (PARTITION BY route_id ORDER BY feed_date DESC) AS rn
			FROM %s
		)
		WHERE rn = 1
	`, cfg.gtfsRoutesSource))
	if err != nil {
		return nil, fmt.Errorf("query gtfs routes: %w", err)
	}
	defer rows.Close()

	names := map[string]string{}
	for rows.Next() {
		var routeID, name string
		if err := rows.Scan(&routeID, &name); err != nil {
			return nil, fmt.Errorf("scan gtfs routes: %w", err)
		}
		names[routeID] = name
	}
	return names, rows.Err()
}

type hotspotRow struct {
	Latitude      float64 `json:"latitude"`
	Longitude     float64 `json:"longitude"`
	AvgDelaySec   float64 `json:"avg_delay_seconds"`
	IncidentCount int64   `json:"incident_count"`
}

func fetchHotspots(db *sql.DB, cfg config, sinceDate time.Time, routeID string) ([]hotspotRow, error) {
	query := fmt.Sprintf(`
		SELECT latitude_bucket, longitude_bucket,
		       SUM(avg_delay_seconds * incident_count) AS delay_weighted_sum,
		       SUM(incident_count) AS total_incidents
		FROM %s
		WHERE service_date >= ?
	`, cfg.delayHotspotsSource)
	args := []any{sinceDate.Format("2006-01-02")}
	if routeID != "" {
		query += " AND route_id = ?"
		args = append(args, routeID)
	}
	query += " GROUP BY latitude_bucket, longitude_bucket"

	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("query delay hotspots: %w", err)
	}
	defer rows.Close()

	var result []hotspotRow
	for rows.Next() {
		var lat, lon, delaySum, incidents float64
		if err := rows.Scan(&lat, &lon, &delaySum, &incidents); err != nil {
			return nil, fmt.Errorf("scan delay hotspots: %w", err)
		}
		if incidents == 0 {
			continue
		}
		result = append(result, hotspotRow{
			Latitude:      lat,
			Longitude:     lon,
			AvgDelaySec:   delaySum / incidents,
			IncidentCount: int64(incidents),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Slice(result, func(i, j int) bool { return result[i].AvgDelaySec > result[j].AvgDelaySec })
	return result, nil
}

type timeseriesPoint struct {
	ServiceDate string  `json:"service_date"`
	OnTimePct   float64 `json:"on_time_pct"`
	AvgDelaySec float64 `json:"avg_delay_seconds"`
	SampleCount int64   `json:"sample_count"`
}

// fetchRouteTimeseries returns one point per service_date for routeID,
// weighting the (possibly several, one-per-hour-of-day) rows on each date
// the same way fetchWorstOffenders weights across dates.
func fetchRouteTimeseries(db *sql.DB, cfg config, routeID string, sinceDate time.Time) ([]timeseriesPoint, error) {
	rows, err := db.Query(fmt.Sprintf(`
		SELECT CAST(service_date AS VARCHAR) AS service_date,
		       SUM(on_time_pct * sample_count) AS on_time_weighted_sum,
		       SUM(avg_delay_seconds * sample_count) AS delay_weighted_sum,
		       SUM(sample_count) AS total_samples
		FROM %s
		WHERE service_date >= ? AND route_id = ?
		GROUP BY service_date
	`, cfg.routePerformanceSource), sinceDate.Format("2006-01-02"), routeID)
	if err != nil {
		return nil, fmt.Errorf("query route timeseries: %w", err)
	}
	defer rows.Close()

	var result []timeseriesPoint
	for rows.Next() {
		var serviceDate string
		var onTimeSum, delaySum, samples float64
		if err := rows.Scan(&serviceDate, &onTimeSum, &delaySum, &samples); err != nil {
			return nil, fmt.Errorf("scan route timeseries: %w", err)
		}
		if samples == 0 {
			continue
		}
		result = append(result, timeseriesPoint{
			ServiceDate: serviceDate,
			OnTimePct:   onTimeSum / samples,
			AvgDelaySec: delaySum / samples,
			SampleCount: int64(samples),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ServiceDate < result[j].ServiceDate })
	return result, nil
}

// fetchLastGPSIngest reports the freshest bronze ingest timestamp DuckDB
// can see, or a zero time if the table is empty/unreachable.
func fetchLastGPSIngest(db *sql.DB, cfg config) (time.Time, error) {
	var lastIngest sql.NullTime
	err := db.QueryRow(fmt.Sprintf(`SELECT max(ingest_timestamp_utc) FROM %s`, cfg.bronzeGPSSource)).Scan(&lastIngest)
	if err != nil {
		return time.Time{}, fmt.Errorf("query bronze gps positions: %w", err)
	}
	return lastIngest.Time, nil
}
