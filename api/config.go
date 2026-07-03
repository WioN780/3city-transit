package main

import (
	"os"
	"time"
)

type config struct {
	port string

	minioEndpoint  string
	minioAccessKey string
	minioSecretKey string
	minioUseSSL    bool

	// Each *Source holds a complete DuckDB table-function call (e.g.
	// "delta_scan('s3://lakehouse/gold/route_performance')") so production
	// (Delta, over S3) and tests (a local Parquet fixture via read_parquet)
	// share every line of query-building code around them.
	routePerformanceSource string
	delayHotspotsSource    string
	gtfsRoutesSource       string
	bronzeGPSSource        string

	dashboardOrigin string
	cacheTTL        time.Duration
}

func loadConfig() config {
	return config{
		port: getEnv("PORT", "8090"),

		minioEndpoint:  getEnv("MINIO_ENDPOINT", "minio:9000"),
		minioAccessKey: getEnv("MINIO_ACCESS_KEY", "minioadmin"),
		minioSecretKey: getEnv("MINIO_SECRET_KEY", "minioadmin"),
		minioUseSSL:    getEnvBool("MINIO_USE_SSL", false),

		routePerformanceSource: getEnv("GOLD_ROUTE_PERFORMANCE_SOURCE", "delta_scan('s3://lakehouse/gold/route_performance')"),
		delayHotspotsSource:    getEnv("GOLD_DELAY_HOTSPOTS_SOURCE", "delta_scan('s3://lakehouse/gold/delay_hotspots')"),
		gtfsRoutesSource:       getEnv("GTFS_ROUTES_SOURCE", "delta_scan('s3://lakehouse/gtfs_static/routes')"),
		bronzeGPSSource:        getEnv("BRONZE_GPS_SOURCE", "delta_scan('s3://lakehouse/bronze/gps_positions')"),

		dashboardOrigin: getEnv("DASHBOARD_ORIGIN", "http://localhost:5173"),
		cacheTTL:        getEnvSeconds("CACHE_TTL_SECONDS", 60),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvBool(key string, fallback bool) bool {
	switch os.Getenv(key) {
	case "true", "1":
		return true
	case "false", "0":
		return false
	default:
		return fallback
	}
}

func getEnvSeconds(key string, fallbackSeconds int) time.Duration {
	if v := os.Getenv(key); v != "" {
		if n, err := time.ParseDuration(v + "s"); err == nil {
			return n
		}
	}
	return time.Duration(fallbackSeconds) * time.Second
}
