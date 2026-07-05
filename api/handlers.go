package main

import (
	"database/sql"
	"encoding/json"
	"log/slog"
	"net/http"
	"regexp"
	"strconv"
	"time"
)

var windowPattern = regexp.MustCompile(`^([1-9][0-9]*)d$`)

// parseWindow turns "7d" into 7 days-ago (UTC, truncated to a date), per
// the ?window=7d convention on both /routes/worst-offenders and /hotspots.
func parseWindow(raw string) (time.Time, string, error) {
	if raw == "" {
		raw = "7d"
	}
	m := windowPattern.FindStringSubmatch(raw)
	if m == nil {
		return time.Time{}, "", errInvalidWindow
	}
	days, _ := strconv.Atoi(m[1])
	since := time.Now().UTC().AddDate(0, 0, -days)
	return since, raw, nil
}

var errInvalidWindow = &apiError{"invalid window: expected a format like \"7d\""}

type apiError struct{ msg string }

func (e *apiError) Error() string { return e.msg }

func writeJSONError(w http.ResponseWriter, status int, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
}

func withCORS(origin string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// cached wraps a handler so identical query strings share one DuckDB scan
// for cfg.cacheTTL -- gold tables refresh hourly, so this only exists to
// absorb bursts of repeat dashboard requests, not to track freshness.
func cached(cache *ttlCache, next func(r *http.Request) ([]byte, int, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Path + "?" + r.URL.RawQuery
		if body, ok := cache.get(key); ok {
			w.Header().Set("Content-Type", "application/json")
			w.Write(body)
			return
		}

		body, status, err := next(r)
		if err != nil {
			writeJSONError(w, status, err)
			return
		}
		if status == http.StatusOK {
			cache.set(key, body)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		w.Write(body)
	}
}

func worstOffendersHandler(db *sql.DB, cfg config) func(r *http.Request) ([]byte, int, error) {
	return func(r *http.Request) ([]byte, int, error) {
		since, window, err := parseWindow(r.URL.Query().Get("window"))
		if err != nil {
			return nil, http.StatusBadRequest, err
		}

		routes, err := fetchWorstOffenders(db, cfg, since)
		if err != nil {
			slog.Error("worst-offenders query failed", "error", err)
			return nil, http.StatusInternalServerError, &apiError{"failed to query route performance"}
		}
		if routes == nil {
			routes = []routePerformanceRow{}
		}

		body, err := json.Marshal(map[string]any{"window": window, "routes": routes})
		if err != nil {
			return nil, http.StatusInternalServerError, err
		}
		return body, http.StatusOK, nil
	}
}

func hotspotsHandler(db *sql.DB, cfg config) func(r *http.Request) ([]byte, int, error) {
	return func(r *http.Request) ([]byte, int, error) {
		since, _, err := parseWindow(r.URL.Query().Get("window"))
		if err != nil {
			return nil, http.StatusBadRequest, err
		}
		routeID := r.URL.Query().Get("route_id")

		hotspots, err := fetchHotspots(db, cfg, since, routeID)
		if err != nil {
			slog.Error("hotspots query failed", "error", err, "route_id", routeID)
			return nil, http.StatusInternalServerError, &apiError{"failed to query delay hotspots"}
		}
		if hotspots == nil {
			hotspots = []hotspotRow{}
		}

		body, err := json.Marshal(map[string]any{"hotspots": hotspots})
		if err != nil {
			return nil, http.StatusInternalServerError, err
		}
		return body, http.StatusOK, nil
	}
}

func routeTimeseriesHandler(db *sql.DB, cfg config) func(r *http.Request) ([]byte, int, error) {
	return func(r *http.Request) ([]byte, int, error) {
		routeID := r.PathValue("route_id")
		if routeID == "" {
			return nil, http.StatusBadRequest, &apiError{"route_id is required"}
		}
		since, window, err := parseWindow(r.URL.Query().Get("window"))
		if err != nil {
			return nil, http.StatusBadRequest, err
		}

		points, err := fetchRouteTimeseries(db, cfg, routeID, since)
		if err != nil {
			slog.Error("route timeseries query failed", "error", err, "route_id", routeID)
			return nil, http.StatusInternalServerError, &apiError{"failed to query route timeseries"}
		}
		if points == nil {
			points = []timeseriesPoint{}
		}

		body, err := json.Marshal(map[string]any{"route_id": routeID, "window": window, "points": points})
		if err != nil {
			return nil, http.StatusInternalServerError, err
		}
		return body, http.StatusOK, nil
	}
}

// healthFreshWindow is how stale the newest bronze GPS ping can be before
// /healthz reports "degraded" -- a few multiples of the poller's 30s cadence.
const healthFreshWindow = 5 * time.Minute

type healthResponse struct {
	Status           string `json:"status"`
	KafkaLag         int    `json:"kafka_lag"`
	LastGPSIngestUTC string `json:"last_gps_ingest_utc"`
}

func healthzHandler(db *sql.DB, cfg config) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		lastIngest, err := fetchLastGPSIngest(db, cfg)
		if err != nil {
			slog.Error("healthz bronze query failed", "error", err)
			json.NewEncoder(w).Encode(healthResponse{Status: "down"})
			return
		}

		status := "degraded"
		lastIngestUTC := ""
		if !lastIngest.IsZero() {
			lastIngestUTC = lastIngest.UTC().Format(time.RFC3339)
			if time.Since(lastIngest) <= healthFreshWindow {
				status = "ok"
			}
		}

		json.NewEncoder(w).Encode(healthResponse{
			Status: status,
			// ponytail: this service only reads gold/bronze tables, it isn't
			// a Kafka consumer -- reporting real lag needs a consumer-group
			// admin client. Wire one up if/when kafka_lag needs to be real.
			KafkaLag:         0,
			LastGPSIngestUTC: lastIngestUTC,
		})
	}
}
