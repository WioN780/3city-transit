package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2"
)

func TestParseWindow(t *testing.T) {
	if _, window, err := parseWindow(""); err != nil || window != "7d" {
		t.Fatalf("expected default window 7d, got %q, err %v", window, err)
	}
	if _, _, err := parseWindow("14d"); err != nil {
		t.Fatalf("expected 14d to be valid, got %v", err)
	}
	for _, bad := range []string{"7", "abc", "-1d", "0d", "7days"} {
		if _, _, err := parseWindow(bad); err == nil {
			t.Errorf("expected %q to be rejected as an invalid window", bad)
		}
	}
}

func TestTTLCache(t *testing.T) {
	c := newTTLCache(30 * time.Millisecond)
	if _, ok := c.get("k"); ok {
		t.Fatal("expected miss on empty cache")
	}
	c.set("k", []byte("v"))
	if body, ok := c.get("k"); !ok || string(body) != "v" {
		t.Fatalf("expected hit with %q, got %q ok=%v", "v", body, ok)
	}
	time.Sleep(40 * time.Millisecond)
	if _, ok := c.get("k"); ok {
		t.Fatal("expected entry to expire after ttl")
	}
}

// testConfig points every table source at the fixture Parquet files in
// testdata/ via plain read_parquet -- no S3/httpfs/delta extensions
// needed, since core DuckDB reads local Parquet out of the box. Production
// wiring (openDuckDB, delta_scan over s3://) is exercised by the manual
// verification in README.md, not by this test.
func testConfig(t *testing.T) config {
	t.Helper()
	abs := func(name string) string {
		p, err := filepath.Abs(filepath.Join("testdata", name))
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	return config{
		routePerformanceSource: fmt.Sprintf("read_parquet('%s')", abs("route_performance.parquet")),
		delayHotspotsSource:    fmt.Sprintf("read_parquet('%s')", abs("delay_hotspots.parquet")),
		gtfsRoutesSource:       fmt.Sprintf("read_parquet('%s')", abs("gtfs_routes.parquet")),
		bronzeGPSSource:        fmt.Sprintf("read_parquet('%s')", abs("bronze_gps.parquet")),
		dashboardOrigin:        "http://localhost:5173",
		cacheTTL:               time.Minute,
	}
}

func testServer(t *testing.T) *httptest.Server {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	cfg := testConfig(t)
	cache := newTTLCache(cfg.cacheTTL)

	mux := http.NewServeMux()
	mux.Handle("/api/v1/routes/worst-offenders", cached(cache, worstOffendersHandler(db, cfg)))
	mux.Handle("/api/v1/routes/{route_id}/timeseries", cached(cache, routeTimeseriesHandler(db, cfg)))
	mux.Handle("/api/v1/hotspots", cached(cache, hotspotsHandler(db, cfg)))
	mux.HandleFunc("/api/v1/healthz", healthzHandler(db, cfg))

	srv := httptest.NewServer(withCORS(cfg.dashboardOrigin, mux))
	t.Cleanup(srv.Close)
	return srv
}

// The fixtures use 2026-07-0x dates and a 10-year window so this test
// keeps passing regardless of the real "today" it happens to run on.
const fixtureWindow = "window=3650d"

func TestWorstOffenders(t *testing.T) {
	srv := testServer(t)

	resp, err := http.Get(srv.URL + "/api/v1/routes/worst-offenders?" + fixtureWindow)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if got := resp.Header.Get("Access-Control-Allow-Origin"); got != "http://localhost:5173" {
		t.Errorf("expected CORS origin header, got %q", got)
	}

	var body struct {
		Window string                `json:"window"`
		Routes []routePerformanceRow `json:"routes"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if len(body.Routes) != 2 {
		t.Fatalf("expected 2 routes, got %d: %+v", len(body.Routes), body.Routes)
	}

	// route 1: weighted on_time_pct = (80*100+40*300)/400 = 50, avg_delay = (60*100+300*300)/400 = 240
	// route 2: single row, on_time_pct = 95, avg_delay = 10 -- so route 1 (worse) ranks first.
	worst := body.Routes[0]
	if worst.RouteID != "1" || worst.RouteName != "1" || worst.Rank != 1 {
		t.Fatalf("expected route 1 ranked worst, got %+v", worst)
	}
	if approx(worst.OnTimePct, 50.0) == false || approx(worst.AvgDelaySec, 240.0) == false {
		t.Fatalf("expected weighted on_time_pct=50 avg_delay=240, got %+v", worst)
	}

	best := body.Routes[1]
	if best.RouteID != "2" || best.Rank != 2 || approx(best.OnTimePct, 95.0) == false {
		t.Fatalf("expected route 2 ranked second with on_time_pct=95, got %+v", best)
	}
}

func TestHotspots(t *testing.T) {
	srv := testServer(t)

	resp, err := http.Get(srv.URL + "/api/v1/hotspots?" + fixtureWindow)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var body struct {
		Hotspots []hotspotRow `json:"hotspots"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if len(body.Hotspots) != 2 {
		t.Fatalf("expected 2 buckets, got %d: %+v", len(body.Hotspots), body.Hotspots)
	}

	// bucket (54.352,18.646): weighted avg = (200*10+400*5)/15 = 266.67, incidents=15
	top := body.Hotspots[0]
	if !approx(top.Latitude, 54.352) || !approx(top.AvgDelaySec, 266.6666) || top.IncidentCount != 15 {
		t.Fatalf("expected hottest bucket avg~266.67 incidents=15, got %+v", top)
	}
}

func TestHotspotsFilteredByRoute(t *testing.T) {
	srv := testServer(t)

	resp, err := http.Get(srv.URL + "/api/v1/hotspots?route_id=2&" + fixtureWindow)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	var body struct {
		Hotspots []hotspotRow `json:"hotspots"`
	}
	json.NewDecoder(resp.Body).Decode(&body)
	if len(body.Hotspots) != 1 || !approx(body.Hotspots[0].AvgDelaySec, 50.0) {
		t.Fatalf("expected exactly route 2's bucket (avg=50), got %+v", body.Hotspots)
	}
}

func TestRouteTimeseries(t *testing.T) {
	srv := testServer(t)

	// route 2 has a single fixture row (on_time_pct=95, avg_delay=10, see
	// TestWorstOffenders), so its timeseries must reduce to exactly one
	// point carrying those same values.
	resp, err := http.Get(srv.URL + "/api/v1/routes/2/timeseries?" + fixtureWindow)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var body struct {
		RouteID string            `json:"route_id"`
		Window  string            `json:"window"`
		Points  []timeseriesPoint `json:"points"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.RouteID != "2" {
		t.Fatalf("expected route_id echoed back, got %q", body.RouteID)
	}
	if len(body.Points) != 1 {
		t.Fatalf("expected 1 point for route 2, got %d: %+v", len(body.Points), body.Points)
	}
	if !approx(body.Points[0].OnTimePct, 95.0) || !approx(body.Points[0].AvgDelaySec, 10.0) {
		t.Fatalf("expected on_time_pct=95 avg_delay=10, got %+v", body.Points[0])
	}

	// route 1 has two fixture rows that weighted-average to on_time_pct=50,
	// avg_delay=240 across however many service_dates they fall on -- so
	// re-weighting the returned points by their sample_count must reproduce
	// that same aggregate regardless of how many points come back.
	resp2, err := http.Get(srv.URL + "/api/v1/routes/1/timeseries?" + fixtureWindow)
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	var body2 struct {
		Points []timeseriesPoint `json:"points"`
	}
	if err := json.NewDecoder(resp2.Body).Decode(&body2); err != nil {
		t.Fatal(err)
	}
	if len(body2.Points) == 0 {
		t.Fatal("expected at least one point for route 1")
	}
	var onTimeSum, delaySum, samples float64
	for _, p := range body2.Points {
		onTimeSum += p.OnTimePct * float64(p.SampleCount)
		delaySum += p.AvgDelaySec * float64(p.SampleCount)
		samples += float64(p.SampleCount)
	}
	if !approx(onTimeSum/samples, 50.0) || !approx(delaySum/samples, 240.0) {
		t.Fatalf("expected re-weighted points to reproduce on_time_pct=50 avg_delay=240, got %+v", body2.Points)
	}
}

func TestRouteTimeseriesMissingRouteID(t *testing.T) {
	srv := testServer(t)

	resp, err := http.Get(srv.URL + "/api/v1/routes//timeseries?" + fixtureWindow)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound && resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 or 404 for missing route_id, got %d", resp.StatusCode)
	}
}

func TestWorstOffendersBadWindow(t *testing.T) {
	srv := testServer(t)

	resp, err := http.Get(srv.URL + "/api/v1/routes/worst-offenders?window=notanumber")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}
	var body map[string]string
	json.NewDecoder(resp.Body).Decode(&body)
	if body["error"] == "" {
		t.Fatal("expected a structured error message")
	}
}

func TestHealthz(t *testing.T) {
	srv := testServer(t)

	resp, err := http.Get(srv.URL + "/api/v1/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var body healthResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.LastGPSIngestUTC != "2026-07-01T08:05:00Z" {
		t.Fatalf("expected last ingest to match fixture max, got %q", body.LastGPSIngestUTC)
	}
}

func approx(got, want float64) bool {
	const eps = 0.01
	d := got - want
	return d < eps && d > -eps
}
