package main

import (
	"log"
	"log/slog"
	"net/http"
)

func main() {
	cfg := loadConfig()

	db, err := openDuckDB(cfg)
	if err != nil {
		log.Fatalf("duckdb setup failed: %v", err)
	}
	defer db.Close()

	cache := newTTLCache(cfg.cacheTTL)

	mux := http.NewServeMux()
	mux.Handle("/api/v1/routes/worst-offenders", cached(cache, worstOffendersHandler(db, cfg)))
	mux.Handle("/api/v1/hotspots", cached(cache, hotspotsHandler(db, cfg)))
	mux.HandleFunc("/api/v1/healthz", healthzHandler(db, cfg))

	slog.Info("api listening", "port", cfg.port)
	log.Fatal(http.ListenAndServe(":"+cfg.port, withCORS(cfg.dashboardOrigin, mux)))
}
