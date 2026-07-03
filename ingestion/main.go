package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg := loadConfig()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	health := NewHealthState()
	publisher := NewPublisher(cfg.kafkaBrokers, cfg.kafkaTopic)
	defer publisher.Close()

	go runHealthServer(ctx, cfg.healthPort, health)

	pollLoop(ctx, cfg, publisher, health)
	logger.Info("ingestion service stopped")
}

func runHealthServer(ctx context.Context, port string, health *HealthState) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", health.Handler())

	server := &http.Server{Addr: ":" + port, Handler: mux}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		server.Shutdown(shutdownCtx)
	}()

	slog.Info("health server listening", "port", port)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		slog.Error("health server failed", "error", err)
	}
}

// pollLoop fetches the feed, publishes to Kafka, and sleeps until the next
// poll. On failure it backs off exponentially (capped) instead of hammering
// the upstream feed or Kafka, and never crashes the process.
func pollLoop(ctx context.Context, cfg config, publisher *Publisher, health *HealthState) {
	client := &http.Client{Timeout: cfg.httpTimeout}
	feedSequence := 0
	consecutiveFailures := 0

	for {
		feedSequence++
		start := time.Now()

		err := pollOnce(ctx, client, cfg, publisher, feedSequence)
		duration := time.Since(start)

		if err != nil {
			consecutiveFailures++
			health.RecordFailure()
			slog.Error("poll failed",
				"feed_sequence", feedSequence,
				"duration_ms", duration.Milliseconds(),
				"consecutive_failures", consecutiveFailures,
				"error", err.Error(),
			)
		} else {
			consecutiveFailures = 0
			health.RecordSuccess(start)
		}

		sleep := nextInterval(cfg.pollInterval, consecutiveFailures)
		select {
		case <-ctx.Done():
			return
		case <-time.After(sleep):
		}
	}
}

func pollOnce(ctx context.Context, client *http.Client, cfg config, publisher *Publisher, feedSequence int) error {
	feed, err := FetchFeed(ctx, client, cfg.feedURL)
	if err != nil {
		return err
	}

	records := MapVehiclePositions(feed, feedSequence)

	if err := publisher.Publish(ctx, records); err != nil {
		return err
	}

	slog.Info("poll completed",
		"feed_sequence", feedSequence,
		"records_published", len(records),
	)
	return nil
}

// nextInterval returns the normal poll interval on success (failures == 0),
// or an exponential backoff capped at maxBackoff while failures persist.
func nextInterval(pollInterval time.Duration, failures int) time.Duration {
	if failures == 0 {
		return pollInterval
	}

	const maxBackoff = 60 * time.Second
	backoff := time.Duration(1<<uint(failures-1)) * time.Second
	if backoff > maxBackoff {
		backoff = maxBackoff
	}
	return backoff
}

type config struct {
	feedURL      string
	kafkaBrokers []string
	kafkaTopic   string
	pollInterval time.Duration
	healthPort   string
	httpTimeout  time.Duration
}

func loadConfig() config {
	return config{
		feedURL:      getEnv("FEED_URL", "http://ckan2.multimediagdansk.pl/gtfs-rt?feed=vehiclePositions"),
		kafkaBrokers: strings.Split(getEnv("KAFKA_BROKERS", "localhost:19092"), ","),
		kafkaTopic:   getEnv("KAFKA_TOPIC", "gps_raw"),
		pollInterval: getEnvSeconds("POLL_INTERVAL_SECONDS", 30),
		healthPort:   getEnv("HEALTH_PORT", "8091"),
		httpTimeout:  getEnvSeconds("HTTP_TIMEOUT_SECONDS", 10),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvSeconds(key string, fallback int) time.Duration {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return time.Duration(n) * time.Second
		}
	}
	return time.Duration(fallback) * time.Second
}
