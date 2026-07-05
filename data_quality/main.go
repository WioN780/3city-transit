package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type Config struct {
	MinioEndpoint    string
	MinioAccessKey   string
	MinioSecretKey   string
	MinioUseSSL      bool
	BucketName       string
	TablePath        string
	CheckInterval    time.Duration
	StalenessLimit   time.Duration
	AlertWebhookURL  string
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg := loadConfig()
	slog.Info("starting data quality watchdog sidecar",
		"minio_endpoint", cfg.MinioEndpoint,
		"bucket", cfg.BucketName,
		"table_path", cfg.TablePath,
		"check_interval_seconds", cfg.CheckInterval.Seconds(),
		"staleness_limit_seconds", cfg.StalenessLimit.Seconds(),
		"has_webhook", cfg.AlertWebhookURL != "",
	)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	minioClient, err := minio.New(cfg.MinioEndpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.MinioAccessKey, cfg.MinioSecretKey, ""),
		Secure: cfg.MinioUseSSL,
	})
	if err != nil {
		slog.Error("failed to initialize minio client", "error", err)
		os.Exit(1)
	}

	ticker := time.NewTicker(cfg.CheckInterval)
	defer ticker.Stop()

	// Run initial check immediately
	checkFreshness(ctx, minioClient, cfg)

	for {
		select {
		case <-ctx.Done():
			slog.Info("watchdog sidecar shutting down")
			return
		case <-ticker.C:
			checkFreshness(ctx, minioClient, cfg)
		}
	}
}

func checkFreshness(ctx context.Context, client *minio.Client, cfg Config) {
	// We check the _delta_log directory because delta commits write JSON files there on every update.
	// This avoids recursively listing large amounts of parquet files.
	prefix := cfg.TablePath
	if prefix != "" && prefix[len(prefix)-1] != '/' {
		prefix += "/"
	}
	prefix += "_delta_log/"

	slog.Debug("checking freshness", "bucket", cfg.BucketName, "prefix", prefix)

	objectCh := client.ListObjects(ctx, cfg.BucketName, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: false,
	})

	var latestTime time.Time
	var latestFile string
	count := 0

	for obj := range objectCh {
		if obj.Err != nil {
			slog.Error("error listing minio objects", "error", obj.Err)
			sendAlert(ctx, cfg, fmt.Sprintf("Error listing MinIO objects: %v", obj.Err))
			return
		}
		count++
		if obj.LastModified.After(latestTime) {
			latestTime = obj.LastModified
			latestFile = obj.Key
		}
	}

	if count == 0 {
		// No delta log files found. Let's check the base path.
		slog.Warn("no delta log files found, checking base table path", "path", cfg.TablePath)
		baseCh := client.ListObjects(ctx, cfg.BucketName, minio.ListObjectsOptions{
			Prefix:    cfg.TablePath,
			Recursive: true,
		})
		for obj := range baseCh {
			if obj.Err != nil {
				slog.Error("error listing minio base objects", "error", obj.Err)
				return
			}
			count++
			if obj.LastModified.After(latestTime) {
				latestTime = obj.LastModified
				latestFile = obj.Key
			}
		}
	}

	if count == 0 {
		slog.Warn("no objects found in table path, table may be empty", "path", cfg.TablePath)
		sendAlert(ctx, cfg, fmt.Sprintf("Bronze GPS positions table is empty at path %s/%s", cfg.BucketName, cfg.TablePath))
		return
	}

	staleness := time.Since(latestTime)
	slog.Info("freshness check complete",
		"latest_file", latestFile,
		"latest_modified_utc", latestTime.Format(time.RFC3339),
		"staleness_seconds", staleness.Seconds(),
	)

	if staleness > cfg.StalenessLimit {
		msg := fmt.Sprintf("Bronze GPS positions table is stale! Last update was %s ago (file: %s, modified: %s)",
			staleness.Round(time.Second), latestFile, latestTime.Format(time.RFC3339))
		slog.Error("staleness alert", "message", msg, "staleness_seconds", staleness.Seconds())
		sendAlert(ctx, cfg, msg)
	}
}

type AlertPayload struct {
	Text      string    `json:"text"`
	Timestamp time.Time `json:"timestamp"`
	Severity  string    `json:"severity"`
}

func sendAlert(ctx context.Context, cfg Config, message string) {
	if cfg.AlertWebhookURL == "" {
		return
	}

	payload := AlertPayload{
		Text:      message,
		Timestamp: time.Now().UTC(),
		Severity:  "CRITICAL",
	}

	body, err := json.Marshal(payload)
	if err != nil {
		slog.Error("failed to marshal alert payload", "error", err)
		return
	}

	req, err := http.NewRequestWithContext(ctx, "POST", cfg.AlertWebhookURL, bytes.NewReader(body))
	if err != nil {
		slog.Error("failed to create alert request", "error", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		slog.Error("failed to send alert webhook", "error", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		slog.Error("webhook returned non-success status", "status", resp.Status)
	} else {
		slog.Info("alert webhook sent successfully")
	}
}

func loadConfig() Config {
	return Config{
		MinioEndpoint:   getEnv("MINIO_ENDPOINT", "minio:9000"),
		MinioAccessKey:  getEnv("MINIO_ACCESS_KEY", "minioadmin"),
		MinioSecretKey:  getEnv("MINIO_SECRET_KEY", "minioadmin"),
		MinioUseSSL:     getEnvBool("MINIO_USE_SSL", false),
		BucketName:      getEnv("BUCKET_NAME", "lakehouse"),
		TablePath:       getEnv("TABLE_PATH", "bronze/gps_positions"),
		CheckInterval:   getEnvDuration("CHECK_INTERVAL_SECONDS", 60) * time.Second,
		StalenessLimit:  getEnvDuration("STALENESS_THRESHOLD_SECONDS", 300) * time.Second,
		AlertWebhookURL: getEnv("ALERT_WEBHOOK_URL", ""),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvBool(key string, fallback bool) bool {
	if v := os.Getenv(key); v != "" {
		b, err := strconv.ParseBool(v)
		if err == nil {
			return b
		}
	}
	return fallback
}

func getEnvDuration(key string, fallback int) time.Duration {
	if v := os.Getenv(key); v != "" {
		n, err := strconv.Atoi(v)
		if err == nil {
			return time.Duration(n)
		}
	}
	return time.Duration(fallback)
}
