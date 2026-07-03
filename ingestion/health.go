package main

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

// degradedThreshold and downThreshold classify consecutive poll failures
// into the health status surfaced at /healthz.
const (
	degradedThreshold = 1
	downThreshold     = 5
)

type HealthState struct {
	mu                  sync.Mutex
	lastSuccessfulPoll  *time.Time
	consecutiveFailures int
}

func NewHealthState() *HealthState {
	return &HealthState{}
}

func (h *HealthState) RecordSuccess(at time.Time) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.lastSuccessfulPoll = &at
	h.consecutiveFailures = 0
}

func (h *HealthState) RecordFailure() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.consecutiveFailures++
}

type healthResponse struct {
	Status                string `json:"status"`
	LastSuccessfulPollUTC string `json:"last_successful_poll_utc"`
	ConsecutiveFailures   int    `json:"consecutive_failures"`
}

func (h *HealthState) snapshot() healthResponse {
	h.mu.Lock()
	defer h.mu.Unlock()

	status := "ok"
	switch {
	case h.consecutiveFailures >= downThreshold:
		status = "down"
	case h.consecutiveFailures >= degradedThreshold:
		status = "degraded"
	}

	lastPoll := ""
	if h.lastSuccessfulPoll != nil {
		lastPoll = h.lastSuccessfulPoll.UTC().Format(time.RFC3339)
	}

	return healthResponse{
		Status:                status,
		LastSuccessfulPollUTC: lastPoll,
		ConsecutiveFailures:   h.consecutiveFailures,
	}
}

func (h *HealthState) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(h.snapshot())
	}
}
