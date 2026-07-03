package main

import (
	"strings"
	"testing"
	"time"
)

func TestHealthSnapshotUsesUTC(t *testing.T) {
	h := NewHealthState()
	h.RecordSuccess(time.Now()) // local time, as the caller passes it

	snap := h.snapshot()
	if !strings.HasSuffix(snap.LastSuccessfulPollUTC, "Z") {
		t.Fatalf("expected last_successful_poll_utc to be UTC (suffix Z), got %q", snap.LastSuccessfulPollUTC)
	}
}

func TestHealthStatusThresholds(t *testing.T) {
	h := NewHealthState()

	if got := h.snapshot().Status; got != "ok" {
		t.Errorf("expected ok with no failures, got %q", got)
	}

	h.RecordFailure()
	if got := h.snapshot().Status; got != "degraded" {
		t.Errorf("expected degraded after 1 failure, got %q", got)
	}

	for i := 0; i < downThreshold-1; i++ {
		h.RecordFailure()
	}
	if got := h.snapshot().Status; got != "down" {
		t.Errorf("expected down after %d failures, got %q", downThreshold, got)
	}

	h.RecordSuccess(time.Now())
	if got := h.snapshot().Status; got != "ok" {
		t.Errorf("expected ok after a success, got %q", got)
	}
}
