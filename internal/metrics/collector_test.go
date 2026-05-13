package metrics_test

import (
	"testing"
	"time"

	"github.com/bicak/notification-system/internal/metrics"
)

func TestCollector_IncrementCounters(t *testing.T) {
	c := &metrics.Collector{}

	c.IncSent()
	c.IncSent()
	c.IncFailed()
	c.IncRetried()

	snap := c.Snapshot()

	if snap.TotalSent != 2 {
		t.Errorf("expected 2 sent, got %d", snap.TotalSent)
	}
	if snap.TotalFailed != 1 {
		t.Errorf("expected 1 failed, got %d", snap.TotalFailed)
	}
	if snap.TotalRetried != 1 {
		t.Errorf("expected 1 retried, got %d", snap.TotalRetried)
	}
}

func TestCollector_SuccessRate(t *testing.T) {
	c := &metrics.Collector{}

	// 3 sent, 1 failed => %75 success
	c.IncSent()
	c.IncSent()
	c.IncSent()
	c.IncFailed()

	snap := c.Snapshot()
	expected := 75.0
	if snap.SuccessRate != expected {
		t.Errorf("expected success rate %.1f, got %.1f", expected, snap.SuccessRate)
	}
}

func TestCollector_LatencyRecording(t *testing.T) {
	c := &metrics.Collector{}

	c.RecordLatency(100 * time.Millisecond)
	c.RecordLatency(200 * time.Millisecond)
	c.RecordLatency(300 * time.Millisecond)

	snap := c.Snapshot()

	if snap.AvgLatencyMs != 200.0 {
		t.Errorf("expected avg latency 200ms, got %.1f", snap.AvgLatencyMs)
	}
}

func TestCollector_EmptySnapshot(t *testing.T) {
	c := &metrics.Collector{}
	snap := c.Snapshot()

	if snap.SuccessRate != 0 {
		t.Errorf("empty collector should have 0 success rate")
	}
	if snap.AvgLatencyMs != 0 {
		t.Errorf("empty collector should have 0 avg latency")
	}
}
