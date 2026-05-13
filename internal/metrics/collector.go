package metrics

import (
	"sync"
	"sync/atomic"
	"time"
)

// Collector stores process-local delivery metrics.
type Collector struct {
	sent    atomic.Int64
	failed  atomic.Int64
	retried atomic.Int64

	// Keep a small rolling latency sample.
	mu        sync.Mutex
	latencies []time.Duration
}

var Global = &Collector{}

func (c *Collector) IncSent() {
	c.sent.Add(1)
}

func (c *Collector) IncFailed() {
	c.failed.Add(1)
}

func (c *Collector) IncRetried() {
	c.retried.Add(1)
}

func (c *Collector) RecordLatency(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.latencies = append(c.latencies, d)
	if len(c.latencies) > 1000 {
		c.latencies = c.latencies[len(c.latencies)-1000:]
	}
}

type Snapshot struct {
	TotalSent    int64   `json:"total_sent"`
	TotalFailed  int64   `json:"total_failed"`
	TotalRetried int64   `json:"total_retried"`
	SuccessRate  float64 `json:"success_rate"`
	AvgLatencyMs float64 `json:"avg_latency_ms"`
	P95LatencyMs float64 `json:"p95_latency_ms"`
}

func (c *Collector) Snapshot() Snapshot {
	sent := c.sent.Load()
	failed := c.failed.Load()
	retried := c.retried.Load()

	var successRate float64
	if total := sent + failed; total > 0 {
		successRate = float64(sent) / float64(total) * 100
	}

	c.mu.Lock()
	lats := make([]time.Duration, len(c.latencies))
	copy(lats, c.latencies)
	c.mu.Unlock()

	avgMs, p95Ms := calcLatencyStats(lats)

	return Snapshot{
		TotalSent:    sent,
		TotalFailed:  failed,
		TotalRetried: retried,
		SuccessRate:  successRate,
		AvgLatencyMs: avgMs,
		P95LatencyMs: p95Ms,
	}
}

func calcLatencyStats(lats []time.Duration) (avg, p95 float64) {
	if len(lats) == 0 {
		return 0, 0
	}

	var total time.Duration
	for _, l := range lats {
		total += l
	}
	avg = float64(total.Milliseconds()) / float64(len(lats))

	sorted := make([]time.Duration, len(lats))
	copy(sorted, lats)
	for i := 0; i < len(sorted); i++ {
		for j := i + 1; j < len(sorted); j++ {
			if sorted[j] < sorted[i] {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}

	idx := int(float64(len(sorted)) * 0.95)
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	p95 = float64(sorted[idx].Milliseconds())

	return avg, p95
}
