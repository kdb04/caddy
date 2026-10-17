// Copyright 2015 Matthew Holt and The Caddy Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package reverseproxy

import (
	"math"
	"sync"
	"time"
)

// latencyTau is the time constant of the decay when a lower sample arrives.
// Higher samples replace the estimates entirely.
const latencyTau = 10 * time.Second

// latencyEstimate tracks response latency using a peak-sensitive moving average.
// Its zero-value is ready to use. State is protected by mutex.
type latencyEstimate struct {
	mu      sync.Mutex
	est     time.Duration
	updated time.Time
	samples uint64
}

type latencySnapshot struct {
	est     time.Duration
	updated time.Time
	samples uint64
}

// observer records a latency sample taken at 'now'. Production callers should
// pass `time.Now()` to preserve its monotonic clock reading.
//
// Older completions can only raise the estimate but not lower it or move the timestamp backwards.
func (l *latencyEstimate) observe(sample time.Duration, now time.Time) {
	sample = max(sample, 0)

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.samples == 0 {
		l.est = sample
	} else {
		l.est = peakEWMA(l.est, sample, now.Sub(l.updated))
	}
	if now.After(l.updated) {
		l.updated = now
	}
	l.samples++
}

// snapshot returns a consistent copy of the state.
func (l *latencyEstimate) snapshot() latencySnapshot {
	l.mu.Lock()
	defer l.mu.Unlock()
	return latencySnapshot{
		est:     l.est,
		updated: l.updated,
		samples: l.samples,
	}
}

// peakEWMA calculates the updated estimate from a sample and elapsed time.
func peakEWMA(est, sample, elapsed time.Duration) time.Duration {
	if sample >= est {
		return sample
	}
	if elapsed <= 0 {
		return est
	}
	weight := math.Exp(-float64(elapsed) / float64(latencyTau))

	// est >= sample >= 0, so the subtraction can't overflow
	v := float64(sample) + float64(est-sample)*weight

	// float64(math.MaxInt64) rounds to 2^63, which cannot be converted to int64.
	if v >= float64(est) {
		return est
	}

	// Clamp rounding errors to the valid range.
	return min(max(time.Duration(math.Round(v)), sample), est)
}
