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

const (
	// latencyTau is the time constant of the decay when a lower sample arrives;
	// a higher sample replaces the estimate.
	latencyTau = 10 * time.Second
	// latencyStaleAfter is how long an estimate remains usable.
	latencyStaleAfter = 6 * latencyTau
	// latencyProbeInterval limits trial requests to unknown upstreams.
	latencyProbeInterval = time.Second
)

// latencyEstimate tracks response latency using a peak-sensitive moving average.
// Its zero value is ready to use.
type latencyEstimate struct {
	mu        sync.Mutex
	est       time.Duration
	updated   time.Time
	samples   uint64
	nextProbe time.Time // earliest time the next trial may be claimed
}

// latencySnapshot is a consistent copy of a latencyEstimate.
type latencySnapshot struct {
	est     time.Duration
	updated time.Time
	samples uint64
}

// observe records a latency sample taken at now. Production callers should
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

// claimProbe reserves a trial request for this upstream.
// At most one claim succeeds per latencyProbeInterval.
func (l *latencyEstimate) claimProbe(now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if now.Before(l.nextProbe) {
		return false
	}
	l.nextProbe = now.Add(latencyProbeInterval)
	return true
}

// known reports whether the snapshot has a recent latency sample.
func (s latencySnapshot) known(now time.Time) bool {
	return s.samples > 0 && now.Sub(s.updated) <= latencyStaleAfter
}

// coldResult holds either a winner or two upstreams to compare.
type coldResult struct {
	winner  *Upstream
	compare [2]*Upstream
}

// resolveCold handles candidate pairs containing an unknown latency estimate.
// While any available upstream is known, an unknown one is returned at most
// once per latencyProbeInterval; claims are not refunded. Without a known
// upstream, the candidate with fewer active requests wins.
//
// intn returns a random number in [0, n).
func resolveCold(a, b *Upstream, pool UpstreamPool, now time.Time, intn func(int) int) coldResult {
	knownA := a.latency.snapshot().known(now)
	knownB := b.latency.snapshot().known(now)

	switch {
	case knownA && knownB:
		return coldResult{compare: [2]*Upstream{a, b}}
	case knownA != knownB:
		known, unknown := a, b
		if knownB {
			known, unknown = b, a
		}
		if unknown.latency.claimProbe(now) {
			return coldResult{winner: unknown}
		}
		return coldResult{winner: known}
	}

	// both unknown: reservoir sample two known
	// available upstreams to bound them against
	var sample [2]*Upstream
	var count int
	for _, u := range pool {
		if !u.Available() || !u.latency.snapshot().known(now) {
			continue
		}
		count++
		if count <= len(sample) {
			sample[count-1] = u
		} else if j := intn(count); j < len(sample) {
			sample[j] = u
		}
	}

	if count == 0 {
		return coldResult{winner: fewerRequests(a, b, intn)}
	}

	first, second := a, b
	if intn(2) == 1 {
		first, second = b, a
	}
	if first.latency.claimProbe(now) {
		return coldResult{winner: first}
	}
	if second.latency.claimProbe(now) {
		return coldResult{winner: second}
	}
	if count == 1 {
		return coldResult{winner: sample[0]}
	}
	return coldResult{compare: sample}
}

// fewerRequests returns the upstream with fewer active requests,
// choosing randomly between equals.
func fewerRequests(a, b *Upstream, intn func(int) int) *Upstream {
	na, nb := a.NumRequests(), b.NumRequests()
	switch {
	case na < nb:
		return a
	case nb < na:
		return b
	case intn(2) == 0:
		return a
	}
	return b
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
