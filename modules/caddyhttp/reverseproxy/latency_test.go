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
	"testing"
	"time"
)

var latencyBase = time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)

type latencyStep struct {
	sample time.Duration
	at     time.Duration // offset from latencyBase
}

func applyLatencySteps(l *latencyEstimate, steps ...latencyStep) {
	for _, s := range steps {
		l.observe(s.sample, latencyBase.Add(s.at))
	}
}

func TestLatencyZeroValueHasNoSamples(t *testing.T) {
	var l latencyEstimate
	got := l.snapshot()
	if got.samples != 0 || got.est != 0 || !got.updated.IsZero() {
		t.Fatalf("zero value should have no data, got %+v", got)
	}

	if s := new(Host).latency.snapshot(); s.samples != 0 {
		t.Fatalf("zero Host should have no latency data, got %+v", s)
	}
}

// Expected values are derived by hand from est = s + (est-s)*exp(-dt/10s),
// not recomputed with the production formula.
func TestLatencyObserve(t *testing.T) {
	const tolerance = 10 * time.Microsecond
	ms := time.Millisecond

	for _, tc := range []struct {
		name        string
		steps       []latencyStep
		want        time.Duration
		wantSamples uint64
	}{
		{"first sample", []latencyStep{{20 * ms, 0}}, 20 * ms, 1},
		{"zero first sample is data", []latencyStep{{0, 0}}, 0, 1},
		{"rising jumps to the peak", []latencyStep{{10 * ms, 0}, {50 * ms, time.Second}}, 50 * ms, 2},
		{"rising at dt=0", []latencyStep{{10 * ms, 0}, {50 * ms, 0}}, 50 * ms, 2},
		{"falling at dt=0 is unchanged", []latencyStep{{50 * ms, 0}, {10 * ms, 0}}, 50 * ms, 2},
		{"falling after 1s", []latencyStep{{50 * ms, 0}, {10 * ms, time.Second}}, 46194 * time.Microsecond, 2},
		{"falling after 10s", []latencyStep{{50 * ms, 0}, {10 * ms, 10 * time.Second}}, 24715 * time.Microsecond, 2},
		{"falling after 30s", []latencyStep{{50 * ms, 0}, {10 * ms, 30 * time.Second}}, 11991 * time.Microsecond, 2},
		{"falling after 60s", []latencyStep{{50 * ms, 0}, {10 * ms, 60 * time.Second}}, 10099 * time.Microsecond, 2},
		{"zero sample after 10s", []latencyStep{{20 * ms, 0}, {0, 10 * time.Second}}, 7358 * time.Microsecond, 2},
		{"zero sample at dt=0 is unchanged", []latencyStep{{20 * ms, 0}, {0, 0}}, 20 * ms, 2},
		{"negative sample counts as zero", []latencyStep{{-5 * ms, 0}}, 0, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var l latencyEstimate
			applyLatencySteps(&l, tc.steps...)
			got := l.snapshot()
			if diff := got.est - tc.want; diff < -tolerance || diff > tolerance {
				t.Errorf("est = %v, want %v ± %v", got.est, tc.want, tolerance)
			}
			if got.samples != tc.wantSamples {
				t.Errorf("samples = %d, want %d", got.samples, tc.wantSamples)
			}
		})
	}
}

func TestLatencyOutOfOrderPeakDominates(t *testing.T) {
	ms := time.Millisecond
	var l latencyEstimate
	// true order: 30ms@0, 10ms@5s, 50ms@10s; arrival order swaps the last two
	applyLatencySteps(&l,
		latencyStep{30 * ms, 0},
		latencyStep{50 * ms, 10 * time.Second},
		latencyStep{10 * ms, 5 * time.Second},
	)
	got := l.snapshot()
	if got.est != 50*ms {
		t.Errorf("est = %v, want 50ms", got.est)
	}
	if want := latencyBase.Add(10 * time.Second); !got.updated.Equal(want) {
		t.Errorf("updated = %v, want %v", got.updated, want)
	}
	if got.samples != 3 {
		t.Errorf("samples = %d, want 3", got.samples)
	}
}

// A late peak is applied at the newer timestamp, so the result is slower
// than the chronological one (64.59ms): the error is on the conservative side.
func TestLatencyOutOfOrderLatePeak(t *testing.T) {
	const tolerance = 10 * time.Microsecond
	ms := time.Millisecond
	var l latencyEstimate
	// true order: 30ms@0, 100ms@5s, 10ms@10s; arrival order swaps the last two
	applyLatencySteps(&l,
		latencyStep{30 * ms, 0},
		latencyStep{10 * ms, 10 * time.Second},
	)
	if diff := l.snapshot().est - 17358*time.Microsecond; diff < -tolerance || diff > tolerance {
		t.Fatalf("est after the newer sample = %v, want 17.358ms", l.snapshot().est)
	}

	l.observe(100*ms, latencyBase.Add(5*time.Second))
	got := l.snapshot()
	if got.est != 100*ms {
		t.Errorf("est = %v, want 100ms", got.est)
	}
	if want := latencyBase.Add(10 * time.Second); !got.updated.Equal(want) {
		t.Errorf("updated = %v, want %v (must not move backwards)", got.updated, want)
	}
}

func TestLatencyUpdatedNeverDecreases(t *testing.T) {
	var l latencyEstimate
	offsets := []time.Duration{3, 1, 7, 7, 2, 9, 0, 5}
	var prev time.Time
	for i, o := range offsets {
		l.observe(time.Duration(i+1)*time.Millisecond, latencyBase.Add(o*time.Second))
		got := l.snapshot().updated
		if got.Before(prev) {
			t.Fatalf("step %d: updated moved backwards from %v to %v", i, prev, got)
		}
		prev = got
	}
	if want := latencyBase.Add(9 * time.Second); !prev.Equal(want) {
		t.Errorf("updated = %v, want %v", prev, want)
	}
}

func TestLatencyExtremeValuesStayInRange(t *testing.T) {
	maxD := time.Duration(math.MaxInt64)

	// est-sample is 1, so float64 arithmetic collapses to 2^63, which does
	// not fit in an int64; the estimate must be returned unchanged.
	var l latencyEstimate
	l.observe(maxD, latencyBase)
	l.observe(maxD-1, latencyBase.Add(time.Second))
	if got := l.snapshot().est; got != maxD {
		t.Errorf("est = %v, want unchanged %v", got, maxD)
	}

	for _, tc := range []struct {
		est, sample, elapsed time.Duration
	}{
		{maxD, 0, time.Nanosecond},
		{maxD, 0, time.Second},
		{maxD, 0, maxD},
		{maxD, maxD / 2, 24 * time.Hour},
		{maxD - 1, maxD - 2, time.Second},
		{1, 0, time.Nanosecond},
		{1, 0, maxD},
	} {
		got := peakEWMA(tc.est, tc.sample, tc.elapsed)
		if got < tc.sample || got > tc.est {
			t.Errorf("peakEWMA(%v, %v, %v) = %v, outside [%v, %v]", tc.est, tc.sample, tc.elapsed, got, tc.sample, tc.est)
		}
	}
}

// Invariants are checked rather than exact values because interleavings
// are not controlled; run with -race.
func TestLatencyConcurrent(t *testing.T) {
	const (
		writers   = 8
		readers   = 4
		perWriter = 2000
		minSample = time.Millisecond
		maxSample = 100 * time.Millisecond
	)

	var l latencyEstimate
	var writing, reading sync.WaitGroup
	done := make(chan struct{})

	for w := range writers {
		writing.Go(func() {
			for i := range perWriter {
				sample := minSample + time.Duration((i*7+w)%100)*time.Millisecond
				sample = min(sample, maxSample)
				// writers use interleaved, partly decreasing timestamps
				at := time.Duration(i*writers+(writers-w)) * time.Millisecond
				l.observe(sample, latencyBase.Add(at))
			}
		})
	}

	errs := make(chan string, readers)
	for range readers {
		reading.Go(func() {
			var prev latencySnapshot
			check := func() bool {
				got := l.snapshot()
				switch {
				case got.samples < prev.samples:
					errs <- "samples decreased"
				case got.updated.Before(prev.updated):
					errs <- "updated moved backwards"
				case got.samples == 0 && (got.est != 0 || !got.updated.IsZero()):
					errs <- "state without samples"
				case got.samples > 0 && (got.est < minSample || got.est > maxSample || got.updated.IsZero()):
					errs <- "estimate or timestamp inconsistent with samples"
				default:
					prev = got
					return true
				}
				return false
			}
			for {
				select {
				case <-done:
					check()
					return
				default:
					if !check() {
						return
					}
				}
			}
		})
	}

	writing.Wait()
	close(done)
	reading.Wait()
	close(errs)
	for msg := range errs {
		t.Error(msg)
	}

	if got := l.snapshot().samples; got != writers*perWriter {
		t.Errorf("samples = %d, want %d", got, writers*perWriter)
	}
}

// The tests below pin the lifecycle that latency state inherits from Host.

func TestLatencyStaticHostLifecycle(t *testing.T) {
	const addr = "192.0.2.70:80"
	h := Handler{}

	// release whatever references remain, even if the test fails midway
	t.Cleanup(func() {
		for {
			if _, ok := hosts.References(addr); !ok {
				return
			}
			_, _ = hosts.Delete(addr)
		}
	})

	u1, u2 := &Upstream{Dial: addr}, &Upstream{Dial: addr}
	h.provisionUpstream(u1, false)
	h.provisionUpstream(u2, false)
	if u1.Host != u2.Host {
		t.Fatal("upstreams with one dial address must share a Host")
	}

	u1.Host.latency.observe(20*time.Millisecond, latencyBase)
	if got := u2.Host.latency.snapshot().samples; got != 1 {
		t.Fatalf("second upstream sees %d samples, want 1", got)
	}

	// one reference released, as when a reload drops the old handler
	if deleted, _ := hosts.Delete(addr); deleted {
		t.Fatal("host removed while a reference remains")
	}
	u3 := &Upstream{Dial: addr}
	h.provisionUpstream(u3, false)
	if u3.Host != u1.Host || u3.Host.latency.snapshot().samples != 1 {
		t.Fatal("latency state should survive while a reference remains")
	}

	// last references released
	for range 2 {
		_, _ = hosts.Delete(addr)
	}
	u4 := &Upstream{Dial: addr}
	h.provisionUpstream(u4, false)
	if u4.Host == u1.Host || u4.Host.latency.snapshot().samples != 0 {
		t.Fatal("a host removed from the pool must come back with no latency data")
	}
}

func TestLatencyDynamicHostLifecycle(t *testing.T) {
	resetDynamicHosts()
	const addr = "192.0.2.71:80"
	h := Handler{}

	u1 := &Upstream{Dial: addr}
	h.provisionUpstream(u1, true)
	u1.Host.latency.observe(20*time.Millisecond, latencyBase)

	// a later request builds a new Upstream for the same address
	u2 := &Upstream{Dial: addr}
	h.provisionUpstream(u2, true)
	if u2.Host != u1.Host || u2.Host.latency.snapshot().samples != 1 {
		t.Fatal("dynamic upstreams for one address should share latency state")
	}

	// age the entry past the idle expiry and run the same sweep as the
	// background cleaner (the cleaner goroutine itself is not exercised)
	dynamicHostsMu.Lock()
	entry := dynamicHosts[addr]
	entry.lastSeen = time.Now().Add(-2 * dynamicHostIdleExpiry)
	dynamicHosts[addr] = entry
	for a, e := range dynamicHosts {
		if time.Since(e.lastSeen) > dynamicHostIdleExpiry {
			delete(dynamicHosts, a)
		}
	}
	dynamicHostsMu.Unlock()

	u3 := &Upstream{Dial: addr}
	h.provisionUpstream(u3, true)
	if u3.Host == u1.Host || u3.Host.latency.snapshot().samples != 0 {
		t.Fatal("an evicted dynamic host must come back with no latency data")
	}
}
