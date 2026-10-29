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
	"fmt"
	"math"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
)

// scoredUpstream returns an upstream with a latency estimate sampled at
// latencyBase and the given number of active requests.
func scoredUpstream(dial string, est time.Duration, active int) *Upstream {
	u := knownUpstream(dial, est, 0)
	u.Host.numRequests.Store(int64(active))
	return u
}

func TestLeastLatencySelectRanking(t *testing.T) {
	const ms = time.Millisecond
	tests := []struct {
		name     string
		a, b     *Upstream
		draw     int // intn(2) result for a full tie, -1 if not expected to be used
		wantPick string
	}{
		{
			// scores 5ms and 20ms
			"lower latency wins at equal load",
			scoredUpstream("a", 5*ms, 0), scoredUpstream("b", 20*ms, 0), -1, "a",
		},
		{
			// scores 5ms*11 = 55ms and 20ms
			"much lower load beats a latency advantage",
			scoredUpstream("a", 5*ms, 10), scoredUpstream("b", 20*ms, 0), -1, "b",
		},
		{
			// scores 10ms*2 = 20ms and 20ms*1 = 20ms, so the idle one wins
			"equal score goes to fewer active requests",
			scoredUpstream("a", 10*ms, 1), scoredUpstream("b", 20*ms, 0), -1, "b",
		},
		{
			"equal score goes to fewer active requests (reversed)",
			scoredUpstream("a", 20*ms, 0), scoredUpstream("b", 10*ms, 1), -1, "a",
		},
		{
			"full tie, first draw",
			scoredUpstream("a", 10*ms, 2), scoredUpstream("b", 10*ms, 2), 0, "a",
		},
		{
			"full tie, second draw",
			scoredUpstream("a", 10*ms, 2), scoredUpstream("b", 10*ms, 2), 1, "b",
		},
		{
			// both slow: the relative ranking still decides, 2s*1 vs 3s*1
			"all candidates slow",
			scoredUpstream("a", 3*time.Second, 0), scoredUpstream("b", 2*time.Second, 0), -1, "b",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			intn := failIntn(t)
			if tt.draw >= 0 {
				intn = scriptedIntn(t, tt.draw)
			}
			pool := UpstreamPool{tt.a, tt.b}

			got := LeastLatencySelection{}.selectAt(pool, latencyBase, intn)

			if got == nil || got.Dial != tt.wantPick {
				t.Errorf("selected %v, want %q", got, tt.wantPick)
			}
		})
	}
}

func TestLeastLatencyZeroEstimate(t *testing.T) {
	const us = time.Microsecond
	tests := []struct {
		name     string
		a, b     *Upstream
		draw     int
		wantPick string
	}{
		{
			"zero estimates are ordered by active requests",
			scoredUpstream("a", 0, 3), scoredUpstream("b", 0, 1), -1, "b",
		},
		{
			// scores 1001ns and 1ns
			"load is visible with a zero estimate",
			scoredUpstream("a", 0, 1000), scoredUpstream("b", time.Nanosecond, 0), -1, "b",
		},
		{
			// against an idle 1µs upstream (score 1000ns): 999ns wins
			"zero estimate wins just below the crossover",
			scoredUpstream("a", 0, 998), scoredUpstream("b", us, 0), -1, "a",
		},
		{
			// score 1000ns on both, so the idle one wins
			"zero estimate at the crossover",
			scoredUpstream("a", 0, 999), scoredUpstream("b", us, 0), -1, "b",
		},
		{
			// score 1001ns against 1000ns
			"zero estimate just above the crossover",
			scoredUpstream("a", 0, 1000), scoredUpstream("b", us, 0), -1, "b",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			intn := failIntn(t)
			if tt.draw >= 0 {
				intn = scriptedIntn(t, tt.draw)
			}

			got := LeastLatencySelection{}.selectAt(UpstreamPool{tt.a, tt.b}, latencyBase, intn)

			if got == nil || got.Dial != tt.wantPick {
				t.Errorf("selected %v, want %q", got, tt.wantPick)
			}
		})
	}
}

func TestLeastLatencyScoreRange(t *testing.T) {
	// an integer active+1 would wrap to a negative number here
	busy := scoredUpstream("busy", time.Nanosecond, math.MaxInt)
	idle := scoredUpstream("idle", time.Nanosecond, 0)
	if got := (LeastLatencySelection{}).selectAt(UpstreamPool{busy, idle}, latencyBase, failIntn(t)); got != idle {
		t.Errorf("selected %v, want the idle upstream", got)
	}

	extreme := scoredUpstream("extreme", time.Duration(math.MaxInt64), math.MaxInt)
	small := scoredUpstream("small", time.Second, 0)
	if got := (LeastLatencySelection{}).selectAt(UpstreamPool{extreme, small}, latencyBase, failIntn(t)); got != small {
		t.Errorf("selected %v, want the small upstream", got)
	}
	score := latencyScore(time.Duration(math.MaxInt64), math.MaxInt)
	if math.IsInf(score, 0) || math.IsNaN(score) || score <= 0 {
		t.Errorf("score = %v, want a finite positive number", score)
	}
}

func TestLeastLatencyNegativeActiveCount(t *testing.T) {
	const ms = time.Millisecond
	for draw, want := range []string{"a", "b"} {
		negative := scoredUpstream("a", 10*ms, -5)
		idle := scoredUpstream("b", 10*ms, 0)

		// a negative count must rank like zero, not ahead of an idle upstream
		got := LeastLatencySelection{}.selectAt(UpstreamPool{negative, idle}, latencyBase, scriptedIntn(t, draw))

		if got.Dial != want {
			t.Errorf("draw %d selected %q, want %q", draw, got.Dial, want)
		}
	}
}

func TestLeastLatencySelectFewUpstreams(t *testing.T) {
	only := scoredUpstream("only", 10*time.Millisecond, 0)
	down := scoredUpstream("down", 1*time.Millisecond, 0)
	down.setHealthy(false)

	if got := (LeastLatencySelection{}).selectAt(nil, latencyBase, failIntn(t)); got != nil {
		t.Errorf("empty pool selected %v, want nil", got)
	}
	if got := (LeastLatencySelection{}).selectAt(UpstreamPool{down}, latencyBase, failIntn(t)); got != nil {
		t.Errorf("pool with no available upstream selected %v, want nil", got)
	}
	if got := (LeastLatencySelection{}).selectAt(UpstreamPool{down, only}, latencyBase, failIntn(t)); got != only {
		t.Errorf("selected %v, want the only available upstream", got)
	}
}

func TestLeastLatencyNeverSelectsUnavailable(t *testing.T) {
	down := scoredUpstream("down", time.Millisecond, 0)
	down.setHealthy(false)
	full := scoredUpstream("full", time.Millisecond, 1)
	full.MaxRequests = 1
	a := scoredUpstream("a", 10*time.Millisecond, 0)
	b := latencyUpstream("b") // unknown
	pool := UpstreamPool{down, full, a, b}

	rng := rand.New(rand.NewPCG(1, 2))
	now := latencyBase
	for range 1000 {
		now = now.Add(100 * time.Millisecond)
		a.latency.observe(10*time.Millisecond, now)
		got := LeastLatencySelection{}.selectAt(pool, now, rng.IntN)
		if got == nil || got == down || got == full {
			t.Fatalf("selected %v", got)
		}
	}
}

// With three available upstreams a given one is in exactly two of the three
// possible pairs, so an unknown upstream with an open claim is chosen for two
// of the three outcomes of the reservoir draw.
func TestLeastLatencyPairSamplingEnumerated(t *testing.T) {
	for draw, wantUnknown := range []bool{true, true, false} {
		k1 := scoredUpstream("k1", 10*time.Millisecond, 0)
		k2 := scoredUpstream("k2", 20*time.Millisecond, 0)
		unknown := latencyUpstream("unknown")
		pool := UpstreamPool{k1, k2, unknown}

		got := LeastLatencySelection{}.selectAt(pool, latencyBase, scriptedIntn(t, draw))

		if (got == unknown) != wantUnknown {
			t.Errorf("draw %d selected %q, unknown selected = %v, want %v", draw, got.Dial, got == unknown, wantUnknown)
		}
		if !wantUnknown && got != k1 {
			t.Errorf("draw %d selected %q, want the lower score of the known pair", draw, got.Dial)
		}
	}
}

func TestLeastLatencyDegradationAndRecovery(t *testing.T) {
	const ms = time.Millisecond
	a := knownUpstream("a", 5*ms, 0)
	b := knownUpstream("b", 5*ms, 0)
	pool := UpstreamPool{a, b}
	sel := LeastLatencySelection{}

	// a slow response raises the estimate at once, so a loses every pairing
	a.latency.observe(200*ms, latencyBase.Add(time.Second))
	for range 2 {
		if got := sel.selectAt(pool, latencyBase.Add(time.Second), failIntn(t)); got != b {
			t.Fatalf("selected %q while a is slow, want b", got.Dial)
		}
	}

	// a minute without samples makes a unknown while b keeps being used
	now := latencyBase.Add(62 * time.Second)
	b.latency.observe(6*ms, now)
	if got := sel.selectAt(pool, now, failIntn(t)); got != a {
		t.Fatalf("selected %q, want a on its trial", got.Dial)
	}
	if got := sel.selectAt(pool, now, failIntn(t)); got != b {
		t.Fatalf("selected %q, want b once the trial is claimed", got.Dial)
	}

	// 5 + 195*exp(-6.1) = 5.437ms, so one fast sample makes a competitive
	a.latency.observe(5*ms, now)
	if est := a.latency.snapshot().est; est < 5427*time.Microsecond || est > 5447*time.Microsecond {
		t.Fatalf("estimate = %v, want about 5.437ms", est)
	}
	if got := sel.selectAt(pool, now, failIntn(t)); got != a {
		t.Errorf("selected %q after recovery, want a", got.Dial)
	}
}

// A seeded simulation of three upstreams, one ten times slower. It checks the
// aggregate behaviour for regressions and is not evidence of real benefit.
func TestLeastLatencySimulation(t *testing.T) {
	latencies := []time.Duration{5 * time.Millisecond, 5 * time.Millisecond, 50 * time.Millisecond}
	pool := make(UpstreamPool, len(latencies))
	for i := range pool {
		pool[i] = latencyUpstream("192.0.2." + string(rune('1'+i)) + ":80")
	}
	type pending struct {
		at time.Time
		i  int
	}
	var inflight []pending
	counts := make([]int, len(pool))
	rng := rand.New(rand.NewPCG(3, 4))

	now := latencyBase
	for range 20000 {
		now = now.Add(time.Millisecond)

		rest := inflight[:0]
		for _, p := range inflight {
			if p.at.After(now) {
				rest = append(rest, p)
				continue
			}
			pool[p.i].Host.countRequest(-1)
			pool[p.i].latency.observe(latencies[p.i], p.at)
		}
		inflight = rest

		got := LeastLatencySelection{}.selectAt(pool, now, rng.IntN)
		for i, u := range pool {
			if u == got {
				counts[i]++
				u.Host.countRequest(1)
				inflight = append(inflight, pending{now.Add(latencies[i]), i})
			}
		}
	}

	t.Logf("selections per upstream (5ms, 5ms, 50ms): %v", counts)
	if counts[2]*4 > counts[0] || counts[2]*4 > counts[1] {
		t.Errorf("slow upstream got %d selections against %d and %d", counts[2], counts[0], counts[1])
	}
}

func TestLeastLatencySelectUsesCurrentTime(t *testing.T) {
	fast := latencyUpstream("fast")
	slow := latencyUpstream("slow")
	fast.Host.observeLatency(5 * time.Millisecond)
	slow.Host.observeLatency(50 * time.Millisecond)

	got := LeastLatencySelection{}.Select(UpstreamPool{slow, fast}, nil, nil)

	if got != fast {
		t.Errorf("selected %v, want the faster upstream", got)
	}
}

func TestLeastLatencyModuleAndCaddyfile(t *testing.T) {
	info := LeastLatencySelection{}.CaddyModule()
	if want := "http.reverse_proxy.selection_policies.least_latency"; string(info.ID) != want {
		t.Errorf("module ID = %q, want %q", info.ID, want)
	}
	if _, ok := info.New().(*LeastLatencySelection); !ok {
		t.Errorf("New returned %T", info.New())
	}

	var s LeastLatencySelection
	if err := s.UnmarshalCaddyfile(caddyfile.NewTestDispenser("least_latency")); err != nil {
		t.Errorf("no arguments: %v", err)
	}
	if err := s.UnmarshalCaddyfile(caddyfile.NewTestDispenser("least_latency 2")); err == nil {
		t.Error("an argument should be rejected")
	}
}

// When both sampled candidates are unknown and their claims are spent, the
// known upstreams drawn from the pool are scored, not the original pair.
func TestLeastLatencyScoresKnownSample(t *testing.T) {
	for _, busy := range []bool{false, true} {
		u1, u2 := latencyUpstream("u1"), latencyUpstream("u2")
		k1 := knownUpstream("k1", 5*time.Millisecond, 0)
		k2 := knownUpstream("k2", 8*time.Millisecond, 0)
		if busy {
			// scores 5ms*11 = 55ms against 8ms
			k1.Host.numRequests.Store(10)
		}
		u1.latency.claimProbe(latencyBase)
		u2.latency.claimProbe(latencyBase)

		// the draws keep u1 and u2 as the pair, then try u1's claim first
		got := LeastLatencySelection{}.selectAt(UpstreamPool{u1, u2, k1, k2}, latencyBase, scriptedIntn(t, 2, 3, 0))

		want := k1
		if busy {
			want = k2
		}
		if got != want {
			t.Errorf("busy=%v selected %q, want %q", busy, got.Dial, want.Dial)
		}
	}
}

// An unknown upstream is chosen at most once per probe interval however many
// selections run at once, including when both sampled candidates are unknown.
func TestLeastLatencyConcurrentBound(t *testing.T) {
	const goroutines, selections = 8, 20000
	known := knownUpstream("known", 10*time.Millisecond, 0)
	unknown := []*Upstream{latencyUpstream("u1"), latencyUpstream("u2"), latencyUpstream("u3")}
	pool := UpstreamPool{known, unknown[0], unknown[1], unknown[2]}

	var tick, latest atomic.Int64
	chosen := make([]atomic.Int64, len(unknown))
	var wg sync.WaitGroup
	for range goroutines {
		wg.Go(func() {
			for range selections {
				// each call gets its own, possibly out of order, time
				at := time.Duration(tick.Add(1)) * time.Millisecond
				for l := latest.Load(); int64(at) > l && !latest.CompareAndSwap(l, int64(at)); l = latest.Load() {
				}
				now := latencyBase.Add(at)
				if now.Sub(known.latency.snapshot().updated) >= 5*time.Second {
					known.latency.observe(10*time.Millisecond, now)
				}

				got := LeastLatencySelection{}.selectAt(pool, now, rand.IntN)
				for i, u := range unknown {
					if got == u {
						chosen[i].Add(1)
					}
				}
			}
		})
	}
	wg.Wait()

	limit := 1 + int(time.Duration(latest.Load())/latencyProbeInterval)
	for i := range unknown {
		if got := int(chosen[i].Load()); got > limit {
			t.Errorf("unknown upstream %d chosen %d times, want at most %d", i, got, limit)
		}
	}
}

// An upstream that never completes a request never gains an estimate, so it
// must be bounded by the probe interval however often it is drawn.
func TestLeastLatencyFailingUpstreamBounded(t *testing.T) {
	const step = 10 * time.Millisecond
	const window = 100 * time.Second
	limit := 1 + int(window/latencyProbeInterval)

	for _, failing := range []int{1, 2} {
		t.Run(fmt.Sprintf("failing=%d", failing), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(uint64(failing), 1))
			pool := UpstreamPool{knownUpstream("good", 5*time.Millisecond, 0)}
			for i := range failing {
				pool = append(pool, latencyUpstream(fmt.Sprintf("failing%d", i)))
			}

			counts := make(map[*Upstream]int)
			for at := time.Duration(0); at < window; at += step {
				now := latencyBase.Add(at)
				got := LeastLatencySelection{}.selectAt(pool, now, rng.IntN)
				counts[got]++
				if got == pool[0] {
					got.latency.observe(5*time.Millisecond, now)
				}
			}

			for _, u := range pool[1:] {
				if counts[u] == 0 || counts[u] > limit {
					t.Errorf("%s selected %d times, want between 1 and %d", u.Dial, counts[u], limit)
				}
			}
		})
	}
}

// Concurrent requests that all draw an unknown upstream must not all be sent
// to it: one reserves the probe and the rest go to the known upstream, and
// the next probe is only available after the interval.
func TestLeastLatencySelectConcurrentProbe(t *testing.T) {
	known := knownUpstream("known", 5*time.Millisecond, 0)
	unknown := latencyUpstream("unknown")
	pool := UpstreamPool{known, unknown}

	wave := func(now time.Time) (probes, others int32) {
		var p, o atomic.Int32
		var wg sync.WaitGroup
		for range 64 {
			wg.Go(func() {
				switch (LeastLatencySelection{}).selectAt(pool, now, rand.IntN) {
				case unknown:
					p.Add(1)
				case known:
					o.Add(1)
				}
			})
		}
		wg.Wait()
		return p.Load(), o.Load()
	}

	if probes, others := wave(latencyBase); probes != 1 || others != 63 {
		t.Errorf("first wave: %d probes and %d known, want 1 and 63", probes, others)
	}
	if probes, _ := wave(latencyBase.Add(latencyProbeInterval - 1)); probes != 0 {
		t.Errorf("wave inside the interval: %d probes, want 0", probes)
	}
	if probes, others := wave(latencyBase.Add(latencyProbeInterval)); probes != 1 || others != 63 {
		t.Errorf("wave after the interval: %d probes and %d known, want 1 and 63", probes, others)
	}
}
