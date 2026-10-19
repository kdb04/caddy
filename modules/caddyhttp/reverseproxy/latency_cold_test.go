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
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// scriptedIntn returns the given values in order and fails
// the test if one is out of range or the script runs out.
func scriptedIntn(t *testing.T, vals ...int) func(int) int {
	t.Helper()
	var i int
	return func(n int) int {
		t.Helper()
		if i >= len(vals) {
			t.Fatalf("intn(%d) called more than the %d scripted times", n, len(vals))
		}
		v := vals[i]
		i++
		if v < 0 || v >= n {
			t.Fatalf("scripted value %d is out of range for intn(%d)", v, n)
		}
		return v
	}
}

// failIntn fails the test if randomness is consulted.
func failIntn(t *testing.T) func(int) int {
	return func(n int) int {
		t.Helper()
		t.Fatalf("unexpected call to intn(%d)", n)
		return 0
	}
}

func knownUpstream(dial string, est, at time.Duration) *Upstream {
	u := latencyUpstream(dial)
	u.latency.observe(est, latencyBase.Add(at))
	return u
}

func TestLatencyKnown(t *testing.T) {
	const ms = time.Millisecond
	tests := []struct {
		name  string
		steps []latencyStep
		now   time.Duration
		want  bool
	}{
		{"never sampled", nil, 0, false},
		{"fresh sample", []latencyStep{{20 * ms, 0}}, 0, true},
		{"zero-latency sample is data", []latencyStep{{0, 0}}, 0, true},
		{"exactly staleAfter old", []latencyStep{{20 * ms, 0}}, latencyStaleAfter, true},
		{"1ns past staleAfter", []latencyStep{{20 * ms, 0}}, latencyStaleAfter + 1, false},
		{"updated after now", []latencyStep{{20 * ms, time.Second}}, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var l latencyEstimate
			applyLatencySteps(&l, tt.steps...)
			if got := l.snapshot().known(latencyBase.Add(tt.now)); got != tt.want {
				t.Errorf("known = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLatencyClaimProbe(t *testing.T) {
	var l latencyEstimate
	now := latencyBase

	if !l.claimProbe(now) {
		t.Fatal("first claim on a fresh upstream should succeed")
	}
	for _, d := range []time.Duration{0, latencyProbeInterval - 1} {
		if l.claimProbe(now.Add(d)) {
			t.Errorf("claim at +%v should fail", d)
		}
	}
	if !l.claimProbe(now.Add(latencyProbeInterval)) {
		t.Error("claim at +interval should succeed")
	}

	// a failed claim must not push the next one back
	next := now.Add(latencyProbeInterval)
	l.claimProbe(next.Add(latencyProbeInterval - 1))
	if !l.claimProbe(next.Add(latencyProbeInterval)) {
		t.Error("claim at +2 intervals should succeed after a failed claim")
	}
}

func TestLatencyClaimProbeConcurrent(t *testing.T) {
	var l latencyEstimate
	var wins atomic.Int32
	var wg sync.WaitGroup
	for range 64 {
		wg.Go(func() {
			if l.claimProbe(latencyBase) {
				wins.Add(1)
			}
		})
	}
	wg.Wait()

	if got := wins.Load(); got != 1 {
		t.Errorf("successful claims = %d, want 1", got)
	}
}

func TestResolveColdBothKnown(t *testing.T) {
	a := knownUpstream("192.0.2.1:80", 10*time.Millisecond, 0)
	b := knownUpstream("192.0.2.2:80", 20*time.Millisecond, 0)

	got := resolveCold(a, b, UpstreamPool{a, b}, latencyBase, failIntn(t))

	if got.winner != nil || got.compare != [2]*Upstream{a, b} {
		t.Errorf("got %+v, want compare of both candidates", got)
	}
}

func TestResolveColdKnownVersusUnknown(t *testing.T) {
	for _, unknownFirst := range []bool{true, false} {
		known := knownUpstream("192.0.2.1:80", 10*time.Millisecond, 0)
		unknown := latencyUpstream("192.0.2.2:80")
		a, b := known, unknown
		if unknownFirst {
			a, b = unknown, known
		}
		pool := UpstreamPool{a, b}

		choose := func(at time.Duration) *Upstream {
			got := resolveCold(a, b, pool, latencyBase.Add(at), failIntn(t))
			if got.compare != [2]*Upstream{} {
				t.Fatalf("got compare %v, want a winner", got.compare)
			}
			return got.winner
		}

		if choose(0) != unknown {
			t.Error("first claim should select the unknown upstream")
		}
		if choose(0) != known {
			t.Error("a second selection at the same time should select the known upstream")
		}
		if choose(latencyProbeInterval-1) != known {
			t.Error("just before the interval should select the known upstream")
		}
		if choose(latencyProbeInterval) != unknown {
			t.Error("at the interval the unknown upstream should be tried again")
		}
	}
}

func TestResolveColdIndependentClaims(t *testing.T) {
	known := knownUpstream("192.0.2.1:80", 10*time.Millisecond, 0)
	u1, u2 := latencyUpstream("192.0.2.2:80"), latencyUpstream("192.0.2.3:80")
	pool := UpstreamPool{known, u1, u2}

	for _, u := range []*Upstream{u1, u2} {
		if got := resolveCold(known, u, pool, latencyBase, failIntn(t)); got.winner != u {
			t.Errorf("new upstream %s should get its own trial", u.Dial)
		}
	}
}

func TestResolveColdBothUnknownOneKnown(t *testing.T) {
	a, b := latencyUpstream("192.0.2.1:80"), latencyUpstream("192.0.2.2:80")
	k := knownUpstream("192.0.2.3:80", 10*time.Millisecond, 0)
	pool := UpstreamPool{a, b, k}
	intn := scriptedIntn(t, 0, 0, 0)

	// the claim order is a then b; each claim is consumed once
	for i, want := range []*Upstream{a, b, k} {
		got := resolveCold(a, b, pool, latencyBase, intn)
		if got.winner != want {
			t.Errorf("selection %d = %v, want %v", i, got.winner, want)
		}
	}
}

func TestResolveColdBothUnknownClaimOrder(t *testing.T) {
	a, b := latencyUpstream("192.0.2.1:80"), latencyUpstream("192.0.2.2:80")
	k := knownUpstream("192.0.2.3:80", 10*time.Millisecond, 0)

	got := resolveCold(a, b, UpstreamPool{a, b, k}, latencyBase, scriptedIntn(t, 1))

	if got.winner != b {
		t.Errorf("winner = %v, want b, which the random order tries first", got.winner)
	}
	if !a.latency.claimProbe(latencyBase) {
		t.Error("only the first successful claim should be consumed")
	}
}

func TestResolveColdBothUnknownTwoKnown(t *testing.T) {
	for _, tt := range []struct {
		name string
		j    int // draw for the third known upstream
		want [2]int
	}{
		{"third known upstream not sampled", 2, [2]int{0, 1}},
		{"third replaces the first", 0, [2]int{2, 1}},
		{"third replaces the second", 1, [2]int{0, 2}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			a, b := latencyUpstream("192.0.2.1:80"), latencyUpstream("192.0.2.2:80")
			ks := []*Upstream{
				knownUpstream("192.0.2.3:80", 10*time.Millisecond, 0),
				knownUpstream("192.0.2.4:80", 10*time.Millisecond, 0),
				knownUpstream("192.0.2.5:80", 10*time.Millisecond, 0),
			}
			pool := UpstreamPool{a, b, ks[0], ks[1], ks[2]}
			a.latency.claimProbe(latencyBase)
			b.latency.claimProbe(latencyBase)

			got := resolveCold(a, b, pool, latencyBase, scriptedIntn(t, tt.j, 0))

			want := [2]*Upstream{ks[tt.want[0]], ks[tt.want[1]]}
			if got.winner != nil || got.compare != want {
				t.Errorf("got %+v, want compare of %v", got, want)
			}
		})
	}
}

func TestResolveColdColdPool(t *testing.T) {
	a, b := latencyUpstream("192.0.2.1:80"), latencyUpstream("192.0.2.2:80")
	c := latencyUpstream("192.0.2.3:80")
	pool := UpstreamPool{a, b, c}
	a.Host.countRequest(3)
	b.Host.countRequest(1)

	// no claims are involved, so the choice repeats
	for range 2 {
		if got := resolveCold(a, b, pool, latencyBase, failIntn(t)); got.winner != b {
			t.Fatalf("winner = %v, want the upstream with fewer requests", got.winner)
		}
	}

	b.Host.countRequest(2)
	for i, want := range []*Upstream{a, b} {
		if got := resolveCold(a, b, pool, latencyBase, scriptedIntn(t, i)); got.winner != want {
			t.Errorf("tie with draw %d = %v, want %v", i, got.winner, want)
		}
	}
}

func TestResolveColdIgnoresUnavailableKnown(t *testing.T) {
	a, b := latencyUpstream("192.0.2.1:80"), latencyUpstream("192.0.2.2:80")
	k := knownUpstream("192.0.2.3:80", 10*time.Millisecond, 0)
	k.setHealthy(false)
	a.Host.countRequest(1)

	got := resolveCold(a, b, UpstreamPool{a, b, k}, latencyBase, scriptedIntn(t, 0))

	if got.winner != b {
		t.Errorf("winner = %v, want the idle candidate, as in a cold pool", got.winner)
	}
}

func TestResolveColdStaleHostIsTriedOnce(t *testing.T) {
	const ms = time.Millisecond
	const stale = latencyStaleAfter + time.Second

	slow := knownUpstream("192.0.2.1:80", 500*ms, 0)
	k := knownUpstream("192.0.2.2:80", 5*ms, stale)
	pool := UpstreamPool{slow, k}
	now := latencyBase.Add(stale)

	if slow.latency.snapshot().known(now) {
		t.Fatal("the slow upstream should be stale")
	}
	if got := resolveCold(slow, k, pool, now, failIntn(t)); got.winner != slow {
		t.Fatalf("winner = %v, want the stale upstream on its trial", got.winner)
	}

	t.Run("fast again", func(t *testing.T) {
		// 5 + 495*exp(-6.1) = 6.110ms
		slow.latency.observe(5*ms, now)
		got := slow.latency.snapshot().est
		if diff := got - 6110*time.Microsecond; diff < -10*time.Microsecond || diff > 10*time.Microsecond {
			t.Errorf("estimate = %v, want about 6.11ms", got)
		}
	})

	t.Run("still slow", func(t *testing.T) {
		var l latencyEstimate
		applyLatencySteps(&l, latencyStep{500 * ms, 0}, latencyStep{500 * ms, stale})
		s := l.snapshot()
		if s.est != 500*ms {
			t.Errorf("estimate = %v, want 500ms", s.est)
		}
		if !s.known(latencyBase.Add(stale + latencyStaleAfter)) {
			t.Error("a fresh sample should keep the upstream known for staleAfter")
		}
		if s.known(latencyBase.Add(stale + latencyStaleAfter + 1)) {
			t.Error("the upstream should go stale again after staleAfter")
		}
	})
}

func TestResolveColdTrialsOnlyAtIntervals(t *testing.T) {
	known := knownUpstream("192.0.2.1:80", 10*time.Millisecond, 0)
	unknown := latencyUpstream("192.0.2.2:80")
	pool := UpstreamPool{known, unknown}

	var trials int
	for at := time.Duration(0); at <= 10*time.Second; at += 250 * time.Millisecond {
		// refresh the known upstream so only the other is under test
		known.latency.observe(10*time.Millisecond, latencyBase.Add(at))
		if resolveCold(known, unknown, pool, latencyBase.Add(at), failIntn(t)).winner == unknown {
			trials++
		}
	}

	if trials != 11 {
		t.Errorf("trials over 10s = %d, want 11", trials)
	}
}

func TestResolveColdConcurrent(t *testing.T) {
	t.Run("known and unknown", func(t *testing.T) {
		known := knownUpstream("192.0.2.1:80", 10*time.Millisecond, 0)
		unknown := latencyUpstream("192.0.2.2:80")
		pool := UpstreamPool{known, unknown}

		var wins atomic.Int32
		var wg sync.WaitGroup
		for range 64 {
			wg.Go(func() {
				if resolveCold(known, unknown, pool, latencyBase, rand.IntN).winner == unknown {
					wins.Add(1)
				}
			})
		}
		wg.Wait()

		if got := wins.Load(); got != 1 {
			t.Errorf("trials = %d, want 1", got)
		}
	})

	t.Run("two unknown", func(t *testing.T) {
		u1, u2 := latencyUpstream("192.0.2.1:80"), latencyUpstream("192.0.2.2:80")
		known := knownUpstream("192.0.2.3:80", 10*time.Millisecond, 0)
		pool := UpstreamPool{u1, u2, known}

		var w1, w2 atomic.Int32
		var wg sync.WaitGroup
		for range 64 {
			wg.Go(func() {
				switch resolveCold(u1, u2, pool, latencyBase, rand.IntN).winner {
				case u1:
					w1.Add(1)
				case u2:
					w2.Add(1)
				}
			})
		}
		wg.Wait()

		if w1.Load() != 1 || w2.Load() != 1 {
			t.Errorf("trials = %d and %d, want 1 each", w1.Load(), w2.Load())
		}
	})
}

// samplePair draws two distinct available upstreams like random_choose.
func samplePair(pool UpstreamPool, intn func(int) int) (a, b *Upstream, ok bool) {
	var sample [2]*Upstream
	var count int
	for _, u := range pool {
		if !u.Available() {
			continue
		}
		count++
		if count <= len(sample) {
			sample[count-1] = u
		} else if j := intn(count); j < len(sample) {
			sample[j] = u
		}
	}
	return sample[0], sample[1], count >= 2
}

// Whenever a known upstream is available, each unknown upstream may be
// returned at most once per probe interval, including when both sampled
// candidates are unknown.
func TestResolveColdBound(t *testing.T) {
	tests := []struct {
		name          string
		known         int
		unknown       int
		becomeKnownAt time.Duration // unknown upstreams get a sample, 0 for never
	}{
		{"one known, three unknown", 1, 3, 0},
		{"two known, three unknown", 2, 3, 0},
		{"unknown upstreams become known", 1, 3, 50 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rng := rand.New(rand.NewPCG(1, 2))
			var pool, knowns, unknowns UpstreamPool
			for range tt.known {
				u := knownUpstream("192.0.2.1:80", 10*time.Millisecond, 0)
				pool, knowns = append(pool, u), append(knowns, u)
			}
			for range tt.unknown {
				u := latencyUpstream("192.0.2.2:80")
				pool, unknowns = append(pool, u), append(unknowns, u)
			}

			now := latencyBase
			returned := make(map[*Upstream]int)
			var promoted bool
			for range 10000 {
				now = now.Add(time.Duration(rng.IntN(20_000_000)))
				for _, u := range knowns {
					if now.Sub(u.latency.snapshot().updated) >= 5*time.Second {
						u.latency.observe(10*time.Millisecond, now)
					}
				}
				if tt.becomeKnownAt > 0 && !promoted && now.Sub(latencyBase) >= tt.becomeKnownAt {
					for _, u := range unknowns {
						u.latency.observe(5*time.Millisecond, now)
					}
					promoted = true
				}

				a, b, ok := samplePair(pool, rng.IntN)
				if !ok {
					t.Fatal("pool should have two available upstreams")
				}
				knownA, knownB := a.latency.snapshot().known(now), b.latency.snapshot().known(now)
				if knownA && knownB {
					continue
				}

				got := resolveCold(a, b, pool, now, rng.IntN)
				chosen := got.winner
				if chosen == nil {
					chosen = got.compare[0]
				}
				if (chosen == a && !knownA) || (chosen == b && !knownB) {
					returned[chosen]++
				}
			}

			// counted only while unknown: up to the promotion, or the whole run
			elapsed := now.Sub(latencyBase)
			if tt.becomeKnownAt > 0 {
				elapsed = tt.becomeKnownAt
			}
			limit := 1 + int(elapsed/latencyProbeInterval)
			for i, u := range unknowns {
				t.Logf("unknown upstream %d returned %d times, limit %d", i, returned[u], limit)
				if returned[u] > limit {
					t.Errorf("unknown upstream %d returned %d times, want at most %d", i, returned[u], limit)
				}
				if returned[u] <= limit/2 {
					t.Errorf("unknown upstream %d returned only %d times, want more than %d", i, returned[u], limit/2)
				}
			}
		})
	}
}
