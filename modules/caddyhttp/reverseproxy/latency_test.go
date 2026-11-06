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
	"context"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
)

// latencyBase is the origin of the synthetic clock used by the latency tests.
var latencyBase = time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)

// noLatency is the estimate of an upstream that has not completed a request.
const noLatency = time.Duration(-1)

// testUpstream returns an upstream with the given latency estimate, sampled
// at latencyBase unless it is noLatency, and number of active requests.
func testUpstream(dial string, est time.Duration, active int) *Upstream {
	u := &Upstream{Host: new(Host), Dial: dial}
	if est != noLatency {
		u.latency.observe(est, latencyBase)
	}
	u.Host.numRequests.Store(int64(active))
	return u
}

func latencySamples(u *Upstream) uint64 { return u.latency.snapshot().samples }

// latencyBackend starts a backend served by handler and returns its address.
func latencyBackend(t *testing.T, handler http.HandlerFunc) string {
	t.Helper()
	backend := httptest.NewServer(handler)
	t.Cleanup(backend.Close)
	return backend.Listener.Addr().String()
}

func statusHandler(status int) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(status) }
}

// latencyHandler returns a handler that records latency while proxying to upstreams.
func latencyHandler(retries int, upstreams ...*Upstream) *Handler {
	h := minimalHandler(retries, upstreams...)
	h.trackLatency = true
	return h
}

func serveLatencyRequest(ctx context.Context, h *Handler) error {
	req := prepareTestRequest(httptest.NewRequestWithContext(ctx, http.MethodGet, "http://example.com/", nil))
	return h.ServeHTTP(httptest.NewRecorder(), req, caddyhttp.HandlerFunc(func(http.ResponseWriter, *http.Request) error {
		return nil
	}))
}

func TestLatencyEstimate(t *testing.T) {
	const ms = time.Millisecond
	type sample struct{ latency, at time.Duration } // at is the offset from latencyBase
	maxDuration := time.Duration(math.MaxInt64)

	for _, tc := range []struct {
		name    string
		samples []sample
		want    time.Duration
	}{
		{"first sample", []sample{{20 * ms, 0}}, 20 * ms},
		{"slower sample replaces the estimate", []sample{{10 * ms, 0}, {50 * ms, time.Second}}, 50 * ms},
		// 10ms + (50ms-10ms)*exp(-10s/10s)
		{"faster sample decays toward it", []sample{{50 * ms, 0}, {10 * ms, 10 * time.Second}}, 24715 * time.Microsecond},
		{"faster sample at the same instant is ignored", []sample{{50 * ms, 0}, {10 * ms, 0}}, 50 * ms},
		{"older sample can raise the estimate", []sample{{30 * ms, 0}, {10 * ms, 10 * time.Second}, {100 * ms, 5 * time.Second}}, 100 * ms},
		{"older sample cannot lower the estimate", []sample{{30 * ms, 0}, {50 * ms, 10 * time.Second}, {10 * ms, 5 * time.Second}}, 50 * ms},
		{"estimate at the int64 limit", []sample{{maxDuration, 0}, {maxDuration - 1, time.Second}}, maxDuration},
	} {
		var l latencyEstimate
		var latest time.Duration
		for _, s := range tc.samples {
			l.observe(s.latency, latencyBase.Add(s.at))
			latest = max(latest, s.at)
		}
		got := l.snapshot()
		if diff := got.est - tc.want; diff < -10*time.Microsecond || diff > 10*time.Microsecond {
			t.Errorf("%s: estimate = %v, want %v", tc.name, got.est, tc.want)
		}
		// the timestamp must never move backwards
		if want := latencyBase.Add(latest); !got.updated.Equal(want) {
			t.Errorf("%s: updated = %v, want %v", tc.name, got.updated, want)
		}
	}
}

// Completed responses are recorded, including server errors; errors are not.
// A retried attempt is recorded on the upstream that served it.
func TestLatencyRecording(t *testing.T) {
	ok := statusHandler(http.StatusOK)
	hang := func(_ http.ResponseWriter, r *http.Request) { <-r.Context().Done() }

	for _, tc := range []struct {
		name      string
		pool      []http.HandlerFunc // upstream behaviours in pool order, nil if nothing listens
		retryOn   string             // retry_match expression; dial errors are always retried
		transport http.RoundTripper
		untracked bool
		wantErr   bool
		want      []uint64 // samples recorded per upstream
	}{
		{name: "success", pool: []http.HandlerFunc{ok}, want: []uint64{1}},
		{name: "server error", pool: []http.HandlerFunc{statusHandler(http.StatusInternalServerError)}, want: []uint64{1}},
		{name: "tracking disabled", pool: []http.HandlerFunc{ok}, untracked: true, want: []uint64{0}},
		{name: "dial failure", pool: []http.HandlerFunc{nil}, wantErr: true, want: []uint64{0}},
		{
			name: "response header timeout", pool: []http.HandlerFunc{hang}, wantErr: true, want: []uint64{0},
			transport: testTransport{&http.Transport{ResponseHeaderTimeout: 30 * time.Millisecond}},
		},
		// round robin starts with the second upstream, which fails
		{
			name: "retried 503 response", pool: []http.HandlerFunc{ok, statusHandler(http.StatusServiceUnavailable)},
			retryOn: "{http.reverse_proxy.status_code} in [503]", want: []uint64{1, 1},
		},
		{name: "retried dial failure", pool: []http.HandlerFunc{ok, nil}, want: []uint64{1, 0}},
	} {
		pool := make(UpstreamPool, len(tc.pool))
		for i, handler := range tc.pool {
			dial := deadUpstreamAddr(t)
			if handler != nil {
				dial = latencyBackend(t, handler)
			}
			pool[i] = testUpstream(dial, noLatency, 0)
		}
		h := latencyHandler(len(pool)-1, pool...)
		h.trackLatency = !tc.untracked
		if tc.transport != nil {
			h.Transport = tc.transport
		}
		if tc.retryOn != "" {
			h.LoadBalancing.RetryMatch = caddyhttp.MatcherSets{{newExpressionMatcher(t, tc.retryOn)}}
		}
		if err := serveLatencyRequest(context.Background(), h); (err != nil) != tc.wantErr {
			t.Errorf("%s: error = %v, want error: %v", tc.name, err, tc.wantErr)
		}
		for i, u := range pool {
			if got := latencySamples(u); got != tc.want[i] {
				t.Errorf("%s: samples of upstream %d = %d, want %d", tc.name, i, got, tc.want[i])
			}
		}
	}
}

func TestLatencyNotRecordedOnClientCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// the client gives up as soon as the upstream has received the request
	u := testUpstream(latencyBackend(t, func(_ http.ResponseWriter, r *http.Request) {
		cancel()
		<-r.Context().Done()
	}), noLatency, 0)

	_ = serveLatencyRequest(ctx, latencyHandler(0, u))
	if got := latencySamples(u); got != 0 {
		t.Errorf("samples = %d, want 0", got)
	}
}

// WebTransport reports its handshake time after the session has ended, so it
// must not be recorded.
func TestLatencyNotRecordedForWebTransport(t *testing.T) {
	u := testUpstream("192.0.2.1:80", noLatency, 0)
	latencyHandler(0, u).recordUpstreamRoundTrip(DialInfo{Upstream: u}, caddy.NewReplacer(), http.StatusOK, time.Millisecond)
	if got := latencySamples(u); got != 0 {
		t.Errorf("samples = %d, want 0", got)
	}
}

// The sample ends when the response headers arrive: it must already exist
// while the upstream is still holding back the body.
func TestLatencyMeasuredUntilResponseHeaders(t *testing.T) {
	const delay = 20 * time.Millisecond
	host := new(Host)
	seen := make(chan latencySnapshot, 1)
	dial := latencyBackend(t, func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(delay)
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		for deadline := time.Now().Add(5 * time.Second); host.latency.snapshot().samples == 0 && time.Now().Before(deadline); {
			time.Sleep(time.Millisecond)
		}
		seen <- host.latency.snapshot()
	})

	_ = serveLatencyRequest(context.Background(), latencyHandler(0, &Upstream{Host: host, Dial: dial}))
	if got := <-seen; got.samples != 1 || got.est < delay {
		t.Errorf("while the body was held back: %d samples, estimate %v, want 1 sample of at least %v", got.samples, got.est, delay)
	}
}

type staticUpstreams struct{ dial string }

func (s staticUpstreams) GetUpstreams(*http.Request) ([]*Upstream, error) {
	return []*Upstream{{Dial: s.dial}}, nil
}

// The estimate lives on the Host, so it is shared by every handler proxying to
// an address, including dynamic upstreams that are rebuilt for each request.
func TestLatencySharedPerHost(t *testing.T) {
	resetDynamicHosts()
	t.Cleanup(resetDynamicHosts)
	dial := latencyBackend(t, statusHandler(http.StatusOK))

	a, b := &Upstream{Dial: dial}, &Upstream{Dial: dial}
	a.fillHost()
	b.fillHost()
	t.Cleanup(func() { _, _ = hosts.Delete(dial); _, _ = hosts.Delete(dial) })
	dynamic := latencyHandler(0)
	dynamic.DynamicUpstreams = staticUpstreams{dial}

	for _, h := range []*Handler{latencyHandler(0, a), latencyHandler(0, b), dynamic, dynamic} {
		if err := serveLatencyRequest(context.Background(), h); err != nil {
			t.Fatal(err)
		}
	}
	if got := latencySamples(a); got != 2 {
		t.Errorf("samples on the static host of two handlers = %d, want 2", got)
	}
	dynamicHostsMu.RLock()
	entry, ok := dynamicHosts[dial]
	dynamicHostsMu.RUnlock()
	if !ok || entry.host.latency.snapshot().samples != 2 {
		t.Errorf("dynamic host persisted = %v, want persisted with 2 samples", ok)
	}
}
