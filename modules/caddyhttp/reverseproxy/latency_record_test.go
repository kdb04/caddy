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
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
)

type timeoutError struct{}

func (timeoutError) Error() string   { return "i/o timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return false }

// signalBody reports its first Read on started, then blocks until release
// is closed, and then returns err (io.EOF when nil).
type signalBody struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
	err     error
}

func (b *signalBody) Read([]byte) (int, error) {
	b.once.Do(func() { close(b.started) })
	<-b.release
	if b.err != nil {
		return 0, b.err
	}
	return 0, io.EOF
}

func (b *signalBody) Close() error { return nil }

func responseWith(status int, body io.ReadCloser) *http.Response {
	return &http.Response{
		StatusCode: status,
		Status:     http.StatusText(status),
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     make(http.Header),
		Body:       body,
	}
}

func latencyHandler(rt http.RoundTripper, upstreams ...*Upstream) *Handler {
	h := minimalHandler(0, upstreams...)
	h.Transport = rt
	h.trackLatency = true
	return h
}

func latencyUpstream(dial string) *Upstream {
	return &Upstream{Host: new(Host), Dial: dial}
}

func latencySamples(u *Upstream) uint64 {
	return u.Host.latency.snapshot().samples
}

func serveLatencyRequest(ctx context.Context, h *Handler) error {
	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "http://example.com/", nil)
	req = prepareTestRequest(req)
	return h.ServeHTTP(httptest.NewRecorder(), req, caddyhttp.HandlerFunc(func(http.ResponseWriter, *http.Request) error {
		return nil
	}))
}

func waitLatency(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func TestLatencyRecordAnyStatus(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusInternalServerError} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			u := latencyUpstream("192.0.2.80:80")
			h := latencyHandler(roundTripFunc(func(*http.Request) (*http.Response, error) {
				return responseWith(status, io.NopCloser(strings.NewReader(""))), nil
			}), u)

			_ = serveLatencyRequest(context.Background(), h)

			if got := latencySamples(u); got != 1 {
				t.Errorf("samples = %d, want 1", got)
			}
		})
	}
}

func TestLatencyRecordClientCancel(t *testing.T) {
	received := make(chan struct{}, 1)
	release := make(chan struct{})
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received <- struct{}{}
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(backend.Close)
	t.Cleanup(func() { close(release) })

	u := latencyUpstream(backend.Listener.Addr().String())
	h := latencyHandler(testTransport{&http.Transport{}}, u)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = serveLatencyRequest(ctx, h)
	}()

	waitLatency(t, received, "backend to receive the request")
	cancel()
	waitLatency(t, done, "ServeHTTP to return")

	if got := latencySamples(u); got != 0 {
		t.Errorf("samples = %d, want 0", got)
	}
	if got := u.NumRequests(); got != 0 {
		t.Errorf("in-flight = %d, want 0", got)
	}
}

func TestLatencyRecordDialFailure(t *testing.T) {
	u := latencyUpstream(deadUpstreamAddr(t))
	h := latencyHandler(testTransport{&http.Transport{}}, u)

	if err := serveLatencyRequest(context.Background(), h); err == nil {
		t.Fatal("expected a dial error")
	}

	if got := latencySamples(u); got != 0 {
		t.Errorf("samples = %d, want 0", got)
	}
}

func TestLatencyRecordTimeoutError(t *testing.T) {
	u := latencyUpstream("192.0.2.81:80")
	h := latencyHandler(roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, timeoutError{}
	}), u)

	_ = serveLatencyRequest(context.Background(), h)

	if got := latencySamples(u); got != 0 {
		t.Errorf("samples = %d, want 0", got)
	}
}

func TestLatencyRecordResponseHeaderTimeout(t *testing.T) {
	release := make(chan struct{})
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(backend.Close)
	t.Cleanup(func() { close(release) })

	u := latencyUpstream(backend.Listener.Addr().String())
	h := latencyHandler(testTransport{&http.Transport{ResponseHeaderTimeout: 30 * time.Millisecond}}, u)

	if err := serveLatencyRequest(context.Background(), h); err == nil {
		t.Fatal("expected a header timeout error")
	}

	if got := latencySamples(u); got != 0 {
		t.Errorf("samples = %d, want 0", got)
	}
}

func TestLatencyRecordStreamingBody(t *testing.T) {
	body := &signalBody{started: make(chan struct{}), release: make(chan struct{})}
	var unblock sync.Once
	t.Cleanup(func() { unblock.Do(func() { close(body.release) }) })

	u := latencyUpstream("192.0.2.82:80")
	h := latencyHandler(roundTripFunc(func(*http.Request) (*http.Response, error) {
		return responseWith(http.StatusOK, body), nil
	}), u)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = serveLatencyRequest(context.Background(), h)
	}()

	// the first body read happens after the sample is taken
	waitLatency(t, body.started, "the body to be read")
	if got := latencySamples(u); got != 1 {
		t.Errorf("samples while streaming = %d, want 1", got)
	}
	if got := u.NumRequests(); got != 1 {
		t.Errorf("in-flight while streaming = %d, want 1", got)
	}

	unblock.Do(func() { close(body.release) })
	waitLatency(t, done, "ServeHTTP to return")
	if got := u.NumRequests(); got != 0 {
		t.Errorf("in-flight after streaming = %d, want 0", got)
	}
	if got := latencySamples(u); got != 1 {
		t.Errorf("samples after streaming = %d, want 1", got)
	}
}

func TestLatencyRecordMeasuresRoundTrip(t *testing.T) {
	const wait = 20 * time.Millisecond

	body := &signalBody{started: make(chan struct{}), release: make(chan struct{})}
	var unblock sync.Once
	t.Cleanup(func() { unblock.Do(func() { close(body.release) }) })

	u := latencyUpstream("192.0.2.87:80")
	h := latencyHandler(roundTripFunc(func(*http.Request) (*http.Response, error) {
		time.Sleep(wait)
		return responseWith(http.StatusOK, body), nil
	}), u)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = serveLatencyRequest(context.Background(), h)
	}()

	// the body is still blocked, so it can't be part of the value
	waitLatency(t, body.started, "the body to be read")
	if got := u.Host.latency.snapshot().est; got < wait {
		t.Errorf("estimate = %v, want at least %v", got, wait)
	}

	unblock.Do(func() { close(body.release) })
	waitLatency(t, done, "ServeHTTP to return")
}

func TestLatencyRecordBodyError(t *testing.T) {
	body := &signalBody{
		started: make(chan struct{}),
		release: make(chan struct{}),
		err:     errors.New("connection reset"),
	}
	close(body.release)

	u := latencyUpstream("192.0.2.83:80")
	h := latencyHandler(roundTripFunc(func(*http.Request) (*http.Response, error) {
		return responseWith(http.StatusOK, body), nil
	}), u)

	// the proxy aborts the handler when the body copy fails
	func() {
		defer func() {
			if v := recover(); v != nil && v != http.ErrAbortHandler {
				panic(v)
			}
		}()
		_ = serveLatencyRequest(context.Background(), h)
	}()

	if got := latencySamples(u); got != 1 {
		t.Errorf("samples = %d, want 1", got)
	}
}

func TestLatencyRecordRetryAttribution(t *testing.T) {
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(good.Close)

	t.Run("dial error then success", func(t *testing.T) {
		// RoundRobin picks index 1 first, then 0
		goodUp := latencyUpstream(good.Listener.Addr().String())
		deadUp := latencyUpstream(deadUpstreamAddr(t))
		h := latencyHandler(testTransport{&http.Transport{}}, goodUp, deadUp)
		h.LoadBalancing.Retries = 1

		if err := serveLatencyRequest(context.Background(), h); err != nil {
			t.Fatal(err)
		}

		if got := latencySamples(deadUp); got != 0 {
			t.Errorf("dead upstream samples = %d, want 0", got)
		}
		if got := latencySamples(goodUp); got != 1 {
			t.Errorf("good upstream samples = %d, want 1", got)
		}
	})

	t.Run("retry_match response then success", func(t *testing.T) {
		bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusServiceUnavailable)
		}))
		t.Cleanup(bad.Close)

		goodUp := latencyUpstream(good.Listener.Addr().String())
		badUp := latencyUpstream(bad.Listener.Addr().String())
		h := latencyHandler(testTransport{&http.Transport{}}, goodUp, badUp)
		h.LoadBalancing.Retries = 1
		h.LoadBalancing.RetryMatch = caddyhttp.MatcherSets{
			caddyhttp.MatcherSet{
				newExpressionMatcher(t, "{http.reverse_proxy.status_code} in [503]"),
			},
		}

		if err := serveLatencyRequest(context.Background(), h); err != nil {
			t.Fatal(err)
		}

		if got := latencySamples(badUp); got != 1 {
			t.Errorf("retried upstream samples = %d, want 1", got)
		}
		if got := latencySamples(goodUp); got != 1 {
			t.Errorf("good upstream samples = %d, want 1", got)
		}
	})
}

func TestLatencyRecordDisabled(t *testing.T) {
	u := latencyUpstream("192.0.2.84:80")
	h := latencyHandler(roundTripFunc(func(*http.Request) (*http.Response, error) {
		return responseWith(http.StatusOK, io.NopCloser(strings.NewReader(""))), nil
	}), u)
	h.trackLatency = false

	_ = serveLatencyRequest(context.Background(), h)

	if got := latencySamples(u); got != 0 {
		t.Errorf("samples = %d, want 0", got)
	}
}

// WebTransport reports its handshake time through this function after the
// session ends, so it must not feed the estimate.
func TestLatencyRecordUpstreamRoundTripDoesNotSample(t *testing.T) {
	h, cancel := newPassiveHandler(t, 1, time.Minute)
	defer cancel()
	u, cleanup := provisionedStaticUpstream(t, h, "192.0.2.85:80")
	defer cleanup()

	h.trackLatency = true
	h.recordUpstreamRoundTrip(DialInfo{Upstream: u}, caddy.NewReplacer(), http.StatusOK, 5*time.Millisecond)

	if got := latencySamples(u); got != 0 {
		t.Errorf("samples = %d, want 0", got)
	}
}

func TestLatencyRecordSharedHost(t *testing.T) {
	const addr = "192.0.2.86:80"
	a, b := &Upstream{Dial: addr}, &Upstream{Dial: addr}
	a.fillHost()
	b.fillHost()
	t.Cleanup(func() {
		for {
			if refs, _ := hosts.References(addr); refs == 0 {
				return
			}
			_, _ = hosts.Delete(addr)
		}
	})

	rt := roundTripFunc(func(*http.Request) (*http.Response, error) {
		return responseWith(http.StatusOK, io.NopCloser(strings.NewReader(""))), nil
	})
	_ = serveLatencyRequest(context.Background(), latencyHandler(rt, a))
	_ = serveLatencyRequest(context.Background(), latencyHandler(rt, b))

	if a.Host != b.Host {
		t.Fatal("handlers with the same dial string should share a Host")
	}
	if got := latencySamples(a); got != 2 {
		t.Errorf("shared samples = %d, want 2", got)
	}
}
