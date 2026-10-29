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
	"io"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
)

// BenchmarkLatencyObserve measures recording samples on a single host
// from many goroutines, the worst case for the estimate's mutex.
func BenchmarkLatencyObserve(b *testing.B) {
	h := new(Host)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for i := 0; pb.Next(); i++ {
			h.observeLatency(10*time.Millisecond + time.Duration(i&1)*2*time.Millisecond)
		}
	})
}

// BenchmarkLatencyHook measures the handler's per-request cost with
// latency tracking disabled and enabled, using a fake transport.
func BenchmarkLatencyHook(b *testing.B) {
	for _, track := range []bool{false, true} {
		b.Run(fmt.Sprintf("trackLatency=%v", track), func(b *testing.B) {
			u := latencyUpstream("192.0.2.200:80")
			h := latencyHandler(roundTripFunc(func(*http.Request) (*http.Response, error) {
				return responseWith(http.StatusOK, io.NopCloser(strings.NewReader(""))), nil
			}), u)
			h.trackLatency = track
			next := caddyhttp.HandlerFunc(func(http.ResponseWriter, *http.Request) error { return nil })

			serve := func() {
				req := prepareTestRequest(httptest.NewRequest(http.MethodGet, "http://example.com/", nil))
				_ = h.ServeHTTP(httptest.NewRecorder(), req, next)
			}
			b.Run("serial", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					serve()
				}
			})
			b.Run("parallel", func(b *testing.B) {
				b.ReportAllocs()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						serve()
					}
				})
			})
		})
	}
}

// selectBenchPool returns n available upstreams. If known is true each has
// an estimate sampled at latencyBase.
func selectBenchPool(n int, known bool) UpstreamPool {
	pool := make(UpstreamPool, n)
	for i := range pool {
		pool[i] = latencyUpstream(fmt.Sprintf("192.0.2.%d:%d", i/250+1, i%250+1))
		if known {
			pool[i].latency.observe(10*time.Millisecond, latencyBase)
		}
	}
	return pool
}

// BenchmarkSelect compares random_choose with least_latency when none or
// all of the upstreams have an estimate. least_latency selects at a fixed
// time, equal to the samples', so that the estimates cannot go stale
// during a long run; this leaves out the cost of reading the clock.
func BenchmarkSelect(b *testing.B) {
	selectors := []struct {
		name string
		pick func(UpstreamPool) *Upstream
	}{
		{"random_choose", func(p UpstreamPool) *Upstream { return RandomChoiceSelection{Choose: 2}.Select(p, nil, nil) }},
		{"least_latency", func(p UpstreamPool) *Upstream {
			return LeastLatencySelection{}.selectAt(p, latencyBase, rand.IntN)
		}},
	}
	for _, n := range []int{10, 100} {
		for _, known := range []bool{false, true} {
			pool := selectBenchPool(n, known)
			for _, sel := range selectors {
				b.Run(fmt.Sprintf("pool=%d/known=%v/%s", n, known, sel.name), func(b *testing.B) {
					b.Run("serial", func(b *testing.B) {
						b.ReportAllocs()
						for b.Loop() {
							sel.pick(pool)
						}
					})
					b.Run("parallel", func(b *testing.B) {
						b.ReportAllocs()
						b.RunParallel(func(pb *testing.PB) {
							for pb.Next() {
								sel.pick(pool)
							}
						})
					})
				})
			}
		}
	}
}
