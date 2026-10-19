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
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// coldBenchPool returns a pool of n upstreams whose first two members are
// available and unknown. A tenth of the rest are unavailable, and the given
// fraction of the others are known. The known upstreams are returned as well.
func coldBenchPool(n int, knownFraction float64) (pool, known UpstreamPool) {
	pool = make(UpstreamPool, n)
	for i := range pool {
		pool[i] = latencyUpstream(fmt.Sprintf("192.0.2.%d:%d", i/250+1, i%250+1))
	}
	var available UpstreamPool
	for i, u := range pool[2:] {
		if i%10 == 0 {
			u.setHealthy(false)
			continue
		}
		available = append(available, u)
	}
	for i, u := range available {
		if float64(i) < knownFraction*float64(len(available)) {
			u.latency.observe(10*time.Millisecond, latencyBase)
			known = append(known, u)
		}
	}
	return pool, known
}

// BenchmarkResolveColdScan measures resolveCold when both candidates are
// unknown, which makes it scan the pool for known upstreams.
func BenchmarkResolveColdScan(b *testing.B) {
	for _, n := range []int{2, 10, 100, 1000} {
		for _, pct := range []int{0, 10, 50, 90} {
			pool, known := coldBenchPool(n, float64(pct)/100)
			name := fmt.Sprintf("pool=%d/known=%d%%", n, pct)

			b.Run(name+"/serial", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					resolveCold(pool[0], pool[1], pool, latencyBase, rand.IntN)
				}
			})

			b.Run(name+"/parallel", func(b *testing.B) {
				b.ReportAllocs()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						resolveCold(pool[0], pool[1], pool, latencyBase, rand.IntN)
					}
				})
			})

			// completions land on known upstreams while selections scan them
			b.Run(name+"/parallel-writers", func(b *testing.B) {
				if len(known) == 0 {
					b.Skip("no known upstreams to write to")
				}
				var stop atomic.Bool
				var wg sync.WaitGroup
				for range 4 {
					wg.Go(func() {
						for !stop.Load() {
							known[rand.IntN(len(known))].latency.observe(10*time.Millisecond, latencyBase)
						}
					})
				}
				b.Cleanup(func() {
					stop.Store(true)
					wg.Wait()
				})
				b.ReportAllocs()
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						resolveCold(pool[0], pool[1], pool, latencyBase, rand.IntN)
					}
				})
			})
		}
	}
}
