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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
)

func policyRaw(t *testing.T, name string, s Selector) json.RawMessage {
	t.Helper()
	m, ok := s.(caddy.Module)
	if !ok {
		t.Fatalf("%T is not a module", s)
	}
	return caddyconfig.JSONModuleObject(m, "policy", name, nil)
}

func TestSelectorConsumesLatency(t *testing.T) {
	ctx, cancel := caddy.NewContext(caddy.Context{Context: context.Background()})
	defer cancel()

	provision := func(t *testing.T, s Selector) Selector {
		t.Helper()
		p, ok := s.(caddy.Provisioner)
		if !ok {
			t.Fatalf("%T is not a provisioner", s)
		}
		if err := p.Provision(ctx); err != nil {
			t.Fatal(err)
		}
		return s
	}
	latency := func(t *testing.T) json.RawMessage { return policyRaw(t, "least_latency", &LeastLatencySelection{}) }
	random := func(t *testing.T) json.RawMessage { return policyRaw(t, "random", RandomSelection{}) }
	header := func(t *testing.T, fallback json.RawMessage) json.RawMessage {
		return policyRaw(t, "header", &HeaderHashSelection{Field: "X-Key", FallbackRaw: fallback})
	}

	tests := []struct {
		name string
		make func(t *testing.T) Selector
		want bool
	}{
		{"least_latency value", func(*testing.T) Selector { return LeastLatencySelection{} }, true},
		{"least_latency pointer", func(*testing.T) Selector { return &LeastLatencySelection{} }, true},
		{"nil", func(*testing.T) Selector { return nil }, false},
		{"random", func(*testing.T) Selector { return RandomSelection{} }, false},
		{"random_choose", func(*testing.T) Selector { return RandomChoiceSelection{Choose: 2} }, false},
		{"least_conn", func(*testing.T) Selector { return LeastConnSelection{} }, false},
		{"first", func(*testing.T) Selector { return FirstSelection{} }, false},
		{"round_robin", func(*testing.T) Selector { return &RoundRobinSelection{} }, false},
		{"query, default fallback", func(t *testing.T) Selector {
			return provision(t, &QueryHashSelection{Key: "k"})
		}, false},
		{"query, random fallback", func(t *testing.T) Selector {
			return provision(t, &QueryHashSelection{Key: "k", FallbackRaw: random(t)})
		}, false},
		{"query, least_latency fallback", func(t *testing.T) Selector {
			return provision(t, &QueryHashSelection{Key: "k", FallbackRaw: latency(t)})
		}, true},
		{"query, not provisioned", func(t *testing.T) Selector {
			return &QueryHashSelection{Key: "k", FallbackRaw: latency(t)}
		}, false},
		{"header, default fallback", func(t *testing.T) Selector {
			return provision(t, &HeaderHashSelection{Field: "X-Key"})
		}, false},
		{"header, least_latency fallback", func(t *testing.T) Selector {
			return provision(t, &HeaderHashSelection{Field: "X-Key", FallbackRaw: latency(t)})
		}, true},
		{"cookie, default fallback", func(t *testing.T) Selector {
			return provision(t, &CookieHashSelection{})
		}, false},
		{"cookie, least_latency fallback", func(t *testing.T) Selector {
			return provision(t, &CookieHashSelection{FallbackRaw: latency(t)})
		}, true},
		{"cookie, header fallback, least_latency fallback", func(t *testing.T) Selector {
			return provision(t, &CookieHashSelection{FallbackRaw: header(t, latency(t))})
		}, true},
		{"cookie, header fallback, random fallback", func(t *testing.T) Selector {
			return provision(t, &CookieHashSelection{FallbackRaw: header(t, random(t))})
		}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := selectorConsumesLatency(tt.make(t)); got != tt.want {
				t.Errorf("selectorConsumesLatency = %v, want %v", got, tt.want)
			}
		})
	}
}

// stopCaddy stops the running config when the test ends.
func stopCaddy(t *testing.T) { t.Cleanup(func() { _ = caddy.Stop() }) }

// runHandlers runs a config with one reverse_proxy route per policy and
// returns the provisioned handlers in order. A nil policy omits load_balancing.
func runHandlers(t *testing.T, policies []string) []*Handler {
	t.Helper()
	var routes []string
	for i, p := range policies {
		lb := ""
		if p != "" {
			lb = fmt.Sprintf(`,"load_balancing":{"selection_policy":%s}`, p)
		}
		routes = append(routes, fmt.Sprintf(
			`{"match":[{"path":["/%d"]}],"handle":[{"handler":"reverse_proxy","upstreams":[{"dial":"127.0.0.1:1"}]%s}]}`, i, lb))
	}
	cfgJSON := fmt.Sprintf(`{"admin":{"disabled":true},"apps":{"http":{"servers":{"srv0":{"listen":["127.0.0.1:0"],"routes":[%s]}}}}}`,
		strings.Join(routes, ","))

	var cfg caddy.Config
	if err := json.Unmarshal([]byte(cfgJSON), &cfg); err != nil {
		t.Fatal(err)
	}
	if err := caddy.Run(&cfg); err != nil {
		t.Fatal(err)
	}

	app, err := caddy.ActiveContext().App("http")
	if err != nil {
		t.Fatal(err)
	}
	srvRoutes := app.(*caddyhttp.App).Servers["srv0"].Routes
	if len(srvRoutes) != len(policies) {
		t.Fatalf("got %d routes, want %d", len(srvRoutes), len(policies))
	}
	handlers := make([]*Handler, len(srvRoutes))
	for i, r := range srvRoutes {
		h, ok := r.Handlers[0].(*Handler)
		if !ok {
			t.Fatalf("route %d handler is %T", i, r.Handlers[0])
		}
		handlers[i] = h
	}
	return handlers
}

func TestProvisionTrackLatency(t *testing.T) {
	stopCaddy(t)

	const latency = `{"policy":"least_latency"}`
	tests := []struct {
		policy string
		want   bool
	}{
		{"", false},
		{`{"policy":"random"}`, false},
		{`{"policy":"least_conn"}`, false},
		{latency, true},
		{`{"policy":"cookie"}`, false},
		{`{"policy":"cookie","fallback":` + latency + `}`, true},
		{`{"policy":"header","field":"X-Key","fallback":` + latency + `}`, true},
		{`{"policy":"query","key":"k","fallback":` + latency + `}`, true},
		{`{"policy":"cookie","fallback":{"policy":"header","field":"X-Key","fallback":` + latency + `}}`, true},
		{`{"policy":"cookie","fallback":{"policy":"header","field":"X-Key","fallback":{"policy":"random"}}}`, false},
	}
	policies := make([]string, len(tests))
	for i, tt := range tests {
		policies[i] = tt.policy
	}

	handlers := runHandlers(t, policies)

	for i, tt := range tests {
		if got := handlers[i].trackLatency; got != tt.want {
			t.Errorf("policy %q: trackLatency = %v, want %v", tt.policy, got, tt.want)
		}
	}

	// the flag belongs to each provisioned handler, so a reload
	// cannot leave a stale value behind
	reloaded := runHandlers(t, []string{`{"policy":"random"}`})
	if reloaded[0].trackLatency {
		t.Error("handler reloaded with random should not track latency")
	}
	if !handlers[3].trackLatency {
		t.Error("the earlier handler's flag should be unchanged")
	}

	reloaded = runHandlers(t, []string{latency})
	if !reloaded[0].trackLatency {
		t.Error("handler reloaded with least_latency should track latency")
	}
}

type staticUpstreams struct{ dial string }

func (s staticUpstreams) GetUpstreams(*http.Request) ([]*Upstream, error) {
	return []*Upstream{{Dial: s.dial}}, nil
}

// Dynamic upstreams are rebuilt for every request, so samples must land on
// the persisted Host.
func TestTrackLatencyDynamicUpstreams(t *testing.T) {
	resetDynamicHosts()
	t.Cleanup(resetDynamicHosts)

	const dial = "192.0.2.90:80"
	h := latencyHandler(roundTripFunc(func(*http.Request) (*http.Response, error) {
		return responseWith(http.StatusOK, io.NopCloser(strings.NewReader(""))), nil
	}))
	h.LoadBalancing.SelectionPolicy = LeastLatencySelection{}
	h.trackLatency = selectorConsumesLatency(h.LoadBalancing.SelectionPolicy)
	h.DynamicUpstreams = staticUpstreams{dial}

	for range 2 {
		if err := serveLatencyRequest(context.Background(), h); err != nil {
			t.Fatal(err)
		}
	}

	dynamicHostsMu.RLock()
	entry, ok := dynamicHosts[dial]
	dynamicHostsMu.RUnlock()
	if !ok {
		t.Fatal("dynamic host was not persisted")
	}
	if got := entry.host.latency.snapshot().samples; got != 2 {
		t.Errorf("samples on the persisted host = %d, want 2", got)
	}
}
