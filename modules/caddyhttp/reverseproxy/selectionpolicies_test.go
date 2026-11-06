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
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
)

func testPool() UpstreamPool {
	return UpstreamPool{
		{Host: new(Host), Dial: "0.0.0.1"},
		{Host: new(Host), Dial: "0.0.0.2"},
		{Host: new(Host), Dial: "0.0.0.3"},
	}
}

func TestRoundRobinPolicy(t *testing.T) {
	pool := testPool()
	rrPolicy := RoundRobinSelection{}
	req, _ := http.NewRequest("GET", "/", nil)

	h := rrPolicy.Select(pool, req, nil)
	// First selected host is 1, because counter starts at 0
	// and increments before host is selected
	if h != pool[1] {
		t.Error("Expected first round robin host to be second host in the pool.")
	}
	h = rrPolicy.Select(pool, req, nil)
	if h != pool[2] {
		t.Error("Expected second round robin host to be third host in the pool.")
	}
	h = rrPolicy.Select(pool, req, nil)
	if h != pool[0] {
		t.Error("Expected third round robin host to be first host in the pool.")
	}
	// mark host as down
	pool[1].setHealthy(false)
	h = rrPolicy.Select(pool, req, nil)
	if h != pool[2] {
		t.Error("Expected to skip down host.")
	}
	// mark host as up
	pool[1].setHealthy(true)

	h = rrPolicy.Select(pool, req, nil)
	if h == pool[2] {
		t.Error("Expected to balance evenly among healthy hosts")
	}
	// mark host as full
	pool[1].countRequest(1)
	pool[1].MaxRequests = 1
	h = rrPolicy.Select(pool, req, nil)
	if h != pool[2] {
		t.Error("Expected to skip full host.")
	}
}

func TestWeightedRoundRobinPolicy(t *testing.T) {
	pool := testPool()
	wrrPolicy := WeightedRoundRobinSelection{
		Weights:     []int{3, 2, 1},
		totalWeight: 6,
	}
	req, _ := http.NewRequest("GET", "/", nil)

	h := wrrPolicy.Select(pool, req, nil)
	if h != pool[0] {
		t.Error("Expected first weighted round robin host to be first host in the pool.")
	}
	h = wrrPolicy.Select(pool, req, nil)
	if h != pool[0] {
		t.Error("Expected second weighted round robin host to be first host in the pool.")
	}
	// Third selected host is 1, because counter starts at 0
	// and increments before host is selected
	h = wrrPolicy.Select(pool, req, nil)
	if h != pool[1] {
		t.Error("Expected third weighted round robin host to be second host in the pool.")
	}
	h = wrrPolicy.Select(pool, req, nil)
	if h != pool[1] {
		t.Error("Expected fourth weighted round robin host to be second host in the pool.")
	}
	h = wrrPolicy.Select(pool, req, nil)
	if h != pool[2] {
		t.Error("Expected fifth weighted round robin host to be third host in the pool.")
	}
	h = wrrPolicy.Select(pool, req, nil)
	if h != pool[0] {
		t.Error("Expected sixth weighted round robin host to be first host in the pool.")
	}

	// mark host as down
	pool[0].setHealthy(false)
	h = wrrPolicy.Select(pool, req, nil)
	if h != pool[1] {
		t.Error("Expected to skip down host.")
	}
	// mark host as up
	pool[0].setHealthy(true)

	h = wrrPolicy.Select(pool, req, nil)
	if h != pool[0] {
		t.Error("Expected to select first host on availability.")
	}
	// mark host as full
	pool[1].countRequest(1)
	pool[1].MaxRequests = 1
	h = wrrPolicy.Select(pool, req, nil)
	if h != pool[2] {
		t.Error("Expected to skip full host.")
	}
}

func TestWeightedRoundRobinPolicyFewerWeightsThanUpstreams(t *testing.T) {
	pool := testPool()
	wrrPolicy := WeightedRoundRobinSelection{
		Weights:     []int{1, 1},
		totalWeight: 2,
	}
	req, _ := http.NewRequest("GET", "/", nil)

	pool[0].setHealthy(false)
	for i := 0; i < 4; i++ {
		h := wrrPolicy.Select(pool, req, nil)
		if h != pool[1] {
			t.Fatalf("Selection %d: got %v, want the only available weighted host %v", i+1, h, pool[1])
		}
	}
}

func TestWeightedRoundRobinPolicySingleWeight(t *testing.T) {
	pool := testPool()
	wrrPolicy := WeightedRoundRobinSelection{
		Weights:     []int{1},
		totalWeight: 1,
	}
	req, _ := http.NewRequest("GET", "/", nil)

	if h := wrrPolicy.Select(pool, req, nil); h != pool[0] {
		t.Errorf("got %v, want the only weighted host %v", h, pool[0])
	}

	pool[0].setHealthy(false)
	if h := wrrPolicy.Select(pool, req, nil); h != nil {
		t.Errorf("got %v, want nil when the only weighted host is unavailable", h)
	}
}

func TestWeightedRoundRobinSelection_Validate(t *testing.T) {
	tests := []struct {
		name    string
		weights []int
		wantErr bool
	}{
		{
			name:    "Valid 0 2 1 case",
			weights: []int{0, 2, 1},
			wantErr: false,
		},
		{
			name:    "Invalid 0 case (single)",
			weights: []int{0},
			wantErr: true,
		},
		{
			name:    "Invalid 0 0 case (multiple)",
			weights: []int{0, 0},
			wantErr: true,
		},
		{
			name:    "Valid weights",
			weights: []int{1, 1, 1},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &WeightedRoundRobinSelection{
				Weights: tt.weights,
			}
			_ = s.Provision(caddy.Context{})
			err := s.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestWeightedRoundRobinPolicyWithZeroWeight(t *testing.T) {
	pool := testPool()
	wrrPolicy := WeightedRoundRobinSelection{
		Weights:     []int{0, 2, 1},
		totalWeight: 3,
	}
	req, _ := http.NewRequest("GET", "/", nil)

	h := wrrPolicy.Select(pool, req, nil)
	if h != pool[1] {
		t.Error("Expected first weighted round robin host to be second host in the pool.")
	}

	h = wrrPolicy.Select(pool, req, nil)
	if h != pool[2] {
		t.Error("Expected second weighted round robin host to be third host in the pool.")
	}

	h = wrrPolicy.Select(pool, req, nil)
	if h != pool[1] {
		t.Error("Expected third weighted round robin host to be second host in the pool.")
	}

	// mark second host as down
	pool[1].setHealthy(false)
	h = wrrPolicy.Select(pool, req, nil)
	if h != pool[2] {
		t.Error("Expect select next available host.")
	}

	h = wrrPolicy.Select(pool, req, nil)
	if h != pool[2] {
		t.Error("Expect select only available host.")
	}
	// mark second host as up
	pool[1].setHealthy(true)

	h = wrrPolicy.Select(pool, req, nil)
	if h != pool[1] {
		t.Error("Expect select first host on availability.")
	}

	// test next select in full cycle
	expected := []*Upstream{pool[1], pool[2], pool[1], pool[1], pool[2], pool[1]}
	for i, want := range expected {
		got := wrrPolicy.Select(pool, req, nil)
		if want != got {
			t.Errorf("Selection %d: got host[%s], want host[%s]", i+1, got, want)
		}
	}
}

func TestLeastConnPolicy(t *testing.T) {
	pool := testPool()
	lcPolicy := LeastConnSelection{}
	req, _ := http.NewRequest("GET", "/", nil)

	pool[0].countRequest(10)
	pool[1].countRequest(10)
	h := lcPolicy.Select(pool, req, nil)
	if h != pool[2] {
		t.Error("Expected least connection host to be third host.")
	}
	pool[2].countRequest(100)
	h = lcPolicy.Select(pool, req, nil)
	if h != pool[0] && h != pool[1] {
		t.Error("Expected least connection host to be first or second host.")
	}
}

func TestIPHashPolicy(t *testing.T) {
	pool := testPool()
	ipHash := IPHashSelection{}
	req, _ := http.NewRequest("GET", "/", nil)

	// We should be able to predict where every request is routed.
	req.RemoteAddr = "172.0.0.1:80"
	h := ipHash.Select(pool, req, nil)
	if h != pool[0] {
		t.Error("Expected ip hash policy host to be the first host.")
	}
	req.RemoteAddr = "172.0.0.2:80"
	h = ipHash.Select(pool, req, nil)
	if h != pool[1] {
		t.Error("Expected ip hash policy host to be the second host.")
	}
	req.RemoteAddr = "172.0.0.3:80"
	h = ipHash.Select(pool, req, nil)
	if h != pool[0] {
		t.Error("Expected ip hash policy host to be the first host.")
	}
	req.RemoteAddr = "172.0.0.4:80"
	h = ipHash.Select(pool, req, nil)
	if h != pool[1] {
		t.Error("Expected ip hash policy host to be the second host.")
	}

	// we should get the same results without a port
	req.RemoteAddr = "172.0.0.1"
	h = ipHash.Select(pool, req, nil)
	if h != pool[0] {
		t.Error("Expected ip hash policy host to be the first host.")
	}
	req.RemoteAddr = "172.0.0.2"
	h = ipHash.Select(pool, req, nil)
	if h != pool[1] {
		t.Error("Expected ip hash policy host to be the second host.")
	}
	req.RemoteAddr = "172.0.0.3"
	h = ipHash.Select(pool, req, nil)
	if h != pool[0] {
		t.Error("Expected ip hash policy host to be the first host.")
	}
	req.RemoteAddr = "172.0.0.4"
	h = ipHash.Select(pool, req, nil)
	if h != pool[1] {
		t.Error("Expected ip hash policy host to be the second host.")
	}

	// we should get a healthy host if the original host is unhealthy and a
	// healthy host is available
	req.RemoteAddr = "172.0.0.4"
	pool[1].setHealthy(false)
	h = ipHash.Select(pool, req, nil)
	if h != pool[2] {
		t.Error("Expected ip hash policy host to be the third host.")
	}

	req.RemoteAddr = "172.0.0.2"
	h = ipHash.Select(pool, req, nil)
	if h != pool[0] {
		t.Error("Expected ip hash policy host to be the first host.")
	}
	pool[1].setHealthy(true)

	req.RemoteAddr = "172.0.0.3"
	pool[2].setHealthy(false)
	h = ipHash.Select(pool, req, nil)
	if h != pool[0] {
		t.Error("Expected ip hash policy host to be the first host.")
	}
	req.RemoteAddr = "172.0.0.4"
	h = ipHash.Select(pool, req, nil)
	if h != pool[1] {
		t.Error("Expected ip hash policy host to be the second host.")
	}

	// We should be able to resize the host pool and still be able to predict
	// where a req will be routed with the same IP's used above
	pool = UpstreamPool{
		{Host: new(Host), Dial: "0.0.0.2"},
		{Host: new(Host), Dial: "0.0.0.3"},
	}
	req.RemoteAddr = "172.0.0.1:80"
	h = ipHash.Select(pool, req, nil)
	if h != pool[0] {
		t.Error("Expected ip hash policy host to be the first host.")
	}
	req.RemoteAddr = "172.0.0.2:80"
	h = ipHash.Select(pool, req, nil)
	if h != pool[0] {
		t.Error("Expected ip hash policy host to be the first host.")
	}
	req.RemoteAddr = "172.0.0.3:80"
	h = ipHash.Select(pool, req, nil)
	if h != pool[0] {
		t.Error("Expected ip hash policy host to be the first host.")
	}
	req.RemoteAddr = "172.0.0.4:80"
	h = ipHash.Select(pool, req, nil)
	if h != pool[0] {
		t.Error("Expected ip hash policy host to be the first host.")
	}

	// We should get nil when there are no healthy hosts
	pool[0].setHealthy(false)
	pool[1].setHealthy(false)
	h = ipHash.Select(pool, req, nil)
	if h != nil {
		t.Error("Expected ip hash policy host to be nil.")
	}

	// Reproduce #4135
	pool = UpstreamPool{
		{Host: new(Host)},
		{Host: new(Host)},
		{Host: new(Host)},
		{Host: new(Host)},
		{Host: new(Host)},
		{Host: new(Host)},
		{Host: new(Host)},
		{Host: new(Host)},
		{Host: new(Host)},
	}
	pool[0].setHealthy(false)
	pool[1].setHealthy(false)
	pool[2].setHealthy(false)
	pool[3].setHealthy(false)
	pool[4].setHealthy(false)
	pool[5].setHealthy(false)
	pool[6].setHealthy(false)
	pool[7].setHealthy(false)
	pool[8].setHealthy(true)

	// We should get a result back when there is one healthy host left.
	h = ipHash.Select(pool, req, nil)
	if h == nil {
		// If it is nil, it means we missed a host even though one is available
		t.Error("Expected ip hash policy host to not be nil, but it is nil.")
	}
}

func TestClientIPHashPolicy(t *testing.T) {
	pool := testPool()
	ipHash := ClientIPHashSelection{}
	req, _ := http.NewRequest("GET", "/", nil)
	req = req.WithContext(context.WithValue(req.Context(), caddyhttp.VarsCtxKey, make(map[string]any)))

	// We should be able to predict where every request is routed.
	caddyhttp.SetVar(req.Context(), caddyhttp.ClientIPVarKey, "172.0.0.1:80")
	h := ipHash.Select(pool, req, nil)
	if h != pool[0] {
		t.Error("Expected ip hash policy host to be the first host.")
	}
	caddyhttp.SetVar(req.Context(), caddyhttp.ClientIPVarKey, "172.0.0.2:80")
	h = ipHash.Select(pool, req, nil)
	if h != pool[1] {
		t.Error("Expected ip hash policy host to be the second host.")
	}
	caddyhttp.SetVar(req.Context(), caddyhttp.ClientIPVarKey, "172.0.0.3:80")
	h = ipHash.Select(pool, req, nil)
	if h != pool[0] {
		t.Error("Expected ip hash policy host to be the first host.")
	}
	caddyhttp.SetVar(req.Context(), caddyhttp.ClientIPVarKey, "172.0.0.4:80")
	h = ipHash.Select(pool, req, nil)
	if h != pool[1] {
		t.Error("Expected ip hash policy host to be the second host.")
	}

	// we should get the same results without a port
	caddyhttp.SetVar(req.Context(), caddyhttp.ClientIPVarKey, "172.0.0.1")
	h = ipHash.Select(pool, req, nil)
	if h != pool[0] {
		t.Error("Expected ip hash policy host to be the first host.")
	}
	caddyhttp.SetVar(req.Context(), caddyhttp.ClientIPVarKey, "172.0.0.2")
	h = ipHash.Select(pool, req, nil)
	if h != pool[1] {
		t.Error("Expected ip hash policy host to be the second host.")
	}
	caddyhttp.SetVar(req.Context(), caddyhttp.ClientIPVarKey, "172.0.0.3")
	h = ipHash.Select(pool, req, nil)
	if h != pool[0] {
		t.Error("Expected ip hash policy host to be the first host.")
	}
	caddyhttp.SetVar(req.Context(), caddyhttp.ClientIPVarKey, "172.0.0.4")
	h = ipHash.Select(pool, req, nil)
	if h != pool[1] {
		t.Error("Expected ip hash policy host to be the second host.")
	}

	// we should get a healthy host if the original host is unhealthy and a
	// healthy host is available
	caddyhttp.SetVar(req.Context(), caddyhttp.ClientIPVarKey, "172.0.0.4")
	pool[1].setHealthy(false)
	h = ipHash.Select(pool, req, nil)
	if h != pool[2] {
		t.Error("Expected ip hash policy host to be the third host.")
	}

	caddyhttp.SetVar(req.Context(), caddyhttp.ClientIPVarKey, "172.0.0.2")
	h = ipHash.Select(pool, req, nil)
	if h != pool[0] {
		t.Error("Expected ip hash policy host to be the first host.")
	}
	pool[1].setHealthy(true)

	caddyhttp.SetVar(req.Context(), caddyhttp.ClientIPVarKey, "172.0.0.3")
	pool[2].setHealthy(false)
	h = ipHash.Select(pool, req, nil)
	if h != pool[0] {
		t.Error("Expected ip hash policy host to be the first host.")
	}
	caddyhttp.SetVar(req.Context(), caddyhttp.ClientIPVarKey, "172.0.0.4")
	h = ipHash.Select(pool, req, nil)
	if h != pool[1] {
		t.Error("Expected ip hash policy host to be the second host.")
	}

	// We should be able to resize the host pool and still be able to predict
	// where a req will be routed with the same IP's used above
	pool = UpstreamPool{
		{Host: new(Host), Dial: "0.0.0.2"},
		{Host: new(Host), Dial: "0.0.0.3"},
	}
	caddyhttp.SetVar(req.Context(), caddyhttp.ClientIPVarKey, "172.0.0.1:80")
	h = ipHash.Select(pool, req, nil)
	if h != pool[0] {
		t.Error("Expected ip hash policy host to be the first host.")
	}
	caddyhttp.SetVar(req.Context(), caddyhttp.ClientIPVarKey, "172.0.0.2:80")
	h = ipHash.Select(pool, req, nil)
	if h != pool[0] {
		t.Error("Expected ip hash policy host to be the first host.")
	}
	caddyhttp.SetVar(req.Context(), caddyhttp.ClientIPVarKey, "172.0.0.3:80")
	h = ipHash.Select(pool, req, nil)
	if h != pool[0] {
		t.Error("Expected ip hash policy host to be the first host.")
	}
	caddyhttp.SetVar(req.Context(), caddyhttp.ClientIPVarKey, "172.0.0.4:80")
	h = ipHash.Select(pool, req, nil)
	if h != pool[0] {
		t.Error("Expected ip hash policy host to be the first host.")
	}

	// We should get nil when there are no healthy hosts
	pool[0].setHealthy(false)
	pool[1].setHealthy(false)
	h = ipHash.Select(pool, req, nil)
	if h != nil {
		t.Error("Expected ip hash policy host to be nil.")
	}

	// Reproduce #4135
	pool = UpstreamPool{
		{Host: new(Host)},
		{Host: new(Host)},
		{Host: new(Host)},
		{Host: new(Host)},
		{Host: new(Host)},
		{Host: new(Host)},
		{Host: new(Host)},
		{Host: new(Host)},
		{Host: new(Host)},
	}
	pool[0].setHealthy(false)
	pool[1].setHealthy(false)
	pool[2].setHealthy(false)
	pool[3].setHealthy(false)
	pool[4].setHealthy(false)
	pool[5].setHealthy(false)
	pool[6].setHealthy(false)
	pool[7].setHealthy(false)
	pool[8].setHealthy(true)

	// We should get a result back when there is one healthy host left.
	h = ipHash.Select(pool, req, nil)
	if h == nil {
		// If it is nil, it means we missed a host even though one is available
		t.Error("Expected ip hash policy host to not be nil, but it is nil.")
	}
}

func TestFirstPolicy(t *testing.T) {
	pool := testPool()
	firstPolicy := FirstSelection{}
	req := httptest.NewRequest(http.MethodGet, "/", nil)

	h := firstPolicy.Select(pool, req, nil)
	if h != pool[0] {
		t.Error("Expected first policy host to be the first host.")
	}

	pool[0].setHealthy(false)
	h = firstPolicy.Select(pool, req, nil)
	if h != pool[1] {
		t.Error("Expected first policy host to be the second host.")
	}
}

func TestQueryHashPolicy(t *testing.T) {
	ctx, cancel := caddy.NewContext(caddy.Context{Context: context.Background()})
	defer cancel()
	queryPolicy := QueryHashSelection{Key: "foo"}
	if err := queryPolicy.Provision(ctx); err != nil {
		t.Errorf("Provision error: %v", err)
		t.FailNow()
	}

	pool := testPool()

	request := httptest.NewRequest(http.MethodGet, "/?foo=1", nil)
	h := queryPolicy.Select(pool, request, nil)
	if h != pool[0] {
		t.Error("Expected query policy host to be the first host.")
	}

	request = httptest.NewRequest(http.MethodGet, "/?foo=100000", nil)
	h = queryPolicy.Select(pool, request, nil)
	if h != pool[1] {
		t.Error("Expected query policy host to be the second host.")
	}

	request = httptest.NewRequest(http.MethodGet, "/?foo=1", nil)
	pool[0].setHealthy(false)
	h = queryPolicy.Select(pool, request, nil)
	if h != pool[2] {
		t.Error("Expected query policy host to be the third host.")
	}

	request = httptest.NewRequest(http.MethodGet, "/?foo=100000", nil)
	h = queryPolicy.Select(pool, request, nil)
	if h != pool[1] {
		t.Error("Expected query policy host to be the second host.")
	}

	// We should be able to resize the host pool and still be able to predict
	// where a request will be routed with the same query used above
	pool = UpstreamPool{
		{Host: new(Host)},
		{Host: new(Host)},
	}

	request = httptest.NewRequest(http.MethodGet, "/?foo=1", nil)
	h = queryPolicy.Select(pool, request, nil)
	if h != pool[0] {
		t.Error("Expected query policy host to be the first host.")
	}

	pool[0].setHealthy(false)
	h = queryPolicy.Select(pool, request, nil)
	if h != pool[1] {
		t.Error("Expected query policy host to be the second host.")
	}

	request = httptest.NewRequest(http.MethodGet, "/?foo=4", nil)
	h = queryPolicy.Select(pool, request, nil)
	if h != pool[1] {
		t.Error("Expected query policy host to be the second host.")
	}

	pool[0].setHealthy(false)
	pool[1].setHealthy(false)
	h = queryPolicy.Select(pool, request, nil)
	if h != nil {
		t.Error("Expected query policy host to be nil.")
	}

	request = httptest.NewRequest(http.MethodGet, "/?foo=aa11&foo=bb22", nil)
	pool = testPool()
	h = queryPolicy.Select(pool, request, nil)
	if h != pool[0] {
		t.Error("Expected query policy host to be the first host.")
	}
}

func TestURIHashPolicy(t *testing.T) {
	pool := testPool()
	uriPolicy := URIHashSelection{}

	request := httptest.NewRequest(http.MethodGet, "/test", nil)
	h := uriPolicy.Select(pool, request, nil)
	if h != pool[2] {
		t.Error("Expected uri policy host to be the third host.")
	}

	pool[2].setHealthy(false)
	h = uriPolicy.Select(pool, request, nil)
	if h != pool[0] {
		t.Error("Expected uri policy host to be the first host.")
	}

	request = httptest.NewRequest(http.MethodGet, "/test_2", nil)
	h = uriPolicy.Select(pool, request, nil)
	if h != pool[0] {
		t.Error("Expected uri policy host to be the first host.")
	}

	// We should be able to resize the host pool and still be able to predict
	// where a request will be routed with the same URI's used above
	pool = UpstreamPool{
		{Host: new(Host)},
		{Host: new(Host)},
	}

	request = httptest.NewRequest(http.MethodGet, "/test", nil)
	h = uriPolicy.Select(pool, request, nil)
	if h != pool[0] {
		t.Error("Expected uri policy host to be the first host.")
	}

	pool[0].setHealthy(false)
	h = uriPolicy.Select(pool, request, nil)
	if h != pool[1] {
		t.Error("Expected uri policy host to be the first host.")
	}

	request = httptest.NewRequest(http.MethodGet, "/test_2", nil)
	h = uriPolicy.Select(pool, request, nil)
	if h != pool[1] {
		t.Error("Expected uri policy host to be the second host.")
	}

	pool[0].setHealthy(false)
	pool[1].setHealthy(false)
	h = uriPolicy.Select(pool, request, nil)
	if h != nil {
		t.Error("Expected uri policy host to be nil.")
	}
}

func TestLeastRequests(t *testing.T) {
	pool := testPool()
	pool[0].Dial = "localhost:8080"
	pool[1].Dial = "localhost:8081"
	pool[2].Dial = "localhost:8082"
	pool[0].setHealthy(true)
	pool[1].setHealthy(true)
	pool[2].setHealthy(true)
	pool[0].countRequest(10)
	pool[1].countRequest(20)
	pool[2].countRequest(30)

	result := leastRequests(pool)

	if result == nil {
		t.Error("Least request should not return nil")
	}

	if result != pool[0] {
		t.Error("Least request should return pool[0]")
	}
}

func TestLeastRequestsAllBusy(t *testing.T) {
	pool := testPool()
	pool[0].countRequest(30)
	pool[1].countRequest(1)
	pool[2].countRequest(20)

	for i := 0; i < 100; i++ {
		if result := leastRequests(pool); result != pool[1] {
			t.Fatalf("iteration %d: got %v, want the least loaded host %v", i, result, pool[1])
		}
	}
}

func TestRandomChoicePolicy(t *testing.T) {
	pool := testPool()
	pool[0].Dial = "localhost:8080"
	pool[1].Dial = "localhost:8081"
	pool[2].Dial = "localhost:8082"
	pool[0].setHealthy(false)
	pool[1].setHealthy(true)
	pool[2].setHealthy(true)
	pool[0].countRequest(10)
	pool[1].countRequest(20)
	pool[2].countRequest(30)

	request := httptest.NewRequest(http.MethodGet, "/test", nil)
	randomChoicePolicy := RandomChoiceSelection{Choose: 2}

	h := randomChoicePolicy.Select(pool, request, nil)

	if h == nil {
		t.Error("RandomChoicePolicy should not return nil")
	}

	if h == pool[0] {
		t.Error("RandomChoicePolicy should not choose pool[0]")
	}
}

func TestRandomChoicePolicyLeastLoaded(t *testing.T) {
	// when the number of available upstreams does not exceed the choose
	// count, all of them must be candidates, so the least-loaded one
	// must always be selected; the pool intentionally starts with an
	// unavailable upstream to verify that reservoir sampling counts
	// available upstreams rather than pool indices
	pool := testPool()
	pool[0].Dial = "localhost:8080"
	pool[1].Dial = "localhost:8081"
	pool[2].Dial = "localhost:8082"
	pool[0].setHealthy(false)
	pool[1].setHealthy(true)
	pool[2].setHealthy(true)
	pool[1].countRequest(30)
	// pool[2] has no active requests

	request := httptest.NewRequest(http.MethodGet, "/test", nil)
	randomChoicePolicy := RandomChoiceSelection{Choose: 2}

	for i := 0; i < 100; i++ {
		h := randomChoicePolicy.Select(pool, request, nil)
		if h != pool[2] {
			t.Fatalf("with 2 available upstreams and choose=2, the least-loaded upstream (pool[2]) must always be selected; got %v on iteration %d", h, i)
		}
	}
}

func TestCookieHashPolicy(t *testing.T) {
	ctx, cancel := caddy.NewContext(caddy.Context{Context: context.Background()})
	defer cancel()
	cookieHashPolicy := CookieHashSelection{}
	if err := cookieHashPolicy.Provision(ctx); err != nil {
		t.Errorf("Provision error: %v", err)
		t.FailNow()
	}

	pool := testPool()
	pool[0].Dial = "localhost:8080"
	pool[1].Dial = "localhost:8081"
	pool[2].Dial = "localhost:8082"
	pool[0].setHealthy(true)
	pool[1].setHealthy(false)
	pool[2].setHealthy(false)
	request := httptest.NewRequest(http.MethodGet, "/test", nil)
	w := httptest.NewRecorder()

	h := cookieHashPolicy.Select(pool, request, w)
	cookieServer1 := w.Result().Cookies()[0]
	if cookieServer1 == nil {
		t.Fatal("cookieHashPolicy should set a cookie")
	}
	if cookieServer1.Name != "lb" {
		t.Error("cookieHashPolicy should set a cookie with name lb")
	}
	if cookieServer1.Secure {
		t.Error("cookieHashPolicy should set cookie Secure attribute to false when request is not secure")
	}
	if h != pool[0] {
		t.Error("Expected cookieHashPolicy host to be the first only available host.")
	}
	pool[1].setHealthy(true)
	pool[2].setHealthy(true)
	request = httptest.NewRequest(http.MethodGet, "/test", nil)
	w = httptest.NewRecorder()
	request.AddCookie(cookieServer1)
	h = cookieHashPolicy.Select(pool, request, w)
	if h != pool[0] {
		t.Error("Expected cookieHashPolicy host to stick to the first host (matching cookie).")
	}
	s := w.Result().Cookies()
	if len(s) != 0 {
		t.Error("Expected cookieHashPolicy to not set a new cookie.")
	}
	pool[0].setHealthy(false)
	request = httptest.NewRequest(http.MethodGet, "/test", nil)
	w = httptest.NewRecorder()
	request.AddCookie(cookieServer1)
	h = cookieHashPolicy.Select(pool, request, w)
	if h == pool[0] {
		t.Error("Expected cookieHashPolicy to select a new host.")
	}
	if w.Result().Cookies() == nil {
		t.Error("Expected cookieHashPolicy to set a new cookie.")
	}
}

func TestCookieHashPolicyWithSecureRequest(t *testing.T) {
	ctx, cancel := caddy.NewContext(caddy.Context{Context: context.Background()})
	defer cancel()
	cookieHashPolicy := CookieHashSelection{}
	if err := cookieHashPolicy.Provision(ctx); err != nil {
		t.Errorf("Provision error: %v", err)
		t.FailNow()
	}

	pool := testPool()
	pool[0].Dial = "localhost:8080"
	pool[1].Dial = "localhost:8081"
	pool[2].Dial = "localhost:8082"
	pool[0].setHealthy(true)
	pool[1].setHealthy(false)
	pool[2].setHealthy(false)

	// Create a test server that serves HTTPS requests
	ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := cookieHashPolicy.Select(pool, r, w)
		if h != pool[0] {
			t.Error("Expected cookieHashPolicy host to be the first only available host.")
		}
	}))
	defer ts.Close()

	// Make a new HTTPS request to the test server
	client := ts.Client()
	request, err := http.NewRequest(http.MethodGet, ts.URL+"/test", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}

	// Check if the cookie set is Secure and has SameSiteNone mode
	cookies := response.Cookies()
	if len(cookies) == 0 {
		t.Fatal("Expected a cookie to be set")
	}
	cookie := cookies[0]
	if !cookie.Secure {
		t.Error("Expected cookie Secure attribute to be true when request is secure")
	}
	if cookie.SameSite != http.SameSiteNoneMode {
		t.Error("Expected cookie SameSite attribute to be None when request is secure")
	}
}

func TestCookieHashPolicyWithFirstFallback(t *testing.T) {
	ctx, cancel := caddy.NewContext(caddy.Context{Context: context.Background()})
	defer cancel()
	cookieHashPolicy := CookieHashSelection{
		FallbackRaw: caddyconfig.JSONModuleObject(FirstSelection{}, "policy", "first", nil),
	}
	if err := cookieHashPolicy.Provision(ctx); err != nil {
		t.Errorf("Provision error: %v", err)
		t.FailNow()
	}

	pool := testPool()
	pool[0].Dial = "localhost:8080"
	pool[1].Dial = "localhost:8081"
	pool[2].Dial = "localhost:8082"
	pool[0].setHealthy(true)
	pool[1].setHealthy(true)
	pool[2].setHealthy(true)
	request := httptest.NewRequest(http.MethodGet, "/test", nil)
	w := httptest.NewRecorder()

	h := cookieHashPolicy.Select(pool, request, w)
	cookieServer1 := w.Result().Cookies()[0]
	if cookieServer1 == nil {
		t.Fatal("cookieHashPolicy should set a cookie")
	}
	if cookieServer1.Name != "lb" {
		t.Error("cookieHashPolicy should set a cookie with name lb")
	}
	if h != pool[0] {
		t.Errorf("Expected cookieHashPolicy host to be the first only available host, got %s", h)
	}
	request = httptest.NewRequest(http.MethodGet, "/test", nil)
	w = httptest.NewRecorder()
	request.AddCookie(cookieServer1)
	h = cookieHashPolicy.Select(pool, request, w)
	if h != pool[0] {
		t.Errorf("Expected cookieHashPolicy host to stick to the first host (matching cookie), got %s", h)
	}
	s := w.Result().Cookies()
	if len(s) != 0 {
		t.Error("Expected cookieHashPolicy to not set a new cookie.")
	}
	pool[0].setHealthy(false)
	request = httptest.NewRequest(http.MethodGet, "/test", nil)
	w = httptest.NewRecorder()
	request.AddCookie(cookieServer1)
	h = cookieHashPolicy.Select(pool, request, w)
	if h != pool[1] {
		t.Errorf("Expected cookieHashPolicy to select the next first available host, got %s", h)
	}
	if w.Result().Cookies() == nil {
		t.Error("Expected cookieHashPolicy to set a new cookie.")
	}
}

func TestCookieHashPolicyWithSecret(t *testing.T) {
	ctx, cancel := caddy.NewContext(caddy.Context{Context: context.Background()})
	defer cancel()
	cookieHashPolicy := CookieHashSelection{Secret: "hunter2"}
	if err := cookieHashPolicy.Provision(ctx); err != nil {
		t.Errorf("Provision error: %v", err)
		t.FailNow()
	}

	pool := testPool()
	pool[0].Dial = "localhost:8080"
	pool[1].Dial = "localhost:8081"
	pool[2].Dial = "localhost:8082"
	pool[0].setHealthy(true)
	pool[1].setHealthy(true)
	pool[2].setHealthy(true)

	request := httptest.NewRequest(http.MethodGet, "/test", nil)
	w := httptest.NewRecorder()
	h := cookieHashPolicy.Select(pool, request, w)
	cookie := w.Result().Cookies()[0]

	// a matching cookie sticks to the same host
	request = httptest.NewRequest(http.MethodGet, "/test", nil)
	w = httptest.NewRecorder()
	request.AddCookie(cookie)
	if got := cookieHashPolicy.Select(pool, request, w); got != h {
		t.Errorf("Expected to stick to host %s, got %s", h, got)
	}
	if len(w.Result().Cookies()) != 0 {
		t.Error("Expected no new cookie for a matching value")
	}

	// a tampered cookie value must not match any host and gets a fresh cookie
	request = httptest.NewRequest(http.MethodGet, "/test", nil)
	w = httptest.NewRecorder()
	request.AddCookie(&http.Cookie{Name: cookie.Name, Value: cookie.Value[:len(cookie.Value)-1] + "0"})
	cookieHashPolicy.Select(pool, request, w)
	if len(w.Result().Cookies()) == 0 {
		t.Error("Expected a new cookie to be set for a non-matching value")
	}
}

// noRand fails a test that consults randomness it expects not to need.
func noRand(n int) int { panic(fmt.Sprintf("unexpected random draw of %d", n)) }

func TestLeastLatencyPolicy(t *testing.T) {
	const ms = time.Millisecond
	down := func(u *Upstream) *Upstream {
		u.setHealthy(false)
		return u
	}
	full := func(u *Upstream) *Upstream {
		u.MaxRequests = 1
		u.Host.numRequests.Store(1)
		return u
	}

	for _, tc := range []struct {
		name string
		pool UpstreamPool
		want string // dial address of the selected upstream, empty for none
	}{
		{"lower latency wins", UpstreamPool{testUpstream("a", 5*ms, 0), testUpstream("b", 20*ms, 0)}, "a"},
		// scores 5ms*11 and 20ms*1
		{"active requests outweigh a latency advantage", UpstreamPool{testUpstream("a", 5*ms, 10), testUpstream("b", 20*ms, 0)}, "b"},
		// both score 20ms
		{"equal scores go to fewer active requests", UpstreamPool{testUpstream("a", 10*ms, 1), testUpstream("b", 20*ms, 0)}, "b"},
		{"zero estimates are ranked by active requests", UpstreamPool{testUpstream("a", 0, 3), testUpstream("b", 0, 1)}, "b"},
		{"without estimates fewer active requests win", UpstreamPool{testUpstream("a", noLatency, 1), testUpstream("b", noLatency, 3)}, "a"},
		{"empty pool", nil, ""},
		{"no available upstream", UpstreamPool{down(testUpstream("a", ms, 0)), full(testUpstream("b", ms, 0))}, ""},
		{"one available upstream", UpstreamPool{down(testUpstream("a", ms, 0)), full(testUpstream("b", ms, 0)), testUpstream("c", ms, 0)}, "c"},
	} {
		// the result must not depend on the order of the pool, and with at most
		// two available upstreams no randomness is needed
		reversed := slices.Clone(tc.pool)
		slices.Reverse(reversed)
		for _, pool := range []UpstreamPool{tc.pool, reversed} {
			var got string
			if u := (LeastLatencySelection{}).selectAt(pool, latencyBase, noRand); u != nil {
				got = u.Dial
			}
			if got != tc.want {
				t.Errorf("%s: selected %q, want %q (first upstream %v)", tc.name, got, tc.want, pool)
			}
		}
	}
}

// Candidates are drawn from the available upstreams and exact ties are broken
// by a random draw; the draws are injected to make both deterministic.
func TestLeastLatencyPolicyRandomChoices(t *testing.T) {
	const ms = time.Millisecond
	draw := func(i int) func(int) int { return func(int) int { return i } }

	// three upstreams make three possible candidate pairs, selected by the
	// draw for the third upstream: (20ms, 10ms), (5ms, 20ms) and (5ms, 10ms)
	pool := UpstreamPool{testUpstream("5ms", 5*ms, 0), testUpstream("10ms", 10*ms, 0), testUpstream("20ms", 20*ms, 0)}
	for i, want := range []string{"10ms", "5ms", "5ms"} {
		if got := (LeastLatencySelection{}).selectAt(pool, latencyBase, draw(i)); got.Dial != want {
			t.Errorf("draw %d selected %q, want %q", i, got.Dial, want)
		}
	}

	for name, pool := range map[string]UpstreamPool{
		"equal estimates": {testUpstream("a", ms, 0), testUpstream("b", ms, 0)},
		"no estimates":    {testUpstream("a", noLatency, 0), testUpstream("b", noLatency, 0)},
	} {
		if a, b := (LeastLatencySelection{}).selectAt(pool, latencyBase, draw(0)), (LeastLatencySelection{}).selectAt(pool, latencyBase, draw(1)); a == b {
			t.Errorf("%s: both draws selected %q", name, a.Dial)
		}
	}

	// Select uses the current time and its own source of randomness
	fast, slow := testUpstream("fast", noLatency, 0), testUpstream("slow", noLatency, 0)
	fast.Host.observeLatency(5 * ms)
	slow.Host.observeLatency(50 * ms)
	if got := (LeastLatencySelection{}).Select(UpstreamPool{slow, fast}, nil, nil); got != fast {
		t.Errorf("Select returned %q, want the faster upstream", got.Dial)
	}
}

// An upstream degrades, goes stale, is tried at a bounded rate and recovers.
func TestLeastLatencyPolicyLifecycle(t *testing.T) {
	const ms = time.Millisecond
	a, b := testUpstream("a", 5*ms, 0), testUpstream("b", 5*ms, 0)
	pick := func(now time.Time) *Upstream {
		return (LeastLatencySelection{}).selectAt(UpstreamPool{a, b}, now, noRand)
	}

	// a slow response raises the estimate at once
	now := latencyBase.Add(time.Second)
	a.latency.observe(200*ms, now)
	if got := pick(now); got != b {
		t.Fatalf("while a is slow: selected %q, want b", got.Dial)
	}

	// a minute without samples makes a unknown: 32 concurrent requests share a
	// single trial of it while 32 concurrent responses record samples for b
	now = latencyBase.Add(62 * time.Second)
	b.latency.observe(6*ms, now)
	samples := latencySamples(b)
	picked := make([]*Upstream, 64)
	var wg sync.WaitGroup
	for i := range picked {
		wg.Go(func() {
			if i%2 == 0 {
				picked[i] = pick(now)
			} else {
				b.latency.observe(6*ms, now)
			}
		})
	}
	wg.Wait()
	if got := latencySamples(b); got != samples+32 {
		t.Errorf("samples of b = %d, want %d", got, samples+32)
	}
	var trials int
	for _, u := range picked {
		if u == a {
			trials++
		}
	}
	if trials != 1 {
		t.Errorf("concurrent trials of a = %d, want 1", trials)
	}

	// a stays unknown, so it is tried once per second and no more often
	trials = 0
	for at := 100 * ms; at <= 10*time.Second; at += 100 * ms {
		now = latencyBase.Add(62*time.Second + at)
		b.latency.observe(6*ms, now)
		if pick(now) == a {
			trials++
		}
	}
	if trials != 10 {
		t.Errorf("trials over 10s = %d, want 10", trials)
	}

	// one fast response makes a competitive again
	a.latency.observe(5*ms, now)
	if got := pick(now); got != a {
		t.Errorf("after recovery: selected %q, want a", got.Dial)
	}
}

// When both candidates are unknown each is still tried only once, and after
// that the known upstreams are compared.
func TestLeastLatencyPolicyBothCandidatesUnknown(t *testing.T) {
	const ms = time.Millisecond
	sel := LeastLatencySelection{}
	u1, u2 := testUpstream("u1", noLatency, 0), testUpstream("u2", noLatency, 0)
	k1, k2, k3 := testUpstream("k1", 5*ms, 0), testUpstream("k2", 8*ms, 0), testUpstream("k3", 30*ms, 0)

	// this draw keeps u1 and u2 as the candidates and claims u2 first
	last := func(n int) int { return n - 1 }
	for i, want := range []*Upstream{u2, u1, k1} {
		if got := sel.selectAt(UpstreamPool{u1, u2, k1}, latencyBase, last); got != want {
			t.Errorf("selection %d = %q, want %q", i, got.Dial, want.Dial)
		}
	}

	// the known upstreams to compare are sampled from the pool: k3 takes the
	// place of k1, so k2 wins
	calls := make(map[int]int)
	replaceFirst := func(n int) int {
		calls[n]++
		if n == 3 && calls[n] == 2 {
			return 0
		}
		return n - 1
	}
	if got := sel.selectAt(UpstreamPool{u1, u2, k1, k2, k3}, latencyBase, replaceFirst); got != k2 {
		t.Errorf("selected %q, want k2 as the faster of the sampled k3 and k2", got.Dial)
	}
}

func TestLeastLatencyUnmarshalCaddyfile(t *testing.T) {
	for input, wantErr := range map[string]bool{"least_latency": false, "least_latency 2": true} {
		var s LeastLatencySelection
		if err := s.UnmarshalCaddyfile(caddyfile.NewTestDispenser(input)); (err != nil) != wantErr {
			t.Errorf("%q: error = %v, want error: %v", input, err, wantErr)
		}
	}
}

// Latency is only recorded for handlers whose policy reads it, including
// through the fallbacks of other policies.
func TestProvisionEnablesLatencyTracking(t *testing.T) {
	const leastLatency = `{"policy":"least_latency"}`
	tests := []struct {
		policy string // empty for the default policy
		want   bool
	}{
		{``, false},
		{`{"policy":"random"}`, false},
		{leastLatency, true},
		{`{"policy":"query","key":"k","fallback":` + leastLatency + `}`, true},
		{`{"policy":"cookie","fallback":{"policy":"header","field":"X-Key","fallback":` + leastLatency + `}}`, true},
	}
	var routes []string
	for i, tc := range tests {
		lb := `{}`
		if tc.policy != "" {
			lb = `{"selection_policy":` + tc.policy + `}`
		}
		routes = append(routes, fmt.Sprintf(
			`{"match":[{"path":["/%d"]}],"handle":[{"handler":"reverse_proxy","upstreams":[{"dial":"127.0.0.1:1"}],"load_balancing":%s}]}`, i, lb))
	}
	var cfg caddy.Config
	err := json.Unmarshal([]byte(`{"admin":{"disabled":true,"config":{"persist":false}},"apps":{"http":{"servers":{"srv0":{"listen":["127.0.0.1:0"],"routes":[`+
		strings.Join(routes, ",")+`]}}}}}`), &cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := caddy.Run(&cfg); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = caddy.Stop() })

	app, err := caddy.ActiveContext().App("http")
	if err != nil {
		t.Fatal(err)
	}
	handlers := app.(*caddyhttp.App).Servers["srv0"].Routes
	for i, tc := range tests {
		if got := handlers[i].Handlers[0].(*Handler).trackLatency; got != tc.want {
			t.Errorf("policy %q: trackLatency = %v, want %v", tc.policy, got, tc.want)
		}
	}
}
