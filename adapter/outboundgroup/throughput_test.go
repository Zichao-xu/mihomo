package outboundgroup

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/adapter/outbound"
	C "github.com/metacubex/mihomo/constant"
)

func newTestTester(opt throughputOption) *throughputTester {
	ctx, cancel := context.WithCancel(context.Background())
	return &throughputTester{throughputOption: opt, ctx: ctx, cancel: cancel, speeds: map[string]uint64{}}
}

func namedDirect(name string) C.Proxy {
	return adapter.NewProxy(outbound.NewDirectWithOption(outbound.DirectOption{BasicOption: outbound.BasicOption{}, Name: name}))
}

func TestThroughputPick(t *testing.T) {
	a, b, c := namedDirect("a"), namedDirect("b"), namedDirect("c")
	proxies := []C.Proxy{a, b, c}
	tt := newTestTester(throughputOption{tolerance: 10})

	if p := tt.pick(proxies, ""); p != nil {
		t.Fatalf("expect nil without results, got %s", p.Name())
	}

	tt.speeds = map[string]uint64{"a": 100, "b": 500, "c": 0}
	if p := tt.pick(proxies, ""); p == nil || p.Name() != "b" {
		t.Fatalf("expect b, got %v", p)
	}

	// a falls within 10% of b, delay decides; both unknown so the first in order wins
	tt.speeds = map[string]uint64{"a": 460, "b": 500}
	if p := tt.pick(proxies, ""); p == nil || p.Name() != "a" {
		t.Fatalf("expect a by tie-break, got %v", p)
	}
}

func TestThroughputMeasure(t *testing.T) {
	chunk := strings.Repeat("x", 64*1024)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// ~6.4MB/s for 2 seconds, longer than the test timeout
		for i := 0; i < 200; i++ {
			if _, err := w.Write([]byte(chunk)); err != nil {
				return
			}
			w.(http.Flusher).Flush()
			time.Sleep(10 * time.Millisecond)
		}
	}))
	defer srv.Close()

	tt := newTestTester(throughputOption{url: srv.URL, timeout: 500 * time.Millisecond, maxBytes: 100 << 20})
	start := time.Now()
	speed, err := tt.measure(namedDirect("direct"))
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("timeout not honored: %v", elapsed)
	}
	if speed < 2_000_000 || speed > 10_000_000 {
		t.Fatalf("unexpected speed %d B/s (err %v)", speed, err)
	}

	// byte cap stops early
	tt.maxBytes = 256 * 1024
	tt.timeout = 5 * time.Second
	start = time.Now()
	if speed, _ = tt.measure(namedDirect("direct")); speed == 0 || time.Since(start) > time.Second {
		t.Fatalf("byte cap not honored: speed %d, %v", speed, time.Since(start))
	}
}

func TestThroughputSharedSerialAndCached(t *testing.T) {
	throughputMu.Lock()
	throughputCache = map[throughputCacheKey]throughputResult{}
	throughputMu.Unlock()

	var active, maxActive, total int32
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		active++
		total++
		if active > maxActive {
			maxActive = active
		}
		mu.Unlock()
		_, _ = w.Write([]byte(strings.Repeat("x", 64*1024)))
		time.Sleep(50 * time.Millisecond)
		mu.Lock()
		active--
		mu.Unlock()
	}))
	defer srv.Close()

	opt := throughputOption{url: srv.URL, timeout: time.Second, maxBytes: 1 << 20, interval: time.Hour}
	proxies := []C.Proxy{namedDirect("p1"), namedDirect("p2"), namedDirect("p3")}

	var wg sync.WaitGroup
	for g := 0; g < 3; g++ {
		tt := newTestTester(opt)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, p := range proxies {
				if tt.measureShared(p) == 0 {
					t.Errorf("zero speed for %s", p.Name())
				}
			}
		}()
	}
	wg.Wait()

	if maxActive != 1 {
		t.Fatalf("tests ran concurrently: max %d", maxActive)
	}
	if total != int32(len(proxies)) {
		t.Fatalf("expect %d downloads shared by all groups, got %d", len(proxies), total)
	}

	// a group with a different test url must not reuse results
	other := newTestTester(throughputOption{url: srv.URL + "/other", timeout: time.Second, maxBytes: 1 << 20, interval: time.Hour})
	other.measureShared(proxies[0])
	if total != int32(len(proxies))+1 {
		t.Fatalf("different params should measure again, total %d", total)
	}
}
