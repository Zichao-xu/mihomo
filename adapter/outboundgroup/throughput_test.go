package outboundgroup

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
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
