package outboundgroup

import (
	"context"
	"io"
	"net"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/metacubex/mihomo/common/atomic"
	"github.com/metacubex/mihomo/common/singledo"
	"github.com/metacubex/mihomo/component/ca"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/log"

	"github.com/metacubex/http"
)

const (
	defaultThroughputURL       = "https://speed.cloudflare.com/__down?bytes=10000000"
	defaultThroughputTimeout   = 3 * time.Second
	defaultThroughputInterval  = time.Hour
	defaultThroughputMaxBytes  = 10_000_000
	defaultThroughputTolerance = 10
	throughputDialTimeout      = 5 * time.Second
	throughputStartupDelay     = 30 * time.Second
)

type throughputOption struct {
	url       string
	timeout   time.Duration
	interval  time.Duration
	maxBytes  int64
	tolerance int // percent
}

// throughputTester measures download speed of every proxy in a group one by one,
// so url-test can pick the fastest proxy instead of the lowest latency one.
// It must not reference the owning URLTest, otherwise the finalizer never runs.
type throughputTester struct {
	throughputOption
	ctx        context.Context
	cancel     context.CancelFunc
	gb         *GroupBase
	fastSingle *singledo.Single[C.Proxy]
	lazy       bool
	lastTouch  atomic.TypedValue[time.Time]
	running    atomic.Bool

	mu       sync.RWMutex
	speeds   map[string]uint64 // bytes per second, 0 means failed
	lastTest time.Time
}

func newThroughputTester(opt throughputOption, gb *GroupBase, fastSingle *singledo.Single[C.Proxy], lazy bool) *throughputTester {
	if opt.url == "" {
		opt.url = defaultThroughputURL
	}
	if opt.timeout <= 0 {
		opt.timeout = defaultThroughputTimeout
	}
	if opt.interval <= 0 {
		opt.interval = defaultThroughputInterval
	}
	if opt.maxBytes <= 0 {
		opt.maxBytes = defaultThroughputMaxBytes
	}
	if opt.tolerance < 0 || opt.tolerance >= 100 {
		opt.tolerance = defaultThroughputTolerance
	}
	ctx, cancel := context.WithCancel(context.Background())
	t := &throughputTester{
		throughputOption: opt,
		ctx:              ctx,
		cancel:           cancel,
		gb:               gb,
		fastSingle:       fastSingle,
		lazy:             lazy,
		speeds:           map[string]uint64{},
	}
	go t.loop()
	return t
}

func (t *throughputTester) touch() {
	t.lastTouch.Store(time.Now())
}

func (t *throughputTester) close() {
	t.cancel()
}

func (t *throughputTester) loop() {
	select {
	case <-time.After(throughputStartupDelay):
	case <-t.ctx.Done():
		return
	}
	t.check()

	ticker := time.NewTicker(t.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if !t.lazy || time.Since(t.lastTouch.Load()) < t.interval {
				t.check()
			} else {
				log.Debugln("[Throughput] %s skip once because we are lazy", t.gb.Name())
			}
		case <-t.ctx.Done():
			return
		}
	}
}

// check tests proxies sequentially, parallel tests would share the uplink and distort results.
func (t *throughputTester) check() {
	if !t.running.CompareAndSwap(false, true) {
		return
	}
	defer t.running.Store(false)

	proxies := t.gb.GetProxies(false)
	log.Infoln("[Throughput] %s start testing %d proxies", t.gb.Name(), len(proxies))
	speeds := make(map[string]uint64, len(proxies))
	for _, proxy := range proxies {
		if t.ctx.Err() != nil {
			return
		}
		speed, err := t.measure(proxy)
		if err != nil {
			log.Debugln("[Throughput] %s %s: %v", t.gb.Name(), proxy.Name(), err)
		}
		speeds[proxy.Name()] = speed
	}

	t.mu.Lock()
	t.speeds = speeds
	t.lastTest = time.Now()
	t.mu.Unlock()
	t.fastSingle.Reset()

	log.Infoln("[Throughput] %s done, top: %s", t.gb.Name(), topSpeeds(speeds, 3))
}

func (t *throughputTester) measure(proxy C.Proxy) (uint64, error) {
	tlsConfig, err := ca.GetTLSConfig(ca.Option{})
	if err != nil {
		return 0, err
	}
	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			var metadata C.Metadata
			if err := metadata.SetRemoteAddress(addr); err != nil {
				return nil, err
			}
			return proxy.DialContext(ctx, &metadata)
		},
		TLSHandshakeTimeout: throughputDialTimeout,
		TLSClientConfig:     tlsConfig,
		DisableKeepAlives:   true,
	}
	defer transport.CloseIdleConnections()

	ctx, cancel := context.WithTimeout(t.ctx, throughputDialTimeout+t.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, t.url, nil)
	if err != nil {
		return 0, err
	}

	resp, err := (&http.Client{Transport: transport}).Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	// latency is covered by url-test itself, only count the transfer phase
	start := time.Now()
	timer := time.AfterFunc(t.timeout, cancel)
	defer timer.Stop()
	n, err := io.Copy(io.Discard, io.LimitReader(resp.Body, t.maxBytes))
	elapsed := time.Since(start)
	if n == 0 || elapsed <= 0 {
		return 0, err
	}
	return uint64(float64(n) / elapsed.Seconds()), nil
}

func (t *throughputTester) speedOf(name string) uint64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.speeds[name]
}

// pick returns the fastest alive proxy; among proxies whose speed is within
// tolerance percent of the fastest, the lowest latency one wins.
// It returns nil when there is no usable result yet, so caller can fall back to latency.
func (t *throughputTester) pick(proxies []C.Proxy, testUrl string) C.Proxy {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var maxSpeed uint64
	for _, proxy := range proxies {
		if !proxy.AliveForTestUrl(testUrl) {
			continue
		}
		if s := t.speeds[proxy.Name()]; s > maxSpeed {
			maxSpeed = s
		}
	}
	if maxSpeed == 0 {
		return nil
	}

	floor := maxSpeed * uint64(100-t.tolerance) / 100
	var best C.Proxy
	for _, proxy := range proxies {
		if !proxy.AliveForTestUrl(testUrl) || t.speeds[proxy.Name()] < floor {
			continue
		}
		if best == nil || proxy.LastDelayForTestUrl(testUrl) < best.LastDelayForTestUrl(testUrl) {
			best = proxy
		}
	}
	return best
}

func (t *throughputTester) snapshot() (map[string]uint64, time.Time) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	speeds := make(map[string]uint64, len(t.speeds))
	for k, v := range t.speeds {
		speeds[k] = v
	}
	return speeds, t.lastTest
}

func topSpeeds(speeds map[string]uint64, n int) string {
	names := make([]string, 0, len(speeds))
	for name, s := range speeds {
		if s > 0 {
			names = append(names, name)
		}
	}
	sort.Slice(names, func(i, j int) bool { return speeds[names[i]] > speeds[names[j]] })
	if len(names) > n {
		names = names[:n]
	}
	out := ""
	for i, name := range names {
		if i > 0 {
			out += ", "
		}
		out += name + " " + formatMbps(speeds[name])
	}
	if out == "" {
		return "none"
	}
	return out
}

func formatMbps(bytesPerSecond uint64) string {
	mbps := float64(bytesPerSecond) * 8 / 1e6
	return strconv.FormatFloat(mbps, 'f', 1, 64) + "Mbps"
}
