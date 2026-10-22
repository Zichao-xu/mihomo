package tunnel

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	N "github.com/metacubex/mihomo/common/net"
	"github.com/metacubex/mihomo/component/expect"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/log"
)

// hedgeIO 在首包到达前,把同一段 TLS 握手同时交给原节点和备选节点,先回者胜,败者关闭。
// 决出胜者后所有读写只走胜者,和普通连接一样。
//
// 能重放的前提是上行只有 ClientHello:两条路各自完成一次独立握手,服务器端没有副作用。
// 一旦上行出现其它内容(0-RTT 数据、明文请求)或超过 16KB,就不再重放,已发起的对冲也放弃。
type hedgeIO struct {
	mu         sync.Mutex
	primary    C.Conn
	alt        C.Conn
	winner     C.Conn
	pending    []byte
	pendErr    error
	buf        []byte
	replayable bool
	started    bool
	aborted    bool
	primFailed bool
	closed     bool
	racers     int
	results    chan raceResult
	done       chan struct{}
	timer      *time.Timer
	start      time.Time
	rdl, wdl   time.Time

	node, altName string
	dial          func(ctx context.Context) (C.Conn, error)
	take          func(node string) (bool, string) // 对冲前问预算,默认 expect.TakeHedge
	obs           *expect.Conn
	events        []hedgeEv // obs 还没接上时先攒着
}

type raceResult struct {
	c    C.Conn
	data []byte
	err  error
}

type hedgeEv struct {
	ev     string
	waited time.Duration
}

var errHedgeDropped = errors.New("hedge dropped")

func newHedgeIO(primary C.Conn, sent []byte, node, alt string, delay time.Duration,
	dial func(context.Context) (C.Conn, error), take func(string) (bool, string)) *hedgeIO {
	h := &hedgeIO{
		primary: primary, node: node, altName: alt, dial: dial, take: take,
		results: make(chan raceResult, 2), done: make(chan struct{}), start: time.Now(),
		racers: 1,
	}
	h.buf = append([]byte(nil), sent...)
	h.replayable = len(h.buf) <= 16<<10 && (len(h.buf) == 0 || isClientHello(h.buf))
	go h.race(primary)
	h.timer = time.AfterFunc(delay, func() {
		h.mu.Lock()
		if h.winner != nil || h.started || h.closed || !h.replayable || len(h.buf) == 0 {
			h.mu.Unlock()
			return
		}
		h.mu.Unlock()
		// 预算和"是否本地故障"的判断在锁外做,它们会锁 expect 的引擎
		ok, why := h.take(node)
		h.mu.Lock()
		if !ok {
			h.noteLocked("skip:"+why, time.Since(h.start))
		} else {
			h.startLocked("fired")
		}
		h.mu.Unlock()
		h.flush()
	})
	return h
}

// isClientHello:缓存里只有 TLS 握手记录,且以 ClientHello 开头
func isClientHello(b []byte) bool {
	if len(b) < 6 || b[0] != 0x16 || b[1] != 0x03 || b[5] != 0x01 {
		return false
	}
	for len(b) >= 5 {
		if b[0] != 0x16 {
			return false
		}
		n := int(b[3])<<8 | int(b[4])
		if len(b) < 5+n {
			return true // 记录还没发完,目前看到的都是握手
		}
		b = b[5+n:]
	}
	return len(b) == 0 || b[0] == 0x16
}

func (h *hedgeIO) race(c C.Conn) {
	b := make([]byte, 32<<10)
	n, err := c.Read(b)
	h.results <- raceResult{c: c, data: b[:n], err: err}
}

func (h *hedgeIO) noteLocked(ev string, waited time.Duration) {
	h.events = append(h.events, hedgeEv{ev, waited})
}

// flush 把攒下的事件交给 expect,在锁外调用
func (h *hedgeIO) flush() {
	h.mu.Lock()
	obs, evs := h.obs, h.events
	if obs != nil {
		h.events = nil
	}
	h.mu.Unlock()
	for _, e := range evs {
		obs.HedgeEvent(e.ev, h.altName, e.waited)
	}
}

func (h *hedgeIO) setObs(o *expect.Conn) {
	h.mu.Lock()
	h.obs = o
	h.mu.Unlock()
	h.flush()
}

func (h *hedgeIO) startLocked(ev string) {
	h.started = true
	h.racers++
	h.noteLocked(ev, time.Since(h.start))
	buf := append([]byte(nil), h.buf...)
	go h.dialAlt(buf)
}

func (h *hedgeIO) dialAlt(buf []byte) {
	ctx, cancel := context.WithTimeout(context.Background(), C.DefaultTCPTimeout)
	defer cancel()
	c, err := h.dial(ctx)
	if err == nil {
		h.mu.Lock()
		if h.rdl != (time.Time{}) {
			_ = c.SetReadDeadline(h.rdl)
		}
		dropped := h.winner != nil || h.closed || h.aborted
		h.mu.Unlock()
		if dropped {
			_ = c.Close()
			h.results <- raceResult{err: errHedgeDropped}
			return
		}
		if _, err = c.Write(buf); err != nil {
			err = fmt.Errorf("replay: %w", err)
			_ = c.Close()
		}
	}
	if err != nil {
		log.Debugln("[Expect] hedge %s -> %s failed: %v", h.node, h.altName, err)
		h.mu.Lock()
		h.noteLocked("alt-fail", time.Since(h.start))
		h.mu.Unlock()
		h.flush()
		h.results <- raceResult{err: err}
		return
	}
	h.mu.Lock()
	if h.winner != nil || h.closed || h.aborted {
		h.mu.Unlock()
		_ = c.Close()
		h.results <- raceResult{err: errHedgeDropped}
		return
	}
	h.alt = c
	h.mu.Unlock()
	go h.race(c)
}

func (h *hedgeIO) decideLocked(w C.Conn) {
	h.winner = w
	h.timer.Stop()
	h.buf = nil
	waited := time.Since(h.start)
	switch {
	case w == h.primary:
		if h.alt != nil {
			_ = h.alt.Close()
		}
		if h.started && !h.aborted {
			h.noteLocked("primary-won", waited)
		}
	case h.primFailed:
		_ = h.primary.Close()
		h.noteLocked("failover", waited)
	default:
		_ = h.primary.Close()
		h.noteLocked("alt-won", waited)
	}
}

func (h *hedgeIO) Read(p []byte) (int, error) {
	h.mu.Lock()
	if h.winner != nil {
		if len(h.pending) > 0 {
			n := copy(p, h.pending)
			h.pending = h.pending[n:]
			h.mu.Unlock()
			return n, nil
		}
		if err := h.pendErr; err != nil {
			h.pendErr = nil
			h.mu.Unlock()
			return 0, err
		}
		w := h.winner
		h.mu.Unlock()
		return w.Read(p)
	}
	h.mu.Unlock()
	for {
		var r raceResult
		select {
		case r = <-h.results:
		case <-h.done:
			return 0, net.ErrClosed
		}
		h.mu.Lock()
		h.racers--
		if len(r.data) == 0 && r.err != nil {
			if r.c == h.primary {
				h.primFailed = true
				// 原节点还没回就断了:不等计时器,立刻换路(不占对冲预算,本来就得重连)
				if !h.started && h.replayable && len(h.buf) > 0 && !h.closed && h.winner == nil {
					h.startLocked("failover-start")
					h.mu.Unlock()
					h.flush()
					continue
				}
			}
			if h.racers > 0 {
				h.mu.Unlock()
				continue
			}
			// 两条路都失败,按原节点的结局交给上层
			h.winner = h.primary
			h.timer.Stop()
			h.mu.Unlock()
			h.flush()
			return 0, r.err
		}
		h.decideLocked(r.c)
		n := copy(p, r.data)
		h.pending = r.data[n:]
		if r.err != nil {
			h.pendErr = r.err
		}
		h.mu.Unlock()
		h.flush()
		return n, nil
	}
}

func (h *hedgeIO) Write(p []byte) (int, error) {
	h.mu.Lock()
	if h.winner != nil {
		w := h.winner
		h.mu.Unlock()
		return w.Write(p)
	}
	if h.replayable {
		h.buf = append(h.buf, p...)
		if len(h.buf) > 16<<10 || !isClientHello(h.buf) {
			h.replayable = false
			h.buf = nil
		}
	}
	if h.started && !h.aborted {
		// 对冲发起后客户端又发了东西:备选那条路没有这些字节,放弃对冲
		h.aborted = true
		if h.alt != nil {
			_ = h.alt.Close()
		}
		h.noteLocked("aborted", time.Since(h.start))
	}
	primary := h.primary
	h.mu.Unlock()
	h.flush()
	return primary.Write(p)
}

func (h *hedgeIO) Close() error {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil
	}
	h.closed = true
	h.timer.Stop()
	close(h.done)
	alt := h.alt
	h.mu.Unlock()
	if alt != nil {
		_ = alt.Close()
	}
	return h.primary.Close()
}

func (h *hedgeIO) current() C.Conn {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.winner != nil {
		return h.winner
	}
	return h.primary
}

func (h *hedgeIO) LocalAddr() net.Addr  { return h.current().LocalAddr() }
func (h *hedgeIO) RemoteAddr() net.Addr { return h.current().RemoteAddr() }

func (h *hedgeIO) SetDeadline(t time.Time) error {
	_ = h.SetReadDeadline(t)
	return h.SetWriteDeadline(t)
}

func (h *hedgeIO) SetReadDeadline(t time.Time) error {
	h.mu.Lock()
	h.rdl = t
	conns := []C.Conn{h.primary, h.alt}
	h.mu.Unlock()
	var err error
	for _, c := range conns {
		if c != nil {
			if e := c.SetReadDeadline(t); e != nil && c == h.primary {
				err = e
			}
		}
	}
	return err
}

func (h *hedgeIO) SetWriteDeadline(t time.Time) error {
	h.mu.Lock()
	h.wdl = t
	c := h.winner
	if c == nil {
		c = h.primary
	}
	h.mu.Unlock()
	return c.SetWriteDeadline(t)
}

// hedgeConn 让 hedgeIO 满足 C.Conn;链路信息跟随胜者
type hedgeConn struct {
	N.ExtendedConn
	io *hedgeIO
}

func (c *hedgeConn) Chains() C.Chain                 { return c.io.current().Chains() }
func (c *hedgeConn) ProviderChains() C.Chain         { return c.io.current().ProviderChains() }
func (c *hedgeConn) AppendToChains(a C.ProxyAdapter) { c.io.primary.AppendToChains(a) }
func (c *hedgeConn) RemoteDestination() string       { return c.io.current().RemoteDestination() }
func (c *hedgeConn) SetExpect(o *expect.Conn)        { c.io.setObs(o) }

// maybeHedge 按实验分组决定是否给这条连接套上对冲。返回的 arm 记进影子日志。
func maybeHedge(remote C.Conn, metadata *C.Metadata, sent []byte) (C.Conn, string) {
	if metadata == nil {
		return remote, ""
	}
	chains := remote.Chains()
	arm := expect.Assign(chains)
	if arm != expect.ArmHedge {
		return remote, arm
	}
	host := metadata.Host
	delay, alt, ok := expect.HedgePlan(chains[0], chains[1], host)
	if !ok {
		return remote, arm
	}
	altProxy, found := Proxies()[alt]
	if !found {
		return remote, arm
	}
	dial := func(ctx context.Context) (C.Conn, error) {
		return altProxy.DialContext(ctx, metadata)
	}
	io := newHedgeIO(remote, sent, chains[0], alt, delay, dial, expect.TakeHedge)
	return &hedgeConn{ExtendedConn: N.NewExtendedConn(io), io: io}, arm
}
