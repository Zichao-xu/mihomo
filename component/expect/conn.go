package expect

import (
	"sync"
	"time"
)

// Conn 跟踪一条 TCP 连接的进度。所有方法对 nil 安全,影子模式关闭时零开销。
type Conn struct {
	mu sync.Mutex
	e  *Engine

	start              time.Time
	node, group, rule  string
	host, src, dst     string
	members            []string
	dial               time.Duration
	shadow             string
	known              int
	up, down           int64
	exchanges          int
	firstWait          time.Duration
	maxWait, maxGap    time.Duration
	flags              int
	sending, delivered bool
	waitFrom, lastDown time.Time
	respBytes          int64
	smallGaps          int // 还没计入节点分布的小间隔(<minGap)个数,攒批写入免得每个包都抢锁
	open               *flag
	closed             bool
}

// flag 是一次"偏离预期"的判定,等结果出来后与当时的预估对照
type flag struct {
	kind   string // hedge:首包前,可透明对冲;kill:已交付字节,只能掐断重连
	signal string // first / wait / gap
	at     time.Time
	from   time.Time
	thr    time.Duration
	scope  string
	alt    string
	est    time.Duration // 如果当时出手,预计多久能拿到回应
}

// Open 在连接建立(已拨通出口)时调用。chains[0] 是实际节点,chains[1] 是做选择的组。
func Open(host, src, dst string, chains []string, rule string, preUp int64) *Conn {
	if std == nil || len(chains) == 0 {
		return nil
	}
	return std.open(time.Now(), host, src, dst, chains, rule, preUp)
}

func (e *Engine) open(now time.Time, host, src, dst string, chains []string, rule string, preUp int64) *Conn {
	c := &Conn{e: e, start: now, node: chains[0], host: host, src: src, dst: dst, rule: rule}
	if len(chains) >= 2 {
		c.group = chains[1]
		e.mu.Lock()
		f := e.members
		e.mu.Unlock()
		if f != nil {
			c.members = f(c.group)
		}
		if len(c.members) > 1 {
			c.shadow, c.known = e.shadowPick(c.members, now)
		}
	}
	if preUp > 0 {
		c.up = preUp
		c.sending = true
		c.waitFrom = now
	}
	e.mu.Lock()
	e.active[c] = struct{}{}
	e.mu.Unlock()
	return c
}

// SetDial 记录拨通出口用了多久(含重试)
func (c *Conn) SetDial(d time.Duration) {
	if c != nil {
		c.setDial(time.Now(), d)
	}
}

func (c *Conn) setDial(now time.Time, d time.Duration) {
	c.mu.Lock()
	c.dial = d
	c.mu.Unlock()
	n := c.e.node(c.node)
	n.mu.Lock()
	n.dial.add(now, d)
	n.mu.Unlock()
}

func (c *Conn) Up(b int64) {
	if c != nil && b > 0 {
		c.up_(time.Now(), b)
	}
}

func (c *Conn) up_(now time.Time, b int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	c.up += b
	if c.sending {
		return
	}
	// 收包途中的小上行是窗口更新之类,不是新请求;下行停了以后的上行才是
	if c.lastDown.IsZero() || b >= smallWrite || now.Sub(c.lastDown) >= 50*time.Millisecond {
		if c.open != nil && c.open.signal == "gap" {
			// 又发了新请求,说明之前那段"停顿"其实是回应已经收完了
			c.resolveLocked(now, "ended")
		}
		c.sending = true
		c.waitFrom = now
	}
}

func (c *Conn) Down(b int64) {
	if c != nil && b > 0 {
		c.down_(time.Now(), b)
	}
}

func (c *Conn) down_(now time.Time, b int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	c.down += b
	if c.sending {
		w := now.Sub(c.waitFrom)
		n := c.e.node(c.node)
		n.mu.Lock()
		if c.delivered {
			n.wait.add(now, w)
		} else {
			n.first.add(now, w)
			c.firstWait = w
		}
		n.mu.Unlock()
		c.e.addHostWait(c.host, now, w)
		if w > c.maxWait {
			c.maxWait = w
		}
		c.exchanges++
		c.sending = false
		c.respBytes = 0
	} else if c.respBytes >= bulkBytes && !c.lastDown.IsZero() {
		if g := now.Sub(c.lastDown); g >= minGap {
			n := c.e.node(c.node)
			n.mu.Lock()
			n.gap.add(now, g)
			n.mu.Unlock()
			if g > c.maxGap {
				c.maxGap = g
			}
		} else if c.smallGaps++; c.smallGaps >= 256 {
			c.flushGapsLocked(now)
		}
	}
	if c.open != nil {
		c.resolveLocked(now, "answered")
	}
	c.respBytes += b
	c.delivered = true
	c.lastDown = now
}

// flushGapsLocked 把攒下的小间隔记进节点分布。它们都低于任何阈值,具体落在哪个桶不影响判定,
// 只影响分位数的排名,所以统一记在 minGap/2。
func (c *Conn) flushGapsLocked(now time.Time) {
	if c.smallGaps == 0 {
		return
	}
	n := c.e.node(c.node)
	n.mu.Lock()
	n.gap.addN(now, minGap/2, float64(c.smallGaps))
	n.mu.Unlock()
	c.smallGaps = 0
}

// check 由 Engine 每 250ms 调用,判断当前是否已经偏离预期
func (c *Conn) check(e *Engine, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.open != nil {
		return
	}
	var f *flag
	switch {
	case c.sending && !c.waitFrom.IsZero():
		first := !c.delivered
		thr, ok := e.waitThreshold(c, now, first)
		if !ok || now.Sub(c.waitFrom) <= thr {
			return
		}
		f = &flag{kind: "kill", signal: "wait", from: c.waitFrom, thr: thr}
		if first {
			f.kind, f.signal = "hedge", "first"
		}
	case !c.sending && c.respBytes >= bulkBytes && !c.lastDown.IsZero():
		c.flushGapsLocked(now)
		thr, ok := e.gapThreshold(c.node, now)
		if !ok || now.Sub(c.lastDown) <= thr {
			return
		}
		f = &flag{kind: "kill", signal: "gap", from: c.lastDown, thr: thr}
	default:
		return
	}
	f.at = now
	f.scope = e.attribute(c.node, now)
	var altFirst, altDial time.Duration
	f.alt, altFirst, altDial = e.alternative(c, now)
	if f.alt != "" {
		f.est = f.thr + altDial + altFirst
	}
	c.open = f
	c.flags++
	e.counters.flag(f)
	e.out.event(map[string]any{
		"t": "flag", "at": stamp(now), "kind": f.kind, "signal": f.signal, "node": c.node, "group": c.group,
		"host": c.host, "src": c.src, "thr_ms": ms(f.thr), "scope": f.scope, "alt": f.alt, "est_ms": ms(f.est),
		"exch": c.exchanges, "down": c.down,
	})
}

// resolveLocked 记录一次判定的结局:answered=等到了回应,ended=其实回应已收完,closed=连接关了也没等到
func (c *Conn) resolveLocked(now time.Time, how string) {
	f := c.open
	c.open = nil
	actual := now.Sub(f.from)
	rec := map[string]any{
		"t": "outcome", "at": stamp(now), "kind": f.kind, "signal": f.signal, "how": how, "node": c.node,
		"group": c.group, "host": c.host, "src": c.src, "scope": f.scope, "thr_ms": ms(f.thr),
		"actual_ms": ms(actual), "alt": f.alt, "est_ms": ms(f.est),
	}
	// 出手能省多少:实际等待 - 出手后的预计等待。只在有备选、且确实是在等回应时才有意义
	save := time.Duration(0)
	if f.alt != "" && how != "ended" {
		save = actual - f.est
		rec["save_ms"] = ms(save)
	}
	c.e.counters.outcome(f, how, save)
	c.e.out.event(rec)
}

func (c *Conn) Close() {
	if c != nil {
		c.close_(time.Now())
	}
}

func (c *Conn) close_(now time.Time) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	c.flushGapsLocked(now)
	if c.open != nil {
		how := "closed"
		if c.open.signal == "gap" {
			how = "ended" // 大块传输停了随后关闭,多半是传完了
		}
		c.resolveLocked(now, how)
	}
	rec := map[string]any{
		"t": "conn", "start": stamp(c.start), "dur_ms": ms(now.Sub(c.start)), "src": c.src, "host": c.host,
		"dst": c.dst, "node": c.node, "group": c.group, "rule": c.rule, "dial_ms": ms(c.dial),
		"up": c.up, "down": c.down, "exch": c.exchanges, "first_ms": -1,
		"max_wait_ms": ms(c.maxWait), "max_gap_ms": ms(c.maxGap), "flags": c.flags,
	}
	if c.exchanges > 0 {
		rec["first_ms"] = ms(c.firstWait)
	}
	if c.sending && !c.waitFrom.IsZero() {
		rec["open_wait_ms"] = ms(now.Sub(c.waitFrom))
	}
	if c.group != "" {
		rec["shadow"] = c.shadow
		rec["known"] = c.known
	}
	hasBytes := c.up > 0 || c.down > 0
	c.mu.Unlock()

	e := c.e
	e.counters.conn(c)
	e.mu.Lock()
	delete(e.active, c)
	e.mu.Unlock()
	if hasBytes {
		e.out.event(rec)
	}
}

// DialFail 记录拨号失败(含重试后仍失败),这是"打不开"最直接的信号
func DialFail(node, group, host, src string, d time.Duration, err error) {
	e := std
	if e == nil {
		return
	}
	e.counters.dialFail()
	e.out.event(map[string]any{
		"t": "dialfail", "at": stamp(time.Now()), "node": node, "group": group, "host": host, "src": src,
		"dur_ms": ms(d), "err": err.Error(),
	})
}

func ms(d time.Duration) int64 { return d.Milliseconds() }

// op 上没有 zoneinfo,统一按北京时间记
var cst = time.FixedZone("CST", 8*3600)

func stamp(t time.Time) string { return t.In(cst).Format("2006-01-02T15:04:05.000") }
