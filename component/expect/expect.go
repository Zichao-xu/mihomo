// Package expect 是预期驱动选路的影子模式:只观察、预测、记录"如果由我来决定会怎样",
// 不改变任何实际转发。
//
// 对每个节点,从真实流量学三个分布:首包(建连后第一次回应)、等待(之后每次请求到回应)、
// 间隔(大块传输中相邻两次收包的间隔)。阈值都取自这些分布的分位数,没有人工设定的时长。
//
// 出手方式受协议约束,不是参数:
//   - 对冲:回应的第一个字节到达之前,已发出的上行字节可以原样重放到另一条路,先回者胜,对应用透明。
//   - 掐断:一旦有字节交给了应用,TLS 会话就钉死在这条路上,只能断开让应用重连。
package expect

import (
	"math/rand"
	"os"
	"sync"
	"time"
)

const (
	halfLife   = 30 * time.Minute
	tick       = 250 * time.Millisecond
	bulkBytes  = 64 << 10 // 本次回应收到这么多字节后,才把收包间隔算作"传输中的间隔"
	minGap     = 20 * time.Millisecond
	smallWrite = 128 // 收包途中的小上行(窗口更新、ACK 类)不算新请求

	minFirst = 10 // 节点首包样本少于此数时不参与影子选路
	minWait  = 30 // 等待分布样本少于此数时不判偏差
	minGapN  = 50
	maxHosts = 5000

	attributeWindow = 3 * time.Second
)

type nodeStat struct {
	mu    sync.Mutex
	first *hist
	wait  *hist
	gap   *hist
	dial  *hist
}

func newNodeStat() *nodeStat {
	return &nodeStat{first: newHist(halfLife), wait: newHist(halfLife), gap: newHist(halfLife), dial: newHist(halfLife)}
}

type hostStat struct {
	wait *hist
	seen time.Time
}

type flagRec struct {
	at   time.Time
	node string
}

// Engine 是全局唯一的影子实例
type Engine struct {
	mu     sync.Mutex
	nodes  map[string]*nodeStat
	hosts  map[string]*hostStat
	active map[*Conn]struct{}
	recent []flagRec

	members func(group string) []string
	out     *writer
	rnd     *rand.Rand

	counters  counters
	hedge     hedgePolicy
	hedgeFile string
}

var std *Engine

// Start 启用影子模式。dir 为空或环境变量 MIHOMO_EXPECT=0 时不启用。
// hedgeFile 是对冲实验的开关文件,内容为进入对冲组的比例。
func Start(dir, hedgeFile string) {
	if dir == "" || os.Getenv("MIHOMO_EXPECT") == "0" {
		return
	}
	if _, err := os.Stat(dir + ".off"); err == nil {
		return
	}
	e := newEngine(newWriter(dir))
	e.hedgeFile = hedgeFile
	e.hedge.p = parseHedge(hedgeFile)
	go e.loop()
	std = e
}

func newEngine(w *writer) *Engine {
	e := &Engine{
		nodes:  map[string]*nodeStat{},
		hosts:  map[string]*hostStat{},
		active: map[*Conn]struct{}{},
		out:    w,
		rnd:    rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	e.counters.init()
	return e
}

// SetGroupMembers 注入"组名 → 成员节点名"的查询,避免本包反向依赖 tunnel
func SetGroupMembers(f func(group string) []string) {
	if std != nil {
		std.mu.Lock()
		std.members = f
		std.mu.Unlock()
	}
}

func (e *Engine) node(name string) *nodeStat {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.nodeLocked(name)
}

func (e *Engine) nodeLocked(name string) *nodeStat {
	n := e.nodes[name]
	if n == nil {
		n = newNodeStat()
		e.nodes[name] = n
	}
	return n
}

func (e *Engine) addHostWait(host string, now time.Time, d time.Duration) {
	if host == "" {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	h := e.hosts[host]
	if h == nil {
		if len(e.hosts) >= maxHosts {
			e.evictHostsLocked(now)
		}
		h = &hostStat{wait: newHist(halfLife)}
		e.hosts[host] = h
	}
	h.wait.add(now, d)
	h.seen = now
}

// evictHostsLocked 丢掉最久没见的一半
func (e *Engine) evictHostsLocked(now time.Time) {
	cut := now.Add(-time.Hour)
	for k, h := range e.hosts {
		if h.seen.Before(cut) {
			delete(e.hosts, k)
		}
	}
	if len(e.hosts) < maxHosts {
		return
	}
	i := 0
	for k := range e.hosts {
		if i%2 == 0 {
			delete(e.hosts, k)
		}
		i++
	}
}

// waitThreshold:这次等待超过多久算偏离预期。
// 节点分布的 p95 和这个站点自己习惯的 p95 取大者,长轮询类站点不会被当成线路问题。
func (e *Engine) waitThreshold(c *Conn, now time.Time, first bool) (time.Duration, bool) {
	n := e.node(c.node)
	n.mu.Lock()
	h := n.wait
	if first {
		h = n.first
	}
	var thr time.Duration
	ok := h.weight(now) >= minWait
	if ok {
		thr = h.quantile(now, 0.95)
	}
	n.mu.Unlock()
	if !ok {
		return 0, false
	}
	if c.host != "" {
		e.mu.Lock()
		if hs := e.hosts[c.host]; hs != nil && hs.wait.weight(now) >= minFirst {
			if ht := hs.wait.quantile(now, 0.95); ht > thr {
				thr = ht
			}
		}
		e.mu.Unlock()
	}
	return thr, true
}

func (e *Engine) gapThreshold(node string, now time.Time) (time.Duration, bool) {
	n := e.node(node)
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.gap.weight(now) < minGapN {
		return 0, false
	}
	thr := n.gap.quantile(now, 0.99)
	// 间隔分布全是毫秒级时,p99 也可能只有几十毫秒,低于人能感知的卡顿没有意义
	if thr < 500*time.Millisecond {
		thr = 500 * time.Millisecond
	}
	return thr, true
}

// alternative 找同组里预期最好的另一个节点,返回它的首包 p50 和建连 p50
func (e *Engine) alternative(c *Conn, now time.Time) (string, time.Duration, time.Duration) {
	best, bestFirst, bestDial := "", time.Duration(0), time.Duration(0)
	for _, m := range c.members {
		if m == c.node {
			continue
		}
		n := e.node(m)
		n.mu.Lock()
		if n.first.weight(now) >= minFirst {
			f := n.first.quantile(now, 0.5)
			d := n.dial.quantile(now, 0.5)
			if best == "" || f+d < bestFirst+bestDial {
				best, bestFirst, bestDial = m, f, d
			}
		}
		n.mu.Unlock()
	}
	return best, bestFirst, bestDial
}

// shadowPick 是影子选路:各候选取同一个随机分位数(0.5~0.99)上的首包时长,最小者胜。
// 随机分位数让比较偏向尾部又带探索;样本不足的节点不参与。
func (e *Engine) shadowPick(members []string, now time.Time) (string, int) {
	e.mu.Lock()
	u := 0.5 + e.rnd.Float64()*0.49
	e.mu.Unlock()
	best, bestV, known := "", time.Duration(0), 0
	for _, m := range members {
		n := e.node(m)
		n.mu.Lock()
		if n.first.weight(now) >= minFirst {
			known++
			v := n.first.quantile(now, u) + n.dial.quantile(now, u)
			if best == "" || v < bestV {
				best, bestV = m, v
			}
		}
		n.mu.Unlock()
	}
	return best, known
}

// attribute 判断偏差的范围:同一时刻多条路一起偏离,是本地或上游的问题,不怪节点
func (e *Engine) attribute(node string, now time.Time) string {
	e.mu.Lock()
	defer e.mu.Unlock()
	keep := e.recent[:0]
	for _, f := range e.recent {
		if now.Sub(f.at) <= attributeWindow {
			keep = append(keep, f)
		}
	}
	e.recent = append(keep, flagRec{at: now, node: node})
	nodes := map[string]bool{}
	for _, f := range e.recent {
		nodes[f.node] = true
	}
	switch {
	case len(nodes) >= 3 || (nodes["DIRECT"] && len(nodes) >= 2):
		return "local"
	case len(nodes) == 2:
		return "multi"
	default:
		return "node"
	}
}

func (e *Engine) loop() {
	t := time.NewTicker(tick)
	snap := time.NewTicker(5 * time.Minute)
	reload := time.NewTicker(30 * time.Second)
	for {
		select {
		case <-reload.C:
			p := parseHedge(e.hedgeFile)
			e.hedge.mu.Lock()
			e.hedge.p = p
			e.hedge.mu.Unlock()
		case now := <-t.C:
			e.scan(now)
		case now := <-snap.C:
			e.snapshot(now)
		}
	}
}

func (e *Engine) scan(now time.Time) {
	e.mu.Lock()
	conns := make([]*Conn, 0, len(e.active))
	for c := range e.active {
		conns = append(conns, c)
	}
	e.mu.Unlock()
	for _, c := range conns {
		c.check(e, now)
	}
}
