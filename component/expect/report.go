package expect

import (
	"sort"
	"sync"
	"time"
)

type outcomeStat struct {
	N        int   `json:"n"`
	Helped   int   `json:"helped"`    // 出手会更快
	Hurt     int   `json:"hurt"`      // 出手反而更慢
	SavedMs  int64 `json:"saved_ms"`  // 出手更快的那些,合计省下的时间
	WastedMs int64 `json:"wasted_ms"` // 出手更慢的那些,合计多花的时间
}

type counters struct {
	mu        sync.Mutex
	since     time.Time
	conns     int
	grouped   int
	shadowHas int
	agree     int
	dialFails int
	flags     map[string]int // kind/signal/scope
	hedges    map[string]int
	outcomes  map[string]*outcomeStat // signal/how
}

func (c *counters) init() {
	if c.flags == nil {
		c.since = time.Now()
		c.flags = map[string]int{}
		c.hedges = map[string]int{}
		c.outcomes = map[string]*outcomeStat{}
	}
}

func (c *counters) conn(x *Conn) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.init()
	c.conns++
	if x.group != "" && len(x.members) > 1 {
		c.grouped++
		if x.shadow != "" {
			c.shadowHas++
			if x.shadow == x.node {
				c.agree++
			}
		}
	}
}

func (c *counters) flag(f *flag) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.init()
	c.flags[f.kind+"/"+f.signal+"/"+f.scope]++
}

func (c *counters) outcome(f *flag, how string, save time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.init()
	k := f.signal + "/" + how
	o := c.outcomes[k]
	if o == nil {
		o = &outcomeStat{}
		c.outcomes[k] = o
	}
	o.N++
	switch {
	case save > 0:
		o.Helped++
		o.SavedMs += save.Milliseconds()
	case save < 0:
		o.Hurt++
		o.WastedMs += -save.Milliseconds()
	}
}

func (c *counters) hedge(ev string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.init()
	c.hedges[ev]++
}

func (c *counters) dialFail() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.init()
	c.dialFails++
}

type Quant struct {
	N   float64 `json:"n"`
	P50 int64   `json:"p50"`
	P90 int64   `json:"p90"`
	P95 int64   `json:"p95"`
	P99 int64   `json:"p99"`
}

type NodeReport struct {
	Name  string `json:"name"`
	First Quant  `json:"first"`
	Wait  Quant  `json:"wait"`
	Gap   Quant  `json:"gap"`
	Dial  Quant  `json:"dial"`
}

type Report struct {
	Since     string                  `json:"since"`
	Active    int                     `json:"active"`
	Conns     int                     `json:"conns"`
	Grouped   int                     `json:"grouped"`
	ShadowHas int                     `json:"shadow_has"`
	Agree     int                     `json:"agree"`
	DialFails int                     `json:"dial_fails"`
	Dropped   int64                   `json:"dropped_events"`
	Flags     map[string]int          `json:"flags"`
	HedgeP    float64                 `json:"hedge_p"`
	Hedges    map[string]int          `json:"hedges"`
	Outcomes  map[string]*outcomeStat `json:"outcomes"`
	Nodes     []NodeReport            `json:"nodes"`
}

func quant(h *hist, now time.Time) Quant {
	return Quant{
		N: float64(int(h.weight(now)*10)) / 10, P50: ms(h.quantile(now, .5)), P90: ms(h.quantile(now, .9)),
		P95: ms(h.quantile(now, .95)), P99: ms(h.quantile(now, .99)),
	}
}

// Snapshot 返回当前学到的各节点分布和影子判定统计;影子模式未启用时返回 nil
func Snapshot() *Report {
	e := std
	if e == nil {
		return nil
	}
	now := time.Now()
	r := &Report{}
	e.mu.Lock()
	r.Active = len(e.active)
	names := make([]string, 0, len(e.nodes))
	for n := range e.nodes {
		names = append(names, n)
	}
	e.mu.Unlock()
	sort.Strings(names)
	for _, name := range names {
		n := e.node(name)
		n.mu.Lock()
		r.Nodes = append(r.Nodes, NodeReport{
			Name: name, First: quant(n.first, now), Wait: quant(n.wait, now), Gap: quant(n.gap, now), Dial: quant(n.dial, now),
		})
		n.mu.Unlock()
	}
	c := &e.counters
	c.mu.Lock()
	c.init()
	r.Since = stamp(c.since)
	r.Conns, r.Grouped, r.ShadowHas, r.Agree, r.DialFails = c.conns, c.grouped, c.shadowHas, c.agree, c.dialFails
	r.Flags = make(map[string]int, len(c.flags))
	for k, v := range c.flags {
		r.Flags[k] = v
	}
	r.Hedges = make(map[string]int, len(c.hedges))
	for k, v := range c.hedges {
		r.Hedges[k] = v
	}
	r.Outcomes = make(map[string]*outcomeStat, len(c.outcomes))
	for k, v := range c.outcomes {
		cp := *v
		r.Outcomes[k] = &cp
	}
	c.mu.Unlock()
	r.Dropped = e.out.dropped.Load()
	r.HedgeP = e.hedgeShare()
	return r
}

func (e *Engine) snapshot(now time.Time) {
	if r := Snapshot(); r != nil {
		e.out.event(map[string]any{"t": "snap", "at": stamp(now), "report": r})
	}
}
