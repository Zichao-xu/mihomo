package expect

import (
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// 对冲:首包迟迟不来时,把已发出的上行字节重放到同组另一个节点,先回者胜。
//
// 只在 TLS 握手阶段做:此时上行只有 ClientHello,重放到另一条路只是另起一次握手,
// 服务器那边没有任何副作用;一旦有应用数据发出,重放就可能让请求执行两次,不做。
//
// 默认关闭。环境变量 MIHOMO_EXPECT_HEDGE=p(0~1),或往开关文件里写 p,打开随机实验:每条符合条件的连接
// 以概率 p 进入 hedge 组,其余进入 control 组,两组只差"允不允许对冲",
// 比较两组的首包分布就是对冲的真实效果(线上随机对照,不靠离线回放)。
const (
	hedgeBudget    = 0.05 // 对冲次数不超过符合条件连接数的 5%(Tail at Scale)
	hedgeBurst     = 20
	hedgeMinDelay  = 50 * time.Millisecond
	ArmHedge       = "hedge"
	ArmControl     = "control"
	maxReplayBytes = 16 << 10
)

type hedgePolicy struct {
	mu     sync.Mutex
	p      float64
	tokens float64
}

func (e *Engine) hedgeShare() float64 {
	e.hedge.mu.Lock()
	defer e.hedge.mu.Unlock()
	return e.hedge.p
}

// parseHedge 先看环境变量,没有再看开关文件(每 30 秒重读,改了不用重启)
func parseHedge(file string) float64 {
	s := os.Getenv("MIHOMO_EXPECT_HEDGE")
	if s == "" && file != "" {
		b, _ := os.ReadFile(file)
		s = strings.TrimSpace(string(b))
	}
	p, err := strconv.ParseFloat(s, 64)
	if err != nil || p <= 0 {
		return 0
	}
	if p > 1 {
		p = 1
	}
	return p
}

// Assign 给一条新连接分组。返回空串表示实验未开启或连接不符合条件。
// 符合条件=经过一个有多个成员的组。无论分到哪组都给预算加点数,所以预算按全部符合条件的连接算。
func Assign(chains []string) string {
	e := std
	if e == nil || len(chains) < 2 {
		return ""
	}
	p := e.hedgeShare()
	if p <= 0 {
		return ""
	}
	e.mu.Lock()
	f := e.members
	u := e.rnd.Float64()
	e.mu.Unlock()
	if f == nil || len(f(chains[1])) < 2 {
		return ""
	}
	e.hedge.mu.Lock()
	e.hedge.tokens += hedgeBudget
	if e.hedge.tokens > hedgeBurst {
		e.hedge.tokens = hedgeBurst
	}
	e.hedge.mu.Unlock()
	if u < p {
		return ArmHedge
	}
	return ArmControl
}

// HedgePlan 告诉调用方这条连接等多久没首包就对冲、对冲到哪个节点。ok=false 表示数据不够,不对冲。
func HedgePlan(node, group, host string) (delay time.Duration, alt string, ok bool) {
	e := std
	if e == nil {
		return 0, "", false
	}
	now := time.Now()
	c := &Conn{e: e, node: node, group: group, host: host}
	e.mu.Lock()
	if e.members != nil {
		c.members = e.members(group)
	}
	e.mu.Unlock()
	thr, ok := e.waitThreshold(c, now, true)
	if !ok {
		return 0, "", false
	}
	alt, _, _ = e.alternative(c, now)
	if alt == "" {
		return 0, "", false
	}
	if thr < hedgeMinDelay {
		thr = hedgeMinDelay
	}
	return thr, alt, true
}

// TakeHedge 在真正对冲前调用:预算不够,或此刻多条路同时出问题(本地故障,换路没用)时拒绝。
func TakeHedge(node string) (bool, string) {
	e := std
	if e == nil {
		return false, "off"
	}
	if e.scopeNow(node, time.Now()) == "local" {
		return false, "local"
	}
	e.hedge.mu.Lock()
	defer e.hedge.mu.Unlock()
	if e.hedge.tokens < 1 {
		return false, "budget"
	}
	e.hedge.tokens--
	return true, ""
}

// scopeNow 只读地看最近的偏差判定,不把这次计入
func (e *Engine) scopeNow(node string, now time.Time) string {
	e.mu.Lock()
	defer e.mu.Unlock()
	nodes := map[string]bool{node: true}
	for _, f := range e.recent {
		if now.Sub(f.at) <= attributeWindow {
			nodes[f.node] = true
		}
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

// SetArm 记录这条连接在实验里的分组
func (c *Conn) SetArm(arm string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.arm = arm
	c.mu.Unlock()
}

// HedgeEvent 记录对冲过程:fired / alt-won / primary-won / failover / aborted / alt-fail / skip:<原因>
//
// alt 胜出时,首包样本不再记给原节点(那不是它的表现),改记一个删失样本:
// "原节点到 waited 为止还没回",是它真实首包时长的下界。
func (c *Conn) HedgeEvent(ev, alt string, waited time.Duration) {
	if c == nil {
		return
	}
	now := time.Now()
	c.mu.Lock()
	c.hedge = ev
	c.hedgeAlt = alt
	if ev == "alt-won" || ev == "failover" {
		c.skipFirst = true
		n := c.e.node(c.node)
		n.mu.Lock()
		n.first.add(now, waited)
		n.mu.Unlock()
	}
	c.mu.Unlock()
	c.e.counters.hedge(ev)
}
