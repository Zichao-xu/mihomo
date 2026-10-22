package expect

import (
	"encoding/json"
	"sync"
	"testing"
	"time"
)

func testEngine(members map[string][]string) *Engine {
	e := newEngine(&writer{ch: make(chan []byte, 1<<16)})
	e.members = func(g string) []string { return members[g] }
	return e
}

func drain(e *Engine) []map[string]any {
	var out []map[string]any
	for {
		select {
		case b := <-e.out.ch:
			var m map[string]any
			json.Unmarshal(b, &m)
			out = append(out, m)
		default:
			return out
		}
	}
}

func ofType(evs []map[string]any, t string) []map[string]any {
	var out []map[string]any
	for _, ev := range evs {
		if ev["t"] == t {
			out = append(out, ev)
		}
	}
	return out
}

var t0 = time.Date(2026, 10, 23, 2, 0, 0, 0, time.UTC)

func at(d time.Duration) time.Time { return t0.Add(d) }

func TestHistQuantile(t *testing.T) {
	h := newHist(time.Hour)
	for i := 1; i <= 100; i++ {
		h.add(t0, time.Duration(i)*10*time.Millisecond)
	}
	for _, c := range []struct {
		q    float64
		want time.Duration
	}{{.5, 500 * time.Millisecond}, {.9, 900 * time.Millisecond}, {.99, 990 * time.Millisecond}} {
		got := h.quantile(t0, c.q)
		if got < c.want || float64(got) > float64(c.want)*1.2 {
			t.Errorf("q%.2f = %v, want ≈%v", c.q, got, c.want)
		}
	}
	// 一个半衰期后权重减半
	if w := h.weight(t0.Add(time.Hour)); w < 49 || w > 51 {
		t.Errorf("decayed weight %v", w)
	}
}

func TestWaitAndGapSampling(t *testing.T) {
	e := testEngine(nil)
	c := e.open(at(0), "a.com", "10.0.0.2", "1.2.3.4:443", []string{"N1"}, "Match", 300) // ClientHello 随拨号发出
	c.down_(at(100*time.Millisecond), 3000)                                              // ServerHello:首包 100ms
	c.up_(at(150*time.Millisecond), 500)                                                 // 请求
	c.down_(at(450*time.Millisecond), 80<<10)                                            // 300ms 后开始回应,大块
	c.up_(at(460*time.Millisecond), 40)                                                  // 窗口更新,不是新请求
	c.down_(at(470*time.Millisecond), 80<<10)
	c.down_(at(1470*time.Millisecond), 80<<10) // 中间停了 1s
	c.close_(at(2 * time.Second))

	n := e.nodes["N1"]
	if w := n.first.weight(at(0)); w != 1 {
		t.Fatalf("first samples %v", w)
	}
	if w := n.wait.weight(at(0)); w != 1 {
		t.Fatalf("wait samples %v (窗口更新被当成请求了?)", w)
	}
	if q := n.wait.quantile(at(0), .5); q < 300*time.Millisecond || q > 360*time.Millisecond {
		t.Fatalf("wait %v", q)
	}
	// 间隔只算回应已过 64KB 之后的:10ms(小间隔,关闭时攒批写入)和 1s 两个
	if w := n.gap.weight(at(0)); w < 1.99 || w > 2 { // 样本相隔 1s 多,有一点衰减
		t.Fatalf("gap samples %v", w)
	}
	conns := ofType(drain(e), "conn")
	if len(conns) != 1 || conns[0]["exch"].(float64) != 2 || conns[0]["max_gap_ms"].(float64) != 1000 {
		t.Fatalf("conn record %v", conns)
	}
}

// 先让节点学到"首包一般 100ms",再来一条等了 2s 的
func primeFirst(e *Engine, node string, d time.Duration, n int) {
	for i := 0; i < n; i++ {
		c := e.open(at(0), "x.com", "10.0.0.2", "1.1.1.1:443", []string{node, "G"}, "Match", 300)
		c.setDial(at(0), 50*time.Millisecond)
		c.down_(at(d), 1000)
		c.close_(at(d + time.Millisecond))
	}
}

func TestHedgeFlagAndOutcome(t *testing.T) {
	e := testEngine(map[string][]string{"G": {"N1", "N2"}})
	primeFirst(e, "N1", 100*time.Millisecond, 40)
	primeFirst(e, "N2", 200*time.Millisecond, 40)
	drain(e)

	c := e.open(at(0), "slow.com", "10.0.0.2", "1.1.1.1:443", []string{"N1", "G"}, "Match", 300)
	c.check(e, at(100*time.Millisecond))
	if c.open != nil {
		t.Fatal("没超出预期就判了偏差")
	}
	c.check(e, at(500*time.Millisecond))
	if c.open == nil || c.open.kind != "hedge" || c.open.alt != "N2" {
		t.Fatalf("应判为可对冲,备选 N2: %+v", c.open)
	}
	c.down_(at(3*time.Second), 1000)
	outs := ofType(drain(e), "outcome")
	if len(outs) != 1 {
		t.Fatalf("outcomes %v", outs)
	}
	o := outs[0]
	// 阈值≈100ms 的 p95 桶上界,备选首包 p50≈200ms + 建连 50ms,实际 3s → 能省两秒多
	if o["how"] != "answered" || o["save_ms"].(float64) < 2000 {
		t.Fatalf("outcome %v", o)
	}
	r := Snapshot()
	_ = r // std 为空时应返回 nil,不 panic
}

func TestGapFalseAlarmResolvedByNewRequest(t *testing.T) {
	e := testEngine(nil)
	// 学到这个节点大块传输的间隔都很短
	for i := 0; i < 60; i++ {
		e.nodeLocked("N1").gap.add(at(0), 30*time.Millisecond)
	}
	c := e.open(at(0), "img.com", "10.0.0.2", "1.1.1.1:443", []string{"N1"}, "Match", 300)
	c.down_(at(100*time.Millisecond), 100<<10) // 一张图收完
	c.check(e, at(1*time.Second))
	if c.open == nil || c.open.signal != "gap" {
		t.Fatalf("应判为传输中断: %+v", c.open)
	}
	c.up_(at(5*time.Second), 600) // 过了一会儿发新请求:说明上一张早就收完了
	outs := ofType(drain(e), "outcome")
	if len(outs) != 1 || outs[0]["how"] != "ended" {
		t.Fatalf("应记为误报 ended: %v", outs)
	}
}

func TestHostHabitRaisesThreshold(t *testing.T) {
	e := testEngine(nil)
	primeFirst(e, "N1", 100*time.Millisecond, 40)
	// 这个站点是长轮询:请求后常常 20s 才回
	for i := 0; i < 20; i++ {
		e.addHostWait("poll.com", at(0), 20*time.Second)
	}
	c := e.open(at(0), "poll.com", "10.0.0.2", "1.1.1.1:443", []string{"N1"}, "Match", 300)
	c.check(e, at(5*time.Second))
	if c.open != nil {
		t.Fatal("站点自己习惯慢,不该算线路偏差")
	}
}

func TestAttribution(t *testing.T) {
	e := testEngine(nil)
	if s := e.attribute("N1", at(0)); s != "node" {
		t.Fatal(s)
	}
	if s := e.attribute("DIRECT", at(time.Second)); s != "local" {
		t.Fatalf("直连也同时偏离 → 本地问题, got %s", s)
	}
	if s := e.attribute("N2", at(10*time.Second)); s != "node" {
		t.Fatalf("窗口外的旧判定不该计入, got %s", s)
	}
}

func TestShadowPickPrefersLowerTail(t *testing.T) {
	e := testEngine(map[string][]string{"G": {"N1", "N2", "N3"}})
	primeFirst(e, "N1", 100*time.Millisecond, 40)
	primeFirst(e, "N2", 800*time.Millisecond, 40)
	drain(e)
	for i := 0; i < 50; i++ {
		got, known := e.shadowPick([]string{"N1", "N2", "N3"}, at(0))
		if got != "N1" || known != 2 {
			t.Fatalf("pick %s known %d", got, known)
		}
	}
}

func TestConcurrentNoRace(t *testing.T) {
	e := testEngine(map[string][]string{"G": {"N1", "N2"}})
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				c := e.open(time.Now(), "h", "s", "d", []string{"N1", "G"}, "r", 10)
				go c.Up(100)
				c.Down(70 << 10)
				c.Down(70 << 10)
				c.Up(200)
				c.Down(10)
				c.Close()
			}
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				e.scan(time.Now().Add(time.Hour))
			}
		}()
	}
	wg.Wait()
	drain(e)
	if len(e.active) != 0 {
		t.Fatalf("active leak %d", len(e.active))
	}
}

func TestAssignAndBudget(t *testing.T) {
	e := testEngine(map[string][]string{"G": {"N1", "N2"}, "Solo": {"N1"}})
	e.hedge.p = 0.5
	old := std
	std = e
	defer func() { std = old }()

	if Assign([]string{"N1"}) != "" || Assign([]string{"N1", "Solo"}) != "" {
		t.Fatal("没有可选备选的连接不该进实验")
	}
	arms := map[string]int{}
	for i := 0; i < 1000; i++ {
		arms[Assign([]string{"N1", "G"})]++
	}
	if arms[ArmHedge] < 400 || arms[ArmControl] < 400 {
		t.Fatalf("分组不均: %v", arms)
	}
	// 1000 条符合条件的连接 → 预算 5% 但封顶 20 次
	n := 0
	for {
		ok, why := TakeHedge("N1")
		if !ok {
			if why != "budget" {
				t.Fatal(why)
			}
			break
		}
		n++
	}
	if n != hedgeBurst {
		t.Fatalf("预算 %d", n)
	}
	for i := 0; i < 20; i++ {
		Assign([]string{"N1", "G"})
	}
	if ok, _ := TakeHedge("N1"); !ok {
		t.Fatal("20 条新连接应攒出 1 次预算")
	}
}

func TestHedgeRefusedOnLocalFault(t *testing.T) {
	e := testEngine(map[string][]string{"G": {"N1", "N2"}})
	e.hedge.p = 1
	e.hedge.tokens = 10
	old := std
	std = e
	defer func() { std = old }()
	now := time.Now()
	e.attribute("N2", now)
	e.attribute("DIRECT", now)
	if ok, why := TakeHedge("N1"); ok || why != "local" {
		t.Fatalf("多条路同时出问题时换路没用: %v %s", ok, why)
	}
}

func TestHedgePlanNeedsData(t *testing.T) {
	e := testEngine(map[string][]string{"G": {"N1", "N2"}})
	old := std
	std = e
	defer func() { std = old }()
	if _, _, ok := HedgePlan("N1", "G", "a.com"); ok {
		t.Fatal("没数据不该对冲")
	}
	now := time.Now()
	for i := 0; i < 40; i++ {
		n1, n2 := e.node("N1"), e.node("N2")
		n1.first.add(now, 100*time.Millisecond)
		n2.first.add(now, 150*time.Millisecond)
	}
	d, alt, ok := HedgePlan("N1", "G", "a.com")
	if !ok || alt != "N2" || d < 100*time.Millisecond || d > 130*time.Millisecond {
		t.Fatalf("plan %v %s %v", d, alt, ok)
	}
}

// 节点退化成"大部分很快、一成卡死"时,p95 本身就落在卡死那一档,按分位数判就永远不出手了。
// 按"再等的期望剩余时间 > 换路成本"判,应该刚过快的那一档就出手。
func TestActAfterBimodalActsEarly(t *testing.T) {
	h := newHist(time.Hour)
	for i := 0; i < 90; i++ {
		h.add(t0, 10*time.Millisecond)
	}
	for i := 0; i < 10; i++ {
		h.add(t0, 3*time.Second)
	}
	if p95 := h.quantile(t0, .95); p95 < 2*time.Second {
		t.Fatalf("前提:p95 已经落在卡死档 %v", p95)
	}
	if got := h.actAfter(t0, h.quantile(t0, .5), 50*time.Millisecond); got > 20*time.Millisecond {
		t.Fatalf("应在快档之后立刻出手, got %v", got)
	}
}

// 平滑长尾、换路又不便宜时,不该一过中位数就换
func TestActAfterSmoothTailWaits(t *testing.T) {
	h := newHist(time.Hour)
	for i := 1; i <= 100; i++ {
		h.add(t0, time.Duration(200+i*8)*time.Millisecond) // 208ms~1000ms 均匀
	}
	got := h.actAfter(t0, h.quantile(t0, .5), 400*time.Millisecond)
	if got < 900*time.Millisecond {
		t.Fatalf("剩余期望始终小于换路成本,应等到分布尽头, got %v", got)
	}
}

func TestHedgePlanDegradedNode(t *testing.T) {
	e := testEngine(map[string][]string{"G": {"N1", "N2"}})
	old := std
	std = e
	defer func() { std = old }()
	now := time.Now()
	n1, n2 := e.node("N1"), e.node("N2")
	for i := 0; i < 90; i++ {
		n1.first.add(now, 5*time.Millisecond)
	}
	for i := 0; i < 10; i++ {
		n1.first.add(now, 3*time.Second)
	}
	for i := 0; i < 40; i++ {
		n2.first.add(now, 20*time.Millisecond)
		n2.dial.add(now, 10*time.Millisecond)
	}
	d, alt, ok := HedgePlan("N1", "G", "")
	if !ok || alt != "N2" || d != hedgeMinDelay {
		t.Fatalf("退化节点应在最短延迟后对冲: %v %s %v", d, alt, ok)
	}
}
