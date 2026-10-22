package tunnel

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	N "github.com/metacubex/mihomo/common/net"
	C "github.com/metacubex/mihomo/constant"
)

type fakeConn struct {
	N.ExtendedConn
	name string
}

func (f *fakeConn) Chains() C.Chain                 { return C.Chain{f.name} }
func (f *fakeConn) ProviderChains() C.Chain         { return nil }
func (f *fakeConn) AppendToChains(a C.ProxyAdapter) {}
func (f *fakeConn) RemoteDestination() string       { return f.name }

// fakeServer 是一条路的"服务器端":收到 want 字节后等 delay 再回 reply
type fakeServer struct {
	got  []byte
	mu   sync.Mutex
	conn net.Conn
}

func newPath(name string, want int, delay time.Duration, reply string) (C.Conn, *fakeServer) {
	cli, srv := net.Pipe()
	fs := &fakeServer{conn: srv}
	go func() {
		b := make([]byte, want)
		n, err := io.ReadFull(srv, b)
		fs.mu.Lock()
		fs.got = b[:n]
		fs.mu.Unlock()
		if err != nil {
			return
		}
		if delay < 0 { // 模拟节点直接断开
			srv.Close()
			return
		}
		time.Sleep(delay)
		srv.Write([]byte(reply))
	}()
	return &fakeConn{ExtendedConn: N.NewExtendedConn(cli), name: name}, fs
}

func (f *fakeServer) received() []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.got
}

var hello = append([]byte{0x16, 0x03, 0x01, 0x00, 0x05, 0x01}, 1, 2, 3, 4)

func allow(string) (bool, string) { return true, "" }

func readAll(t *testing.T, h *hedgeIO, n int) string {
	t.Helper()
	b := make([]byte, n)
	if _, err := io.ReadFull(h, b); err != nil {
		t.Fatalf("read: %v", err)
	}
	return string(b)
}

func events(h *hedgeIO) []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []string
	for _, e := range h.events {
		out = append(out, e.ev)
	}
	return out
}

func TestHedgeAltWins(t *testing.T) {
	prim, ps := newPath("P", len(hello), 2*time.Second, "slow")
	alt, as := newPath("A", len(hello), 10*time.Millisecond, "fast")
	h := newHedgeIO(prim, nil, "P", "A", 50*time.Millisecond, func(context.Context) (C.Conn, error) { return alt, nil }, allow)
	defer h.Close()
	go h.Write(hello)
	if got := readAll(t, h, 4); got != "fast" {
		t.Fatalf("got %q", got)
	}
	if string(as.received()) != string(hello) {
		t.Fatal("备选节点没收到重放的 ClientHello")
	}
	_ = ps
	if ev := events(h); len(ev) != 2 || ev[0] != "fired" || ev[1] != "alt-won" {
		t.Fatalf("events %v", ev)
	}
	if h.current() != alt {
		t.Fatal("胜者应是备选")
	}
}

func TestHedgePrimaryFastNoHedge(t *testing.T) {
	prim, _ := newPath("P", len(hello), 0, "ok")
	dialed := false
	h := newHedgeIO(prim, nil, "P", "A", 200*time.Millisecond, func(context.Context) (C.Conn, error) {
		dialed = true
		return nil, errors.New("x")
	}, allow)
	defer h.Close()
	go h.Write(hello)
	if got := readAll(t, h, 2); got != "ok" {
		t.Fatal(got)
	}
	time.Sleep(300 * time.Millisecond)
	if dialed || len(events(h)) != 0 {
		t.Fatalf("原节点按时回应,不该对冲: %v", events(h))
	}
}

func TestHedgeFailoverOnPrimaryReset(t *testing.T) {
	prim, _ := newPath("P", len(hello), -1, "")
	alt, _ := newPath("A", len(hello), 0, "alive")
	go prim.Write(hello) // 真实流程里 ClientHello 在拨号时就已经写给原节点
	h := newHedgeIO(prim, hello, "P", "A", time.Hour, func(context.Context) (C.Conn, error) { return alt, nil }, allow)
	defer h.Close()
	if got := readAll(t, h, 5); got != "alive" {
		t.Fatal(got)
	}
	if ev := events(h); len(ev) != 2 || ev[0] != "failover-start" || ev[1] != "failover" {
		t.Fatalf("events %v", ev)
	}
}

func TestHedgeNotReplayableWaitsPrimary(t *testing.T) {
	req := []byte("GET / HTTP/1.1\r\n\r\n")
	prim, _ := newPath("P", len(req), 300*time.Millisecond, "resp")
	h := newHedgeIO(prim, nil, "P", "A", 50*time.Millisecond, func(context.Context) (C.Conn, error) {
		t.Error("明文请求不能重放")
		return nil, errors.New("x")
	}, allow)
	defer h.Close()
	go h.Write(req)
	if got := readAll(t, h, 4); got != "resp" {
		t.Fatal(got)
	}
}

func TestHedgeAbortedByLaterWrite(t *testing.T) {
	prim, ps := newPath("P", len(hello)+3, 0, "prim")
	alt, _ := newPath("A", len(hello), 0, "")
	gate := make(chan struct{})
	h := newHedgeIO(prim, nil, "P", "A", 30*time.Millisecond, func(context.Context) (C.Conn, error) {
		<-gate // 备选还在拨号时客户端又发了数据
		return alt, nil
	}, allow)
	defer h.Close()
	go func() {
		h.Write(hello)
		time.Sleep(100 * time.Millisecond)
		h.Write([]byte{0x17, 0, 0})
		close(gate)
	}()
	if got := readAll(t, h, 4); got != "prim" {
		t.Fatal(got)
	}
	if len(ps.received()) != len(hello)+3 {
		t.Fatal("原节点应收到全部上行")
	}
	ev := events(h)
	if len(ev) < 2 || ev[0] != "fired" || ev[1] != "aborted" {
		t.Fatalf("events %v", ev)
	}
}

func TestHedgeBudgetSkip(t *testing.T) {
	prim, _ := newPath("P", len(hello), 200*time.Millisecond, "late")
	h := newHedgeIO(prim, nil, "P", "A", 30*time.Millisecond, func(context.Context) (C.Conn, error) {
		t.Error("预算不够不该拨备选")
		return nil, errors.New("x")
	}, func(string) (bool, string) { return false, "budget" })
	defer h.Close()
	go h.Write(hello)
	if got := readAll(t, h, 4); got != "late" {
		t.Fatal(got)
	}
	if ev := events(h); len(ev) != 1 || ev[0] != "skip:budget" {
		t.Fatalf("events %v", ev)
	}
}

func TestHedgeBothFail(t *testing.T) {
	prim, _ := newPath("P", len(hello), -1, "")
	go prim.Write(hello)
	h := newHedgeIO(prim, hello, "P", "A", time.Hour, func(context.Context) (C.Conn, error) {
		return nil, errors.New("alt down")
	}, allow)
	defer h.Close()
	if _, err := h.Read(make([]byte, 4)); err == nil {
		t.Fatal("两条路都断了应返回错误")
	}
}

func TestIsClientHello(t *testing.T) {
	if !isClientHello(hello) {
		t.Fatal("hello")
	}
	if !isClientHello(hello[:7]) {
		t.Fatal("半条记录也算")
	}
	if isClientHello(append(append([]byte{}, hello...), 0x17, 3, 3, 0, 1, 9)) {
		t.Fatal("带应用数据就不能重放")
	}
	if isClientHello([]byte("GET / HTTP/1.1")) {
		t.Fatal("明文")
	}
}
