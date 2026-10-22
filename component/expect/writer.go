package expect

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"time"
)

// 按小时分文件,总量超过上限就从最旧的删。默认目录在内存盘上,不能放任增长
const maxTotalBytes = 150 << 20

// writer 异步写 JSONL,队列满了就丢,绝不阻塞转发路径
type writer struct {
	dir     string
	ch      chan []byte
	dropped atomic.Int64
}

func newWriter(dir string) *writer {
	w := &writer{dir: dir, ch: make(chan []byte, 8192)}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return w
	}
	go w.run()
	return w
}

func (w *writer) event(rec map[string]any) {
	b, err := json.Marshal(rec)
	if err != nil {
		return
	}
	select {
	case w.ch <- b:
	default:
		w.dropped.Add(1)
	}
}

func (w *writer) run() {
	var (
		f    *os.File
		bw   *bufio.Writer
		hour string
		n    int
	)
	flush := time.NewTicker(2 * time.Second)
	for {
		select {
		case b := <-w.ch:
			h := time.Now().In(cst).Format("20060102-15")
			if h != hour || f == nil {
				if bw != nil {
					bw.Flush()
					f.Close()
				}
				var err error
				f, err = os.OpenFile(filepath.Join(w.dir, "expect-"+h+".jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
				if err != nil {
					f, bw = nil, nil
					continue
				}
				bw = bufio.NewWriterSize(f, 64<<10)
				hour = h
			}
			bw.Write(b)
			bw.WriteByte('\n')
		case <-flush.C:
			if bw != nil {
				bw.Flush()
			}
			if n++; n%30 == 0 {
				w.prune()
			}
		}
	}
}

func (w *writer) prune() {
	entries, err := os.ReadDir(w.dir)
	if err != nil {
		return
	}
	var names []string
	sizes := map[string]int64{}
	var total int64
	for _, e := range entries {
		if !strings.HasPrefix(e.Name(), "expect-") {
			continue
		}
		if info, err := e.Info(); err == nil {
			names = append(names, e.Name())
			sizes[e.Name()] = info.Size()
			total += info.Size()
		}
	}
	sort.Strings(names)
	// 最新的那个正在写,不删
	for i := 0; i < len(names)-1 && total > maxTotalBytes; i++ {
		os.Remove(filepath.Join(w.dir, names[i]))
		total -= sizes[names[i]]
	}
}
