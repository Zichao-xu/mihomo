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

const keepFiles = 48 // 按小时分文件,留两天

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
				w.prune()
			}
			bw.Write(b)
			bw.WriteByte('\n')
		case <-flush.C:
			if bw != nil {
				bw.Flush()
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
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "expect-") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	for i := 0; i < len(names)-keepFiles; i++ {
		os.Remove(filepath.Join(w.dir, names[i]))
	}
}
