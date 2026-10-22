package expect

import (
	"testing"
	"time"
)

// 热路径开销:每收发一块数据都会调一次
func BenchmarkDownBulk(b *testing.B) {
	e := testEngine(nil)
	c := e.open(time.Now(), "h", "s", "d", []string{"N1"}, "r", 100)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Down(16 << 10)
	}
}

func BenchmarkUpDownParallelConns(b *testing.B) {
	e := testEngine(nil)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		c := e.open(time.Now(), "h", "s", "d", []string{"N1"}, "r", 100)
		for pb.Next() {
			c.Up(40)
			c.Down(16 << 10)
		}
	})
}
