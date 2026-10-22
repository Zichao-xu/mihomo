package expect

import (
	"math"
	"time"
)

// hist 是按时间衰减的对数直方图,记录某个信号(等待/间隔)的分布。
// 旧样本的权重按半衰期指数衰减,所以分布跟着线路状况走,不需要窗口参数。
// 分桶是 1/4 倍频程,1ms 到约 17 分钟,分位数误差在 ±9% 以内。
const (
	binsPerOctave = 4
	numBins       = 20*binsPerOctave + 1
)

type hist struct {
	w      [numBins]float64
	total  float64
	decay  time.Time
	halfLf time.Duration
}

func newHist(halfLife time.Duration) *hist {
	return &hist{halfLf: halfLife}
}

func binOf(d time.Duration) int {
	ms := float64(d) / float64(time.Millisecond)
	if ms < 1 {
		return 0
	}
	i := int(math.Log2(ms)*binsPerOctave) + 1
	if i >= numBins {
		return numBins - 1
	}
	return i
}

// upper 返回桶的上界,分位数取上界,宁可估大不估小
func upper(i int) time.Duration {
	if i == 0 {
		return time.Millisecond
	}
	return time.Duration(math.Exp2(float64(i)/binsPerOctave) * float64(time.Millisecond))
}

func (h *hist) age(now time.Time) {
	if h.decay.IsZero() {
		h.decay = now
		return
	}
	dt := now.Sub(h.decay)
	if dt < time.Second {
		return
	}
	f := math.Exp2(-float64(dt) / float64(h.halfLf))
	for i := range h.w {
		h.w[i] *= f
	}
	h.total *= f
	h.decay = now
}

func (h *hist) add(now time.Time, d time.Duration) {
	h.addN(now, d, 1)
}

func (h *hist) addN(now time.Time, d time.Duration, n float64) {
	h.age(now)
	h.w[binOf(d)] += n
	h.total += n
}

// weight 是衰减后的有效样本数
func (h *hist) weight(now time.Time) float64 {
	h.age(now)
	return h.total
}

func (h *hist) quantile(now time.Time, q float64) time.Duration {
	h.age(now)
	if h.total <= 0 {
		return 0
	}
	target := q * h.total
	acc := 0.0
	for i, w := range h.w {
		acc += w
		if acc >= target {
			return upper(i)
		}
	}
	return upper(numBins - 1)
}

// mid 是桶的代表值(上下界的几何平均)
func mid(i int) float64 {
	if i == 0 {
		return 0.5
	}
	return math.Sqrt(float64(upper(i-1))*float64(upper(i))) / float64(time.Millisecond)
}

// actAfter 回答"等到什么时候该换路":从 from 开始往后看,已经等了 t 还没结束时,
// 按这个分布再等下去的期望剩余时间 E[T-t | T>t] 一旦超过换路成本 cost,就返回这个 t。
// 超出分布里见过的一切(剩余样本不足半个)也返回——那已经是异常了。
// 这样阈值随分布形状走:双峰(大部分很快、少数卡死)会很早出手,平滑的长尾不会急着换。
func (h *hist) actAfter(now time.Time, from, cost time.Duration) time.Duration {
	h.age(now)
	costMs := float64(cost) / float64(time.Millisecond)
	for i := binOf(from); i < numBins; i++ {
		t := float64(upper(i)) / float64(time.Millisecond)
		mass, sum := 0.0, 0.0
		for j := i + 1; j < numBins; j++ {
			if h.w[j] > 0 {
				mass += h.w[j]
				sum += h.w[j] * (mid(j) - t)
			}
		}
		if mass < 0.5 || sum/mass > costMs {
			return upper(i)
		}
	}
	return upper(numBins - 1)
}

// tailAbove 是分布里超过 d 的比例
func (h *hist) tailAbove(now time.Time, d time.Duration) float64 {
	h.age(now)
	if h.total <= 0 {
		return 0
	}
	b := binOf(d)
	acc := 0.0
	for i := b + 1; i < numBins; i++ {
		acc += h.w[i]
	}
	return acc / h.total
}
