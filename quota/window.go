package quota

import "time"

// Window 是一个固定长度的滚动（tumbling）时间窗口。
type Window struct {
	Size time.Duration
}

var (
	WindowMinute = Window{Size: time.Minute}
	WindowHour   = Window{Size: time.Hour}
	WindowDay    = Window{Size: 24 * time.Hour}
)

// Key 返回 t 所属窗口的编号。
func (w Window) Key(t time.Time) int64 { return t.UnixNano() / int64(w.Size) }

// Bounds 返回窗口的起止时间。
func (w Window) Bounds(key int64) (start, end time.Time) {
	start = time.Unix(0, key*int64(w.Size)).UTC()
	return start, start.Add(w.Size)
}

// RemainingFraction 返回 t 所在窗口剩余部分的比例，范围 (0,1]。
func (w Window) RemainingFraction(t time.Time) float64 {
	start, end := w.Bounds(w.Key(t))
	if !t.After(start) {
		return 1
	}
	return float64(end.Sub(t)) / float64(end.Sub(start))
}
