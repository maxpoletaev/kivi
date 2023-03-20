package counter

import "sync/atomic"

type Counter int32

func (c *Counter) Add(delta int32) int32 {
	return atomic.AddInt32((*int32)(c), delta)
}

func (c *Counter) Get() int32 {
	return atomic.LoadInt32((*int32)(c))
}

func Greater(a, b Counter) bool {
	overflow := a.Get() < 0 && b.Get() >= 0
	return a.Get() > b.Get() || overflow
}

func Less(a, b Counter) bool {
	overflow := a.Get() >= 0 && b.Get() < 0
	return a.Get() < b.Get() || overflow
}

func Equal(a, b Counter) bool {
	return a.Get() == b.Get()
}
