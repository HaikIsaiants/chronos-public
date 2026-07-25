package core

import "math"

type VirtualClock struct {
	now int64
}

func NewVirtualClock(start int64) (*VirtualClock, error) {
	if start < 0 {
		return nil, ErrTimeRegression
	}
	return &VirtualClock{now: start}, nil
}

func (c *VirtualClock) Now() int64 {
	return c.now
}

func (c *VirtualClock) Advance(delta int64) error {
	if delta < 0 {
		return ErrTimeRegression
	}
	if delta > math.MaxInt64-c.now {
		return ErrTimeOverflow
	}
	c.now += delta
	return nil
}

func (c *VirtualClock) Set(at int64) error {
	if at < c.now {
		return ErrTimeRegression
	}
	c.now = at
	return nil
}
