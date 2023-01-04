package util

import "time"

func GetMonotonicUs() int64 {
	return time.Now().UnixNano() / 1e3
}

func GetMillionSeconds() int64 {
	return time.Now().UnixMilli()
}

type TimeVal struct {
	Sec  int64 // seconds
	Usec int64 // microseconds
}

func (t *TimeVal) MillionSeconds() int {
	return int(t.Sec*1000 + t.Usec/1000)
}
