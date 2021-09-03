package pkg

import "time"

func GetMonotonicUs() int64 {
	return time.Now().UnixNano() / 1e3
}

type TimeVal struct {
	Duration time.Duration
}
